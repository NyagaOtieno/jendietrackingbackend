import "dotenv/config";

import { pgPool }      from "../config/db.js";
import { initRedis }   from "../config/redis.js";
import {
  initMariaSync,
  runMariaSync,
  runLiveSync,
  syncVehicles,
} from "../services/mariaSync.service.js";

const MAX_RETRY       = 5;
const BUFFER_INTERVAL = 5_000;
const SYNC_INTERVAL   = 60_000;
const VEHICLE_INTERVAL = 1_800_000;

let isRunning = false;
let batchTimer, liveTimer, syncTimer, vehicleTimer;

// ── Buffer processor — writes ingestion buffer directly to DB ────
async function processBatch() {
  if (isRunning) return;
  isRunning = true;

  try {
    const { rows } = await pgPool.query(`
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT 1000
    `);

    if (!rows.length) return;

    const success = [];

    for (const row of rows) {
      try {
        const payload = JSON.parse(row.payload);
        const items   = Array.isArray(payload.batch) ? payload.batch : [payload];

        for (const item of items) {
          const { deviceId, latitude, longitude,
                  speed = 0, heading = 0, signalTime = null } = item;
          if (!deviceId || latitude == null || longitude == null) continue;

          const dt = signalTime ? new Date(signalTime) : null;

          await pgPool.query(`
            INSERT INTO telemetry
              (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
            VALUES ($1,$2,$3,$4,$5,$6,NOW())
            ON CONFLICT DO NOTHING
          `, [deviceId, +latitude, +longitude, +speed, +heading, dt]);

          await pgPool.query(`
            INSERT INTO latest_positions
              (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
            ON CONFLICT (device_id) DO UPDATE SET
              latitude    = EXCLUDED.latitude,
              longitude   = EXCLUDED.longitude,
              speed_kph   = EXCLUDED.speed_kph,
              heading     = EXCLUDED.heading,
              device_time = EXCLUDED.device_time,
              received_at = NOW(),
              updated_at  = NOW()
            WHERE EXCLUDED.device_time > latest_positions.device_time
               OR latest_positions.device_time IS NULL
          `, [deviceId, +latitude, +longitude, +speed, +heading, dt]);
        }

        success.push(row.id);
      } catch (e) {
        const retry  = (row.retry_count || 0) + 1;
        const status = retry >= MAX_RETRY ? "FAILED" : "PENDING";
        console.error(`[Buffer] Row ${row.id} failed (attempt ${retry}):`, e.message);
        await pgPool.query(
          `UPDATE telemetry_ingestion_buffer SET retry_count=$2, status=$3 WHERE id=$1`,
          [row.id, retry, status]
        ).catch(() => {});
      }
    }

    if (success.length) {
      await pgPool.query(
        `UPDATE telemetry_ingestion_buffer
         SET status='PROCESSED', processed_at=NOW()
         WHERE id=ANY($1)`,
        [success]
      );
      console.log(`[Buffer] ✅ Processed ${success.length} rows`);
    }
  } catch (e) {
    console.error("[Buffer] processBatch error:", e.message);
  } finally {
    isRunning = false;
  }
}

// ── Start ────────────────────────────────────────────────────────
function startWorker() {
  if (batchTimer) return;
  console.log("[Worker] ⚡ liveSync every 1000ms");

  // 1s live sync — most important, keeps latest_positions fresh
  liveTimer    = setInterval(() =>
    runLiveSync().catch(e => console.error("[Worker] liveSync error:", e.message)),
    1000
  );

  // 5s buffer drain
  batchTimer   = setInterval(processBatch, BUFFER_INTERVAL);

  // 60s full history sync
  syncTimer    = setInterval(() =>
    runMariaSync().catch(e => console.error("[Worker] mariaSync error:", e.message)),
    SYNC_INTERVAL
  );

  // 30m vehicle sync
  vehicleTimer = setInterval(() =>
    syncVehicles().catch(e => console.error("[Worker] vehicleSync error:", e.message)),
    VEHICLE_INTERVAL
  );

  // Kick off immediately
  runLiveSync().catch(() => {});
  processBatch();
}

// ── Stop ─────────────────────────────────────────────────────────
export function stopTelemetryBufferWorker() {
  [liveTimer, batchTimer, syncTimer, vehicleTimer].forEach(clearInterval);
  liveTimer = batchTimer = syncTimer = vehicleTimer = null;
  console.log("🛑 Worker stopped");
}

// ── Bootstrap ────────────────────────────────────────────────────
async function main() {
  // Redis — optional
  try {
    await initRedis();
    console.log("✅ Redis connected");
  } catch (e) {
    console.warn("⚠️  Redis unavailable:", e.message);
  }

  // PostgreSQL — wait up to 30s
  for (let i = 1; i <= 10; i++) {
    try {
      await pgPool.query("SELECT 1");
      console.log("✅ PostgreSQL connected");
      break;
    } catch (e) {
      console.warn(`⏳ PG not ready (${i}/10): ${e.message}`);
      if (i === 10) {
        console.warn("⚠️  PG still unavailable — starting anyway, will retry");
      } else {
        await new Promise(r => setTimeout(r, 3000));
      }
    }
  }

  // MariaSync init — loads checkpoint + device cache
  try {
    await initMariaSync();
  } catch (e) {
    console.warn("⚠️  MariaSync init failed (will retry via intervals):", e.message);
  }

  startWorker();

  process.on("SIGINT",  () => { stopTelemetryBufferWorker(); process.exit(0); });
  process.on("SIGTERM", () => { stopTelemetryBufferWorker(); process.exit(0); });
  process.on("unhandledRejection", r =>
    console.error("[Worker] Unhandled rejection:", r?.message || r)
  );
}

main();