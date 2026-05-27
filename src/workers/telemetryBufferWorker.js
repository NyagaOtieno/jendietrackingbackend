import "dotenv/config";

import { pgPool } from "../config/db.js";
import { runMariaSync, syncVehicles } from "../services/mariaSync.service.js";
import { setLatestPositionsBatch } from "../services/redisLatestPosition.js";
import { initRedis } from "../config/redis.js";

let isRunning = false;

const INTERVAL         = 5000;    // process buffer every 5s
const SYNC_INTERVAL    = 60000;   // MariaDB sync every 60s
const VEHICLE_INTERVAL = 1800000; // vehicle sync every 30min

let batchTimer;
let syncTimer;
let vehicleTimer;

const MAX_RETRY = 5;

// ─────────────────────────────────────────────────────────────────────────────
// BATCH PROCESSOR
// Reads PENDING rows from telemetry_ingestion_buffer, writes them to
// telemetry + latest_positions in Postgres, then updates Redis.
// ─────────────────────────────────────────────────────────────────────────────
async function processBatch() {
  if (isRunning) return;
  isRunning = true;

  let rows = [];

  try {
    const result = await pgPool.query(`
      SELECT id, payload, retry_count
      FROM telemetry_ingestion_buffer
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT 1000
    `);

    rows = result.rows || [];
    console.log(`[Worker] fetched ${rows.length} rows`);

  } catch (err) {
    console.error("[Worker] DB read error:", err.message);

    // IMPORTANT: do NOT crash worker
    isRunning = false;
    return;
  }

  if (rows.length === 0) {
    isRunning = false;
    return;
  }

  const success = [];
  const redisBatch = [];

  for (const row of rows) {
    try {
      const payload = JSON.parse(row.payload);

      const items = Array.isArray(payload.batch)
        ? payload.batch
        : [payload];

      for (const item of items) {
        const {
          deviceId,
          latitude,
          longitude,
          speed = 0,
          heading = 0,
          signalTime = null,
        } = item;

        if (!deviceId || latitude == null || longitude == null) continue;

        const deviceTime = signalTime ? new Date(signalTime) : new Date();

        // TELEMETRY
        await pgPool.query(
          `INSERT INTO telemetry
           (device_id, latitude, longitude, speed_kph, heading, device_time)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT DO NOTHING`,
          [deviceId, latitude, longitude, speed, heading, deviceTime]
        );

        // LATEST POSITIONS (CRITICAL)
        await pgPool.query(
          `INSERT INTO latest_positions
           (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
           ON CONFLICT (device_id) DO UPDATE SET
             latitude = EXCLUDED.latitude,
             longitude = EXCLUDED.longitude,
             speed_kph = EXCLUDED.speed_kph,
             heading = EXCLUDED.heading,
             device_time = EXCLUDED.device_time,
             updated_at = NOW()`,
          [deviceId, latitude, longitude, speed, heading, deviceTime]
        );

        // REDIS
        redisBatch.push({
          deviceId,
          lat: latitude,
          lon: longitude,
          speed,
          heading,
          dt: deviceTime,
        });
      }

      success.push(row.id);

    } catch (err) {
      console.error("[Worker] row error:", err.message);
    }
  }

  // MARK PROCESSED
  if (success.length) {
    await pgPool.query(
      `UPDATE telemetry_ingestion_buffer
       SET status='PROCESSED', processed_at=NOW()
       WHERE id = ANY($1)`,
      [success]
    );
  }

  // REDIS PUSH
  if (redisBatch.length) {
    await setLatestPositionsBatch(redisBatch);
  }

  isRunning = false;
}
// ─────────────────────────────────────────────────────────────────────────────
// START / STOP
// ─────────────────────────────────────────────────────────────────────────────
export function startTelemetryBufferWorker() {
  console.log("🚀 Telemetry Buffer Worker started");
  if (batchTimer) return;

  batchTimer   = setInterval(processBatch,          INTERVAL);
  syncTimer    = setInterval(() => runMariaSync(),   SYNC_INTERVAL);
  vehicleTimer = setInterval(() => syncVehicles(),   VEHICLE_INTERVAL);

  // Run immediately on start
  processBatch();
  runMariaSync();
}

export function stopTelemetryBufferWorker() {
  clearInterval(batchTimer);
  clearInterval(syncTimer);
  clearInterval(vehicleTimer);
  batchTimer = syncTimer = vehicleTimer = null;
  console.log("🛑 Worker stopped");
}

// ─────────────────────────────────────────────────────────────────────────────
// BOOTSTRAP — runs when executed directly via PM2 / node
// ─────────────────────────────────────────────────────────────────────────────
async function main() {
  try {
    await initRedis();
    console.log("✅ Redis connected");
  } catch (e) {
    console.warn("⚠️  Redis unavailable, continuing without it:", e.message);
  }

  startTelemetryBufferWorker();

  process.on("SIGINT",  () => { stopTelemetryBufferWorker(); process.exit(0); });
  process.on("SIGTERM", () => { stopTelemetryBufferWorker(); process.exit(0); });
}

main();