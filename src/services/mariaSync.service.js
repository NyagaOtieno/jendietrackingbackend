// src/services/mariaSync.service.js
import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import cron from "node-cron";

// ─────────────────────────────────────────────
// MARIADB CONNECTION
// ─────────────────────────────────────────────
const mariaPool = createPool({
  host:            process.env.MARIADB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIADB_PORT     || 3306),
  user:            process.env.MARIADB_USER     || "root",
  password:        process.env.MARIADB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIADB_DATABASE || "uradi",
  connectionLimit: 5,
  connectTimeout:  15000,
  acquireTimeout:  15000,
});

function N(val) {
  if (val === null || val === undefined) return null;
  const n = typeof val === "bigint" ? Number(val) : Number(val);
  return Number.isFinite(n) ? n : null;
}

async function getMariaConn(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (e) {
      if (i === retries - 1) throw e;
      await new Promise(r => setTimeout(r, 3000 * (i + 1)));
    }
  }
}

// ─────────────────────────────────────────────
// SYNC STATE — track last successful sync time
// ─────────────────────────────────────────────
let lastSyncTime = null; // ISO string, updated after each successful run

// ─────────────────────────────────────────────
// STEP 1: SYNC VEHICLES + DEVICES
// ─────────────────────────────────────────────
async function syncVehicles() {
  const conn = await getMariaConn();
  try {
    const rows = await conn.query(`
      SELECT 
        r.id            AS maria_id,
        r.numberplate   AS plate_number,
        r.unitname      AS unit_name,
        r.unitid        AS device_uid,
        r.positionid    AS positionid,
        r.accountid     AS account_id
      FROM registration r
      WHERE r.unitid IS NOT NULL AND r.unitid != ''
      LIMIT 100000
    `);

    let upserted = 0;
    for (const row of rows) {
      try {
        // Upsert vehicle
        const vRes = await pgPool.query(`
          INSERT INTO vehicles (id, plate_number, unit_name, serial, status, account_id)
          VALUES ($1, $2, $3, $4, '00', $5)
          ON CONFLICT (id) DO UPDATE SET
            plate_number = EXCLUDED.plate_number,
            unit_name    = EXCLUDED.unit_name,
            serial       = EXCLUDED.serial,
            account_id   = EXCLUDED.account_id
          RETURNING id
        `, [N(row.maria_id), row.plate_number, row.unit_name, row.device_uid, N(row.account_id)]);

        if (vRes.rows.length) {
          // Upsert device
          await pgPool.query(`
            INSERT INTO devices (device_uid, vehicle_id, positionid)
            VALUES ($1, $2, $3)
            ON CONFLICT (device_uid) DO UPDATE SET
              vehicle_id  = EXCLUDED.vehicle_id,
              positionid  = CASE 
                              WHEN EXCLUDED.positionid > devices.positionid 
                              THEN EXCLUDED.positionid 
                              ELSE devices.positionid 
                            END
          `, [row.device_uid, N(row.maria_id), N(row.positionid) ?? 0]);
          upserted++;
        }
      } catch (e) {
        // skip individual row errors
      }
    }
    console.log(`[MariaSync] Vehicle sync: ${upserted}/${rows.length} upserted`);
  } finally {
    conn.release();
  }
}

// ─────────────────────────────────────────────
// STEP 2: SYNC TELEMETRY + LATEST POSITIONS
// Key change: use servertime window instead of positionid
// ─────────────────────────────────────────────
async function syncTelemetry() {
  const conn = await getMariaConn();
  try {
    // Look back 2 hours OR since last sync (whichever is earlier)
    // This ensures we never miss data even if sync was delayed
    const lookback = lastSyncTime
      ? `GREATEST(NOW() - INTERVAL 2 HOUR, '${lastSyncTime}')`
      : `NOW() - INTERVAL 2 HOUR`;

    const since = lastSyncTime
      ? new Date(Math.min(Date.now() - 2 * 3600_000, new Date(lastSyncTime).getTime()))
      : new Date(Date.now() - 2 * 3600_000);

    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    console.log(`[MariaSync] Fetching events since ${sinceStr}`);

    // Get ALL latest events per device in one query (most efficient)
    const rows = await conn.query(`
      SELECT 
        e.id          AS event_id,
       e.deviceid    AS device_uid,
       e.latitude    AS latitude,
        e.longitude   AS longitude,
        e.speed       AS speed_kph,
        e.course      AS heading,
        e.servertime  AS received_at,
        e.devicetime  AS device_time,
        e.altitude    AS altitude,
        e.event       AS event_type
      FROM eventData e
      INNER JOIN (
        SELECT deviceid, MAX(id) as max_id
      FROM eventData
        WHERE servertime > ?
          AND latitude != 0 AND longitude != 0
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
        GROUP BY deviceid
      ) latest ON e.unitid = latest.unitid AND e.id = latest.max_id
      ORDER BY e.servertime DESC
      LIMIT 10000
    `, [sinceStr]);

    console.log(`[MariaSync] Got ${rows.length} latest-position rows from MariaDB`);

    if (rows.length === 0) {
      console.log(`[MariaSync] No new events since ${sinceStr}`);
      lastSyncTime = new Date().toISOString();
      return;
    }

    // Also get all telemetry rows (not just latest) for the telemetry table
    const telemetryRows = await conn.query(`
     SELECT 
  e.id          AS event_id,
  d.uniqueid    AS device_uid,
  e.latitude    AS latitude,
  e.longitude   AS longitude,
  e.speed       AS speed_kph,
  e.course      AS heading,
  e.servertime  AS received_at,
  e.devicetime  AS device_time,
  e.altitude    AS altitude
FROM eventData e
JOIN device d ON e.deviceid = d.id
      WHERE e.servertime > ?
        AND e.lat != 0 AND e.lng != 0
        AND e.lat BETWEEN -90 AND 90
        AND e.lng BETWEEN -180 AND 180
      ORDER BY e.servertime ASC
      LIMIT 50000
    `, [sinceStr]);

    console.log(`[MariaSync] Got ${telemetryRows.length} telemetry rows`);

    // ── UPSERT TELEMETRY (historical, multiple per device) ──
    let telInserted = 0;
    const CHUNK = 500;
    for (let i = 0; i < telemetryRows.length; i += CHUNK) {
      const chunk = telemetryRows.slice(i, i + CHUNK);
      for (const row of chunk) {
        try {
          const devRes = await pgPool.query(
            `SELECT id, vehicle_id FROM devices WHERE device_uid = $1 LIMIT 1`,
            [String(row.device_uid)]
          );
          if (!devRes.rows.length) continue;

          const { id: device_id, vehicle_id } = devRes.rows[0];
          await pgPool.query(`
            INSERT INTO telemetry
              (device_id, vehicle_id, event_id, latitude, longitude, speed_kph, heading, device_time, received_at, altitude, event_type)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (device_id, event_id) DO NOTHING
          `, [
            device_id, vehicle_id,
            N(row.event_id),
            N(row.latitude), N(row.longitude),
            N(row.speed_kph), N(row.heading),
            row.device_time, row.received_at,
            N(row.altitude), String(row.event_type ?? ""),
          ]);
          telInserted++;
        } catch { /* skip */ }
      }
    }

    // ── UPSERT LATEST_POSITIONS (ONE ROW PER DEVICE) ──
    // This is the table the API reads — must always reflect the newest known position
    let posUpserted = 0;
    for (const row of rows) {
      try {
        const devRes = await pgPool.query(
          `SELECT id, vehicle_id FROM devices WHERE device_uid = $1 LIMIT 1`,
          [String(row.device_uid)]
        );
        if (!devRes.rows.length) continue;

        const { id: device_id, vehicle_id } = devRes.rows[0];

        await pgPool.query(`
          INSERT INTO latest_positions
            (device_id, vehicle_id, latitude, longitude, speed_kph, heading, device_time, received_at, altitude, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
          ON CONFLICT (device_id) DO UPDATE SET
            vehicle_id  = EXCLUDED.vehicle_id,
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            speed_kph   = EXCLUDED.speed_kph,
            heading     = EXCLUDED.heading,
            device_time = EXCLUDED.device_time,
            received_at = EXCLUDED.received_at,
            altitude    = EXCLUDED.altitude,
            updated_at  = NOW()
          WHERE EXCLUDED.device_time >= latest_positions.device_time
             OR latest_positions.device_time IS NULL
        `, [
          device_id, vehicle_id,
          N(row.latitude), N(row.longitude),
          N(row.speed_kph), N(row.heading),
          row.device_time, row.received_at,
          N(row.altitude),
        ]);
        posUpserted++;

        // Also update positionid checkpoint in devices
        if (N(row.event_id)) {
          await pgPool.query(`
            UPDATE devices SET positionid = $1
            WHERE id = $2 AND $1 > positionid
          `, [N(row.event_id), device_id]);
        }
      } catch { /* skip */ }
    }

    lastSyncTime = new Date().toISOString();
    console.log(`[MariaSync] ✅ latest_positions upserted: ${posUpserted}, telemetry inserted: ${telInserted}`);

  } catch (e) {
    console.error(`[MariaSync] ❌ Telemetry sync failed:`, e.message);
  } finally {
    conn.release();
  }
}

// ─────────────────────────────────────────────
// MAIN SYNC FUNCTION
// ─────────────────────────────────────────────
let isRunning = false;

export async function runSync() {
  if (isRunning) {
    console.log("[MariaSync] Already running, skipping.");
    return;
  }
  isRunning = true;
  const start = Date.now();
  console.log(`[MariaSync] 🔄 Sync started at ${new Date().toISOString()}`);
  try {
    await syncTelemetry(); // positions first (faster, most critical)
  } catch (e) {
    console.error("[MariaSync] Sync error:", e.message);
  } finally {
    isRunning = false;
    console.log(`[MariaSync] ✅ Sync done in ${((Date.now() - start)/1000).toFixed(1)}s`);
  }
}

// ─────────────────────────────────────────────
// STARTUP + CRON — every 1 minute
// ─────────────────────────────────────────────
export async function initMariaSync() {
  console.log("[MariaSync] Initializing...");

  // Sync vehicles once on startup
  try { await syncVehicles(); } catch (e) {
    console.error("[MariaSync] Vehicle sync failed:", e.message);
  }

  // Immediate first telemetry sync
  await runSync();

  // Then every 1 minute
  cron.schedule("* * * * *", async () => {
    await runSync();
  });

  // Sync vehicles every 30 minutes
  cron.schedule("*/30 * * * *", async () => {
    try { await syncVehicles(); } catch (e) {
      console.error("[MariaSync] Vehicle sync failed:", e.message);
    }
  });

  console.log("[MariaSync] ✅ Cron scheduled — every 1 minute");
}
// Alias for compatibility (DO NOT REMOVE)
export const runMariaSync = runSync;