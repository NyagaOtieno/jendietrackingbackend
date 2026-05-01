// src/services/mariaSync.service.js
import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import cron from "node-cron";

// ─────────────────────────────────────────────
// MARIADB CONNECTION
// ─────────────────────────────────────────────
const mariaPool = createPool({
  host: process.env.MARIADB_HOST || "18.218.110.222",
  port: Number(process.env.MARIADB_PORT || 3306),
  user: process.env.MARIADB_USER || "root",
  password: process.env.MARIADB_PASSWORD || "nairobiyetu",
  database: process.env.MARIADB_DATABASE || "uradi",
  connectionLimit: 5,
  connectTimeout: 15000,
  acquireTimeout: 15000,
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
// SYNC STATE
// ─────────────────────────────────────────────
let lastSyncTime = null;

// ─────────────────────────────────────────────
// STEP 1: SYNC VEHICLES + DEVICES (UNCHANGED)
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
        const vRes = await pgPool.query(`
          INSERT INTO vehicles (id, plate_number, unit_name, serial, status, account_id)
          VALUES ($1,$2,$3,$4,'00',$5)
          ON CONFLICT (id) DO UPDATE SET
            plate_number = EXCLUDED.plate_number,
            unit_name    = EXCLUDED.unit_name,
            serial       = EXCLUDED.serial,
            account_id   = EXCLUDED.account_id
          RETURNING id
        `, [
          N(row.maria_id),
          row.plate_number,
          row.unit_name,
          row.device_uid,
          N(row.account_id)
        ]);

        if (vRes.rows.length) {
          await pgPool.query(`
            INSERT INTO devices (device_uid, vehicle_id, positionid)
            VALUES ($1,$2,$3)
            ON CONFLICT (device_uid) DO UPDATE SET
              vehicle_id = EXCLUDED.vehicle_id,
              positionid = CASE 
                WHEN EXCLUDED.positionid > devices.positionid 
                THEN EXCLUDED.positionid 
                ELSE devices.positionid 
              END
          `, [
            row.device_uid,
            N(row.maria_id),
            N(row.positionid) ?? 0
          ]);

          upserted++;
        }
      } catch {}
    }

    console.log(`[MariaSync] Vehicle sync: ${upserted}/${rows.length}`);
  } finally {
    conn.release();
  }
}

// ─────────────────────────────────────────────
// STEP 2: TELEMETRY SYNC (EVENTDATA FIXED)
// ─────────────────────────────────────────────
async function syncTelemetry() {
  const conn = await getMariaConn();

  try {
    const since = lastSyncTime
      ? new Date(Math.min(Date.now() - 2 * 3600_000, new Date(lastSyncTime).getTime()))
      : new Date(Date.now() - 2 * 3600_000);

    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    console.log(`[MariaSync] Fetching events since ${sinceStr}`);

    // ─────────────────────────────────────────────
    // FIXED: eventData schema alignment
    // ─────────────────────────────────────────────
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
        e.alarmcode   AS event_type
      FROM eventData e
      INNER JOIN (
        SELECT deviceid, MAX(id) as max_id
        FROM eventData
        WHERE servertime > ?
          AND latitude != 0 
          AND longitude != 0
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
        GROUP BY deviceid
      ) latest
      ON e.deviceid = latest.deviceid
      AND e.id = latest.max_id
      ORDER BY e.servertime DESC
      LIMIT 10000
    `, [sinceStr]);

    console.log(`[MariaSync] Latest rows: ${rows.length}`);

    const telemetryRows = await conn.query(`
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
        e.alarmcode   AS event_type
      FROM eventData e
      WHERE e.servertime > ?
        AND e.latitude != 0 
        AND e.longitude != 0
        AND e.latitude BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
      ORDER BY e.servertime ASC
      LIMIT 50000
    `, [sinceStr]);

    let telInserted = 0;
    let posUpserted = 0;

    // ─────────────────────────────────────────────
    // TELEMETRY INSERT
    // ─────────────────────────────────────────────
    for (const row of telemetryRows) {
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
          device_id,
          vehicle_id,
          N(row.event_id),
          N(row.latitude),
          N(row.longitude),
          N(row.speed_kph),
          N(row.heading),
          row.device_time,
          row.received_at,
          N(row.altitude),
          String(row.event_type ?? "")
        ]);

        telInserted++;
      } catch {}
    }

    // ─────────────────────────────────────────────
    // LATEST POSITIONS
    // ─────────────────────────────────────────────
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
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
          ON CONFLICT (device_id) DO UPDATE SET
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            speed_kph   = EXCLUDED.speed_kph,
            heading     = EXCLUDED.heading,
            device_time = EXCLUDED.device_time,
            received_at = EXCLUDED.received_at,
            altitude    = EXCLUDED.altitude,
            updated_at  = NOW()
        `, [
          device_id,
          vehicle_id,
          N(row.latitude),
          N(row.longitude),
          N(row.speed_kph),
          N(row.heading),
          row.device_time,
          row.received_at,
          N(row.altitude)
        ]);

        posUpserted++;
      } catch {}
    }

    lastSyncTime = new Date().toISOString();

    console.log(`[MariaSync] ✅ telemetry: ${telInserted}, latest: ${posUpserted}`);

  } catch (e) {
    console.error(`[MariaSync] ❌ Telemetry sync failed:`, e.message);
  } finally {
    conn.release();
  }
}

// ─────────────────────────────────────────────
// MAIN SYNC
// ─────────────────────────────────────────────
let isRunning = false;

export async function runSync() {
  if (isRunning) return;

  isRunning = true;
  const start = Date.now();

  console.log(`[MariaSync] 🔄 Sync started`);

  try {
    await syncTelemetry();
  } finally {
    isRunning = false;
    console.log(`[MariaSync] ✅ Done in ${(Date.now() - start) / 1000}s`);
  }
}

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────
export async function initMariaSync() {
  console.log("[MariaSync] Initializing...");

  await syncVehicles();
  await runSync();

  cron.schedule("* * * * *", runSync);
  cron.schedule("*/30 * * * *", syncVehicles);

  console.log("[MariaSync] Cron running");
}

export const runMariaSync = runSync;