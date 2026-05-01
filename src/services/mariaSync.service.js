import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";

/**
 * =========================
 * LOGGER
 * =========================
 */
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

/**
 * =========================
 * STATE
 * =========================
 */
export let isSyncRunning = false;

/**
 * =========================
 * MARIA POOL (named export for createPool)
 * =========================
 */
export const mariaPool = createPool({
  host:            process.env.MARIADB_HOST     || process.env.MARIA_DB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIADB_PORT     || process.env.MARIA_DB_PORT     || 3306),
  user:            process.env.MARIADB_USER     || process.env.MARIA_DB_USER     || "root",
  password:        process.env.MARIADB_PASSWORD || process.env.MARIA_DB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIADB_DATABASE || process.env.MARIA_DB_NAME     || "uradi",
  connectionLimit: 5,
  connectTimeout:  20000,
  acquireTimeout:  20000,
});

/**
 * =========================
 * CONFIG
 * =========================
 */
const EVENTS_BATCH  = Number(process.env.EVENTS_BATCH  || 500);
const DEVICE_BATCH  = Number(process.env.DEVICE_BATCH  || 300);

/**
 * =========================
 * SAFE NUMBER
 * =========================
 */
const N = (v) => {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
};

/**
 * =========================
 * LOCK
 * =========================
 */
async function acquireLock() {
  try {
    const res = await pgPool.query("SELECT pg_try_advisory_lock(778899) AS locked");
    return res.rows?.[0]?.locked === true;
  } catch (e) {
    log("error", "Lock error", { error: e.message });
    return false;
  }
}

async function releaseLock() {
  try {
    await pgPool.query("SELECT pg_advisory_unlock(778899)");
  } catch (e) {
    log("error", "Unlock error", { error: e.message });
  }
}

/**
 * =========================
 * DEVICE MAP CACHE
 * uid → { pgDeviceId, pgVehicleId }
 * =========================
 */
let deviceMapCache = new Map();

async function loadDeviceMap() {
  const { rows } = await pgPool.query(
    "SELECT d.id, d.device_uid, d.vehicle_id FROM devices d WHERE d.device_uid IS NOT NULL"
  );
  deviceMapCache = new Map(rows.map(r => [r.device_uid, { pgDeviceId: r.id, pgVehicleId: r.vehicle_id }]));
  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

/**
 * =========================
 * VEHICLE SYNC
 * Pull from MariaDB registration → upsert vehicles + devices in PG
 * =========================
 */
export async function syncVehicles() {
  let conn;
  try {
    conn = await mariaPool.getConnection();
    log("info", "MariaDB connected for vehicle sync");

    // MariaDB schema: registration(serial, reg_no, vmodel, pstatus)
    // device table: (id, uniqueid) where uniqueid = '0' + serial
    const rows = await conn.query(`
      SELECT
        r.serial,
        r.reg_no,
        r.vmodel,
        r.pstatus,
        d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL AND r.serial != ''
      LIMIT 5000
    `);

    let count = 0;
    for (const r of rows) {
      const serial = String(r.serial || "").trim();
      if (!serial) continue;

      const plateNumber = String(r.reg_no || serial).substring(0, 30);

      try {
        await pgPool.query(`
          INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
          VALUES ($1, $2, $3, $4, $5, NOW())
          ON CONFLICT (serial) DO UPDATE SET
            plate_number = EXCLUDED.plate_number,
            unit_name    = EXCLUDED.unit_name,
            model        = EXCLUDED.model,
            status       = EXCLUDED.status
        `, [
          serial,
          plateNumber,
          `Unit ${serial}`,
          String(r.vmodel || ""),
          String(r.pstatus || "inactive"),
        ]);

        if (r.device_uid) {
          await pgPool.query(`
            INSERT INTO devices (device_uid, serial, positionid)
            VALUES ($1, $2, 0)
            ON CONFLICT (device_uid) DO NOTHING
          `, [String(r.device_uid), serial]);

          // Link vehicle_id to device
          await pgPool.query(`
            UPDATE devices d
            SET vehicle_id = v.id
            FROM vehicles v
            WHERE v.serial = $1
              AND d.device_uid = $2
              AND d.vehicle_id IS NULL
          `, [serial, String(r.device_uid)]);
        }
        count++;
      } catch { /* skip individual row */ }
    }

    log("info", "Vehicle sync complete", { vehicles: count });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

/**
 * =========================
 * TELEMETRY + LATEST POSITIONS SYNC
 *
 * Strategy: servertime rolling window — NEVER gets stuck.
 * - Fetches all events from MariaDB in last 2h
 * - For each device: upsert latest_positions (1 row per device)
 * - Batch insert all rows into telemetry (historical)
 * =========================
 */
export async function syncTelemetry() {
  let conn;
  try {
    conn = await mariaPool.getConnection();
    log("info", "Maria connected (telemetry)");

    // Rolling 2-hour window — catches up even if sync was delayed
    const since = new Date(Date.now() - 2 * 3600_000);
    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    log("info", "Fetching latest positions per device", { since: sinceStr });

    /**
     * STEP A: Get ONE latest row per device (for latest_positions upsert)
     * eventData.deviceid links to device.id, device.uniqueid = our device_uid
     */
    const latestRows = await conn.query(`
      SELECT
        e.id            AS event_id,
        d.uniqueid      AS device_uid,
        e.latitude,
        e.longitude,
        e.speed         AS speed_kph,
        e.course        AS heading,
        e.devicetime    AS device_time,
        e.servertime    AS received_at,
        e.altitude
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE servertime > ?
          AND latitude  BETWEEN -90  AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      ORDER BY e.servertime DESC
      LIMIT ?
    `, [sinceStr, DEVICE_BATCH]);

    log("info", "Got latest rows from MariaDB", { count: latestRows.length });

    /**
     * STEP B: Get ALL rows in window (for telemetry table — historical)
     */
    const allRows = await conn.query(`
      SELECT
        e.id            AS event_id,
        d.uniqueid      AS device_uid,
        e.latitude,
        e.longitude,
        e.speed         AS speed_kph,
        e.course        AS heading,
        e.devicetime    AS device_time,
        e.servertime    AS received_at,
        e.altitude
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.servertime > ?
        AND e.latitude  BETWEEN -90  AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.servertime ASC
      LIMIT ?
    `, [sinceStr, EVENTS_BATCH]);

    log("info", "Got all telemetry rows from MariaDB", { count: allRows.length });

    /**
     * STEP C: Upsert latest_positions — ONE ROW PER DEVICE, always most recent
     */
    let posUpserted = 0;
    for (const row of latestRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;

      const { pgDeviceId, pgVehicleId } = cached;
      const eventId = N(row.event_id);
      const lat     = N(row.latitude);
      const lon     = N(row.longitude);

      if (lat === null || lon === null) continue;

      try {
        await pgPool.query(`
          INSERT INTO latest_positions (
            device_id, vehicle_id, latitude, longitude,
            speed_kph, heading, device_time, received_at,
            altitude, updated_at
          )
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
          pgDeviceId, pgVehicleId,
          lat, lon,
          N(row.speed_kph) ?? 0,
          N(row.heading)   ?? 0,
          row.device_time,
          row.received_at,
          N(row.altitude),
        ]);

        // Update positionid checkpoint
        if (eventId) {
          await pgPool.query(`
            UPDATE devices SET positionid = GREATEST(positionid, $1)
            WHERE id = $2
          `, [eventId, pgDeviceId]);
        }

        posUpserted++;
      } catch (e) {
        log("warn", "latest_positions upsert failed", { uid: row.device_uid, error: e.message });
      }
    }

    log("info", "latest_positions upserted", { count: posUpserted });

    /**
     * STEP D: Batch insert telemetry (historical rows, skip duplicates)
     */
    let telInserted = 0;
    for (const row of allRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;

      const { pgDeviceId, pgVehicleId } = cached;
      const eventId = N(row.event_id);
      const lat     = N(row.latitude);
      const lon     = N(row.longitude);

      if (lat === null || lon === null || !eventId) continue;

      try {
        await pgPool.query(`
          INSERT INTO telemetry (
            device_id, vehicle_id, event_id, latitude, longitude,
            speed_kph, heading, device_time, received_at, altitude
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (device_id, event_id) DO NOTHING
        `, [
          pgDeviceId, pgVehicleId, eventId,
          lat, lon,
          N(row.speed_kph) ?? 0,
          N(row.heading)   ?? 0,
          row.device_time,
          row.received_at,
          N(row.altitude),
        ]);
        telInserted++;
      } catch { /* skip */ }
    }

    log("info", "Telemetry sync done", { total: telInserted });

  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

/**
 * =========================
 * MAIN SYNC ENTRY POINT
 * =========================
 */
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) {
    log("warn", "MariaSync skipped (lock not acquired)");
    return;
  }

  isSyncRunning = true;
  const start = Date.now();
  log("info", "MariaSync started");

  try {
    await loadDeviceMap();
    await syncTelemetry();
    log("info", "MariaSync completed", { ms: Date.now() - start });
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}