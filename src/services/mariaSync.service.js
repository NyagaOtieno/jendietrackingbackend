// src/services/mariaSync.service.js
import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
export let isSyncRunning = false;
let lastSyncTime = null;

// ─────────────────────────────────────────────
// MARIA POOL
// ─────────────────────────────────────────────
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

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 500);
const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 300);

// ─────────────────────────────────────────────
// SAFE NUMBER
// ─────────────────────────────────────────────
function N(v) {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ─────────────────────────────────────────────
// MARIA CONNECTION WITH RETRY
// ─────────────────────────────────────────────
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
// ADVISORY LOCK (prevent double-run)
// ─────────────────────────────────────────────
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

// ─────────────────────────────────────────────
// DEVICE MAP CACHE
// device_uid (string) → { pgDeviceId, pgVehicleId }
// Loaded once per sync cycle — avoids N+1 DB queries
// ─────────────────────────────────────────────
let deviceMapCache = new Map();

async function loadDeviceMap() {
  const { rows } = await pgPool.query(
    "SELECT id, device_uid, vehicle_id FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceMapCache = new Map(
    rows.map(r => [r.device_uid, { pgDeviceId: r.id, pgVehicleId: r.vehicle_id }])
  );
  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────────────────────
// REDIS CACHE — latest position per device
// Key: pos:{device_uid} (string uid for fast frontend lookup)
// TTL: 5 minutes — auto-expires stale devices
// ─────────────────────────────────────────────
async function cacheLatestPositions(rows) {
  if (!rows.length) return;

  try {
    const pipeline = redis.pipeline();

    for (const row of rows) {
      const uid = String(row.device_uid);
      const key = `pos:${uid}`;

      pipeline.hset(key, {
        lat:       String(row.latitude  ?? 0),
        lon:       String(row.longitude ?? 0),
        speed:     String(row.speed_kph ?? 0),
        heading:   String(row.heading   ?? 0),
        time:      row.device_time  ? new Date(row.device_time).toISOString()  : "",
        received:  row.received_at  ? new Date(row.received_at).toISOString()  : "",
      });

      pipeline.expire(key, 300); // 5 min TTL
      pipeline.sadd("active_devices", uid);
    }

    await pipeline.exec();
    log("info", "Redis cache updated", { count: rows.length });
  } catch (e) {
    log("warn", "Redis cache skipped", { error: e.message });
  }
}

// ─────────────────────────────────────────────
// STEP 1: VEHICLE SYNC
// MariaDB registration → PostgreSQL vehicles + devices
// ─────────────────────────────────────────────
export async function syncVehicles() {
  const conn = await getMariaConn();
  try {
    // Use both MariaDB schemas (registration + device)
    // registration.unitid = the string uid (e.g. "010043685410")
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
        // Trim plate_number to fit varchar(30)
        const plate = String(row.plate_number || row.device_uid || "").substring(0, 30);

        const vRes = await pgPool.query(`
          INSERT INTO vehicles (id, plate_number, unit_name, serial, status, account_id)
          VALUES ($1, $2, $3, $4, '00', $5)
          ON CONFLICT (id) DO UPDATE SET
            plate_number = EXCLUDED.plate_number,
            unit_name    = EXCLUDED.unit_name,
            serial       = EXCLUDED.serial,
            account_id   = EXCLUDED.account_id
          RETURNING id
        `, [
          N(row.maria_id),
          plate,
          row.unit_name,
          row.device_uid,
          N(row.account_id),
        ]);

        if (vRes.rows.length) {
          await pgPool.query(`
            INSERT INTO devices (device_uid, vehicle_id, positionid)
            VALUES ($1, $2, $3)
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
            N(row.positionid) ?? 0,
          ]);

          upserted++;
        }
      } catch { /* skip individual row */ }
    }

    log("info", "Vehicle sync complete", { vehicles: upserted, total: rows.length });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// STEP 2: TELEMETRY SYNC
//
// KEY FIX: e.deviceid is a numeric FK to device.id (MariaDB internal)
// We JOIN device d ON d.id = e.deviceid to get d.uniqueid
// d.uniqueid = our devices.device_uid (the string "010043685410")
//
// Strategy: rolling 2h servertime window — never gets stuck
// - latest_positions: 1 row per device (most recent)
// - telemetry: all rows in window (historical)
// - Redis: pipeline cache of latest per device (real-time layer)
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  const conn = await getMariaConn();

  try {
    // Rolling window: 2 hours back, or since last sync (whichever is earlier)
    const since = lastSyncTime
      ? new Date(Math.min(Date.now() - 2 * 3600_000, new Date(lastSyncTime).getTime()))
      : new Date(Date.now() - 2 * 3600_000);

    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    log("info", "Fetching events since", { since: sinceStr });

    // ─────────────────────────────────────────
    // QUERY A: ONE latest row per device (latest_positions + Redis)
    // JOIN device to get uniqueid (string uid) — NOT the numeric deviceid
    // ─────────────────────────────────────────
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
        e.altitude,
        e.alarmcode     AS event_type
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

    log("info", "Latest rows from MariaDB", { count: latestRows.length });

    // ─────────────────────────────────────────
    // QUERY B: ALL rows in window (telemetry historical)
    // ─────────────────────────────────────────
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
        e.altitude,
        e.alarmcode     AS event_type
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.servertime > ?
        AND e.latitude  BETWEEN -90  AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.servertime ASC
      LIMIT ?
    `, [sinceStr, EVENTS_BATCH]);

    log("info", "All telemetry rows from MariaDB", { count: allRows.length });

    let posUpserted = 0;
    let telInserted = 0;

    // ─────────────────────────────────────────
    // UPSERT latest_positions (1 row per device, always most recent)
    // ─────────────────────────────────────────
    for (const row of latestRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;

      const { pgDeviceId, pgVehicleId } = cached;
      const lat = N(row.latitude);
      const lon = N(row.longitude);
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

        // Advance positionid checkpoint
        const eventId = N(row.event_id);
        if (eventId) {
          await pgPool.query(`
            UPDATE devices SET positionid = GREATEST(positionid, $1) WHERE id = $2
          `, [eventId, pgDeviceId]);
        }

        posUpserted++;
      } catch (e) {
        log("warn", "latest_positions upsert failed", { uid: row.device_uid, error: e.message });
      }
    }

    log("info", "latest_positions upserted", { count: posUpserted });

    // ─────────────────────────────────────────
    // REDIS pipeline — cache latest per device for real-time reads
    // Frontend /api/positions/latest can read from Redis instead of PG
    // ─────────────────────────────────────────
    await cacheLatestPositions(latestRows);

    // ─────────────────────────────────────────
    // INSERT telemetry (historical, all rows, skip duplicates)
    // ─────────────────────────────────────────
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
            speed_kph, heading, device_time, received_at, altitude, event_type
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          ON CONFLICT (device_id, event_id) DO NOTHING
        `, [
          pgDeviceId, pgVehicleId, eventId,
          lat, lon,
          N(row.speed_kph) ?? 0,
          N(row.heading)   ?? 0,
          row.device_time,
          row.received_at,
          N(row.altitude),
          String(row.event_type ?? ""),
        ]);
        telInserted++;
      } catch { /* skip duplicate */ }
    }

    lastSyncTime = new Date().toISOString();

    log("info", "Telemetry sync done", { telemetry: telInserted, latest: posUpserted });

  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// MAIN SYNC (locked, single-instance)
// ─────────────────────────────────────────────
let isRunning = false;

export async function runSync() {
  if (isRunning) return;

  isRunning = true;
  const start = Date.now();
  log("info", "MariaSync started");

  try {
    await syncTelemetry();
    log("info", "MariaSync completed", { ms: Date.now() - start });
  } finally {
    isRunning = false;
    log("info", `MariaSync done in ${((Date.now() - start) / 1000).toFixed(1)}s`);
  }
}

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

// ─────────────────────────────────────────────
// INIT (called by worker on startup)
// ─────────────────────────────────────────────
export async function initMariaSync() {
  log("info", "MariaSync initializing...");
  await syncVehicles();
  await loadDeviceMap();
  await syncTelemetry();
  log("info", "MariaSync init complete");
}