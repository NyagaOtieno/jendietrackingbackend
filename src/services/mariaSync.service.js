// src/services/mariaSync.service.js
import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool }    from "../config/db.js";
import { redis }     from "../config/redis.js";
// ← self-import REMOVED: was causing "loadDeviceMap already declared" crash

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
   
  connectionLimit: 15,
  acquireTimeout: 60000,
  connectTimeout: 30000,
  idleTimeout: 30000,
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
// ─────────────────────────────────────────────
let deviceMapCache = new Map();

async function loadDeviceMap() {
  const { rows } = await pgPool.query(
    "SELECT id, device_uid, vehicle_id FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceMapCache = new Map(
  rows.map(r => [
    r.device_uid,
    {
      pgDeviceId: Number(r.id),
      pgVehicleId: r.vehicle_id ? Number(r.vehicle_id) : null
    }
  ])
);
  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────────────────────
// REDIS CACHE — latest position per device
// TTL: 5 minutes
// ─────────────────────────────────────────────
async function cacheLatestPositions(rows) {
  if (!rows.length) return;
  try {
    const pipeline = redis.pipeline();
    for (const row of rows) {
      const uid = String(row.device_uid);
      const key = `pos:${uid}`;
      pipeline.hset(key, {
        lat:      String(row.latitude  ?? 0),
        lon:      String(row.longitude ?? 0),
        speed:    String(row.speed_kph ?? 0),
        heading:  String(row.heading   ?? 0),
        time:     row.device_time ? new Date(row.device_time).toISOString() : "",
        received: row.received_at ? new Date(row.received_at).toISOString() : "",
      });
      pipeline.expire(key, 300);
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
// FIX: correct MariaDB column names
// registration table has: serial, reg_no, vmodel, pstatus
// device table joined via: uniqueid = CONCAT('0', serial)
// ─────────────────────────────────────────────
export async function syncVehicles() {
  const conn = await getMariaConn();
  try {
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

    let upserted = 0;

    for (const row of rows) {
      const serial = String(row.serial || "").trim();
      if (!serial) continue;

      // Trim plate to fit varchar(30)
      const plate = String(row.reg_no || serial).substring(0, 30);

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
          plate,
          `Unit ${serial}`,
          String(row.vmodel || ""),
          String(row.pstatus || "inactive"),
        ]);

        if (row.device_uid) {
          await pgPool.query(`
            INSERT INTO devices (device_uid, serial, positionid)
            VALUES ($1, $2, 0)
            ON CONFLICT (device_uid) DO NOTHING
          `, [String(row.device_uid), serial]);

          // Link vehicle_id → device
          await pgPool.query(`
            UPDATE devices d
            SET vehicle_id = v.id
            FROM vehicles v
            WHERE v.serial = $1
              AND d.device_uid = $2
              AND d.vehicle_id IS NULL
          `, [serial, String(row.device_uid)]);
        }
        upserted++;
      } catch (e) {
        log("warn", "Vehicle row failed", { serial, error: e.message });
      }
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
// FIX: insert columns match actual schema:
//   latest_positions: device_id, latitude, longitude,
//                     speed_kph, heading, device_time,
//                     received_at, updated_at
//   telemetry:        device_id, latitude, longitude,
//                     speed_kph, heading, device_time,
//                     received_at
//
// KEY: e.deviceid is numeric FK to device.id
//      JOIN device d ON d.id = e.deviceid → d.uniqueid = our device_uid string
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  const conn = await getMariaConn();

  try {
    const since = lastSyncTime
      ? new Date(Math.min(Date.now() - 2 * 3600_000, new Date(lastSyncTime).getTime()))
      : new Date(Date.now() - 2 * 3600_000);

    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    log("info", "Fetching events since", { since: sinceStr });

    // ─── QUERY A: one latest row per device ───────────────────────────────
    const latestRows = await conn.query(`
      SELECT
        e.id            AS event_id,
        d.uniqueid      AS device_uid,
        e.latitude,
        e.longitude,
        e.speed         AS speed_kph,
        e.course        AS heading,
        e.devicetime    AS device_time,
        e.servertime    AS received_at
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

    // ─── QUERY B: all rows in window (historical telemetry) ───────────────
    const allRows = await conn.query(`
      SELECT
        e.id            AS event_id,
        d.uniqueid      AS device_uid,
        e.latitude,
        e.longitude,
        e.speed         AS speed_kph,
        e.course        AS heading,
        e.devicetime    AS device_time,
        e.servertime    AS received_at
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

    // ─── BULK UPSERT latest_positions (was row-by-row — 290 round-trips → 2) ──
    {
      const BATCH = 200;
      let posUpserted = 0;
      for (let i = 0; i < latestRows.length; i += BATCH) {
        const chunk  = latestRows.slice(i, i + BATCH);
        const vals   = [];
        const params = [];
        let   p      = 1;

        for (const row of chunk) {
          const cached = deviceMapCache.get(String(row.device_uid));
          if (!cached) continue;
          const lat = N(row.latitude);
          const lon = N(row.longitude);
          if (lat === null || lon === null) continue;

          vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
          params.push(
            cached.pgDeviceId, lat, lon,
            N(row.speed_kph) ?? 0,
            N(row.heading)   ?? 0,
            row.device_time,
            row.received_at
          );
          posUpserted++;
        }

        if (!vals.length) continue;
        await pgPool.query(`
          INSERT INTO latest_positions
            (device_id, latitude, longitude, speed_kph, heading,
             device_time, received_at, updated_at)
          VALUES ${vals.join(",")}
          ON CONFLICT (device_id) DO UPDATE SET
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            speed_kph   = EXCLUDED.speed_kph,
            heading     = EXCLUDED.heading,
            device_time = EXCLUDED.device_time,
            received_at = EXCLUDED.received_at,
            updated_at  = NOW()
          WHERE EXCLUDED.device_time >= latest_positions.device_time
             OR latest_positions.device_time IS NULL
        `, params);
      }
      log("info", "latest_positions upserted", { count: posUpserted });
    }

    // ─── REDIS pipeline ───────────────────────────────────────────────────
    await cacheLatestPositions(latestRows);

    // ─── BULK INSERT telemetry (was row-by-row — 480 round-trips → 3) ──────
    {
      const BATCH = 200;
      let telInserted = 0;
      for (let i = 0; i < allRows.length; i += BATCH) {
        const chunk  = allRows.slice(i, i + BATCH);
        const vals   = [];
        const params = [];
        let   p      = 1;

        for (const row of chunk) {
          const cached = deviceMapCache.get(String(row.device_uid));
          if (!cached) continue;
          const lat = N(row.latitude);
          const lon = N(row.longitude);
          if (lat === null || lon === null) continue;

          vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
          params.push(
            cached.pgDeviceId, lat, lon,
            N(row.speed_kph) ?? 0,
            N(row.heading)   ?? 0,
            row.device_time,
            row.received_at
          );
          telInserted++;
        }

        if (!vals.length) continue;
        await pgPool.query(`
          INSERT INTO telemetry
            (device_id, latitude, longitude, speed_kph, heading,
             device_time, received_at)
          VALUES ${vals.join(",")}
          ON CONFLICT DO NOTHING
        `, params);
      }
      log("info", "Telemetry sync done", { telemetry: telInserted, latest: 0 });
    }

    lastSyncTime = new Date().toISOString();

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

// ─────────────────────────────────────────────────────────────────────────────
// QUICK SYNC — only latest position per active device (last 2 min), bulk upsert
// Completes in ~1 second. Called every 10 s by the worker for near-real-time.
// ─────────────────────────────────────────────────────────────────────────────
let isQuickRunning = false;

export async function runQuickSync() {
  if (isQuickRunning)       return;
  if (!deviceMapCache.size) return; // device map not loaded yet

  isQuickRunning = true;
  let conn;
  try {
    conn = await getMariaConn();
    const sinceStr = new Date(Date.now() - 2 * 60_000)
      .toISOString().slice(0, 19).replace("T", " ");

    const rows = await conn.query(`
      SELECT
        d.uniqueid   AS device_uid,
        e.latitude,  e.longitude,
        e.speed      AS speed_kph,
        e.course     AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
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
      LIMIT 5000
    `, [sinceStr]);

    conn.release(); conn = null;
    if (!rows.length) return;

    let upserted = 0;
    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);
      const vals  = [], params = [];
      let p = 1;
      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;
        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(cached.pgDeviceId, lat, lon, N(r.speed_kph)??0, N(r.heading)??0, r.device_time, r.received_at);
        upserted++;
      }
      if (!vals.length) continue;
      await pgPool.query(`
        INSERT INTO latest_positions
          (device_id,latitude,longitude,speed_kph,heading,device_time,received_at,updated_at)
        VALUES ${vals.join(",")}
        ON CONFLICT (device_id) DO UPDATE SET
          latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
          speed_kph=EXCLUDED.speed_kph, heading=EXCLUDED.heading,
          device_time=EXCLUDED.device_time, received_at=EXCLUDED.received_at,
          updated_at=NOW()
        WHERE EXCLUDED.device_time >= latest_positions.device_time
           OR latest_positions.device_time IS NULL
      `, params);
    }
    log("info", "quickSync complete", { active: rows.length, upserted });
  } catch (e) {
    log("error", "quickSync error", { error: e.message });
  } finally {
    try { conn?.release(); } catch {}
    isQuickRunning = false;
  }
}