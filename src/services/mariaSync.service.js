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
let lastEventId  = 0;   // ← PRIMARY KEY checkpoint; avoids servertime full-table-scan

// ─────────────────────────────────────────────
// MARIA POOL
// ─────────────────────────────────────────────
export const mariaPool = createPool({
  host:            process.env.MARIADB_HOST     || process.env.MARIA_DB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIADB_PORT     || process.env.MARIA_DB_PORT     || 3306),
  user:            process.env.MARIADB_USER     || process.env.MARIA_DB_USER     || "root",
  password:        process.env.MARIADB_PASSWORD || process.env.MARIA_DB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIADB_DATABASE || process.env.MARIA_DB_NAME     || "uradi",
  connectionLimit: 10,
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
// ─────────────────────────────────────────────
let deviceMapCache = new Map();

export async function loadDeviceMap() {
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
          String(row.vmodel  || ""),
          String(row.pstatus || "inactive"),
        ]);

        if (row.device_uid) {
          await pgPool.query(`
            INSERT INTO devices (device_uid, serial, positionid)
            VALUES ($1, $2, 0)
            ON CONFLICT (device_uid) DO NOTHING
          `, [String(row.device_uid), serial]);

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
// STEP 2: TELEMETRY SYNC (full, uses id checkpoint)
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  const conn = await getMariaConn();

  try {
    // ── Init checkpoint from DB on first run (PK lookup — instant) ───────
    if (lastEventId === 0) {
      const r = await conn.query("SELECT COALESCE(MAX(id),0)+0 AS maxid FROM eventData");
      const maxId = Number(r[0].maxid);
      lastEventId = Math.max(0, maxId - 5000);
      log("info", "Checkpoint init", { lastEventId });
    }

    log("info", "Fetching events since id", { lastEventId });

    // ── QUERY A: latest row per device (PRIMARY KEY — no table scan) ─────
    const latestRows = await conn.query(
      `SELECT
         e.id+0        AS event_id,
         d.uniqueid    AS device_uid,
         e.latitude+0  AS latitude,
         e.longitude+0 AS longitude,
         e.speed+0     AS speed_kph,
         e.course+0    AS heading,
         e.devicetime  AS device_time,
         e.servertime  AS received_at
       FROM eventData e
       INNER JOIN device d ON d.id = e.deviceid
       INNER JOIN (
         SELECT deviceid, MAX(id) AS max_id
         FROM eventData
         WHERE id > ?
           AND latitude  BETWEEN -90  AND 90
           AND longitude BETWEEN -180 AND 180
           AND NOT (latitude = 0 AND longitude = 0)
         GROUP BY deviceid
       ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
       ORDER BY e.id DESC
       LIMIT ?`,
      [lastEventId, DEVICE_BATCH]
    );
    log("info", "Latest rows from MariaDB", { count: latestRows.length });

    // ── QUERY B: all rows since checkpoint (PRIMARY KEY — no table scan) ─
    const allRows = await conn.query(
      `SELECT
         e.id+0        AS event_id,
         d.uniqueid    AS device_uid,
         e.latitude+0  AS latitude,
         e.longitude+0 AS longitude,
         e.speed+0     AS speed_kph,
         e.course+0    AS heading,
         e.devicetime  AS device_time,
         e.servertime  AS received_at
       FROM eventData e
       INNER JOIN device d ON d.id = e.deviceid
       WHERE e.id > ?
         AND e.latitude  BETWEEN -90  AND 90
         AND e.longitude BETWEEN -180 AND 180
         AND NOT (e.latitude = 0 AND e.longitude = 0)
       ORDER BY e.id ASC
       LIMIT ?`,
      [lastEventId, EVENTS_BATCH]
    );
    log("info", "All telemetry rows from MariaDB", { count: allRows.length });

    let posUpserted = 0;
    let telInserted = 0;

    // ── UPSERT latest_positions ──────────────────────────────────────────
    for (const row of latestRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;
      const { pgDeviceId } = cached;
      const lat = N(row.latitude);
      const lon = N(row.longitude);
      if (lat === null || lon === null) continue;

      try {
        await pgPool.query(
          `INSERT INTO latest_positions (
             device_id, latitude, longitude,
             speed_kph, heading, device_time, received_at, updated_at
           )
           VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
           ON CONFLICT (device_id) DO UPDATE SET
             latitude    = EXCLUDED.latitude,
             longitude   = EXCLUDED.longitude,
             speed_kph   = EXCLUDED.speed_kph,
             heading     = EXCLUDED.heading,
             device_time = EXCLUDED.device_time,
             received_at = EXCLUDED.received_at,
             updated_at  = NOW()`,
          [pgDeviceId, lat, lon,
           N(row.speed_kph) ?? 0, N(row.heading) ?? 0,
           row.device_time, row.received_at]
        );
        const eventId = N(row.event_id);
        if (eventId) {
          await pgPool.query(
            `UPDATE devices SET positionid = GREATEST(positionid,$1) WHERE id=$2`,
            [eventId, pgDeviceId]
          );
        }
        posUpserted++;
      } catch (e) {
        log("warn", "latest_positions upsert failed", { uid: row.device_uid, error: e.message });
      }
    }
    log("info", "latest_positions upserted", { count: posUpserted });

    await cacheLatestPositions(latestRows);

    // ── INSERT telemetry ─────────────────────────────────────────────────
    for (const row of allRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;
      const { pgDeviceId } = cached;
      const lat = N(row.latitude);
      const lon = N(row.longitude);
      if (lat === null || lon === null) continue;
      try {
        await pgPool.query(
          `INSERT INTO telemetry (
             device_id, latitude, longitude,
             speed_kph, heading, device_time, received_at
           )
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT DO NOTHING`,
          [pgDeviceId, lat, lon,
           N(row.speed_kph) ?? 0, N(row.heading) ?? 0,
           row.device_time, row.received_at]
        );
        telInserted++;
      } catch (e) {
        log("warn", "telemetry insert failed", { uid: row.device_uid, error: e.message });
      }
    }

    // ── Advance checkpoint ───────────────────────────────────────────────
    if (allRows.length > 0) {
      const maxSeen = Math.max(...allRows.map(r => Number(r.event_id)));
      if (maxSeen > lastEventId) lastEventId = maxSeen;
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
// QUICK SYNC — latest position per device
// Uses id > lastEventId (PRIMARY KEY — instant)
// Called every 5s for near-real-time map updates
// ─────────────────────────────────────────────
let isQuickRunning = false;

export async function runQuickSync() {
  if (isQuickRunning)       return;
  if (!deviceMapCache.size) return;
  isQuickRunning = true;
  let conn;
  try {
    conn = await getMariaConn();

    // ── sinceId: use lastEventId; if 0, get MAX(id)-50000 (PK, instant) ─
    let sinceId = lastEventId > 0 ? Math.max(0, lastEventId - 500) : 0;
    if (sinceId === 0) {
      const r = await conn.query("SELECT COALESCE(MAX(id),0)+0 AS m FROM eventData");
      sinceId = Math.max(0, Number(r[0].m) - 50000);
      lastEventId = sinceId;
    }

    const rows = await conn.query(
      `SELECT
         d.uniqueid    AS device_uid,
         e.latitude+0  AS latitude,
         e.longitude+0 AS longitude,
         e.speed+0     AS speed_kph,
         e.course+0    AS heading,
         e.devicetime  AS device_time,
         e.servertime  AS received_at
       FROM eventData e
       INNER JOIN device d ON d.id = e.deviceid
       INNER JOIN (
         SELECT deviceid, MAX(id) AS max_id
         FROM eventData
         WHERE id > ?
           AND latitude  BETWEEN -90  AND 90
           AND longitude BETWEEN -180 AND 180
           AND NOT (latitude = 0 AND longitude = 0)
         GROUP BY deviceid
       ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
       LIMIT 5000`,
      [sinceId]
    );

    conn.release(); conn = null;

    if (!rows.length) {
      log("info", "quickSync — no new rows", { sinceId });
      return;
    }

    let upserted = 0;
    for (let i = 0; i < rows.length; i += 200) {
      const chunk  = rows.slice(i, i + 200);
      const vals   = [], params = [];
      let p = 1;
      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;
        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(cached.pgDeviceId, lat, lon,
          N(r.speed_kph) ?? 0, N(r.heading) ?? 0,
          r.device_time, r.received_at);
        upserted++;
      }
      if (!vals.length) continue;
      await pgPool.query(
        `INSERT INTO latest_positions
           (device_id,latitude,longitude,speed_kph,heading,device_time,received_at,updated_at)
         VALUES ${vals.join(",")}
         ON CONFLICT (device_id) DO UPDATE SET
           latitude    = EXCLUDED.latitude,
           longitude   = EXCLUDED.longitude,
           speed_kph   = EXCLUDED.speed_kph,
           heading     = EXCLUDED.heading,
           device_time = EXCLUDED.device_time,
           received_at = EXCLUDED.received_at,
           updated_at  = NOW()`,
        params
      );
    }
    log("info", "quickSync complete", { active: rows.length, upserted, sinceId });

  } catch (e) {
    log("error", "quickSync error", { error: e.message });
  } finally {
    try { conn?.release(); } catch {}
    isQuickRunning = false;
  }
}

// ─────────────────────────────────────────────
// MAIN SYNC (locked, single-instance)
// ─────────────────────────────────────────────
let isRunning = false;

export async function runMariaSync() {
  if (isRunning) return;
  isRunning = true;
  const start = Date.now();
  log("info", "MariaSync started");
  try {
    await loadDeviceMap();
    await syncTelemetry();
    log("info", "MariaSync completed", { ms: Date.now() - start });
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isRunning = false;
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