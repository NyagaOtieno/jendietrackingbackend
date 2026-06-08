// src/services/mariaSync.service.js
import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool }     from "../config/db.js";
import { redis }      from "../config/redis.js";

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
export let isSyncRunning = false;
let lastEventId = 0; // persisted to PG so restarts don't lose progress

// ─────────────────────────────────────────────
// MARIA POOL
// ─────────────────────────────────────────────
export const mariaPool = createPool({
  host:            process.env.MARIADB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIADB_PORT     || 3306),
  user:            process.env.MARIADB_USER     || "root",
  password:        process.env.MARIADB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIADB_DATABASE || "uradi",
  connectionLimit: 5,
  connectTimeout:  10000,
  acquireTimeout:  10000,
});

// ─────────────────────────────────────────────
// PERSISTENT LIVE CONNECTION
// Reused across every quickSync tick — eliminates 200-500ms
// connection overhead per second.
// ─────────────────────────────────────────────
let _liveConn = null;

async function getLiveConn() {
  if (_liveConn) {
    try {
      await _liveConn.query("SELECT 1"); // health-check, instant
      return _liveConn;
    } catch {
      try { _liveConn.release(); } catch {}
      _liveConn = null;
    }
  }
  _liveConn = await mariaPool.getConnection();
  return _liveConn;
}

// ─────────────────────────────────────────────
// SAFE NUMBER
// ─────────────────────────────────────────────
function N(v) {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ─────────────────────────────────────────────
// MARIA CONNECTION (for non-live operations)
// ─────────────────────────────────────────────
async function getMariaConn(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try { return await mariaPool.getConnection(); }
    catch (e) {
      if (i === retries - 1) throw e;
      await new Promise(r => setTimeout(r, 2000 * (i + 1)));
    }
  }
}

// ─────────────────────────────────────────────
// DEVICE MAP CACHE
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
// CHECKPOINT — persisted in PG sync_checkpoints table
// Survives worker restarts without losing progress.
// ─────────────────────────────────────────────
async function ensureCheckpointTable() {
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS sync_checkpoints (
      key        TEXT PRIMARY KEY,
      value      BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

async function loadCheckpoint() {
  try {
    await ensureCheckpointTable();
    const { rows } = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = 'lastEventId'"
    );
    if (rows.length && Number(rows[0].value) > 0) {
      lastEventId = Number(rows[0].value);
      log("info", "Checkpoint restored", { lastEventId });
    }
  } catch (e) {
    log("warn", "Checkpoint load failed", { error: e.message });
  }
}

async function saveCheckpoint(id) {
  if (id <= lastEventId) return;
  lastEventId = id;
  try {
    await pgPool.query(
      `INSERT INTO sync_checkpoints (key, value, updated_at)
       VALUES ('lastEventId', $1, NOW())
       ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [id]
    );
  } catch (e) {
    log("warn", "Checkpoint save failed", { error: e.message });
  }
}

// ─────────────────────────────────────────────
// REDIS CACHE
// ─────────────────────────────────────────────
async function cachePositions(rows) {
  if (!rows.length) return;
  try {
    const pipeline = redis.pipeline();
    for (const row of rows) {
      const uid = String(row.device_uid);
      pipeline.hset(`pos:${uid}`, {
        lat:      String(row.latitude  ?? 0),
        lon:      String(row.longitude ?? 0),
        speed:    String(row.speed_kph ?? 0),
        heading:  String(row.heading   ?? 0),
        time:     row.device_time ? new Date(row.device_time).toISOString() : "",
        received: row.received_at ? new Date(row.received_at).toISOString() : "",
      });
      pipeline.expire(`pos:${uid}`, 60);
    }
    await pipeline.exec();
  } catch (e) {
    log("warn", "Redis cache skipped", { error: e.message });
  }
}

// ─────────────────────────────────────────────
// VEHICLE SYNC — batched bulk upsert
// FIX: was 5000 individual PG queries → PG connection crash → worker restart.
// Now 50 batch queries total. Runs every 30 min, never blocks live sync.
// ─────────────────────────────────────────────
export async function syncVehicles() {
  const conn = await getMariaConn();
  try {
    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus, d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL AND r.serial != ''
      LIMIT 5000
    `);
    conn.release();

    const BATCH = 100;
    let upserted = 0;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH).filter(r => String(r.serial || "").trim());
      if (!chunk.length) continue;

      // Bulk vehicle upsert
      const vVals = [], vParams = [];
      let p = 1;
      for (const row of chunk) {
        const serial = String(row.serial).trim();
        const plate  = String(row.reg_no || serial).substring(0, 30);
        vVals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        vParams.push(serial, plate, `Unit ${serial}`,
          String(row.vmodel || ""), String(row.pstatus || "inactive"));
      }
      try {
        await pgPool.query(
          `INSERT INTO vehicles (serial,plate_number,unit_name,model,status,created_at)
           VALUES ${vVals.join(",")}
           ON CONFLICT (serial) DO UPDATE SET
             plate_number = EXCLUDED.plate_number,
             unit_name    = EXCLUDED.unit_name,
             model        = EXCLUDED.model,
             status       = EXCLUDED.status`,
          vParams
        );
      } catch (e) {
        log("warn", "Vehicle batch failed", { batch: i, error: e.message });
        continue;
      }

      // Bulk device upsert
      const withDevice = chunk.filter(r => r.device_uid);
      if (withDevice.length) {
        const dVals = [], dParams = [];
        let dp = 1;
        for (const row of withDevice) {
          dVals.push(`($${dp++},$${dp++},0)`);
          dParams.push(String(row.device_uid), String(row.serial).trim());
        }
        try {
          await pgPool.query(
            `INSERT INTO devices (device_uid,serial,positionid)
             VALUES ${dVals.join(",")}
             ON CONFLICT (device_uid) DO NOTHING`,
            dParams
          );
          await pgPool.query(
            `UPDATE devices d SET vehicle_id = v.id
             FROM vehicles v
             WHERE v.serial = d.serial
               AND d.vehicle_id IS NULL
               AND d.device_uid = ANY($1::text[])`,
            [withDevice.map(r => String(r.device_uid))]
          );
        } catch (e) {
          log("warn", "Device batch failed", { error: e.message });
        }
      }
      upserted += chunk.length;
    }
    log("info", "Vehicle sync complete", { vehicles: upserted, total: rows.length });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
    try { conn?.release(); } catch {}
  }
}

// ─────────────────────────────────────────────
// LIVE SYNC — 1-second updates
//
// KEY DESIGN:
// 1. Persistent MariaDB connection (no reconnect overhead per tick)
// 2. WHERE id > lastEventId (PRIMARY KEY range — returns in <10ms)
// 3. Tiny window: only new rows since last tick
// 4. Bulk PG upsert in one query per 200 rows
// 5. Checkpoint saved to PG after each successful tick
// ─────────────────────────────────────────────
let isLiveRunning = false;

export async function runLiveSync() {
  if (isLiveRunning)        return;
  if (!deviceMapCache.size) return;
  isLiveRunning = true;

  try {
    const conn = await getLiveConn();

    // Seed checkpoint on first run — query MAX(id) once (fast PK scan)
    if (lastEventId === 0) {
      const r = await conn.query("SELECT COALESCE(MAX(id),0)+0 AS m FROM eventData");
      const maxId = Number(r[0].m);
      // Start from 2000 events back ≈ last 30-60 seconds of data
      lastEventId = Math.max(0, maxId - 2000);
      await saveCheckpoint(lastEventId);
      log("info", "Live sync seeded", { lastEventId, maxId });
    }

    // THE CORE QUERY — hits PRIMARY KEY, returns in <10ms even on huge tables
    const rows = await conn.query(
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
       LIMIT 5000`,
      [lastEventId]
    );

    if (!rows.length) return;

    // Bulk upsert into latest_positions
    let upserted = 0;
    let maxSeen  = lastEventId;

    for (let i = 0; i < rows.length; i += 500) {
      const chunk  = rows.slice(i, i + 500);
      const vals   = [], params = [];
      let p = 1;

      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(
          cached.pgDeviceId, lat, lon,
          N(r.speed_kph) ?? 0,
          N(r.heading)   ?? 0,
          r.device_time,
          r.received_at
        );

        const eid = Number(r.event_id);
        if (eid > maxSeen) maxSeen = eid;
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

    // Advance checkpoint
    if (maxSeen > lastEventId) {
      await saveCheckpoint(maxSeen);
    }

    // Broadcast via Socket.IO if available
    if (global.io && upserted > 0) {
      global.io.emit("positions:updated", { count: upserted, ts: Date.now() });
    }

    await cachePositions(rows);

    if (upserted > 0) {
      log("info", "liveSync", { upserted, lastEventId });
    }

  } catch (e) {
    log("error", "liveSync error", { error: e.message });
    // Reset persistent connection on error so next tick reconnects
    try { _liveConn?.release(); } catch {}
    _liveConn = null;
  } finally {
    isLiveRunning = false;
  }
}

// ─────────────────────────────────────────────
// FULL SYNC — writes telemetry history table
// Runs every 60s, separate from live sync
// ─────────────────────────────────────────────
let isFullRunning = false;

export async function runMariaSync() {
  if (isFullRunning) return;
  isFullRunning = true;
  const start = Date.now();
  log("info", "Full sync started");
  try {
    await loadDeviceMap();

    const conn = await getMariaConn();
    const sinceId = Math.max(0, lastEventId - 5000);

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
       LIMIT 1000`,
      [sinceId]
    );
    conn.release();

    let inserted = 0;
    for (const row of allRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;
      const lat = N(row.latitude), lon = N(row.longitude);
      if (lat == null || lon == null) continue;
      try {
        await pgPool.query(
          `INSERT INTO telemetry
             (device_id,latitude,longitude,speed_kph,heading,device_time,received_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT DO NOTHING`,
          [cached.pgDeviceId, lat, lon,
           N(row.speed_kph) ?? 0, N(row.heading) ?? 0,
           row.device_time, row.received_at]
        );
        inserted++;
      } catch {}
    }
    log("info", "Full sync done", { telemetry: inserted, ms: Date.now() - start });
  } catch (e) {
    log("error", "Full sync failed", { error: e.message });
  } finally {
    isFullRunning = false;
  }
}

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────
export async function initMariaSync() {
  await ensureCheckpointTable();
  await loadCheckpoint();
  await loadDeviceMap();
  log("info", "MariaSync initialized", { lastEventId, devices: deviceMapCache.size });
}

// Legacy exports
export async function runQuickSync() { return runLiveSync(); }
export async function syncTelemetry() { return runMariaSync(); }