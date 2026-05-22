// src/services/mariaSync.service.js

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v == null) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

/* ─────────────────────────────────────────────
   MARIA DB POOL (HARDENED)
──────────────────────────────────────────── */
export const mariaPool = createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  port: Number(process.env.MARIA_PORT) || 3306,
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 10,
  connectTimeout: 15000,
  acquireTimeout: 20000,
  resetAfterUse: true,
});

export const getMariaConn = () => mariaPool.getConnection();

/* ─────────────────────────────────────────────
   DEVICE CACHE
──────────────────────────────────────────── */
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(
    "SELECT id, device_uid, vehicle_id FROM devices WHERE device_uid IS NOT NULL"
  );

  deviceMapCache.clear();

  for (const r of res.rows || []) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
      pgVehicleId: r.vehicle_id,
    });
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

/* ─────────────────────────────────────────────
   VEHICLE SYNC
──────────────────────────────────────────── */
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const limit = Number(process.env.VEHICLE_BATCH || 5000);

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        d.name AS device_name
      FROM device d
      LIMIT ?
    `, [limit]);

    let count = 0;

    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
      if (!uid) continue;

      await pgPool.query(
        `INSERT INTO devices (device_uid, name)
         VALUES ($1, $2)
         ON CONFLICT (device_uid)
         DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()`,
        [uid, r.device_name || uid]
      );

      count++;
    }

    log("info", "Vehicle sync complete", { vehicles: count });
  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) {
      try { conn.release(); } catch {}
    }
  }
}

/* ─────────────────────────────────────────────
   CHECKPOINT
──────────────────────────────────────────── */
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = 0;

async function getCheckpoint() {
  if (_lastEventId) return _lastEventId;

  try {
    const r = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = $1",
      [CHECKPOINT_KEY]
    );

    _lastEventId = r.rows?.[0] ? Number(r.rows[0].value) : 0;
  } catch {
    _lastEventId = 0;
  }

  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;

  try {
    await pgPool.query(`
      INSERT INTO sync_checkpoints (key, value)
      VALUES ($1, $2)
      ON CONFLICT (key)
      DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    `, [CHECKPOINT_KEY, String(id)]);
  } catch {}
}

/* ─────────────────────────────────────────────
   REDIS CACHE (SAFE)
──────────────────────────────────────────── */
async function cacheLatestPositions(rows) {
  try {
    if (!redis?.pipeline) return;

    const pipe = redis.pipeline();

    for (const r of rows) {
      pipe.set(`pos:${r.device_uid}`, JSON.stringify(r), "EX", 3600);
    }

    await pipe.exec();
  } catch (e) {
    log("warn", "Redis cache error", { error: e.message });
  }
}

/* ─────────────────────────────────────────────
   TELEMETRY SYNC (HARDENED)
──────────────────────────────────────────── */
export async function syncTelemetry() {
  const lastEventId = await getCheckpoint();

  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.id AS event_id,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.id > ?
      LIMIT 2000
    `, [lastEventId]);

    conn.release();
    conn = null;

    if (!rows.length) return;

    let maxId = lastEventId;

    const BATCH = 200;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);

      const vals = [];
      const params = [];
      let p = 1;

      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;

        const lat = N(r.latitude);
        const lon = N(r.longitude);
        if (lat == null || lon == null) continue;

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);

        params.push(
          cached.pgDeviceId,
          lat,
          lon,
          N(r.speed_kph) || 0,
          N(r.heading) || 0,
          r.device_time,
          r.received_at
        );

        if (Number(r.event_id) > maxId) maxId = Number(r.event_id);
      }

      if (!vals.length) continue;

      await pgPool.query(`
        INSERT INTO telemetry
        (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
        VALUES ${vals.join(",")}
        ON CONFLICT DO NOTHING
      `, params);
    }

    await saveCheckpoint(maxId);

    await cacheLatestPositions(rows);

    log("info", "Telemetry sync done", { count: rows.length });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) {
      try { conn.release(); } catch {}
    }
  }
}

/* ─────────────────────────────────────────────
   LOCK
──────────────────────────────────────────── */
export let isSyncRunning = false;

/* ─────────────────────────────────────────────
   QUICK SYNC (FIXED + EXPORTED)
──────────────────────────────────────────── */
export let isQuickRunning = false;

export async function runQuickSync() {
  if (isQuickRunning) return;
  if (!deviceMapCache.size) return;

  isQuickRunning = true;
  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      ORDER BY e.id DESC
      LIMIT 1000
    `);

    conn.release();
    conn = null;

    if (!rows.length) return;

    let upserted = 0;

    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);

      const vals = [];
      const params = [];
      let p = 1;

      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;

        const lat = N(r.latitude);
        const lon = N(r.longitude);
        if (lat == null || lon == null) continue;

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`);

        params.push(
          cached.pgDeviceId,
          lat,
          lon,
          N(r.speed_kph) || 0,
          N(r.heading) || 0,
          r.device_time
        );

        upserted++;
      }

      if (!vals.length) continue;

      await pgPool.query(`
        INSERT INTO latest_positions
        (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
        VALUES ${vals.join(",")}
        ON CONFLICT (device_id)
        DO UPDATE SET
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          speed_kph = EXCLUDED.speed_kph,
          heading = EXCLUDED.heading,
          device_time = EXCLUDED.device_time,
          received_at = NOW(),
          updated_at = NOW()
      `, params);
    }

    log("info", "quickSync complete", { upserted });

  } catch (e) {
    log("error", "quickSync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
    isQuickRunning = false;
  }
}

/* ─────────────────────────────────────────────
   MAIN EXPORTS
──────────────────────────────────────────── */
export async function runMariaSync() {
  if (isSyncRunning) return;

  isSyncRunning = true;

  try {
    await loadDeviceMap();
    await syncTelemetry();
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isSyncRunning = false;
  }
}

export async function initMariaSync() {
  await syncVehicles();
  await loadDeviceMap();
  await syncTelemetry();
}