import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/* ───────────────────────────────
   MARIA DB POOL (SAFE)
─────────────────────────────── */
export const mariaPool = createPool({
  host: process.env.MARIA_HOST || "localhost",
  port: Number(process.env.MARIA_PORT) || 3306,
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,           // 🔥 reduced for 1GB VPS stability
  acquireTimeout: 20000,
  connectTimeout: 15000,
  resetAfterUse: true,
});

export const getMariaConn = () => mariaPool.getConnection();

/* ───────────────────────────────
   DEVICE CACHE
─────────────────────────────── */
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(`
    SELECT id, device_uid, vehicle_id
    FROM devices
    WHERE device_uid IS NOT NULL
  `);

  deviceMapCache.clear();

  for (const r of res.rows || []) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
      pgVehicleId: r.vehicle_id,
    });
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

/* ───────────────────────────────
   VEHICLE SYNC (FIXED SCHEMA SAFE)
─────────────────────────────── */
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT d.uniqueid AS device_uid
      FROM device d
      LIMIT 5000
    `);

    conn.release();
    conn = null;

    let count = 0;

    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
      if (!uid) continue;

      // ❌ FIX: removed "name" column (was crashing your DB)
      await pgPool.query(`
        INSERT INTO devices (device_uid)
        VALUES ($1)
        ON CONFLICT (device_uid)
        DO UPDATE SET updated_at = NOW()
      `, [uid]);

      count++;
    }

    log("info", "Vehicle sync complete", { count });
  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

/* ───────────────────────────────
   CHECKPOINT
─────────────────────────────── */
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

/* ───────────────────────────────
   REDIS CACHE SAFE
─────────────────────────────── */
async function cacheLatestPositions(rows) {
  if (!redis?.pipeline) return;

  try {
    const pipe = redis.pipeline();

    for (const r of rows) {
      pipe.set(`pos:${r.device_uid}`, JSON.stringify(r), "EX", 3600);
    }

    await pipe.exec();
  } catch (e) {
    log("warn", "Redis cache error", { error: e.message });
  }
}

/* ───────────────────────────────
   TELEMETRY SYNC
─────────────────────────────── */
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
      LIMIT 1500
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
    if (conn) try { conn.release(); } catch {}
  }
}

/* ───────────────────────────────
   EXPORTS (CRITICAL FIX)
─────────────────────────────── */
export async function runMariaSync() {
  await loadDeviceMap();
  await syncTelemetry();
}

export async function runQuickSync() {
  // safe alias for worker compatibility
  return syncTelemetry();
}