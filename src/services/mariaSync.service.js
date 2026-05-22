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

// ─── MariaDB pool (OPTIMIZED FOR LOW MEMORY VPS) ─────────────────────────────
export const mariaPool = createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  port: Number(process.env.MARIA_PORT) || 3306,
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",

  connectionLimit: 5,       // 🔥 reduced for stability
  queueLimit: 0,
  acquireTimeout: 30000,
  connectTimeout: 15000,
  resetAfterUse: true,
});

export const getMariaConn = () => mariaPool.getConnection();

// ─── DEVICE MAP CACHE ────────────────────────────────────────────────────────
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(
    "SELECT d.id, d.device_uid, d.vehicle_id FROM devices d WHERE d.device_uid IS NOT NULL"
  );

  deviceMapCache.clear();

  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
      pgVehicleId: r.vehicle_id,
    });
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─── VEHICLE SYNC ────────────────────────────────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const limit = Number(process.env.VEHICLE_BATCH || 2000);

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

      await pgPool.query(`
        INSERT INTO devices (device_uid, name)
        VALUES ($1, $2)
        ON CONFLICT (device_uid)
        DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
      `, [uid, r.device_name || uid]);

      count++;
    }

    log("info", "Vehicle sync complete", { vehicles: count, total: rows.length });

  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─── CHECKPOINT ──────────────────────────────────────────────────────────────
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = 0;

async function getCheckpoint() {
  if (_lastEventId) return _lastEventId;

  try {
    const r = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = $1",
      [CHECKPOINT_KEY]
    );

    _lastEventId = r.rows[0] ? Number(r.rows[0].value) : 0;

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

// ─── REDIS CACHE ─────────────────────────────────────────────────────────────
async function cacheLatestPositions(rows) {
  try {
    if (!redis?.pipeline) return;

    const pipe = redis.pipeline();

    for (const r of rows) {
      pipe.set(
        `pos:${r.device_uid}`,
        JSON.stringify(r),
        "EX",
        3600
      );
    }

    await pipe.exec();

  } catch (e) {
    log("warn", "Redis cache error", { error: e.message });
  }
}

// ─── TELEMETRY SYNC ──────────────────────────────────────────────────────────
export async function syncTelemetry() {
  const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 150);
  const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 200);
  const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 2);

  const lastEventId = await getCheckpoint();
  const sinceMs = Date.now() - HISTORY_HOURS * 3600000;
  const sinceStr = new Date(sinceMs).toISOString().slice(0, 19).replace("T", " ");

  let conn;

  try {
    conn = await getMariaConn();

    log("info", "Fetching events", { lastEventId });

    // ─── LATEST PER DEVICE (FIXED ORDERING) ────────────────────────────────
    const latestRows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE id > ?
          AND servertime > ?
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest
      ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      ORDER BY e.id DESC
      LIMIT ?
    `, [lastEventId, sinceStr, DEVICE_BATCH]);

    // ─── FULL EVENTS ────────────────────────────────────────────────────────
    const allRows = await conn.query(`
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
        AND e.servertime > ?
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
        AND NOT (latitude = 0 AND longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `, [lastEventId, sinceStr, EVENTS_BATCH]);

    conn.release();
    conn = null;

    // ─── UPSERT LATEST POSITIONS ───────────────────────────────────────────
    const BATCH = 150;
    let posCount = 0;

    for (let i = 0; i < latestRows.length; i += BATCH) {
      const chunk = latestRows.slice(i, i + BATCH);

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
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
          r.device_time
        );

        posCount++;
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

    await cacheLatestPositions(latestRows);

    // ─── TELEMETRY HISTORY ────────────────────────────────────────────────
    let maxId = lastEventId;
    let telCount = 0;

    for (let i = 0; i < allRows.length; i += BATCH) {
      const chunk = allRows.slice(i, i + BATCH);

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
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
          r.device_time,
          r.received_at
        );

        telCount++;
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

    if (maxId > lastEventId) await saveCheckpoint(maxId);

    log("info", "Telemetry sync done", {
      telemetry: telCount,
      latest: posCount
    });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─── LOCK ─────────────────────────────────────────────────────────────────────
export let isSyncRunning = false;

async function acquireLock() {
  try {
    const r = await pgPool.query(`
      INSERT INTO sync_locks (key, locked_at)
      VALUES ('mariasync', NOW())
      ON CONFLICT (key)
      DO UPDATE SET locked_at = NOW()
      WHERE sync_locks.locked_at < NOW() - INTERVAL '2 minutes'
      RETURNING key
    `);

    return r.rowCount > 0;
  } catch {
    return false;
  }
}

async function releaseLock() {
  try {
    await pgPool.query(
      "DELETE FROM sync_locks WHERE key = 'mariasync'"
    );
  } catch {}
}

// ─── MAIN SYNC ───────────────────────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    log("info", "MariaSync started");

    await loadDeviceMap();
    await syncTelemetry();

    log("info", "MariaSync completed");

  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
export async function initMariaSync() {
  log("info", "MariaSync initializing...");

  await syncVehicles();
  await loadDeviceMap();
  await syncTelemetry();

  log("info", "MariaSync init complete");
}

// ─── QUICK SYNC (NOW EXPORTED PROPERLY) ──────────────────────────────────────
let isQuickRunning = false;

export async function runQuickSync() {
  if (isQuickRunning) return;
  if (!deviceMapCache.size) return;

  isQuickRunning = true;

  let conn;

  try {
    conn = await getMariaConn();

    const since = new Date(Date.now() - 120000)
      .toISOString()
      .slice(0, 19)
      .replace("T", " ");

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE servertime > ?
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest
      ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      LIMIT 3000
    `, [since]);

    conn.release();
    conn = null;

    if (!rows.length) return;

    const BATCH = 150;
    let upserted = 0;

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

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`);

        params.push(
          cached.pgDeviceId,
          lat,
          lon,
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
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
    if (conn) conn.release();
    isQuickRunning = false;
  }
}