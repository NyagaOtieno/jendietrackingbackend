import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { setLatestPositionsBatch } from "./redisLatestPosition.js";

/* ─────────────────────────────
   LOGGER
──────────────────────────── */
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));

/* ─────────────────────────────
   HELPERS
──────────────────────────── */
function key(v) {
  return String(v || "").trim();
}

function N(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/* ─────────────────────────────
   MARIA DB POOL
──────────────────────────── */
export const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST || "18.218.110.222",
  port: Number(process.env.MARIA_DB_PORT) || 3306,
  user: process.env.MARIA_DB_USER || "root",
  password: process.env.MARIA_DB_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
  connectTimeout: 15000,
  acquireTimeout: 20000,
});

export const getMariaConn = () => mariaPool.getConnection();

/* ─────────────────────────────
   DEVICE CACHE (CRITICAL FIX)
──────────────────────────── */
export const deviceMapCache = new Map();

export async function loadDeviceMap() {
  try {
    const res = await pgPool.query(`
      SELECT id, device_uid
      FROM devices
      WHERE device_uid IS NOT NULL
    `);

    deviceMapCache.clear();

    for (const r of res.rows) {
      const uid = key(r.device_uid);
      deviceMapCache.set(uid, r.id);
    }

    log("info", "Device cache loaded", {
      count: deviceMapCache.size
    });

  } catch (e) {
    log("error", "Device cache load failed", {
      error: e.message
    });
  }
}

/* ─────────────────────────────
   CHECKPOINT (FIXED SAFETY)
──────────────────────────── */
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = null;

async function getCheckpoint() {
  if (_lastEventId !== null) return _lastEventId;

  try {
    const r = await pgPool.query(
      `SELECT value FROM sync_checkpoints WHERE "key" = $1`,
      [CHECKPOINT_KEY]
    );

    _lastEventId = r.rows?.[0]
      ? Number(r.rows[0].value)
      : 0;

  } catch {
    _lastEventId = 0;
  }

  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;

  try {
    await pgPool.query(`
      INSERT INTO sync_checkpoints ("key", value)
      VALUES ($1, $2)
      ON CONFLICT ("key")
      DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    `, [CHECKPOINT_KEY, String(id)]);

  } catch (e) {
    log("warn", "Checkpoint save failed", { error: e.message });
  }
}

/* ─────────────────────────────
   VEHICLE SYNC (UNCHANGED)
──────────────────────────── */
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT uniqueid AS device_uid, name AS device_name
      FROM device
      WHERE uniqueid IS NOT NULL AND uniqueid != ''
      LIMIT 5000
    `);

    conn.release();

    let count = 0;

    for (const r of rows) {
      const uid = key(r.device_uid);
      if (!uid) continue;

      await pgPool.query(`
        INSERT INTO devices (device_uid, label)
        VALUES ($1, $2)
        ON CONFLICT (device_uid)
        DO UPDATE SET label = EXCLUDED.label, updated_at = NOW()
      `, [uid, r.device_name || uid]);

      count++;
    }

    log("info", "Vehicle sync complete", { count });

  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

/* ─────────────────────────────
   TELEMETRY SYNC (FIXED CORE)
──────────────────────────── */
export async function syncTelemetry() {
  const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 300);
  const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 500);
  const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 2);

  const lastEventId = await getCheckpoint();

  const sinceStr = new Date(Date.now() - HISTORY_HOURS * 3600000)
    .toISOString()
    .slice(0, 19)
    .replace("T", " ");

  let conn;

  try {
    conn = await getMariaConn();

    log("info", "Fetching events", { lastEventId });

    /* ───── latest per device ───── */
    const latestRows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.id AS event_id,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE id > ? AND servertime > ?
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest
      ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      LIMIT ?
    `, [lastEventId, sinceStr, DEVICE_BATCH]);

    /* ───── history rows ───── */
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
      WHERE e.id > ? AND e.servertime > ?
        AND e.latitude BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `, [lastEventId, sinceStr, EVENTS_BATCH]);

    conn.release();

    /* ───── latest positions ───── */
    const redisPositions = [];

    for (const r of latestRows) {
      const uid = key(r.device_uid);
      const deviceId = deviceMapCache.get(uid);

      if (!deviceId) {
        log("warn", "UNMAPPED DEVICE", { uid });
        continue;
      }

      const lat = N(r.latitude);
      const lon = N(r.longitude);

      if (lat == null || lon == null) continue;

      redisPositions.push({
        deviceId,
        lat,
        lon,
        speed: N(r.speed_kph) ?? 0,
        heading: N(r.heading) ?? 0,
        dt: new Date(r.device_time)
      });
    }

    if (redisPositions.length) {
      await setLatestPositionsBatch(redisPositions);
    }

    /* ───── history insert (BATCHED FIX) ───── */
    let inserted = 0;
    let maxId = lastEventId;

    const batchSize = 200;

    for (let i = 0; i < allRows.length; i += batchSize) {
      const chunk = allRows.slice(i, i + batchSize);

      const queries = [];

      for (const r of chunk) {
        const uid = key(r.device_uid);
        const deviceId = deviceMapCache.get(uid);

        if (!deviceId) continue;

        const lat = N(r.latitude);
        const lon = N(r.longitude);

        if (lat == null || lon == null) continue;

        queries.push(pgPool.query(`
          INSERT INTO telemetry (
            device_id,
            latitude,
            longitude,
            speed_kph,
            heading,
            device_time,
            received_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT DO NOTHING
        `, [
          deviceId,
          lat,
          lon,
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
          r.device_time,
          r.received_at
        ]));

        inserted++;
        if (Number(r.event_id) > maxId) maxId = Number(r.event_id);
      }

      await Promise.all(queries);
    }

    if (maxId > lastEventId) {
      await saveCheckpoint(maxId);
    }

    log("info", "Telemetry sync done", {
      inserted,
      latestDevices: latestRows.length,
      historyRows: allRows.length
    });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

/* ─────────────────────────────
   MASTER SYNC
──────────────────────────── */
let _syncRunning = false;

export async function runMariaSync() {
  if (_syncRunning) return;
  _syncRunning = true;

  try {
    await loadDeviceMap();
    await syncTelemetry();
  } finally {
    _syncRunning = false;
  }
}