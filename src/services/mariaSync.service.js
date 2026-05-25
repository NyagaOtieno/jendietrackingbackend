import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { setLatestPositionsBatch } from "./redisLatestPosition.js";

// ─────────────────────────────
// Logger
// ─────────────────────────────
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta,
  }));

// ─────────────────────────────
// Helpers
// ─────────────────────────────
const key = (v) => String(v || "").trim();

const N = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// ─────────────────────────────
// MariaDB Pool
// ─────────────────────────────
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

const getMariaConn = () => mariaPool.getConnection();

// ─────────────────────────────
// Device Cache
// ─────────────────────────────
export const deviceMapCache = new Map();

export async function loadDeviceMap() {
  try {
    const res = await pgPool.query(`
      SELECT id, device_uid
      FROM devices
      WHERE device_uid IS NOT NULL
    `);

    const rows = res?.rows || [];

    deviceMapCache.clear();

    for (const r of rows) {
      deviceMapCache.set(key(r.device_uid), r.id);
    }

    log("info", "Device cache loaded", { count: deviceMapCache.size });
  } catch (e) {
    log("error", "loadDeviceMap failed", { error: e.message });
  }
}

// ─────────────────────────────
// Checkpoint
// ─────────────────────────────
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = null;

async function getCheckpoint() {
  if (_lastEventId !== null) return _lastEventId;

  const r = await pgPool.query(
    "SELECT value FROM sync_checkpoints WHERE key = $1",
    [CHECKPOINT_KEY]
  );

  _lastEventId = r?.rows?.[0]?.value ? Number(r.rows[0].value) : 0;
  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;

  await pgPool.query(
    `
    INSERT INTO sync_checkpoints (key, value)
    VALUES ($1, $2)
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
  `,
    [CHECKPOINT_KEY, String(id)]
  );
}

// ─────────────────────────────
// VEHICLES SYNC
// ─────────────────────────────
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

    for (const r of rows || []) {
      const uid = key(r.device_uid);
      if (!uid) continue;

      await pgPool.query(
        `
        INSERT INTO devices (device_uid, label)
        VALUES ($1, $2)
        ON CONFLICT (device_uid)
        DO UPDATE SET label = EXCLUDED.label, updated_at = NOW()
      `,
        [uid, r.device_name || uid]
      );
    }

    log("info", "Vehicle sync complete", { total: rows.length });
  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─────────────────────────────
// TELEMETRY SYNC (FIXED)
// ─────────────────────────────
export async function syncTelemetry() {
  const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 1000);
  const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 2);

  const lastEventId = await getCheckpoint();

  const sinceStr = new Date(Date.now() - HISTORY_HOURS * 3600000)
    .toISOString()
    .slice(0, 19)
    .replace("T", " ");

  let conn;

  try {
    conn = await getMariaConn();

    log("info", "SYNC START", { lastEventId });

    const rows = await conn.query(
      `
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
      JOIN device d ON d.id = e.deviceid
      WHERE e.id > ?
        AND e.servertime > ?
        AND e.latitude BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `,
      [lastEventId, sinceStr, EVENTS_BATCH]
    );

    const historyValues = [];
    const historyParams = [];
    const redisBatch = [];

    let p = 1;
    let maxId = lastEventId;

    for (const r of rows || []) {
      const deviceId = deviceMapCache.get(key(r.device_uid));
      if (!deviceId) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);
      if (lat === null || lon === null) continue;

      historyValues.push(
        `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`
      );

      historyParams.push(
        deviceId,
        lat,
        lon,
        N(r.speed_kph) ?? 0,
        N(r.heading) ?? 0,
        r.device_time,
        r.received_at
      );

      redisBatch.push({
        deviceId,
        lat,
        lon,
        speed: N(r.speed_kph) ?? 0,
        heading: N(r.heading) ?? 0,
        dt: new Date(r.device_time),
      });

      if (r.event_id > maxId) maxId = r.event_id;
    }

    if (historyValues.length) {
      await pgPool.query(
        `
        INSERT INTO telemetry (
          device_id,
          latitude,
          longitude,
          speed_kph,
          heading,
          device_time,
          received_at
        )
        VALUES ${historyValues.join(",")}
        ON CONFLICT DO NOTHING
      `,
        historyParams
      );
    }

    if (redisBatch.length) {
      await setLatestPositionsBatch(redisBatch);
    }

    if (maxId > lastEventId) {
      await saveCheckpoint(maxId);
    }

    log("info", "SYNC COMPLETE", {
      processed: rows.length,
      checkpoint: maxId,
    });
  } catch (e) {
    log("error", "syncTelemetry failed", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─────────────────────────────
// MASTER SYNC
// ─────────────────────────────
let running = false;

export async function runMariaSync() {
  if (running) return;
  running = true;

  try {
    await loadDeviceMap();
    await syncTelemetry();
  } finally {
    running = false;
  }
}