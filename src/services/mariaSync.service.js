import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { setLatestPositionsBatch } from "./redisLatestPosition.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));

function key(v) {
  return String(v || "").trim();
}

function N(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

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

export const getMariaConn = () => mariaPool.getConnection();

// ─────────────────────────────
// Device Cache
// ─────────────────────────────
export const deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(`
    SELECT id, device_uid
    FROM devices
    WHERE device_uid IS NOT NULL
  `);

  deviceMapCache.clear();

  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid).trim(), r.id);
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────
// CHECKPOINT
// ─────────────────────────────
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = null;

async function getCheckpoint() {
  if (_lastEventId !== null) return _lastEventId;

  const r = await pgPool.query(
    "SELECT value FROM sync_checkpoints WHERE key = $1",
    [CHECKPOINT_KEY]
  );

  _lastEventId = r.rows?.[0]?.value ? Number(r.rows[0].value) : 0;
  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;

  await pgPool.query(`
    INSERT INTO sync_checkpoints (key, value)
    VALUES ($1, $2)
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
  `, [CHECKPOINT_KEY, String(id)]);
}

// ─────────────────────────────
// VEHICLE SYNC (UNCHANGED SAFE)
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

    conn.release();

    for (const r of rows) {
      const uid = key(r.device_uid);
      if (!uid) continue;

      await pgPool.query(`
        INSERT INTO devices (device_uid, label)
        VALUES ($1, $2)
        ON CONFLICT (device_uid)
        DO UPDATE SET label = EXCLUDED.label, updated_at = NOW()
      `, [uid, r.device_name || uid]);
    }

    log("info", "Vehicle sync complete", { total: rows.length });

  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─────────────────────────────
// TELEMETRY SYNC (FIXED CORE)
// ─────────────────────────────
export async function syncTelemetry() {
  const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 300);
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

    log("info", "Fetching events", { lastEventId });

    // ───── latest per device ─────
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
      JOIN device d ON d.id = e.deviceid
      JOIN (
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
      LIMIT ?
    `, [lastEventId, sinceStr, DEVICE_BATCH]);

    // ───── history batch ─────
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
      JOIN device d ON d.id = e.deviceid
      WHERE e.id > ?
        AND e.servertime > ?
        AND e.latitude BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `, [lastEventId, sinceStr, EVENTS_BATCH]);

    conn.release();

    // ─────────────────────────────
    // FIXED BULK INSERT (CORRECT INDEXING)
    // ─────────────────────────────
    const historyValues = [];
    const historyParams = [];

    let p = 1;
    let maxId = lastEventId;
    let inserted = 0;

    for (const r of allRows) {
      const deviceId = deviceMapCache.get(key(r.device_uid));
      if (!deviceId) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);
      if (lat == null || lon == null) continue;

      historyValues.push(
        `($${p},$${p+1},$${p+2},$${p+3},$${p+4},$${p+5},$${p+6})`
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

      p += 7;

      if (r.event_id > maxId) maxId = r.event_id;
      inserted++;
    }

    if (historyValues.length) {
      const result = await pgPool.query(`
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
      `, historyParams);

      log("info", "Telemetry inserted", {
        inserted,
        rowCount: result.rowCount
      });
    }

    // ─────────────────────────────
    // FIXED latest_positions BULK UPSERT
    // ─────────────────────────────
    const latestValues = [];
    const latestParams = [];

    let i = 1;

    for (const r of latestRows) {
      const deviceId = deviceMapCache.get(key(r.device_uid));
      if (!deviceId) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);
      if (lat == null || lon == null) continue;

      latestValues.push(
        `($${i},$${i+1},$${i+2},$${i+3},$${i+4},$${i+5})`
      );

      latestParams.push(
        deviceId,
        lat,
        lon,
        N(r.speed_kph) ?? 0,
        N(r.heading) ?? 0,
        r.device_time
      );

      i += 6;
    }

    if (latestValues.length) {
      await pgPool.query(`
        INSERT INTO latest_positions (
          device_id,
          latitude,
          longitude,
          speed_kph,
          heading,
          device_time,
          received_at,
          updated_at
        )
        VALUES ${latestValues.join(",")}
        ON CONFLICT (device_id)
        DO UPDATE SET
          latitude=EXCLUDED.latitude,
          longitude=EXCLUDED.longitude,
          speed_kph=EXCLUDED.speed_kph,
          heading=EXCLUDED.heading,
          device_time=EXCLUDED.device_time,
          updated_at=NOW()
      `, latestParams);

      await setLatestPositionsBatch(
        latestRows.map(r => ({
          deviceId: deviceMapCache.get(key(r.device_uid)),
          lat: N(r.latitude),
          lon: N(r.longitude),
          speed: N(r.speed_kph) ?? 0,
          heading: N(r.heading) ?? 0,
          dt: new Date(r.device_time)
        }))
      );
    }

    if (maxId > lastEventId) {
      await saveCheckpoint(maxId);
    }

    log("info", "Telemetry sync done", {
      inserted,
      latestDevices: latestRows.length,
      checkpoint: maxId
    });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) conn.release();
  }
}

// ─────────────────────────────
// MASTER SYNC
// ─────────────────────────────
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