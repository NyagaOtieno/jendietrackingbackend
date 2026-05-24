import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

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
  connectionLimit: 3,
  connectTimeout: 15000,
  acquireTimeout: 20000,
});

export const getMariaConn = () => mariaPool.getConnection();

// ─────────────────────────────
// Device Cache (Postgres mapping)
// ─────────────────────────────
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(`
    SELECT id, device_uid FROM devices WHERE device_uid IS NOT NULL
  `);

  deviceMapCache.clear();

  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
    });
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────
// TELEMETRY SYNC (FIXED CORE)
// ─────────────────────────────
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

// ─────────────────────────────
// MAIN TELEMETRY SYNC
// ─────────────────────────────
export async function syncTelemetry() {
  const lastEventId = await getCheckpoint();

  let conn;

  try {
    conn = await getMariaConn();

    log("info", "Fetching telemetry", { lastEventId });

    // 🔥 FIXED: use deviceid directly (NO device.uniqueid mapping)
    const rows = await conn.query(`
      SELECT
        e.id AS event_id,
        e.deviceid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      WHERE e.id > ?
      ORDER BY e.id ASC
      LIMIT 1000
    `, [lastEventId]);

    conn.release();
    conn = null;

    if (!rows.length) {
      log("info", "No telemetry rows");
      return;
    }

    let maxId = lastEventId;
    let inserted = 0;

    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);

      const values = [];
      const params = [];
      let p = 1;

      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));

        if (!cached) continue;

        const lat = N(r.latitude);
        const lon = N(r.longitude);

        if (lat == null || lon == null) continue;

        values.push(
          `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`
        );

        params.push(
          cached.pgDeviceId,
          lat,
          lon,
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
          r.device_time
        );

        inserted++;
        if (Number(r.event_id) > maxId) maxId = Number(r.event_id);
      }

      if (!values.length) continue;

      await pgPool.query(`
        INSERT INTO latest_positions
          (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
        VALUES ${values.join(",")}
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

    if (maxId > lastEventId) {
      await saveCheckpoint(maxId);
    }

    log("info", "Telemetry sync done", { inserted, rows: rows.length });

  } catch (e) {
    log("error", "syncTelemetry failed", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─────────────────────────────
// VEHICLE SYNC (UNCHANGED LOGIC SAFE)
// ─────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        d.name AS device_name
      FROM device d
      LIMIT 5000
    `);

    conn.release();

    let count = 0;

    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
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
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─────────────────────────────
// RUN WRAPPER
// ─────────────────────────────
let running = false;

export async function runMariaSync() {
  if (running) return;
  running = true;

  try {
    await loadDeviceMap();
    await syncTelemetry();
  } catch (e) {
    log("error", "runMariaSync failed", { error: e.message });
  } finally {
    running = false;
  }
}

// ─────────────────────────────
// QUICK SYNC (optional safe mode)
// ─────────────────────────────
export async function runQuickSync() {
  try {
    await syncTelemetry();
  } catch (e) {
    log("error", "quickSync failed", { error: e.message });
  }
}