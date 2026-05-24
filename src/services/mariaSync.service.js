import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

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

export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(
    "SELECT d.id, d.device_uid FROM devices d WHERE d.device_uid IS NOT NULL"
  );

  deviceMapCache.clear();
  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
    });
  }

  log("info", "Device cache loaded", {
    count: deviceMapCache.size,
  });
}

// ─────────────────────────────────────────────
// VEHICLES
// ─────────────────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();

    const rows = await conn.query(`
      SELECT d.uniqueid AS device_uid, d.name AS device_name
      FROM registration r
      INNER JOIN device d ON d.uniqueid = CONCAT(r.serial, '0')
      LIMIT 5000
    `);

    conn.release();

    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
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

    log("info", "Vehicle sync complete", {
      vehicles: rows.length,
    });
  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─────────────────────────────────────────────
// TELEMETRY CORE
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConn();

    const lastEventId = 0;

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
      WHERE e.id > ${lastEventId}
      ORDER BY e.id ASC
      LIMIT 500
    `);

    conn.release();

    let inserted = 0;

    for (const r of rows) {
      const cached = deviceMapCache.get(String(r.device_uid));
      if (!cached) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);

      if (lat == null || lon == null) continue;

      await pgPool.query(
        `
        INSERT INTO telemetry
          (device_id,latitude,longitude,speed_kph,heading,device_time,received_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT DO NOTHING
        `,
        [
          cached.pgDeviceId,
          lat,
          lon,
          N(r.speed_kph) ?? 0,
          N(r.heading) ?? 0,
          r.device_time,
          r.received_at,
        ]
      );

      inserted++;
    }

    log("info", "Telemetry sync done", {
      inserted,
      total: rows.length,
    });
  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─────────────────────────────────────────────
// MAIN RUN
// ─────────────────────────────────────────────
let running = false;

export async function runMariaSync() {
  if (running) return;
  running = true;

  const t0 = Date.now();

  try {
    log("info", "MariaSync started");

    await loadDeviceMap();
    await syncTelemetry();

    log("info", "MariaSync completed", {
      ms: Date.now() - t0,
    });
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    running = false;
  }
}