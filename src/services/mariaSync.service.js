import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

// ─────────────────────────────────────────────
// LOGGING
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
export let isSyncRunning = false;
let lastSyncTime = null;

// ─────────────────────────────────────────────
// MARIA POOL
// ─────────────────────────────────────────────
export const mariaPool = createPool({
  host: process.env.MARIADB_HOST || "18.218.110.222",
  port: Number(process.env.MARIADB_PORT || 3306),
  user: process.env.MARIADB_USER || "root",
  password: process.env.MARIADB_PASSWORD || "nairobiyetu",
  database: process.env.MARIADB_DATABASE || "uradi",
  connectionLimit: 5,
});

// ─────────────────────────────────────────────
// SAFE NUMBER
// ─────────────────────────────────────────────
const N = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

// ─────────────────────────────────────────────
// CONNECTION
// ─────────────────────────────────────────────
async function getMariaConn() {
  return await mariaPool.getConnection();
}

// ─────────────────────────────────────────────
// DEVICE MAP
// ─────────────────────────────────────────────
let deviceMapCache = new Map();

async function loadDeviceMap() {
  const { rows } = await pgPool.query(`
    SELECT id, device_uid, vehicle_id
    FROM devices
    WHERE device_uid IS NOT NULL
  `);

  deviceMapCache = new Map(
    rows.map(r => [String(r.device_uid).trim(), {
      pgDeviceId: r.id,
      pgVehicleId: r.vehicle_id
    }])
  );

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────────────────────
// VEHICLE SYNC (SAFE)
// ─────────────────────────────────────────────
export async function syncVehicles() {
  const conn = await getMariaConn();

  try {
    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus, d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      LIMIT 2000
    `);

    for (const r of rows) {
      const serial = String(r.serial || "").trim();
      if (!serial) continue;

      await pgPool.query(`
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,NOW())
        ON CONFLICT (serial) DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          model = EXCLUDED.model,
          status = EXCLUDED.status
      `, [
        serial,
        String(r.reg_no || serial),
        `Unit ${serial}`,
        String(r.vmodel || ""),
        String(r.pstatus || "inactive")
      ]);
    }

    log("info", "Vehicle sync complete", { count: rows.length });

  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// TELEMETRY SYNC (FIXED)
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  const conn = await getMariaConn();

  try {
    const since = new Date(Date.now() - 2 * 3600_000);
    const sinceStr = since.toISOString().slice(0, 19).replace("T", " ");

    log("info", "Fetching telemetry", { since: sinceStr });

    const rows = await conn.query(`
      SELECT e.id AS event_id,
             d.uniqueid AS device_uid,
             e.latitude,
             e.longitude,
             e.speed AS speed_kph,
             e.course AS heading,
             e.devicetime,
             e.servertime
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.servertime > ?
      LIMIT 2000
    `, [sinceStr]);

    log("info", "Fetched rows", { count: rows.length });

    let inserted = 0;

    for (const r of rows) {
      const cached = deviceMapCache.get(String(r.device_uid).trim());

      if (!cached) {
        log("warn", "Missing device map", { uid: r.device_uid });
        continue;
      }

      const lat = N(r.latitude);
      const lon = N(r.longitude);
      if (lat === null || lon === null) continue;

      await pgPool.query(`
        INSERT INTO telemetry (
          device_id, latitude, longitude,
          speed_kph, heading, device_time, received_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT DO NOTHING
      `, [
        cached.pgDeviceId,
        lat,
        lon,
        N(r.speed_kph) || 0,
        N(r.heading) || 0,
        r.devicetime,
        r.servertime
      ]);

      inserted++;
    }

    lastSyncTime = new Date().toISOString();

    log("info", "Telemetry sync done", { inserted });

  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// MAIN SYNC
// ─────────────────────────────────────────────
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

// ─────────────────────────────────────────────
// INIT
// ─────────────────────────────────────────────
export async function initMariaSync() {
  await syncVehicles();
  await loadDeviceMap();
  await syncTelemetry();
}