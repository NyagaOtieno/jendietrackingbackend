import dotenv from "dotenv";
dotenv.config();

import mysql from "mysql2/promise";
import { pgPool } from "../config/db.js";

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) => {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      level,
      msg,
      ...meta,
    })
  );
};

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─────────────────────────────────────────────
// MYSQL / MARIADB POOL (UNIFIED)
// ─────────────────────────────────────────────
export const mariaPool = mysql.createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// ─────────────────────────────────────────────
// INIT (FIXES worker crash)
// ─────────────────────────────────────────────
export async function initMariaDB() {
  try {
    const conn = await mariaPool.getConnection();
    log("info", "MariaDB/MySQL connected");
    conn.release();
  } catch (err) {
    log("error", "MariaDB connection failed", { error: err.message });
  }
}

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 200);
const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 200);

// ─────────────────────────────────────────────
// LOCK
// ─────────────────────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─────────────────────────────────────────────
// SAFE NUMBER
// ─────────────────────────────────────────────
const N = (v) => {
  if (v === null || v === undefined) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

// ─────────────────────────────────────────────
// VEHICLE SYNC
// ─────────────────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const [rows] = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
      LIMIT 5000
    `);

    let count = 0;

    for (const r of rows) {
      const serial = String(r.serial || "").trim();
      if (!serial) continue;

      await pgPool.query(
        `
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,NOW())
        ON CONFLICT (serial) DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status
        `,
        [
          serial,
          r.reg_no || serial,
          `Unit ${serial}`,
          r.vmodel || "",
          r.pstatus || "inactive",
        ]
      );

      if (r.device_uid) {
        await pgPool.query(
          `
          INSERT INTO devices (device_uid, serial, positionid)
          VALUES ($1,$2,0)
          ON CONFLICT (device_uid) DO NOTHING
          `,
          [String(r.device_uid), serial]
        );
      }

      count++;
    }

    log("info", "Vehicle sync complete", { vehicles: count });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// DEVICE SYNC
// ─────────────────────────────────────────────
async function syncDevice(device, conn) {
  const deviceUid = device.device_uid;
  const lastId = Number(device.positionid) || 0;

  if (!deviceUid) return { count: 0, maxId: lastId };

  const [dev] = await conn.query(
    "SELECT id FROM device WHERE uniqueid=? LIMIT 1",
    [deviceUid]
  );

  if (!dev.length) return { count: 0, maxId: lastId };

  const mariaDeviceId = N(dev[0].id);

  const pg = await pgPool.query(
    "SELECT id FROM devices WHERE device_uid=$1 LIMIT 1",
    [deviceUid]
  );

  if (!pg.rows.length) return { count: 0, maxId: lastId };

  const pgDeviceId = pg.rows[0].id;

  const [rows] = await conn.query(
    `
    SELECT id, devicetime, servertime, latitude, longitude, speed, course
    FROM eventData
    WHERE deviceid=? AND id > ?
    ORDER BY id ASC
    LIMIT ?
    `,
    [mariaDeviceId, lastId, EVENTS_BATCH]
  );

  if (!rows.length) return { count: 0, maxId: lastId };

  let maxId = lastId;
  const valid = [];

  for (const r of rows) {
    const id = N(r.id);
    if (id > maxId) maxId = id;

    const lat = Number(r.latitude);
    const lon = Number(r.longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    if (lat === 0 && lon === 0) continue;

    valid.push({
      pgDeviceId,
      id,
      lat,
      lon,
      speed: Number(r.speed) || 0,
      heading: Number(r.course) || 0,
      dt: new Date(r.devicetime || r.servertime),
    });
  }

  if (!valid.length) return { count: 0, maxId };

  for (const v of valid) {
    await pgPool.query(
      `
      INSERT INTO telemetry (
        device_id, latitude, longitude, speed_kph, heading, device_time
      )
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (device_id, device_time) DO NOTHING
      `,
      [v.pgDeviceId, v.lat, v.lon, v.speed, v.heading, v.dt]
    );
  }

  return { count: valid.length, maxId };
}

// ─────────────────────────────────────────────
// TELEMETRY SYNC
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices"
    );

    let total = 0;
    let processed = 0;

    for (const d of devices) {
      if (processed >= DEVICE_BATCH) break;

      const r = await syncDevice(d, conn);

      if (r.count > 0) {
        total += r.count;

        await pgPool.query(
          `UPDATE devices SET positionid = GREATEST(positionid,$1) WHERE device_uid=$2`,
          [r.maxId, d.device_uid]
        );
      }

      processed++;
    }

    log("info", "Telemetry sync complete", { total, processed });
  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}
export async function initMariaDB() {
  try {
    const conn = await mariaPool.getConnection();
    console.log("MariaDB/MySQL connected");
    conn.release();
  } catch (err) {
    console.error("DB connection failed", err);
  }
}
// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    log("info", "MariaSync started");

    await syncVehicles();
    await syncTelemetry();

    log("info", "MariaSync completed");
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}