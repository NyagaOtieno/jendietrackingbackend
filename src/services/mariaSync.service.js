import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
});

let isSyncRunning = false;
export { isSyncRunning };

// ───────────────── LOCK ─────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ───────────────── CONNECTION ─────────────────
async function getMariaConnection() {
  return mariaPool.getConnection();
}

// ───────────────── VEHICLES SYNC ─────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConnection();

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.id AS maria_device_id,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    for (const r of rows) {
      const serial = String(r.serial).trim();
      if (!serial) continue;

      await pgPool.query(
        `INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (serial) DO UPDATE SET
           plate_number = EXCLUDED.plate_number,
           unit_name = EXCLUDED.unit_name,
           model = EXCLUDED.model,
           status = EXCLUDED.status`,
        [
          serial,
          String(r.reg_no || `PLATE_${serial}`).substring(0, 100),
          `Unit ${serial}`,
          r.vmodel || "",
          r.pstatus || "inactive",
          r.install_date || new Date(),
        ]
      );

      if (r.device_uid) {
        await pgPool.query(
          `INSERT INTO devices (device_uid, serial, positionid)
           VALUES ($1,$2,0)
           ON CONFLICT (device_uid) DO UPDATE SET serial = EXCLUDED.serial`,
          [String(r.device_uid), serial]
        );
      }
    }
  } finally {
    conn?.release();
  }
}

// ───────────────── DEVICE SYNC ─────────────────
async function syncDeviceTelemetry(device, conn) {
  try {
    const deviceUid = device.device_uid;
    const lastId = Number(device.positionid || 0);

    const devRes = await conn.query(
      "SELECT id FROM device WHERE uniqueid = ? LIMIT 1",
      [deviceUid]
    );

    if (!devRes.length) return { count: 0, maxId: lastId };

    const mariaDeviceId = devRes[0].id;

    const pgRes = await pgPool.query(
      "SELECT id FROM devices WHERE device_uid = $1 LIMIT 1",
      [deviceUid]
    );

    if (!pgRes.rows.length) return { count: 0, maxId: lastId };

    const pgDeviceId = pgRes.rows[0].id;

    const rows = await conn.query(
      `SELECT id, servertime, devicetime, latitude, longitude, speed, course, alarmcode
       FROM eventData
       WHERE deviceid = ? AND id > ?
       ORDER BY id ASC LIMIT 5000`,
      [mariaDeviceId, lastId]
    );

    if (!rows.length) return { count: 0, maxId: lastId };

    let maxId = lastId;
    const inserts = [];
    let latest = null;

    for (const r of rows) {
      if (r.id > maxId) maxId = r.id;

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      const item = {
        pgDeviceId,
        lat,
        lon,
        speed: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        deviceTime: new Date(r.devicetime || r.servertime),
      };

      const valid =
        Number.isFinite(lat) &&
        Number.isFinite(lon) &&
        lat !== 0 &&
        lon !== 0;

      if (valid) latest = item;

      inserts.push(item);
    }

    // BULK INSERT (SAFE)
    if (inserts.length) {
      const values = [];
      const placeholders = inserts
        .map((r, i) => {
          const b = i * 6;
          values.push(
            r.pgDeviceId,
            r.lat,
            r.lon,
            r.speed,
            r.heading,
            r.deviceTime
          );
          return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`;
        })
        .join(",");

      await pgPool.query(
        `INSERT INTO telemetry
         (device_id, latitude, longitude, speed_kph, heading, device_time)
         VALUES ${placeholders}
         ON CONFLICT DO NOTHING`,
        values
      );
    }

    // latest position
    if (latest) {
      await pgPool.query(
        `INSERT INTO latest_positions
         (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
         ON CONFLICT (device_id) DO UPDATE SET
           latitude = EXCLUDED.latitude,
           longitude = EXCLUDED.longitude,
           speed_kph = EXCLUDED.speed_kph,
           heading = EXCLUDED.heading,
           device_time = EXCLUDED.device_time,
           updated_at = NOW()`,
        [
          latest.pgDeviceId,
          latest.lat,
          latest.lon,
          latest.speed,
          latest.heading,
          latest.deviceTime,
        ]
      );
    }

    return { count: inserts.length, maxId };
  } catch (err) {
    return { count: 0, maxId: device.positionid || 0 };
  }
}

// ───────────────── TELEMETRY SYNC ─────────────────
export async function syncTelemetry() {
  const conn = await getMariaConnection();

  try {
    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices WHERE device_uid IS NOT NULL"
    );

    for (const d of devices) {
      const res = await syncDeviceTelemetry(d, conn);

      await pgPool.query(
        `UPDATE devices
         SET positionid = GREATEST(positionid, $1)
         WHERE device_uid = $2`,
        [res.maxId, d.device_uid]
      );
    }
  } finally {
    conn.release();
  }
}

// ───────────────── RUNNER ─────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    await syncVehicles();
    await syncTelemetry();
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}