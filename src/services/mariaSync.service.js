import dotenv from "dotenv";
dotenv.config();

import * as mariadb from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

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
// MARIA POOL
// ─────────────────────────────────────────────
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
});

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 200);
const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 300);

// ─────────────────────────────────────────────
// POSTGRES LOCK
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
const N = (v) => Number(v) || 0;

// ─────────────────────────────────────────────
// VEHICLE SYNC (UNCHANGED SAFE)
// ─────────────────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await mariaPool.getConnection();
    log("info", "MariaDB connected for vehicle sync");

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
      LIMIT 5000
    `);

    let count = 0;

    for (const r of rows) {
      const serial = String(r.serial).trim();
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
// DEVICE SYNC (FIXED DUPLICATES SAFE)
// ─────────────────────────────────────────────
async function syncDevice(device, conn) {
  const deviceUid = device.device_uid;
  const lastId = Number(device.positionid) || 0;

  if (!deviceUid) return { count: 0, maxId: lastId };

  const dev = await conn.query(
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

  const rows = await conn.query(
    `
    SELECT id, devicetime, servertime, latitude, longitude, speed, course
    FROM eventData
    WHERE deviceid = ?
      AND id > ?
    ORDER BY id ASC
    LIMIT ?
    `,
    [mariaDeviceId, lastId, EVENTS_BATCH]
  );

  if (!rows.length) return { count: 0, maxId: lastId };

  let maxId = lastId;
  const values = [];

  for (const r of rows) {
    const id = N(r.id);
    if (id > maxId) maxId = id;

    const lat = Number(r.latitude);
    const lon = Number(r.longitude);

    if (!lat || !lon) continue;

    const dt = r.devicetime ? new Date(r.devicetime) : new Date();

    values.push([
      pgDeviceId,
      id,
      lat,
      lon,
      Number(r.speed) || 0,
      Number(r.course) || 0,
      dt,
    ]);
  }

  if (!values.length) return { count: 0, maxId };

  // ─────────────────────────────────────────────
  // SAFE UPSERT (FIXES YOUR CRASH)
  // ─────────────────────────────────────────────
  const placeholders = values
    .map(
      (_, i) =>
        `($${i * 7 + 1},$${i * 7 + 2},$${i * 7 + 3},$${i * 7 + 4},$${i * 7 + 5},$${i * 7 + 6},$${i * 7 + 7})`
    )
    .join(",");

  const flat = values.flat();

  await pgPool.query(
    `
    INSERT INTO telemetry
    (device_id, event_id, latitude, longitude, speed_kph, heading, device_time)
    VALUES ${placeholders}
    ON CONFLICT (device_id, event_id)
    DO UPDATE SET
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      speed_kph = EXCLUDED.speed_kph,
      heading = EXCLUDED.heading,
      device_time = EXCLUDED.device_time
    `,
    flat
  );

  const latest = values[values.length - 1];

  await pgPool.query(
    `
    INSERT INTO latest_positions
    (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
    ON CONFLICT (device_id) DO UPDATE SET
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      speed_kph = EXCLUDED.speed_kph,
      heading = EXCLUDED.heading,
      device_time = EXCLUDED.device_time,
      updated_at = NOW()
    `,
    [
      pgDeviceId,
      latest[2],
      latest[3],
      latest[4],
      latest[5],
      latest[6],
    ]
  );

  return { count: values.length, maxId };
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

    for (const d of devices.slice(0, DEVICE_BATCH)) {
      const r = await syncDevice(d, conn);

      if (r.count > 0) {
        total += r.count;

        await pgPool.query(
          `UPDATE devices SET positionid = $1 WHERE device_uid = $2`,
          [r.maxId, d.device_uid]
        );
      }
    }

    log("info", "Telemetry sync complete", { total });
  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
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