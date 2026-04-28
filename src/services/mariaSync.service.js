import dotenv from "dotenv";
dotenv.config();

import mariadb from "mariadb";
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
// MARIA DB POOL (FIXED ESM SAFE)
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
const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 200);

// ─────────────────────────────────────────────
// LOCK (Postgres advisory lock)
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
  return Number(v) || 0;
};

// ─────────────────────────────────────────────
// VEHICLE SYNC
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
// DEVICE SYNC (SAFE + NO DUPLICATES)
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
    WHERE deviceid=?
      AND id > ?
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

    if (!lat || !lon) continue;

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

  // ─────────────────────────────────────────────
  // FIX: PREVENT DUPLICATES BEFORE INSERT
  // ─────────────────────────────────────────────
  const ids = valid.map((v) => v.id);

  const existing = await pgPool.query(
    `
    SELECT event_id FROM telemetry
    WHERE device_id = $1 AND event_id = ANY($2::bigint[])
    `,
    [pgDeviceId, ids]
  );

  const existingSet = new Set(existing.rows.map((r) => Number(r.event_id)));

  const filtered = valid.filter((v) => !existingSet.has(v.id));

  if (!filtered.length) return { count: 0, maxId };

  const values = [];
  const placeholders = filtered.map((v, i) => {
    const b = i * 7;
    values.push(
      v.pgDeviceId,
      v.id,
      v.lat,
      v.lon,
      v.speed,
      v.heading,
      v.dt
    );
    return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6},$${b + 7})`;
  });

  await pgPool.query(
    `
    INSERT INTO telemetry
    (device_id, event_id, latitude, longitude, speed_kph, heading, device_time)
    VALUES ${placeholders.join(",")}
    `,
    values
  );

  const latest = filtered.reduce((a, b) => (a.dt > b.dt ? a : b));

  await pgPool.query(
    `
    INSERT INTO latest_positions
    (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
    ON CONFLICT (device_id)
    DO UPDATE SET
      latitude = EXCLUDED.latitude,
      longitude = EXCLUDED.longitude,
      speed_kph = EXCLUDED.speed_kph,
      heading = EXCLUDED.heading,
      device_time = EXCLUDED.device_time,
      received_at = NOW(),
      updated_at = NOW()
    `,
    [
      latest.pgDeviceId,
      latest.lat,
      latest.lon,
      latest.speed,
      latest.heading,
      latest.dt,
    ]
  );

  return { count: filtered.length, maxId };
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
          `
          UPDATE devices
          SET positionid = GREATEST(positionid,$1)
          WHERE device_uid=$2
          `,
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

// ─────────────────────────────────────────────
// MAIN SYNC
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