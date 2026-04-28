import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";

// ─── Logger ─────────────────────────────────────────────
const log = (level, msg, meta = {}) => {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));
};

// ─── State ─────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─── MariaDB Pool ──────────────────────────────────────
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 10,
});

// ─── CONFIG ─────────────────────────────────────────────
const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 5);
const BATCH_SIZE = 2000;

// ─── LOCK CONTROL ──────────────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─── SAFE NUMBER ───────────────────────────────────────
const N = (v) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "bigint") return Number(v);
  return Number(v) || 0;
};

async function getMariaConnection() {
  return mariaPool.getConnection();
}

// ─── VEHICLE SYNC ──────────────────────────────────────
export async function syncVehicles() {
  let conn;
  try {
    conn = await getMariaConnection();

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    for (const r of rows) {
      const serial = String(r.serial).trim();
      if (!serial) continue;

      const plate = (r.reg_no || serial).toString().trim();

      await pgPool.query(
        `INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (serial) DO UPDATE SET
           plate_number=EXCLUDED.plate_number,
           unit_name=EXCLUDED.unit_name,
           model=EXCLUDED.model,
           status=EXCLUDED.status`,
        [serial, plate, `Unit ${serial}`, r.vmodel || "", r.pstatus || "inactive", r.install_date || new Date()]
      );

      if (r.device_uid) {
        await pgPool.query(
          `INSERT INTO devices (device_uid, serial, positionid)
           VALUES ($1,$2,0)
           ON CONFLICT (device_uid) DO UPDATE SET serial=EXCLUDED.serial`,
          [String(r.device_uid), serial]
        );
      }
    }

    log("info", "Vehicle sync complete");

  } finally {
    conn?.release();
  }
}

// ─── DEVICE SYNC (🔥 FIXED CORE) ───────────────────────
async function syncDevice(device, conn) {
  const deviceUid = device.device_uid;
  let currentLastId = Number(device.positionid) || 0;

  if (!deviceUid) return 0;

  // get Maria device id
  const dev = await conn.query(
    "SELECT id FROM device WHERE uniqueid=? LIMIT 1",
    [deviceUid]
  );
  if (!dev.length) return 0;

  const mariaDeviceId = N(dev[0].id);

  // get PG device id
  const pg = await pgPool.query(
    "SELECT id FROM devices WHERE device_uid=$1 LIMIT 1",
    [deviceUid]
  );
  if (!pg.rows.length) return 0;

  const pgDeviceId = pg.rows[0].id;

  let total = 0;

  while (true) {
    const rows = await conn.query(
      `SELECT id, servertime, devicetime, latitude, longitude,
              speed, course
       FROM eventData
       WHERE deviceid=? AND id>? 
       ORDER BY id ASC 
       LIMIT ${BATCH_SIZE}`,
      [mariaDeviceId, currentLastId]
    );

    if (!rows.length) break;

    const vals = [];
    let maxId = currentLastId;

    for (const r of rows) {
      const id = N(r.id);
      if (id > maxId) maxId = id;

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      if (!lat || !lon) continue;

      const dt = r.devicetime ? new Date(r.devicetime) : new Date(r.servertime);

      vals.push(pgDeviceId, id, lat, lon, Number(r.speed) || 0, Number(r.course) || 0, dt);
    }

    if (vals.length) {
      const placeholders = [];
      for (let i = 0; i < vals.length / 7; i++) {
        const b = i * 7;
        placeholders.push(`($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6},$${b + 7})`);
      }

      await pgPool.query(
        `INSERT INTO telemetry
         (device_id, event_id, latitude, longitude, speed_kph, heading, device_time)
         VALUES ${placeholders.join(",")}
         ON CONFLICT (device_id, event_id) DO NOTHING`,
        vals
      );

      const latest = vals.slice(-7);

      await pgPool.query(
        `INSERT INTO latest_positions
         (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,NOW(),NOW())
         ON CONFLICT (device_id) DO UPDATE SET
           latitude=EXCLUDED.latitude,
           longitude=EXCLUDED.longitude,
           speed_kph=EXCLUDED.speed_kph,
           heading=EXCLUDED.heading,
           device_time=EXCLUDED.device_time,
           received_at=NOW(),
           updated_at=NOW()`,
        latest
      );
    }

    currentLastId = maxId;

    // 🔥 save progress IMMEDIATELY
    await pgPool.query(
      "UPDATE devices SET positionid=$1 WHERE device_uid=$2",
      [currentLastId, deviceUid]
    );

    total += rows.length;

    console.log(`SYNC ${deviceUid}: +${rows.length}`);
  }

  return total;
}

// ─── TELEMETRY RUNNER ──────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices"
    );

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const batch = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.all(
        batch.map((d) => syncDevice(d, conn))
      );

      total += results.reduce((a, b) => a + b, 0);
    }

    log("info", "Telemetry sync complete", { total });

  } finally {
    conn?.release();
  }
}

// ─── RUNNER ────────────────────────────────────────────
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