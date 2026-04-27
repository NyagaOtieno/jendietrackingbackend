import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

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

// ─── MariaDB Pool (NO DEPRECATION, SAFE FALLBACKS) ─────
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
  connectTimeout: 15000,
  acquireTimeout: 20000,
});

// ─── CONFIG ─────────────────────────────────────────────
const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 10);

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

// ─── SAFE CONVERTER (CRITICAL FIX) ──────────────────────
const N = (v) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "bigint") return Number(v);
  return Number(v) || 0;
};

// ─── CONNECTION ─────────────────────────────────────────
async function getMariaConnection() {
  return mariaPool.getConnection();
}

// ─── 1. VEHICLES SYNC ───────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConnection();
    log("info", "MariaDB connected for vehicle sync");

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.id AS device_id, d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    let vehicles = 0;
    let devices = 0;

    for (const r of rows) {
      const serial = String(r.serial).trim();
      if (!serial) continue;

      const plate = (r.reg_no || serial).toString().trim().substring(0, 100);

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

      vehicles++;

      if (r.device_uid) {
        await pgPool.query(
          `INSERT INTO devices (device_uid, serial, positionid)
           VALUES ($1,$2,0)
           ON CONFLICT (device_uid) DO UPDATE SET serial=EXCLUDED.serial`,
          [String(r.device_uid), serial]
        );

        devices++;
      }
    }

    log("info", "Vehicle sync complete", { vehicles, devices });

  } finally {
    conn?.release();
  }
}

// ─── 2. DEVICE SYNC ────────────────────────────────────
async function syncDevice(device, conn) {
  const deviceUid = device.device_uid;
  const lastId = Number(device.positionid) || 0;

  if (!deviceUid || deviceUid.replace(/0/g, "") === "") {
    return { count: 0, maxId: lastId };
  }

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
    `SELECT id, servertime, devicetime, latitude, longitude,
            speed, course, alarmcode
     FROM eventData
     WHERE deviceid=? AND id>? ORDER BY id ASC LIMIT 2000`,
    [mariaDeviceId, lastId]
  );

  if (!rows.length) return { count: 0, maxId: lastId };

  let maxId = lastId;
  const valid = [];

  for (const r of rows) {
    const id = N(r.id);
    if (id > maxId) maxId = id;

    const lat = Number(r.latitude);
    const lon = Number(r.longitude);

    const dt = r.devicetime ? new Date(r.devicetime) : new Date(r.servertime);

    const ok =
      Number.isFinite(lat) &&
      Number.isFinite(lon) &&
      lat !== 0 &&
      lon !== 0;

    if (ok) {
      valid.push({
        pgDeviceId,
        id,
        lat,
        lon,
        speed: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        dt
      });
    }
  }

  if (valid.length) {
    const vals = [];
    const ph = valid.map((v, i) => {
      const b = i * 6;
      vals.push(v.pgDeviceId, v.id, v.lat, v.lon, v.speed, v.heading, v.dt);
      return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6},$${b + 7})`;
    });

    await pgPool.query(
      `INSERT INTO telemetry
       (device_id, event_id, latitude, longitude, speed_kph, heading, device_time)
       VALUES ${ph.join(",")}
       ON CONFLICT (device_id, event_id) DO NOTHING`,
      vals
    );
  }

  if (valid.length) {
    const latest = valid.reduce((a, b) =>
      a.dt > b.dt ? a : b
    );

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
         updated_at=NOW()
       WHERE latest_positions.device_time IS NULL
          OR EXCLUDED.device_time > latest_positions.device_time`,
      [
        latest.pgDeviceId,
        latest.lat,
        latest.lon,
        latest.speed,
        latest.heading,
        latest.dt
      ]
    );
  }

  return { count: valid.length, maxId };
}

// ─── 3. TELEMETRY RUNNER ───────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices"
    );

    let total = 0;

    for (const d of devices) {
      const r = await syncDevice(d, conn);

      if (r.count > 0) {
        total += r.count;

        await pgPool.query(
          "UPDATE devices SET positionid=GREATEST(positionid,$1) WHERE device_uid=$2",
          [r.maxId, d.device_uid]
        );
      }
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