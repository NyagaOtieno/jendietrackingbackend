import dotenv from "dotenv";
dotenv.config();

import mariadb from "mariadb";
import { pgPool } from "../config/db.js";

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) => {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));
};

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─────────────────────────────────────────────
// MARIA DB POOL
// ─────────────────────────────────────────────
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
  connectTimeout: 15000,
});

// ─────────────────────────────────────────────
// CONFIG (TUNED FOR STABILITY)
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
  if (typeof v === "bigint") return Number(v);
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
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
      LIMIT 5000
    `);

    let vehicles = 0;

    for (const r of rows) {
      const serial = String(r.serial).trim();
      if (!serial) continue;

      const plate = (r.reg_no || serial).toString().trim();

      await pgPool.query(
        `INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
         VALUES ($1,$2,$3,$4,$5,NOW())
         ON CONFLICT (serial) DO UPDATE SET
           plate_number=EXCLUDED.plate_number,
           unit_name=EXCLUDED.unit_name,
           model=EXCLUDED.model,
           status=EXCLUDED.status`,
        [serial, plate, `Unit ${serial}`, r.vmodel || "", r.pstatus || "inactive"]
      );

      if (r.device_uid) {
        await pgPool.query(
          `INSERT INTO devices (device_uid, serial, positionid)
           VALUES ($1,$2,COALESCE((SELECT positionid FROM devices WHERE device_uid=$1),0))
           ON CONFLICT (device_uid) DO NOTHING`,
          [String(r.device_uid), serial]
        );
      }

      vehicles++;
    }

    log("info", "Vehicle sync complete", { vehicles });

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

  // Maria device ID
  const dev = await conn.query(
    "SELECT id FROM device WHERE uniqueid=? LIMIT 1",
    [deviceUid]
  );

  if (!dev.length) return { count: 0, maxId: lastId };

  const mariaDeviceId = N(dev[0].id);

  // PG device ID
  const pg = await pgPool.query(
    "SELECT id FROM devices WHERE device_uid=$1 LIMIT 1",
    [deviceUid]
  );

  if (!pg.rows.length) return { count: 0, maxId: lastId };

  const pgDeviceId = pg.rows[0].id;

  // 🔥 ONLY RECENT DATA (prevents overload)
  const rows = await conn.query(
    `SELECT id, devicetime, servertime, latitude, longitude, speed, course
     FROM eventData
     WHERE deviceid=? 
       AND id > ?
       AND servertime >= NOW() - INTERVAL 15 MINUTE
     ORDER BY id ASC
     LIMIT ?`,
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

    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat === 0 || lon === 0) continue;

    const dt = r.devicetime
      ? new Date(r.devicetime)
      : new Date(r.servertime);

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

  if (!valid.length) return { count: 0, maxId };

  // ── INSERT TELEMETRY
  const values = [];
  const placeholders = valid.map((v, i) => {
    const base = i * 7;
    values.push(v.pgDeviceId, v.id, v.lat, v.lon, v.speed, v.heading, v.dt);
    return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7})`;
  });

  await pgPool.query(
    `INSERT INTO telemetry
     (device_id, event_id, latitude, longitude, speed_kph, heading, device_time)
     VALUES ${placeholders.join(",")}
     ON CONFLICT (device_id, event_id) DO NOTHING`,
    values
  );

  // ── UPDATE LATEST POSITION
  const latest = valid.reduce((a, b) => (a.dt > b.dt ? a : b));

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
          "UPDATE devices SET positionid = GREATEST(positionid,$1) WHERE device_uid=$2",
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
// MAIN RUNNER
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