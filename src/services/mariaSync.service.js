import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

// ─── Structured Logger ───────────────────────────────────────────────────────
const log = (level, msg, meta = {}) => {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));
};

// ─── State ────────────────────────────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─── MariaDB Pool ─────────────────────────────────────────────────────────────
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
  connectTimeout: 10000,
  acquireTimeout: 15000,
});

const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 10);

// ─── Advisory Lock ────────────────────────────────────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─── Maria Connection ────────────────────────────────────────────────────────
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await Promise.race([
        mariaPool.getConnection(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("MariaDB timeout")), 12000)
        ),
      ]);
    } catch (err) {
      log("warn", "Maria retry", { attempt: i + 1, error: err.message });
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 1. VEHICLES SYNC
// ──────────────────────────────────────────────────────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConnection();
    log("info", "Maria connected (vehicles)");

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.id AS maria_device_id,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    let vehicles = 0;
    let devices = 0;

    for (const r of rows) {
      const serial = String(r.serial).trim();
      if (!serial) continue;

      const plate = String(r.reg_no || `PLATE_${serial}`).substring(0, 100);

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
          plate,
          `Unit ${serial}`,
          r.vmodel || "",
          r.pstatus || "inactive",
          r.install_date || new Date(),
        ]
      );

      vehicles++;

      if (r.device_uid) {
        await pgPool.query(
          `INSERT INTO devices (device_uid, serial, positionid)
           VALUES ($1,$2,0)
           ON CONFLICT (device_uid) DO UPDATE SET serial = EXCLUDED.serial`,
          [String(r.device_uid), serial]
        );

        await pgPool.query(
          `UPDATE devices
           SET vehicle_id = (SELECT id FROM vehicles WHERE serial = $1 LIMIT 1)
           WHERE device_uid = $2 AND vehicle_id IS NULL`,
          [serial, String(r.device_uid)]
        );

        devices++;
      }
    }

    log("info", "Vehicles synced", { vehicles, devices });

  } finally {
    conn?.release();
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 2. DEVICE TELEMETRY
// ──────────────────────────────────────────────────────────────────────────────
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
    let latestValid = null;

    for (const r of rows) {
      if (r.id > maxId) maxId = r.id;

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      const deviceTime = new Date(r.devicetime || r.servertime);

      const valid =
        Number.isFinite(lat) &&
        Number.isFinite(lon) &&
        lat !== 0 &&
        lon !== 0;

      const item = {
        pgDeviceId,
        lat,
        lon,
        speed: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        deviceTime
      };

      if (valid) latestValid = item;

      inserts.push(item);
    }

    // ─── BULK INSERT (FIXED SAFE VERSION)
    if (inserts.length) {
      const values = [];
      const placeholders = inserts.map((r, i) => {
        const b = i * 6;
        values.push(
          r.pgDeviceId,
          r.lat,
          r.lon,
          r.speed,
          r.heading,
          r.deviceTime
        );
        return `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`;
      }).join(",");

      await pgPool.query(
        `INSERT INTO telemetry
         (device_id, latitude, longitude, speed_kph, heading, device_time)
         VALUES ${placeholders}
         ON CONFLICT DO NOTHING`,
        values
      );
    }

    // ─── latest_positions (ALWAYS SAFE)
    if (latestValid) {
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
          latestValid.pgDeviceId,
          latestValid.lat,
          latestValid.lon,
          latestValid.speed,
          latestValid.heading,
          latestValid.deviceTime
        ]
      );
    }

    return { count: inserts.length, maxId };

  } catch (err) {
    log("error", "Device sync failed", {
      deviceUid: device.device_uid,
      error: err.message
    });

    return { count: 0, maxId: device.positionid || 0 };
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 3. SYNC TELEMETRY
// ──────────────────────────────────────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();
    log("info", "Maria connected (telemetry)");

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices WHERE device_uid IS NOT NULL"
    );

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map(d => syncDeviceTelemetry(d, conn))
      );

      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        const d = chunk[j];

        if (r.status === "fulfilled") {
          total += r.value.count;

          await pgPool.query(
            `UPDATE devices
             SET positionid = GREATEST(positionid, $1)
             WHERE device_uid = $2`,
            [r.value.maxId, d.device_uid]
          );
        }
      }
    }

    log("info", "Sync complete", { total });

  } finally {
    conn?.release();
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// 4. RUNNER
// ──────────────────────────────────────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    await syncVehicles();
    await syncTelemetry();
  } catch (err) {
    log("error", "Sync failed", { error: err.message });
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}