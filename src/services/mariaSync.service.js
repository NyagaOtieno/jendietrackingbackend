import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

// ─── State ───────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─── MariaDB Pool ────────────────────────────────────────
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

// ─── Lock ────────────────────────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─── Connection helper ────────────────────────────────────
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// ─────────────────────────────────────────────────────────
// 1. SYNC VEHICLES + DEVICE MAP (STRICT LINK)
// ─────────────────────────────────────────────────────────
export async function syncVehicles() {
  let conn;
  try {
    conn = await getMariaConnection();

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.install_date, r.pstatus,
             d.id AS device_id, d.uniqueid
      FROM registration r
      LEFT JOIN device d
        ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    for (const r of rows) {
      const serial = String(r.serial).trim();
      const uid = r.uniqueid;

      if (!uid) continue;

      // PG vehicle sync
      await pgPool.query(
        `INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (serial) DO UPDATE SET
           plate_number = EXCLUDED.plate_number,
           model = EXCLUDED.model,
           status = EXCLUDED.status`,
        [
          serial,
          r.reg_no || serial,
          `Unit ${serial}`,
          r.vmodel || "",
          r.pstatus || "inactive",
          r.install_date || new Date()
        ]
      );

      // STRICT device sync (ONLY IF MATCHED)
      await pgPool.query(
        `INSERT INTO devices (device_uid, serial, positionid)
         VALUES ($1,$2,0)
         ON CONFLICT (device_uid) DO UPDATE SET serial = EXCLUDED.serial`,
        [uid, serial]
      );
    }

    console.log(`✅ Vehicle sync complete`);
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────────────────
// 2. DEVICE CACHE (IMPORTANT OPTIMIZATION)
// ─────────────────────────────────────────────────────────
async function loadDeviceMap(conn) {
  const rows = await conn.query(`
    SELECT id, uniqueid
    FROM device
  `);

  const map = new Map();
  for (const r of rows) {
    map.set(r.uniqueid, r.id); // uniqueid → device.id
  }
  return map;
}

// ─────────────────────────────────────────────────────────
// 3. TELEMETRY SYNC (STRICT PROTOCOL ENFORCED)
// ─────────────────────────────────────────────────────────
async function syncDeviceTelemetry(device, conn, deviceMap) {
  try {
    const deviceUid = device.device_uid;
    const lastId = Number(device.positionid || 0);

    const mariaDeviceId = deviceMap.get(deviceUid);

    if (!mariaDeviceId) return { count: 0, maxId: lastId };

    const rows = await conn.query(
      `SELECT id, servertime, devicetime, latitude, longitude,
              speed, course, alarmcode
       FROM eventData
       WHERE deviceid = ? AND id > ?
       ORDER BY id ASC
       LIMIT 5000`,
      [mariaDeviceId, lastId]
    );

    if (!rows.length) return { count: 0, maxId: lastId };

    let maxId = lastId;

    const mapped = rows.map(r => {
      if (r.id > maxId) maxId = r.id;

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      const time =
        r.devicetime && new Date(r.devicetime).getFullYear() > 2000
          ? new Date(r.devicetime)
          : new Date(r.servertime);

      return {
        deviceUid,
        deviceId: mariaDeviceId,
        eventId: r.id,
        lat,
        lon,
        speed: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        time,
        alarmcode: r.alarmcode
      };
    });

    // ─── insert telemetry ───
    for (const r of mapped) {
      await pgPool.query(
        `INSERT INTO telemetry
         (device_id, latitude, longitude, speed_kph, heading, device_time)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT DO NOTHING`,
        [r.deviceId, r.lat, r.lon, r.speed, r.heading, r.time]
      );

      // latest position
      await pgPool.query(
        `INSERT INTO latest_positions
         (device_id, latitude, longitude, speed_kph, heading, device_time, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,NOW())
         ON CONFLICT (device_id) DO UPDATE SET
           latitude = EXCLUDED.latitude,
           longitude = EXCLUDED.longitude,
           speed_kph = EXCLUDED.speed_kph,
           heading = EXCLUDED.heading,
           device_time = EXCLUDED.device_time`,
        [r.deviceId, r.lat, r.lon, r.speed, r.heading, r.time]
      );

      // alerts
      if (r.alarmcode) {
        publishAlert(r.deviceUid, {
          type: "alarm",
          message: r.alarmcode,
        });
      }
    }

    return { count: mapped.length, maxId };
  } catch (err) {
    console.error("Telemetry error:", err.message);
    return { count: 0, maxId: device.positionid || 0 };
  }
}

// ─────────────────────────────────────────────────────────
// 4. MAIN SYNC LOOP
// ─────────────────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const deviceMap = await loadDeviceMap(conn);

    const { rows: devices } = await pgPool.query(`
      SELECT device_uid, positionid
      FROM devices
      WHERE device_uid IS NOT NULL
    `);

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map(d =>
          syncDeviceTelemetry(d, conn, deviceMap)
        )
      );

      for (let j = 0; j < results.length; j++) {
        const res = results[j];
        const dev = chunk[j];

        if (res.status === "fulfilled" && res.value.count > 0) {
          total += res.value.count;

          await pgPool.query(
            `UPDATE devices
             SET positionid = GREATEST(positionid, $1)
             WHERE device_uid = $2`,
            [res.value.maxId, dev.device_uid]
          );
        }
      }
    }

    console.log(`✅ Telemetry synced: ${total}`);
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────────────────
// 5. RUNNER
// ─────────────────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    console.log("🚀 MariaSync started");

    await syncVehicles();
    await syncTelemetry();

    console.log("✅ MariaSync complete");
  } catch (err) {
    console.error("❌ Sync error:", err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}