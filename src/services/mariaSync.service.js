import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─────────────────────────────────────────────
// MARIA DB POOL
// ─────────────────────────────────────────────
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 5,
  connectTimeout: 12000,
  acquireTimeout: 15000,
});

const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 10);

// ─────────────────────────────────────────────
// POSTGRES LOCK
// ─────────────────────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0]?.locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─────────────────────────────────────────────
// MARIA CONNECTION
// ─────────────────────────────────────────────
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn(`Maria retry ${i + 1}: ${err.message}`);
      if (i === retries - 1) throw err;
      await new Promise((r) => setTimeout(r, 1500));
    }
  }
}

// ─────────────────────────────────────────────
// VEHICLE SYNC
// ─────────────────────────────────────────────
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

    let vehicles = 0;
    let devices = 0;

    for (const r of rows) {
      const serial = String(r.serial).trim();
      const plate = (r.reg_no || `PLATE_${serial}`).substring(0, 100);

      await pgPool.query(
        `
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial) DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status
        `,
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
          `
          INSERT INTO devices (device_uid, serial, positionid)
          VALUES ($1,$2,0)
          ON CONFLICT (device_uid) DO UPDATE SET serial = EXCLUDED.serial
          `,
          [String(r.device_uid), serial]
        );

        devices++;
      }
    }

    console.log(`Vehicles synced: ${vehicles}, Devices: ${devices}`);
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// DEVICE TELEMETRY SYNC
// ─────────────────────────────────────────────
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
      `
      SELECT id, servertime, devicetime, latitude, longitude,
             speed, course, alarmcode
      FROM eventData
      WHERE deviceid = ? AND id > ?
      ORDER BY id ASC LIMIT 2000
      `,
      [mariaDeviceId, lastId]
    );

    if (!rows.length) return { count: 0, maxId: lastId };

    let maxId = lastId;
    const values = [];
    const placeholders = [];

    const validRows = [];

    for (const r of rows) {
      if (r.id > maxId) maxId = r.id;

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      if (!lat || !lon) continue;

      const deviceTime =
        r.devicetime && new Date(r.devicetime).getFullYear() > 2000
          ? new Date(r.devicetime)
          : new Date(r.servertime);

      validRows.push({
        pgDeviceId,
        deviceUid,
        lat,
        lon,
        speed: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        deviceTime,
        alarmcode: r.alarmcode,
      });
    }

    if (validRows.length === 0) {
      return { count: 0, maxId };
    }

    validRows.forEach((r, i) => {
      const b = i * 6;

      values.push(
        r.pgDeviceId,
        r.lat,
        r.lon,
        r.speed,
        r.heading,
        r.deviceTime
      );

      placeholders.push(
        `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`
      );
    });

    await pgPool.query(
      `
      INSERT INTO telemetry
      (device_id, latitude, longitude, speed_kph, heading, device_time)
      VALUES ${placeholders.join(",")}
      ON CONFLICT DO NOTHING
      `,
      values
    );

    // ✅ FIXED: ONLY ONE latest declaration (NO DUPLICATES)
    const latest = validRows[validRows.length - 1];

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
        latest.pgDeviceId,
        latest.lat,
        latest.lon,
        latest.speed,
        latest.heading,
        latest.deviceTime,
      ]
    );

    return { count: validRows.length, maxId };
  } catch (err) {
    console.error(`Device error ${device.device_uid}:`, err.message);
    return { count: 0, maxId: device.positionid || 0 };
  }
}

// ─────────────────────────────────────────────
// ORCHESTRATOR
// ─────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices"
    );

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map((d) => syncDeviceTelemetry(d, conn))
      );

      for (let j = 0; j < results.length; j++) {
        if (results[j].status === "fulfilled") {
          const res = results[j].value;

          total += res.count;

          await pgPool.query(
            `
            UPDATE devices
            SET positionid = GREATEST(positionid, $1)
            WHERE device_uid = $2
            `,
            [res.maxId, chunk[j].device_uid]
          );
        }
      }
    }

    await pgPool.query(`
      DELETE FROM telemetry
      WHERE device_time < NOW() - INTERVAL '72 hours'
    `);

    console.log(`Synced telemetry: ${total}`);
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────────────────────
// RUNNER
// ─────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    console.log("Maria sync started");
    await syncVehicles();
    await syncTelemetry();
    console.log("Maria sync complete");
  } catch (err) {
    console.error("Sync failed:", err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}