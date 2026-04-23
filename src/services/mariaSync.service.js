import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { publishAlert } from "../queue/publisher.js";

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

// ─── MariaDB Connection with timeout ─────────────────────────────────────────
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const conn = await Promise.race([
        mariaPool.getConnection(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("MariaDB connect timeout")), 12000)
        ),
      ]);
      return conn;
    } catch (err) {
      console.warn(`⚠️ Maria retry ${i + 1}: ${err.message}`);
      if (i === retries - 1) throw err;
      await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

// ─── 1. Sync Vehicles + Devices ───────────────────────────────────────────────
export async function syncVehicles() {
  let conn;
  try {
    conn = await getMariaConnection();
    console.log("🔗 MariaDB connected for vehicle sync");

    const rows = await conn.query(`
      SELECT
        r.serial,
        r.reg_no,
        r.vmodel,
        r.install_date,
        r.pstatus,
        d.id       AS maria_device_id,
        d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
    `);

    let vehicles = 0;
    let devices = 0;

    for (const r of rows) {
      if (!r.serial) continue;

      const serial = String(r.serial).trim();
      const plate =
        (r.reg_no || `PLATE_${serial}`)
          .toString()
          .trim()
          .substring(0, 100);

      await pgPool.query(
        `INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
         VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (serial) DO UPDATE SET
           plate_number = EXCLUDED.plate_number,
           unit_name    = EXCLUDED.unit_name,
           model        = EXCLUDED.model,
           status       = EXCLUDED.status`,
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

    console.log(`✅ Vehicles: ${vehicles} | Devices: ${devices}`);
  } finally {
    conn?.release();
  }
}

// ─── 2. Sync Telemetry for One Device ─────────────────────────────────────────
async function syncDeviceTelemetry(device, conn) {
  try {
    const deviceUid = device.device_uid;
    const lastId = Number(device.positionid) || 0;

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
      `SELECT id, servertime, devicetime, latitude, longitude,
              speed, course, alarmcode
       FROM eventData
       WHERE deviceid = ? AND id > ?
       ORDER BY id ASC LIMIT 5000`,
      [mariaDeviceId, lastId]
    );

    if (!rows.length) {
      console.log(`📭 ${deviceUid}: no new data`);
      return { count: 0, maxId: lastId };
    }

    console.log(`📥 ${deviceUid}: fetched ${rows.length}`);

    let maxId = lastId;
    const mapped = [];
    const validRows = [];

    for (const r of rows) {
      if (Number(r.id) > maxId) maxId = Number(r.id);

      const lat = Number(r.latitude);
      const lon = Number(r.longitude);

      const rawTime = r.devicetime ? new Date(r.devicetime) : null;
      const deviceTime =
        rawTime && rawTime.getFullYear() > 2000
          ? rawTime
          : new Date(r.servertime);

      const item = {
        pgDeviceId,
        deviceUid,
        eventId: Number(r.id),
        deviceTime,
        latitude: lat,
        longitude: lon,
        speedKph: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        alarmcode: r.alarmcode || null,
      };

      const valid =
        Number.isFinite(lat) &&
        Number.isFinite(lon) &&
        lat >= -90 &&
        lat <= 90 &&
        lon >= -180 &&
        lon <= 180 &&
        !(lat === 0 && lon === 0);

      mapped.push(item);
      if (valid) validRows.push(item);
    }

    console.log(`✅ ${deviceUid}: valid ${validRows.length}`);

    // ─── A. Insert telemetry history
    const insertRows = mapped;

    if (insertRows.length) {
      const values = [];
      const placeholders = insertRows.map((r, i) => {
        const b = i * 6;
        values.push(
          r.pgDeviceId,
          r.latitude,
          r.longitude,
          r.speedKph,
          r.heading,
          r.deviceTime
        );
        return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5},$${b + 6})`;
      });

      await pgPool.query(
        `INSERT INTO telemetry
         (device_id, latitude, longitude, speed_kph, heading, device_time)
         VALUES ${placeholders.join(",")}
         ON CONFLICT DO NOTHING`,
        values
      );
    }

    // ─── B. Latest position (ONLY valid)
    if (validRows.length) {
      const latest = validRows.reduce((a, b) =>
        a.deviceTime > b.deviceTime ? a : b
      );

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
           updated_at = NOW()
         WHERE latest_positions.device_time IS NULL
            OR EXCLUDED.device_time > latest_positions.device_time`,
        [
          latest.pgDeviceId,
          latest.latitude,
          latest.longitude,
          latest.speedKph,
          latest.heading,
          latest.deviceTime,
        ]
      );
    } else {
      console.log(`⚠️ ${deviceUid}: no valid coordinates`);
    }

    // ─── C. Alerts
    for (const r of mapped) {
      if (r.alarmcode) {
        try {
          publishAlert(r.deviceUid, {
            deviceId: r.deviceUid,
            type: "alarm",
            severity: "warning",
            message: `Alarm: ${r.alarmcode}`,
          });
        } catch {}
      }
    }

    return { count: mapped.length, maxId };
  } catch (err) {
    console.error(`❌ Device ${device.device_uid}: ${err.message}`);
    return { count: 0, maxId: device.positionid || 0 };
  }
}

// ─── 3. Sync Telemetry ─────────────────────────────────────────────────────────
export async function syncTelemetry() {
  let conn;
  try {
    conn = await getMariaConnection();
    console.log("🔗 MariaDB connected for telemetry sync");

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices WHERE device_uid IS NOT NULL"
    );

    console.log(`🔄 Syncing ${devices.length} devices`);

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map((d) => syncDeviceTelemetry(d, conn))
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

        if (res.status === "rejected") {
          console.error(`❌ ${dev.device_uid}: ${res.reason?.message}`);
        }
      }
    }

    if (total > 0) console.log(`✅ Synced: ${total} rows`);
  } finally {
    conn?.release();
  }
}

// ─── 4. Runner ────────────────────────────────────────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    console.log("🚀 Maria Sync started");
    await syncVehicles();
    await syncTelemetry();
    console.log("✅ Maria Sync complete");
  } catch (err) {
    console.error("❌ Sync error:", err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}