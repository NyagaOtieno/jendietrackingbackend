import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';
import { setLatestPosition } from '../services/cache.service.js';
import { getIO } from '../socket/server.js';

let isSyncRunning = false;
export { isSyncRunning };

const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || 'uradi',
  connectionLimit: 30,
  acquireTimeout: 30000,
});

const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 25);

async function acquireLock() {
  const res = await pgPool.query("SELECT pg_try_advisory_lock(778899) AS locked");
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn("⚠️ Maria retry", i + 1, err.code);
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1500));
    }
  }
}

// =====================
// 1. VEHICLES + DEVICES
// Chain: registration.serial → CONCAT(0, serial) = device.uniqueid → device.id
// =====================
export async function syncVehicles() {
  let conn;
  try {
    conn = await getMariaConnection();

    // ✅ Join registration → device in ONE query
    const rows = await conn.query(`
      SELECT
        r.serial,
        r.reg_no,
        r.vmodel,
        r.install_date,
        r.pstatus,
        d.id        AS device_id,
        d.uniqueid  AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
    `);

    let vehicleCount = 0;
    let deviceCount = 0;

    for (const r of rows) {
      if (!r.serial) continue;

      const serial = String(r.serial).trim();
      const plate = (r.reg_no || "PLATE_" + serial).toString().trim().substring(0, 100);

      // Upsert vehicle
      await pgPool.query(
        "INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (serial) DO UPDATE SET plate_number = EXCLUDED.plate_number, unit_name = EXCLUDED.unit_name, model = EXCLUDED.model, status = EXCLUDED.status",
        [serial, plate, "Unit " + serial, r.vmodel || "", r.pstatus || "inactive", r.install_date || new Date()]
      );
      vehicleCount++;

      // Upsert device only if device exists in MariaDB
      if (r.device_uid) {
        await pgPool.query(
          "INSERT INTO devices (device_uid, serial, positionid) VALUES ($1,$2,0) ON CONFLICT (device_uid) DO UPDATE SET serial = EXCLUDED.serial",
          [String(r.device_uid), serial]
        );
        deviceCount++;
      }
    }

    console.log("✅ Vehicles synced:", vehicleCount, "| Devices linked:", deviceCount);
  } finally {
    conn?.release();
  }
}

// =====================
// 2. TELEMETRY PER DEVICE
// Chain: device.uniqueid → device.id (maria) → eventData.deviceid
// =====================
async function syncDeviceTelemetry(device, conn) {
  try {
    const deviceUid = device.device_uid;
    const lastId = Number(device.positionid) || 0;

    // Get maria device int id
    const devRes = await conn.query(
      "SELECT id FROM device WHERE uniqueid = ? LIMIT 1",
      [deviceUid]
    );
    if (!devRes.length) return { count: 0, maxId: lastId };
    const mariaDeviceId = devRes[0].id;

    // Get PG integer device id
    const pgRes = await pgPool.query(
      "SELECT id FROM devices WHERE device_uid = $1 LIMIT 1",
      [deviceUid]
    );
    if (!pgRes.rows.length) return { count: 0, maxId: lastId };
    const pgDeviceId = pgRes.rows[0].id;

    // ✅ Query eventData using device.id (correct FK — NOT positionid)
    const rows = await conn.query(
      "SELECT id, servertime, devicetime, latitude, longitude, speed, course, alarmcode FROM eventData WHERE deviceid = ? AND id > ? ORDER BY id ASC LIMIT 5000",
      [mariaDeviceId, lastId]
    );

    if (!rows.length) return { count: 0, maxId: lastId };

    let maxId = lastId;
    const mapped = rows.map(r => {
      if (r.id > maxId) maxId = r.id;
      return {
        pgDeviceId,
        deviceUid,
        receivedAt: new Date(r.servertime),
        deviceTime: r.devicetime ? new Date(r.devicetime) : null,
        latitude: Number(r.latitude) || 0,
        longitude: Number(r.longitude) || 0,
        speedKph: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        alarmcode: r.alarmcode || null,
        eventId: r.id,
      };
    });

    // Insert into telemetry
    if (mapped.length) {
      const values = [];
      const placeholders = mapped.map((r, i) => {
        const b = i * 6;
        values.push(r.pgDeviceId, r.latitude, r.longitude, r.speedKph, r.heading, r.deviceTime);
        return "($" + (b+1) + ",$" + (b+2) + ",$" + (b+3) + ",$" + (b+4) + ",$" + (b+5) + ",$" + (b+6) + ")";
      });
      await pgPool.query(
        "INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, heading, device_time) VALUES " + placeholders.join(",") + " ON CONFLICT DO NOTHING",
        values
      );
    }

    try { publishTelemetryBatch(mapped); } catch {}

    let io;
    try { io = getIO(); } catch { io = null; }

    for (const r of mapped) {
      try { await setLatestPosition(r.deviceUid, r); } catch {}
      if (io) io.emit("vehicle:update", { deviceId: r.deviceUid, ...r });
      if (r.alarmcode) {
        const alert = { deviceId: r.deviceUid, type: "alarm", severity: "warning", message: "Alarm: " + r.alarmcode };
        try { publishAlert(r.deviceUid, alert); } catch {}
        if (io) io.emit("alert", alert);
      }
    }

    return { count: mapped.length, maxId };
  } catch (err) {
    console.error("❌ Device", device.device_uid, "error:", err.message);
    return { count: 0, maxId: device.positionid || 0 };
  }
}

// =====================
// 3. TELEMETRY ORCHESTRATOR
// =====================
export async function syncTelemetry() {
  let conn;
  try {
    conn = await getMariaConnection();
    const { rows: devices } = await pgPool.query("SELECT device_uid, positionid FROM devices WHERE device_uid IS NOT NULL");
    console.log("🔄 Syncing telemetry for", devices.length, "devices");
    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);
      const results = await Promise.allSettled(chunk.map(d => syncDeviceTelemetry(d, conn)));

      for (let j = 0; j < results.length; j++) {
        const res = results[j];
        const dev = chunk[j];
        if (res.status === "fulfilled" && res.value.count > 0) {
          total += res.value.count;
          await pgPool.query(
            "UPDATE devices SET positionid = GREATEST(positionid, $1) WHERE device_uid = $2",
            [res.value.maxId, dev.device_uid]
          );
          console.log("📦", dev.device_uid, "→", res.value.count, "rows");
        }
        if (res.status === "rejected") console.error("❌", dev.device_uid, ":", res.reason?.message);
      }
    }
    console.log("✅ Telemetry sync complete:", total, "rows");
  } finally {
    conn?.release();
  }
}

// =====================
// 4. RUNNER
// =====================
export async function runMariaSync() {
  if (isSyncRunning) return;
  const locked = await acquireLock();
  if (!locked) return;
  isSyncRunning = true;
  try {
    console.log("🚀 Maria Sync started");
    await syncVehicles();
    await syncTelemetry();
    console.log("✅ Maria Sync completed");
  } catch (err) {
    console.error("❌ Sync failed:", err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}
