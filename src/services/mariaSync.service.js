import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';
import { setLatestPosition } from '../services/cache.service.js';
import { getIO } from '../socket/server.js';

// =========================
// GLOBAL LOCK
// =========================
let isSyncRunning = false;
export { isSyncRunning };

// =========================
// MARIA POOL
// =========================
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || 'uradi',
  connectionLimit: 30,
  acquireTimeout: 30000,
});

// =========================
// CONFIG
// =========================
const INSERT_BATCH = Number(process.env.INSERT_BATCH || 200);
const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 25);

// =========================
// CACHE
// =========================
const deviceIdCache = new Map();

// =========================
// LOCK
// =========================
async function acquireLock() {
  const res = await pgPool.query(
    `SELECT pg_try_advisory_lock(778899) AS locked`
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query(`SELECT pg_advisory_unlock(778899)`);
}

// =========================
// MARIA CONNECTION
// =========================
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn(`⚠️ Maria retry ${i + 1}`, err.code);
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1500));
    }
  }
}

// =========================
// VEHICLES SYNC (UNCHANGED)
// =========================
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConnection();

    const rows = await conn.query(`
      SELECT serial, reg_no, vmodel, install_date, pstatus
      FROM registration
    `);

    for (const r of rows) {
      if (!r.serial) continue;

      const serialKey = String(r.serial);
      const plate = (r.reg_no || `PLATE_${serialKey}`)
        .toString()
        .trim()
        .substring(0, 100);

      await pgPool.query(
        `INSERT INTO vehicles
        (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial)
        DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status`,
        [
          serialKey,
          plate,
          `Unit ${serialKey}`,
          r.vmodel || '',
          r.pstatus || 'inactive',
          r.install_date || new Date(),
        ]
      );
    }

    console.log(`✅ Vehicles synced: ${rows.length}`);
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// DEVICES SYNC
// =========================
export async function syncDevices() {
  let conn;

  try {
    conn = await getMariaConnection();

    const rows = await conn.query(`
      SELECT id, uniqueid, positionid
      FROM device
    `);

    for (const r of rows) {
      if (!r.uniqueid) continue;

      deviceIdCache.set(r.uniqueid, r.id);

      await pgPool.query(
        `INSERT INTO devices (device_uid, serial, positionid)
         VALUES ($1,$2,$3)
         ON CONFLICT (device_uid)
         DO UPDATE SET
           serial = EXCLUDED.serial,
           positionid = EXCLUDED.positionid`,
        [r.uniqueid, r.uniqueid, r.positionid || 0]
      );
    }

    console.log(`✅ Devices synced: ${rows.length}`);
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// 🔥 FIXED: SAFE TELEMETRY WRITE
// =========================
async function persistTelemetryBatch(batch) {
  if (!batch.length) return;

  const values = [];
  const placeholders = [];
  let i = 1;

  for (const r of batch) {
    placeholders.push(
      `($${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`
    );

    values.push(
      r.deviceId,
      r.receivedAt,
      r.deviceTime,
      r.latitude,
      r.longitude,
      r.speedKph,
      r.heading,
      r.alarmcode
    );
  }

  const query = `
    INSERT INTO telemetry (
      device_id,
      recorded_at,
      device_time,
      latitude,
      longitude,
      speed,
      heading,
      alarmcode
    )
    VALUES ${placeholders.join(',')}
    ON CONFLICT (device_id, recorded_at)
    DO NOTHING
  `;

  await pgPool.query(query, values);
}

// =========================
// BUFFER (ZERO LOSS SAFETY NET)
// =========================
async function persistBuffer(deviceId, deviceUid, rows) {
  if (!rows.length) return;

  for (const r of rows) {
    await pgPool.query(
      `
      INSERT INTO telemetry_ingestion_buffer
      (device_uid, device_id, positionid, payload, status)
      VALUES ($1,$2,$3,$4,'PENDING')
      ON CONFLICT DO NOTHING
      `,
      [
        deviceUid,
        deviceId,
        r.positionid,
        JSON.stringify(r)
      ]
    );
  }
}

// =========================
// DEVICE SYNC CORE
// =========================
async function syncDeviceTelemetry(device, conn) {
  try {
    const deviceUid = device.device_uid;
    const lastPositionId = device.positionid || 0;

    let deviceId = deviceIdCache.get(deviceUid);

    if (!deviceId) {
      const res = await conn.query(
        `SELECT id FROM device WHERE uniqueid = ? LIMIT 1`,
        [deviceUid]
      );

      deviceId = res?.[0]?.id;
      if (deviceId) deviceIdCache.set(deviceUid, deviceId);
    }

    if (!deviceId) return { count: 0, maxPositionId: lastPositionId };

    const rows = await conn.query(
      `
      SELECT positionid, servertime, devicetime,
             latitude, longitude, speed, course, alarmcode
      FROM eventData
      WHERE deviceid = ?
      AND positionid > ?
      ORDER BY positionid ASC
      LIMIT 5000
      `,
      [deviceId, lastPositionId]
    );

    if (!rows.length) return { count: 0, maxPositionId: lastPositionId };

    let maxPositionId = lastPositionId;

    const mapped = rows.map(r => {
      if (r.positionid > maxPositionId) maxPositionId = r.positionid;

      return {
        deviceId: deviceUid,
        receivedAt: new Date(r.servertime),
        deviceTime: r.devicetime ? new Date(r.devicetime) : null,
        latitude: Number(r.latitude) || 0,
        longitude: Number(r.longitude) || 0,
        speedKph: Number(r.speed) || 0,
        heading: Number(r.course) || 0,
        alarmcode: r.alarmcode || null,
        positionid: r.positionid
      };
    });

    // 1. GUARANTEED WRITE
    try {
      await persistTelemetryBatch(mapped);
    } catch (err) {
      console.error('PRIMARY WRITE FAILED → BUFFERING', err.message);
      await persistBuffer(deviceId, deviceUid, mapped);
    }

    // 2. QUEUE
    try {
      publishTelemetryBatch(mapped);
    } catch {}

    // 3. REALTIME
    let io;
    try { io = getIO(); } catch { io = null; }

    for (const r of mapped) {
      await setLatestPosition(deviceUid, r);

      if (io) io.emit('vehicle:update', { deviceId: deviceUid, ...r });

      if (r.alarmcode) {
        const alert = {
          deviceId: deviceUid,
          type: 'alarm',
          severity: 'warning',
          message: `Alarm code: ${r.alarmcode}`,
        };

        try { publishAlert(deviceUid, alert); } catch {}
        if (io) io.emit('alert', alert);
      }
    }

    return { count: mapped.length, maxPositionId };

  } catch (err) {
    console.error(`❌ Device ${device.device_uid} error:`, err.message);
    return { count: 0, maxPositionId: device.positionid || 0 };
  }
}

// =========================
// ORCHESTRATOR
// =========================
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const { rows: devices } = await pgPool.query(`
      SELECT device_uid, positionid FROM devices
    `);

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map(d => syncDeviceTelemetry(d, conn))
      );

      for (let i = 0; i < results.length; i++) {
        const res = results[i];
        const dev = chunk[i];

        if (res.status === 'fulfilled' && res.value.count > 0) {
          total += res.value.count;

          await pgPool.query(
            `UPDATE devices
             SET positionid = GREATEST(positionid, $1)
             WHERE device_uid = $2`,
            [res.value.maxPositionId, dev.device_uid]
          );
        }
      }
    }

    console.log(`✅ Telemetry sync complete: ${total}`);

  } finally {
    if (conn) conn.release();
  }
}

// =========================
// RUN
// =========================
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    await syncVehicles();
    await syncDevices();
    await syncTelemetry();
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}