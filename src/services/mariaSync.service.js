import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';
import { setLatestPosition } from '../services/cache.service.js';
import { getIO } from '../socket/server.js';

// =========================
// GLOBAL STATE
// =========================
let isSyncRunning = false;
export { isSyncRunning };

// =========================
// MARIA DB POOL
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
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 1500));
    }
  }
}

// =========================
// VEHICLES SYNC
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

      const serial = String(r.serial);

      await pgPool.query(
        `
        INSERT INTO vehicles
        (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial) DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status
        `,
        [
          serial,
          (r.reg_no || `PLATE_${serial}`).substring(0, 100),
          `Unit ${serial}`,
          r.vmodel || '',
          r.pstatus || 'inactive',
          r.install_date || new Date(),
        ]
      );
    }
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
        `
        INSERT INTO devices (device_uid, serial, positionid)
        VALUES ($1,$2,$3)
        ON CONFLICT (device_uid) DO UPDATE SET
          serial = EXCLUDED.serial,
          positionid = EXCLUDED.positionid
        `,
        [r.uniqueid, r.uniqueid, r.positionid || 0]
      );
    }
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// SAFE TELEMETRY INSERT
// =========================
async function persistTelemetry(batch) {
  if (!batch.length) return;

  const values = [];
  const placeholders = [];
  let i = 1;

  for (const r of batch) {
    placeholders.push(
      `($${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++})`
    );

    values.push(
      r.deviceId,
      r.recordedAt,
      r.latitude,
      r.longitude,
      r.speed,
      r.heading,
      r.alarmcode
    );
  }

  const query = `
    INSERT INTO telemetry (
      device_id,
      recorded_at,
      latitude,
      longitude,
      speed,
      heading,
      alarmcode
    )
    VALUES ${placeholders.join(',')}
    ON CONFLICT (device_id, recorded_at) DO NOTHING
  `;

  await pgPool.query(query, values);
}

// =========================
// BUFFER (ZERO LOSS SAFETY NET)
// =========================
async function persistBuffer(deviceId, deviceUid, rows) {
  for (const r of rows) {
    await pgPool.query(
      `
      INSERT INTO telemetry_ingestion_buffer
      (device_uid, device_id, positionid, payload, status)
      VALUES ($1,$2,$3,$4,'PENDING')
      `,
      [
        deviceUid,
        deviceId,
        r.positionid || 0,
        JSON.stringify(r)
      ]
    );
  }
}

// =========================
// DEVICE TELEMETRY SYNC (FIXED)
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

    // 🔥 FIX: NO ASSUMPTION ABOUT eventData POSITION COLUMN
    const rows = await conn.query(
      `
      SELECT id, servertime, devicetime,
             latitude, longitude, speed, course, alarmcode
      FROM eventData
      WHERE deviceid = ?
      ORDER BY id ASC
      LIMIT 5000
      `,
      [deviceId]
    );

    let maxId = 0;

    const mapped = rows.map(r => {
      if (r.id > maxId) maxId = r.id;

      return {
        deviceId: deviceUid,
        recordedAt: new Date(r.servertime),
        latitude: r.latitude,
        longitude: r.longitude,
        speed: r.speed,
        heading: r.course,
        alarmcode: r.alarmcode,
        positionid: r.id
      };
    });

    // PRIMARY WRITE
    try {
      await persistTelemetry(mapped);
    } catch (err) {
      await persistBuffer(deviceId, deviceUid, mapped);
    }

    return { count: mapped.length, maxPositionId: maxId };

  } catch (err) {
    return { count: 0, maxPositionId: 0 };
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

    for (const d of devices) {
      await syncDeviceTelemetry(d, conn);
    }

  } finally {
    if (conn) conn.release();
  }
}

// =========================
// RUNNER
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