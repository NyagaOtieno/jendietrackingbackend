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
const BATCH_SIZE = 200;
const DEVICE_CONCURRENCY = 25;

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
async function getMariaConnection() {
  return mariaPool.getConnection();
}

// =========================
// VEHICLES SYNC (UNCHANGED SAFE)
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

      await pgPool.query(
        `
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial)
        DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          model = EXCLUDED.model,
          status = EXCLUDED.status
        `,
        [
          r.serial,
          r.reg_no || `PLATE_${r.serial}`,
          `Unit ${r.serial}`,
          r.vmodel || '',
          r.pstatus || 'inactive',
          r.install_date || new Date(),
        ]
      );
    }

  } finally {
    conn?.release();
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
      SELECT id, uniqueid FROM device
    `);

    for (const r of rows) {
      deviceIdCache.set(r.uniqueid, r.id);

      await pgPool.query(
        `
        INSERT INTO devices (device_uid, serial)
        VALUES ($1,$2)
        ON CONFLICT (device_uid)
        DO UPDATE SET serial = EXCLUDED.serial
        `,
        [r.uniqueid, r.uniqueid]
      );
    }

  } finally {
    conn?.release();
  }
}

// =========================
// DEAD LETTER TABLE WRITE
// =========================
async function writeDeadLetter(deviceUid, error, batch) {
  await pgPool.query(
    `
    INSERT INTO telemetry_dead_letter (device_uid, error, payload, created_at)
    VALUES ($1,$2,$3,NOW())
    `,
    [deviceUid, error, JSON.stringify(batch)]
  );
}

// =========================
// GUARANTEED WRITE
// =========================
async function persistTelemetry(batch, deviceUid) {
  if (!batch.length) return;

  const values = [];
  const placeholders = [];
  let i = 1;

  for (const r of batch) {
    placeholders.push(
      `($${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++})`
    );

    values.push(
      r.deviceId,
      r.recordedAt,
      r.deviceTime,
      r.latitude,
      r.longitude,
      r.speed,
      r.heading,
      r.eventId
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
      external_event_id
    )
    VALUES ${placeholders.join(',')}
    ON CONFLICT (device_id, external_event_id)
    DO NOTHING
  `;

  try {
    await pgPool.query(query, values);
  } catch (err) {
    await writeDeadLetter(deviceUid, err.message, batch);
    throw err;
  }
}

// =========================
// DEVICE TELEMETRY SYNC (FIXED)
// =========================
async function syncDeviceTelemetry(device, conn) {
  const deviceUid = device.device_uid;
  const lastId = device.positionid || 0;

  // Get Maria device id
  const devRes = await conn.query(
    `SELECT id FROM device WHERE uniqueid = ? LIMIT 1`,
    [deviceUid]
  );

  const mariaDeviceId = devRes?.[0]?.id;
  if (!mariaDeviceId) return { count: 0, maxId: lastId };

  // ✅ FIXED QUERY (NO positionid EXISTS)
  const rows = await conn.query(
    `
    SELECT id, servertime, devicetime,
           latitude, longitude, speed, course, alarmcode
    FROM eventData
    WHERE deviceid = ?
    AND id > ?
    ORDER BY id ASC
    LIMIT 5000
    `,
    [mariaDeviceId, lastId]
  );

  if (!rows.length) return { count: 0, maxId: lastId };

  let maxId = lastId;

  const mapped = rows.map(r => {
    if (r.id > maxId) maxId = r.id;

    return {
      deviceId: deviceUid,
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

  // Insert telemetry
  if (mapped.length) {
    const values = [];
    const placeholders = mapped.map((r, i) => {
      const b = i * 7;

      values.push(
        r.deviceId,
        r.receivedAt,
        r.deviceTime,
        r.latitude,
        r.longitude,
        r.speedKph,
        r.heading
      );

      return `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7})`;
    });

    await pgPool.query(
      `
      INSERT INTO telemetry (
        device_uid,
        recorded_at,
        device_time,
        latitude,
        longitude,
        speed_kph,
        heading
      )
      VALUES ${placeholders.join(',')}
      ON CONFLICT DO NOTHING
      `,
      values
    );
  }

  return { count: mapped.length, maxId };
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