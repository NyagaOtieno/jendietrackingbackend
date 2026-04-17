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
// DEAD LETTER (UNCHANGED BUT SAFE)
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
// 🔥 FIXED: GUARANTEED WRITE (RETRY SAFE)
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
// DEVICE TELEMETRY SYNC (FIXED CORE)
// =========================
async function syncDeviceTelemetry(device, conn) {
  const deviceUid = device.device_uid;

  // 🔥 FIX: this is NOT positionid anymore
  const lastEventId = device.positionid || 0;

  let deviceId = deviceIdCache.get(deviceUid);

  if (!deviceId) {
    const res = await conn.query(
      `SELECT id FROM device WHERE uniqueid = ? LIMIT 1`,
      [deviceUid]
    );

    deviceId = res?.[0]?.id;
    if (!deviceId) return { count: 0, maxId: lastEventId };

    deviceIdCache.set(deviceUid, deviceId);
  }

  // 🔥 FIXED QUERY (eventData.id is the cursor)
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
    [deviceId, lastEventId]
  );

  if (!rows.length) return { count: 0, maxId: lastEventId };

  let maxId = lastEventId;

  const mapped = rows.map(r => {
    if (r.id > maxId) maxId = r.id;

    return {
      deviceId: deviceUid,
      recordedAt: new Date(r.servertime),
      deviceTime: r.devicetime ? new Date(r.devicetime) : null,
      latitude: Number(r.latitude) || 0,
      longitude: Number(r.longitude) || 0,
      speed: Number(r.speed) || 0,
      heading: Number(r.course) || 0,
      alarmcode: r.alarmcode,
      eventId: r.id
    };
  });

  // =========================
  // 1. PRIMARY DB WRITE
  // =========================
  await persistTelemetry(mapped, deviceUid);

  // =========================
  // 2. BUFFER (REAL ZERO LOSS FIX)
  // =========================
  await pgPool.query(
    `
    INSERT INTO telemetry_ingestion_buffer
    (device_uid, device_id, positionid, payload, status)
    SELECT $1,$2,$3,$4,'PENDING'
    WHERE NOT EXISTS (
      SELECT 1 FROM telemetry_ingestion_buffer
      WHERE device_uid = $1 AND positionid = $3
    )
    `,
    [
      deviceUid,
      deviceId,
      maxId,
      JSON.stringify(mapped)
    ]
  );

  // =========================
  // 3. STREAM
  // =========================
  try {
    publishTelemetryBatch(mapped);
  } catch {}

  return { count: mapped.length, maxId };
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

      for (let j = 0; j < results.length; j++) {
        const res = results[j];
        const dev = chunk[j];

        if (res.status === 'fulfilled') {
          total += res.value.count;

          await pgPool.query(
            `
            UPDATE devices
            SET positionid = GREATEST(positionid, $1)
            WHERE device_uid = $2
            `,
            [res.value.maxId, dev.device_uid]
          );
        }
      }
    }

  } finally {
    conn?.release();
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