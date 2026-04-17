// src/services/mariaSync.service.js
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
// MARIA DB POOL
// =========================
const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || 'uradi',
  connectionLimit: 20,
  acquireTimeout: 30000,
});

// =========================
// CONFIG
// =========================
const INSERT_BATCH = Number(process.env.INSERT_BATCH || 200);
const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 20);

// =========================
// DB BULK INSERT (NEW 🔥)
// =========================
async function insertTelemetryBatch(batch) {
  if (!batch.length) return;

  const values = [];
  const placeholders = [];

  batch.forEach((r, i) => {
    const base = i * 8;

    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8})`
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
  });

  try {
    await pgPool.query(`
      INSERT INTO telemetry
      (device_id, received_at, device_time, latitude, longitude, speed_kph, heading, alarmcode)
      VALUES ${placeholders.join(',')}
      ON CONFLICT DO NOTHING
    `, values);
  } catch (err) {
    console.error('❌ Bulk insert failed:', err.message);
  }
}

// =========================
// DISTRIBUTED LOCK
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
// CONNECTION
// =========================
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn(`⚠️ Maria retry ${i + 1}`, err.code);
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// =========================
// TELEMETRY PER DEVICE
// =========================
async function syncDeviceTelemetry(device) {
  let conn;

  try {
    conn = await getMariaConnection();

    const deviceUid = device.device_uid;
    const lastPositionId = device.positionid || 0;

    const rows = await conn.query(
      `
      SELECT positionid, servertime, devicetime,
             latitude, longitude, speed, course, alarmcode
      FROM eventData
      WHERE deviceid = (
        SELECT id FROM device WHERE uniqueid = ?
      )
      AND positionid > ?
      ORDER BY positionid ASC
      LIMIT 5000
    `,
      [deviceUid, lastPositionId]
    );

    if (!rows.length) {
      return { count: 0, maxPositionId: lastPositionId };
    }

    let maxPositionId = lastPositionId;

    const mapped = rows.map(e => {
      if (e.positionid > maxPositionId) {
        maxPositionId = e.positionid;
      }

      return {
        deviceId: deviceUid,
        receivedAt: new Date(e.servertime),
        deviceTime: e.devicetime ? new Date(e.devicetime) : null,
        latitude: Number(e.latitude),
        longitude: Number(e.longitude),
        speedKph: Number(e.speed || 0),
        heading: Number(e.course || 0),
        alarmcode: e.alarmcode || null,
        hasAlert: !!e.alarmcode,
      };
    });

    // SOCKET SAFE
    let io;
    try { io = getIO(); } catch { io = null; }

    for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
      const batch = mapped.slice(i, i + INSERT_BATCH);

      // 🔥 1. TRY QUEUE (NON-BLOCKING)
      try {
        publishTelemetryBatch(batch);
      } catch (err) {
        console.warn('⚠️ Queue failed, fallback to DB');
      }

      // 🔥 2. ALWAYS INSERT INTO DB (CRITICAL FIX)
      await insertTelemetryBatch(batch);

      for (const r of batch) {
        await setLatestPosition(deviceUid, r);

        if (io) {
          io.emit('vehicle:update', {
            deviceId: deviceUid,
            ...r,
          });
        }

        if (r.hasAlert) {
          const alert = {
            deviceId: deviceUid,
            type: 'alarm',
            severity: 'warning',
            message: `Alarm code: ${r.alarmcode}`,
          };

          try {
            publishAlert(deviceUid, alert);
          } catch {}

          if (io) io.emit('alert', alert);
        }
      }
    }

    return { count: mapped.length, maxPositionId };

  } catch (err) {
    console.error(`❌ Device ${device.device_uid} sync error:`, err.message);
    return { count: 0, maxPositionId: device.positionid || 0 };
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// TELEMETRY ORCHESTRATOR
// =========================
export async function syncTelemetry() {
  const { rows: devices } = await pgPool.query(`
    SELECT device_uid, positionid FROM devices
  `);

  console.log(`🔄 Syncing telemetry for ${devices.length} devices`);

  let total = 0;

  for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
    const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

    await Promise.allSettled(
      chunk.map(async d => {
        const res = await syncDeviceTelemetry(d);

        if (res.count > 0) {
          total += res.count;

          await pgPool.query(
            `UPDATE devices
             SET positionid = GREATEST(positionid, $1)
             WHERE device_uid = $2`,
            [res.maxPositionId, d.device_uid]
          );
        }
      })
    );
  }

  console.log(`✅ Telemetry sync complete — ${total}`);
}

// =========================
// MAIN ENTRY
// =========================
export async function runMariaSync() {
  if (isSyncRunning) {
    console.log('⏳ Sync skipped (same instance)');
    return;
  }

  const locked = await acquireLock();
  if (!locked) {
    console.log('⏳ Sync skipped (another instance)');
    return;
  }

  isSyncRunning = true;

  try {
    console.log('🚀 Maria Sync started');

    await syncTelemetry(); // vehicles/devices can be scheduled separately

    console.log('✅ Maria Sync completed');

  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}