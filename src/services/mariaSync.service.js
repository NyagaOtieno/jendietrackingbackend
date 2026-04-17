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
const INSERT_BATCH = Number(process.env.INSERT_BATCH || 100);
const DEVICE_CONCURRENCY = Number(process.env.DEVICE_CONCURRENCY || 20);

// =========================
// LOCAL CACHE (🔥 NEW)
// =========================
const deviceIdCache = new Map();

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
  } catch (err) {
    console.error('❌ syncVehicles error:', err.message);
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

      deviceIdCache.set(r.uniqueid, r.id); // 🔥 cache it

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
  } catch (err) {
    console.error('❌ syncDevices error:', err.message);
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// TELEMETRY PER DEVICE
// =========================
async function syncDeviceTelemetry(device, conn) {
  try {
    const deviceUid = device.device_uid;
    const lastPositionId = device.positionid || 0;

    // 🔥 USE CACHE (avoids subquery)
    let deviceId = deviceIdCache.get(deviceUid);

    if (!deviceId) {
      const res = await conn.query(
        `SELECT id FROM device WHERE uniqueid = ? LIMIT 1`,
        [deviceUid]
      );
      deviceId = res?.[0]?.id;
      if (deviceId) deviceIdCache.set(deviceUid, deviceId);
    }

    if (!deviceId) {
      console.warn(`⚠️ Device not found in Maria: ${deviceUid}`);
      return { count: 0, maxPositionId: lastPositionId };
    }

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
        latitude: Number(e.latitude) || 0,
        longitude: Number(e.longitude) || 0,
        speedKph: Number(e.speed) || 0,
        heading: Number(e.course) || 0,
        alarmcode: e.alarmcode || null,
        hasAlert: !!e.alarmcode,
      };
    });

    let io;
    try { io = getIO(); } catch { io = null; }

    for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
      const batch = mapped.slice(i, i + INSERT_BATCH);

      // ✅ SAFE QUEUE
      try {
        publishTelemetryBatch(batch);
      } catch (err) {
        console.warn('⚠️ Queue publish failed:', err.message);
      }

      for (const r of batch) {
        try {
          await setLatestPosition(deviceUid, r);
        } catch {}

        if (io) {
          io.emit('vehicle:update', { deviceId: deviceUid, ...r });
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

      await new Promise(r => setImmediate(r));
    }

    return { count: mapped.length, maxPositionId };

  } catch (err) {
    console.error(`❌ Device ${device.device_uid} sync error:`, err.message);
    return { count: 0, maxPositionId: device.positionid || 0 };
  }
}

// =========================
// TELEMETRY ORCHESTRATOR
// =========================
export async function syncTelemetry() {
  let conn;

  try {
    conn = await getMariaConnection();

    const { rows: devices } = await pgPool.query(`
      SELECT device_uid, positionid FROM devices
    `);

    console.log(`🔄 Syncing telemetry for ${devices.length} devices`);

    let total = 0;

    for (let i = 0; i < devices.length; i += DEVICE_CONCURRENCY) {
      const chunk = devices.slice(i, i + DEVICE_CONCURRENCY);

      const results = await Promise.allSettled(
        chunk.map(d => syncDeviceTelemetry(d, conn))
      );

      for (let idx = 0; idx < results.length; idx++) {
        const res = results[idx];
        const device = chunk[idx];

        if (res.status === 'fulfilled' && res.value.count > 0) {
          total += res.value.count;

          await pgPool.query(
            `UPDATE devices
             SET positionid = GREATEST(positionid, $1)
             WHERE device_uid = $2`,
            [res.value.maxPositionId, device.device_uid]
          );
        }
      }
    }

    console.log(`✅ Telemetry sync complete — ${total}`);

  } catch (err) {
    console.error('❌ syncTelemetry error:', err.message);
  } finally {
    if (conn) conn.release();
  }
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

    await syncVehicles();
    await syncDevices();
    await syncTelemetry();

    console.log('✅ Maria Sync completed');

  } catch (err) {
    console.error('❌ runMariaSync fatal error:', err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}