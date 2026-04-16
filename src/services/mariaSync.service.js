// src/services/mariaSync.service.js
import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';

// =========================
// MARIA DB
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
// POSTGRES DISTRIBUTED LOCK (CRITICAL)
// =========================
async function acquireLock() {
  const res = await pgPool.query(`
    SELECT pg_try_advisory_lock(778899) AS locked
  `);
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query(`SELECT pg_advisory_unlock(778899)`);
}

// =========================
// CONNECTION HELPER
// =========================
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

// =========================
// 1️⃣ VEHICLES SYNC (SAFE UPSERT)
// =========================
export async function syncVehicles() {
  const conn = await getMariaConnection();

  try {
    const rows = await conn.query(`
      SELECT serial, reg_no, vmodel, install_date, pstatus
      FROM registration
    `);

    for (const r of rows) {
      if (!r.serial) continue;

      const serialKey = String(r.serial).padStart(1, '0');
      const plate = (r.reg_no || `PLATE_${serialKey}`).trim();

      await pgPool.query(`
        INSERT INTO vehicles
          (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial)
        DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status
      `, [
        serialKey,
        plate,
        `Unit ${serialKey}`,
        r.vmodel || '',
        r.pstatus || 'inactive',
        r.install_date || new Date()
      ]);
    }

    console.log(`✅ Vehicles synced: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// 2️⃣ DEVICES SYNC (POSITIONID SAFE)
// =========================
export async function syncDevices() {
  const conn = await getMariaConnection();

  try {
    const rows = await conn.query(`
      SELECT id, uniqueid, positionid
      FROM device
    `);

    for (const r of rows) {
      if (!r.uniqueid) continue;

      await pgPool.query(`
        INSERT INTO devices (device_uid, serial, positionid)
        VALUES ($1,$2,$3)
        ON CONFLICT (device_uid)
        DO UPDATE SET
          serial = EXCLUDED.serial,
          positionid = EXCLUDED.positionid
      `, [
        r.uniqueid,
        r.uniqueid,
        r.positionid || 0
      ]);
    }

    console.log(`✅ Devices synced: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// 3️⃣ TELEMETRY PER DEVICE
// =========================
async function syncDeviceTelemetry(device) {
  const conn = await getMariaConnection();

  try {
    const deviceUid = device.device_uid;
    const lastPositionId = device.positionid || 0;

    const rows = await conn.query(`
      SELECT id, positionid, servertime, devicetime,
             latitude, longitude, speed, course, alarmcode
      FROM eventData
      WHERE deviceid = (
        SELECT id FROM device WHERE uniqueid = ?
      )
      AND positionid > ?
      ORDER BY positionid ASC
      LIMIT 5000
    `, [deviceUid, lastPositionId]);

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

    for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
      const batch = mapped.slice(i, i + INSERT_BATCH);

      publishTelemetryBatch(batch);

      for (const r of batch) {
        if (r.hasAlert) {
          publishAlert(deviceUid, {
            type: 'alarm',
            severity: 'warning',
            message: `Alarm code: ${r.alarmcode}`,
          });
        }
      }
    }

    return { count: mapped.length, maxPositionId };

  } finally {
    conn.release();
  }
}

// =========================
// 4️⃣ TELEMETRY ORCHESTRATOR (SAFE)
// =========================
export async function syncTelemetry() {
  const result = await pgPool.query(`
    SELECT device_uid, positionid
    FROM devices
  `);

  const devices = result.rows;

  console.log(`🔄 Syncing telemetry for ${devices.length} devices`);

  let total = 0;
  let offset = 0;

  while (offset < devices.length) {
    const chunk = devices.slice(offset, offset + DEVICE_CONCURRENCY);
    offset += DEVICE_CONCURRENCY;

    const results = await Promise.allSettled(
      chunk.map(async (d) => {
        const res = await syncDeviceTelemetry(d);

        if (res.count > 0) {
          total += res.count;

          await pgPool.query(`
            UPDATE devices
            SET positionid = GREATEST(positionid, $1)
            WHERE device_uid = $2
          `, [res.maxPositionId, d.device_uid]);
        }

        return res.count;
      })
    );

    for (const r of results) {
      if (r.status === 'rejected') {
        console.error('❌ Sync error:', r.reason?.message);
      }
    }
  }

  console.log(`✅ Telemetry sync complete — ${total}`);
}

// =========================
// 5️⃣ MAIN ENTRY (BULLETPROOF)
// =========================
export async function runMariaSync() {
  const locked = await acquireLock();

  if (!locked) {
    console.log('⏳ Sync skipped (lock active)');
    return;
  }

  try {
    console.log('🚀 Maria Sync started');

    await syncVehicles();
    await syncDevices();
    await syncTelemetry();

    console.log('✅ Maria Sync completed');

  } catch (err) {
    console.error('❌ Sync failed:', err.message);
  } finally {
    await releaseLock();
  }
}