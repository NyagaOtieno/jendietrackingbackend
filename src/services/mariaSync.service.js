import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';

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

const DEVICE_CONCURRENCY = 25;
const deviceIdCache = new Map(); // uniqueid → pg integer id

// =========================
// LOCK
// =========================
async function acquireLock() {
  const res = await pgPool.query(`SELECT pg_try_advisory_lock(778899) AS locked`);
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query(`SELECT pg_advisory_unlock(778899)`);
}

// =========================
// VEHICLES SYNC
// =========================
export async function syncVehicles() {
  let conn;
  try {
    conn = await mariaPool.getConnection();
    const rows = await conn.query(`
      SELECT serial, reg_no, vmodel, install_date, pstatus
      FROM registration
    `);

    for (const r of rows) {
      if (!r.serial) continue;
      // ✅ Fix 1: truncate plate_number to 100 chars
      const plate = (r.reg_no || `PLATE_${r.serial}`).trim().substring(0, 100);

      await pgPool.query(`
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (serial)
        DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          model = EXCLUDED.model,
          status = EXCLUDED.status
      `, [
        String(r.serial),
        plate,
        `Unit ${r.serial}`,
        r.vmodel || '',
        r.pstatus || 'inactive',
        r.install_date || new Date(),
      ]);
    }

    console.log(`✅ Vehicles synced: ${rows.length}`);
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
    conn = await mariaPool.getConnection();
    const rows = await conn.query(`SELECT id, uniqueid FROM device`);

    for (const r of rows) {
      if (!r.uniqueid) continue;

      const res = await pgPool.query(`
        INSERT INTO devices (device_uid, serial, positionid)
        VALUES ($1,$2,0)
        ON CONFLICT (device_uid)
        DO UPDATE SET serial = EXCLUDED.serial
        RETURNING id
      `, [r.uniqueid, r.uniqueid]);

      // ✅ Fix 2: cache the PG integer id
      deviceIdCache.set(r.uniqueid, res.rows[0].id);
    }

    console.log(`✅ Devices synced: ${rows.length}`);
  } finally {
    conn?.release();
  }
}

// =========================
// TELEMETRY PER DEVICE
// =========================
async function syncDeviceTelemetry(device, conn) {
  const deviceUid = device.device_uid;
  const lastId = device.positionid || 0;

  // ✅ Fix 3: get integer PG device id
  let pgDeviceId = deviceIdCache.get(deviceUid);
  if (!pgDeviceId) {
    const res = await pgPool.query(
      `SELECT id FROM devices WHERE device_uid = $1 LIMIT 1`,
      [deviceUid]
    );
    if (!res.rows.length) return { count: 0, maxId: lastId };
    pgDeviceId = res.rows[0].id;
    deviceIdCache.set(deviceUid, pgDeviceId);
  }

  // Get maria device id
  let mariaDeviceId = null;
  const devRes = await conn.query(
    `SELECT id FROM device WHERE uniqueid = ? LIMIT 1`,
    [deviceUid]
  );
  mariaDeviceId = devRes?.[0]?.id;
  if (!mariaDeviceId) return { count: 0, maxId: lastId };

  const rows = await conn.query(`
    SELECT id, servertime, devicetime,
           latitude, longitude, speed, course, alarmcode
    FROM eventData
    WHERE deviceid = ?
    AND id > ?
    ORDER BY id ASC
    LIMIT 5000
  `, [mariaDeviceId, lastId]);

  if (!rows.length) return { count: 0, maxId: lastId };

  let maxId = lastId;

  const mapped = rows.map(r => {
    if (r.id > maxId) maxId = r.id;
    return {
      pgDeviceId,        // integer for telemetry insert
      deviceUid,         // string for alerts
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

  // ✅ Fix 4: correct column names matching your schema
  if (mapped.length) {
    const values = [];
    const placeholders = mapped.map((r, i) => {
      const b = i * 6;
      values.push(
        r.pgDeviceId,
        r.latitude,
        r.longitude,
        r.speedKph,
        r.heading,
        r.deviceTime,
      );
      return `($${b+1},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6})`;
    });

    await pgPool.query(`
      INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, heading, device_time)
      VALUES ${placeholders.join(',')}
      ON CONFLICT DO NOTHING
    `, values);
  }

  // Publish to queue
  try { publishTelemetryBatch(mapped); } catch {}

  // Publish alerts
  for (const r of mapped) {
    if (r.alarmcode) {
      try {
        publishAlert(r.deviceUid, {
          type: 'alarm',
          severity: 'warning',
          message: `Alarm code: ${r.alarmcode}`,
        });
      } catch {}
    }
  }

  return { count: mapped.length, maxId };
}

// =========================
// TELEMETRY ORCHESTRATOR
// =========================
export async function syncTelemetry() {
  let conn;
  try {
    conn = await mariaPool.getConnection();
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

      for (let j = 0; j < results.length; j++) {
        const res = results[j];
        const dev = chunk[j];

        if (res.status === 'fulfilled' && res.value.count > 0) {
          total += res.value.count;
          await pgPool.query(`
            UPDATE devices
            SET positionid = GREATEST(positionid, $1)
            WHERE device_uid = $2
          `, [res.value.maxId, dev.device_uid]);

          console.log(`📦 ${dev.device_uid} → ${res.value.count} rows`);
        }

        if (res.status === 'rejected') {
          console.error(`❌ ${dev.device_uid}:`, res.reason?.message);
        }
      }
    }

    console.log(`✅ Telemetry sync complete: ${total} rows`);
  } finally {
    conn?.release();
  }
}

// =========================
// MAIN RUNNER
// =========================
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    console.log('🚀 Maria Sync started');
    await syncVehicles();
    await syncDevices();
    await syncTelemetry();
    console.log('✅ Maria Sync completed');
  } catch (err) {
    console.error('❌ Sync failed:', err.message);
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}