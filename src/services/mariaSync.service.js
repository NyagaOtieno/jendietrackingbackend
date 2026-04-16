// src/services/mariaSync.service.js
import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';

// =========================
// GLOBAL SYNC LOCK (PREVENT DOUBLE RUNS)
// =========================
let isSyncRunning = false;

// =========================
// MariaDB Pool
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
// Connection helper
// =========================
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn(`⚠️ MariaDB connection attempt ${i + 1} failed:`, err.code);
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// =========================
// CONFIG
// =========================
const INSERT_BATCH = parseInt(process.env.INSERT_BATCH || '100', 10);
const DEVICE_CONCURRENCY = parseInt(process.env.DEVICE_CONCURRENCY || '20', 10);

// =========================
// 1️⃣ VEHICLES SYNC (UNCHANGED FUNCTIONALITY)
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

      const serialKey = `0${r.serial}`;
      const plate = (r.reg_no || `PLATE_${serialKey}`).trim();

      await pgPool.query(
        `
        INSERT INTO vehicles
          (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (serial)
        DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name    = EXCLUDED.unit_name,
          model        = EXCLUDED.model,
          status       = EXCLUDED.status,
          created_at   = EXCLUDED.created_at
        `,
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
    conn.release();
  }
}

// =========================
// 2️⃣ DEVICES SYNC (WITH POSITIONID SUPPORT)
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

      const deviceUid = r.uniqueid;

      await pgPool.query(
        `
        INSERT INTO devices (device_uid, serial, positionid)
        VALUES ($1, $2, $3)
        ON CONFLICT (device_uid) DO UPDATE SET
          serial      = EXCLUDED.serial,
          positionid  = EXCLUDED.positionid
        `,
        [
          deviceUid,
          deviceUid,
          r.positionid || 0
        ]
      );
    }

    console.log(`✅ Devices synced with positionid: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// 3️⃣ TELEMETRY SYNC PER DEVICE (POSITIONID INCREMENTAL)
// =========================
async function syncDeviceTelemetry(device) {
  const conn = await getMariaConnection();

  try {
    const deviceUid = device.device_uid;
    const lastPositionId = device.positionid || 0;

    const rows = await conn.query(
      `
      SELECT
        id,
        deviceid,
        positionid,
        servertime,
        devicetime,
        latitude,
        longitude,
        speed,
        course,
        alarmcode
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
        speedKph: e.speed,
        heading: e.course,
        alarmcode: e.alarmcode || null,
        hasAlert: !!e.alarmcode,
      };
    });

    for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
      const batch = mapped.slice(i, i + INSERT_BATCH);

      publishTelemetryBatch(batch);

      for (const row of batch) {
        if (row.hasAlert) {
          publishAlert(deviceUid, {
            type: 'alarm',
            severity: 'warning',
            message: `Alarm code: ${row.alarmcode}`,
          });
        }
      }
    }

    return {
      count: mapped.length,
      maxPositionId
    };

  } finally {
    conn.release();
  }
}

// =========================
// 4️⃣ TELEMETRY ORCHESTRATOR (SAFE + NO DOUBLE RUN)
// =========================
export async function syncTelemetry() {
  if (isSyncRunning) {
    console.log('⏳ Sync skipped: already running');
    return;
  }

  isSyncRunning = true;

  try {
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

            await pgPool.query(
              `
              UPDATE devices
              SET positionid = $1
              WHERE device_uid = $2
              `,
              [res.maxPositionId, d.device_uid]
            );
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

  } finally {
    isSyncRunning = false;
  }
}

// =========================
// 5️⃣ MAIN ENTRY
// =========================
export async function runMariaSync() {
  console.log('🚀 Maria Sync started');

  await syncVehicles();
  await syncDevices();
  await syncTelemetry();

  console.log('✅ Maria Sync completed');
}