// src/services/mariaSync.service.js
import dotenv from 'dotenv';
dotenv.config();

import { createPool } from 'mariadb';
import { pgPool } from '../config/db.js';
import { publishTelemetryBatch, publishAlert } from '../queue/publisher.js';

// =========================
// MariaDB Pool with safer defaults
// =========================
const mariaPool = createPool({
  host:            process.env.MARIA_DB_HOST,
  port:            Number(process.env.MARIA_DB_PORT || 3306),
  user:            process.env.MARIA_DB_USER,
  password:        process.env.MARIA_DB_PASSWORD,
  database:        process.env.MARIA_DB_NAME || 'uradi',
  connectionLimit: 20,
  acquireTimeout:  30000,
  socketPath:      undefined,
});

// Helper to retry connection on failure
async function getMariaConnection(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await mariaPool.getConnection();
    } catch (err) {
      console.warn(`⚠️ MariaDB connection attempt ${i + 1} failed:`, err.code);
      if (i === retries - 1) throw err;
      await new Promise(res => setTimeout(res, 2000)); // wait 2s before retry
    }
  }
}

// =========================
// Config
// =========================
const INSERT_BATCH       = parseInt(process.env.INSERT_BATCH       || '100', 10);
const DEVICE_CONCURRENCY = parseInt(process.env.DEVICE_CONCURRENCY || '20',  10);

// =========================
// Vehicles Sync
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

      await pgPool.query(
        `
        INSERT INTO devices (device_uid, serial, label, positionid)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (device_uid) DO UPDATE SET
          serial = EXCLUDED.serial,
          label = EXCLUDED.label,
          positionid = EXCLUDED.positionid
        `,
        [
          r.uniqueid,
          r.uniqueid,
          `Device ${r.uniqueid}`,
          r.positionid || 0
        ]
      );
    }

    console.log(`✅ Devices synced with positionid`);
  } finally {
    conn.release();
  }
}
// =========================
// Devices Sync
// =========================
export async function syncDevices() {
  const conn = await getMariaConnection();
  try {
    const rows = await conn.query(`SELECT serial, reg_no FROM registration`);

    for (const r of rows) {
      const serialKey = r.serial ? `0${r.serial}` : null;
      if (!serialKey) continue;

      await pgPool.query(
        `INSERT INTO devices (device_uid, serial, label)
         VALUES ($1, $2, $3)
         ON CONFLICT (device_uid) DO UPDATE SET
           serial = EXCLUDED.serial,
           label  = EXCLUDED.label`,
        [serialKey, serialKey, r.reg_no || serialKey]
      );
    }

    console.log(`✅ Devices synced: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// Per-device Telemetry Sync
// =========================
async function syncDeviceTelemetry(device) {
  const conn = await getMariaConnection();

  try {
    const deviceUid = device.serial;        // uniqueid
    const lastPositionId = device.positionid || 0;

    const rows = await conn.query(
      `
      SELECT
        id,
        deviceid,
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
      AND id > ?
      ORDER BY id ASC
      LIMIT 5000
      `,
      [deviceUid, lastPositionId]
    );

    if (!rows.length) return { count: 0, maxId: lastPositionId };

    const mapped = rows.map(e => ({
      deviceId: deviceUid,
      receivedAt: new Date(e.servertime),
      deviceTime: e.devicetime ? new Date(e.devicetime) : null,
      latitude: Number(e.latitude),
      longitude: Number(e.longitude),
      speedKph: e.speed,
      heading: e.course,
      alarmcode: e.alarmcode || null,
      hasAlert: !!e.alarmcode,
    }));

    let maxEventId = lastPositionId;

    for (const r of rows) {
      if (r.id > maxEventId) maxEventId = r.id;
    }

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
      maxId: maxEventId
    };

  } finally {
    conn.release();
  }
}

// =========================
// Telemetry Sync — Parallel
// =========================
export async function syncTelemetry() {
  const conn = await getMariaConnection();

  let devices;

  try {
    devices = await pgPool.query(`
      SELECT id, serial, positionid
      FROM devices
    `);
  } finally {
    conn.release();
  }

  console.log(`🔄 Syncing telemetry for ${devices.rows.length} devices`);

  let total = 0;

  const chunkSize = DEVICE_CONCURRENCY;
  let offset = 0;

  while (offset < devices.rows.length) {
    const chunk = devices.rows.slice(offset, offset + chunkSize);
    offset += chunkSize;

    const results = await Promise.allSettled(
      chunk.map(async (d) => {
        const res = await syncDeviceTelemetry({
          serial: d.serial,
          positionid: d.positionid
        });

        if (res.count > 0) {
          total += res.count;

          // 🔥 UPDATE CURSOR BACK TO POSTGRES
          await pgPool.query(
            `UPDATE devices SET positionid = $1 WHERE serial = $2`,
            [res.maxId, d.serial]
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
}

// =========================
// Main Sync Entry Point
// =========================
export async function runMariaSync() {
  console.log('🚀 Maria Sync started');
  await syncVehicles();
  await syncDevices();
  await syncTelemetry();
  console.log('✅ Maria Sync completed');
}