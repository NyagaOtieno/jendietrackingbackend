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

      // 1️⃣ FORCE CONSISTENCY: remove wrong plate → serial mismatch
      await pgPool.query(
        `
        DELETE FROM vehicles
        WHERE plate_number = $1
        AND serial IS DISTINCT FROM $2
        `,
        [plate, serialKey]
      );

      // 2️⃣ ENSURE SERIAL UNIQUENESS SAFETY
      await pgPool.query(
        `
        DELETE FROM vehicles
        WHERE serial = $1
        AND plate_number IS DISTINCT FROM $2
        `,
        [serialKey, plate]
      );

      // 3️⃣ UPSERT (TRUE SOURCE OF TRUTH = SERIAL)
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

    console.log(`✅ Vehicles synced safely + enforced uniqueness: ${rows.length}`);
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
async function syncDeviceTelemetry(deviceId) {
  const conn = await getMariaConnection();
  try {
    const lastSyncedRes = await pgPool.query(
      `SELECT MAX(received_at) AS last_synced FROM telemetry WHERE device_id = $1`,
      [deviceId]
    );

    const lastSynced = lastSyncedRes.rows[0]?.last_synced
      ? new Date(lastSyncedRes.rows[0].last_synced)
      : new Date(0);

    const lastSyncedMaria = lastSynced.toISOString().slice(0, 19).replace('T', ' ');

    const rows = await conn.query(
      `SELECT
         protocol, deviceid, servertime, devicetime, fixtime,
         valid, latitude, longitude, altitude, speed, course,
         address, attributes, accuracy, network, statuscode,
         alarmcode, speedlimit, odometer, isRead,
         signalwireconnected, powerwireconnected, eactime
       FROM eventData
       WHERE deviceid = ?
         AND servertime > ?
       ORDER BY servertime ASC
       LIMIT 5000`,
      [deviceId, lastSyncedMaria]
    );

    if (!rows.length) return 0;

    const mapped = rows.map(e => ({
      deviceId,
      receivedAt:  new Date(e.servertime),
      deviceTime:  e.devicetime ? new Date(e.devicetime) : null,
      latitude:    Number(e.latitude)  || 0,
      longitude:   Number(e.longitude) || 0,
      speedKph:    e.speed  != null ? Number(e.speed)  : null,
      heading:     e.course != null ? Number(e.course) : null,
      alarmcode:   e.alarmcode || null,
      hasAlert:    !!e.alarmcode,
    }));

    let published = 0;
    for (let i = 0; i < mapped.length; i += INSERT_BATCH) {
      const batch = mapped.slice(i, i + INSERT_BATCH);
      publishTelemetryBatch(batch);

      for (const row of batch) {
        if (row.hasAlert) {
          publishAlert(deviceId, {
            type:     'alarm',
            severity: 'warning',
            message:  `Alarm code: ${row.alarmcode}`,
          });
        }
      }

      published += batch.length;
    }

    return published;
  } finally {
    conn.release();
  }
}

// =========================
// Telemetry Sync — Parallel
// =========================
export async function syncTelemetry() {
  const conn = await getMariaConnection();
  let registrations;

  try {
    registrations = await conn.query('SELECT serial FROM registration');
  } finally {
    conn.release();
  }

  console.log(`🔄 Syncing telemetry for ${registrations.length} devices (${DEVICE_CONCURRENCY} parallel)...`);

  let totalPublished = 0;
  let offset = 0;

  while (offset < registrations.length) {
    const chunk = registrations.slice(offset, offset + DEVICE_CONCURRENCY);
    offset += DEVICE_CONCURRENCY;

    const results = await Promise.allSettled(
      chunk.map(async (r) => {
        const serialKey = `0${r.serial}`;

        const devRes = await pgPool.query(
          `SELECT id FROM devices WHERE serial = $1 LIMIT 1`,
          [serialKey]
        );
        if (!devRes.rows.length) return 0;

        const deviceId = devRes.rows[0].id;
        const count = await syncDeviceTelemetry(deviceId);
        if (count > 0) console.log(`📦 ${serialKey} → ${count} rows queued`);
        return count;
      })
    );

    for (const r of results) {
      if (r.status === 'fulfilled') totalPublished += r.value;
      else console.error('❌ Device sync error:', r.reason?.message);
    }
  }

  console.log(`✅ Telemetry sync complete — ${totalPublished} rows published to queue`);
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