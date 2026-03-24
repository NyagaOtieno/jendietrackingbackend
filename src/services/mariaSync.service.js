// src/services/mariaSync.service.js

import * as mariadb from 'mariadb';
import pkg from 'pg';
import fs from 'fs';

const { Pool } = pkg;

// =========================
// 1️⃣ MariaDB Pool
// =========================
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || '18.218.110.222',
  user: process.env.MARIA_USER || 'root',
  password: process.env.MARIA_PASSWORD || 'nairobiyetu',
  database: process.env.MARIA_DB || 'uradi',
  connectionLimit: 5,
});

// =========================
// 2️⃣ PostgreSQL Pool
// =========================
function getPgHost() {
  if (process.env.PG_HOST) return process.env.PG_HOST;
  try {
    const cgroup = fs.readFileSync('/proc/1/cgroup', 'utf8').toLowerCase();
    if (cgroup.includes('docker') || cgroup.includes('kubepods')) return 'tracking_postgres';
  } catch {}
  return '127.0.0.1';
}

const pgPool = new Pool({
  host: getPgHost(),
  port: Number(process.env.PG_PORT || 5432),
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'postgres',
  database: process.env.PG_DATABASE || 'tracking_platform',
});

// =========================
// 3️⃣ Config
// =========================
const FETCH_LIMIT = parseInt(process.env.FETCH_LIMIT || '500', 10);
const INSERT_BATCH = parseInt(process.env.INSERT_BATCH || '500', 10);
const CRON_INTERVAL = parseInt(process.env.CRON_INTERVAL || '300000', 10);

// =========================
// 4️⃣ Vehicles Sync
// =========================
export async function syncVehicles() {
  const conn = await mariaPool.getConnection();
  try {
    const rows = await conn.query(`
      SELECT
        serial,
        reg_no,
        vmodel,
        dealer,
        install_date,
        pstatus
      FROM registration
    `);

    if (!rows.length) return;

  await pgPool.query(
  `INSERT INTO vehicles
    (serial, plate_number, unit_name, make, model, year, status, created_at)
   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
   ON CONFLICT (serial) DO UPDATE SET
     plate_number = EXCLUDED.plate_number,
     unit_name = EXCLUDED.unit_name,
     make = EXCLUDED.make,
     model = EXCLUDED.model,
     year = EXCLUDED.year,
     status = EXCLUDED.status,
     created_at = EXCLUDED.created_at`,
  [
    serialKey,
    r.reg_no || '',
    `Unit ${serialKey}`,
    null,
    r.vmodel || '',
    null,
    r.pstatus || 'inactive',
    r.install_date || new Date(),
  ]
);
    console.log(`Vehicles synced: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// 5️⃣ Telemetry Sync
// =========================
export async function syncTelemetry() {
  const conn = await mariaPool.getConnection();
  try {
    // Load vehicles from PG
    const pgVehiclesRes = await pgPool.query(
      'SELECT id, serial, plate_number FROM vehicles'
    );
    const vehicleMap = new Map(pgVehiclesRes.rows.map(v => [v.serial, v]));

    // Load devices from MariaDB
    const devices = await conn.query(
      'SELECT id, uniqueid, imei, simno, protocol FROM device'
    );

    for (const d of devices) {
      const serialKey = d.uniqueid.startsWith('0') ? d.uniqueid : `0${d.uniqueid}`;
      let vehicle = vehicleMap.get(serialKey);

      // If vehicle missing, create it automatically
      if (!vehicle) {
        const res = await pgPool.query(
          `INSERT INTO vehicles (serial, unit_name, status, created_at)
           VALUES ($1,$2,$3,$4)
           RETURNING id, serial, plate_number`,
          [serialKey, `Unit ${serialKey}`, 'active', new Date()]
        );
        vehicle = res.rows[0];
        vehicleMap.set(serialKey, vehicle);
        console.log(`➕ Creating missing vehicle ${serialKey}`);
      }

      // Fetch last telemetry time for this vehicle
     const lastRes = await pgPool.query(
  'SELECT MAX(signal_time) AS lasttime FROM telemetry WHERE vehicle_id=$1',
  [vehicle.id]
);
      const lastTime = lastRes.rows[0].lasttime || '2000-01-01 00:00:00';

      // Fetch events from MariaDB
      let offset = 0;
      while (true) {
        const events = await conn.query(
          `SELECT latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid=? AND servertime>? 
           ORDER BY servertime ASC
           LIMIT ? OFFSET ?`,
          [d.id, lastTime, FETCH_LIMIT, offset]
        );
        if (!events.length) break;

        // Insert telemetry into PG
        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const values = [];
          const placeholders = batch.map((e, idx) => {
            const off = idx * 9;
            values.push(
              vehicle.id,
              vehicle.plate_number || '',
              d.imei || null,
              d.simno || null,
              d.protocol || null,
              e.latitude,
              e.longitude,
              e.speed || 0,
              e.servertime
            );
            return `($${off + 1},$${off + 2},$${off + 3},$${off + 4},$${off + 5},$${off + 6},$${off + 7},$${off + 8},$${off + 9})`;
          }).join(',');

          const query = `
            INSERT INTO telemetry
            (vehicle_id, label, imei, sim_number, protocol_type, latitude, longitude, speed_kph, device_time)
            VALUES ${placeholders}
            ON CONFLICT (vehicle_id, device_time) DO NOTHING
          `;
          await pgPool.query(query, values);
        }

        console.log(`📦 Telemetry synced: ${vehicle.serial} - ${events.length} rows`);
        offset += events.length;
      }
    }
  } finally {
    conn.release();
  }
}

// =========================
// 6️⃣ Main Sync
// =========================
export async function runMariaSync() {
  console.log('🚀 Production Maria Sync Started');
  await syncVehicles();
  await syncTelemetry();
  console.log('✅ Maria Sync completed');
}

// =========================
// 7️⃣ Cron
// =========================
export function startMariaSyncCron(interval = CRON_INTERVAL) {
  runMariaSync().catch(console.error);
  setInterval(() => runMariaSync().catch(console.error), interval);
}