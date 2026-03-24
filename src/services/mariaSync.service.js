// src/services/mariaSync.service.js
import dotenv from 'dotenv';
dotenv.config();
import { createPool } from "mariadb";
import { Pool } from "pg";
import fs from "fs";

// =========================
// 1️⃣ PostgreSQL Pool
// =========================
function getPgHost() {
  if (process.env.PG_HOST) return process.env.PG_HOST;
  try {
    const cgroup = fs.readFileSync("/proc/1/cgroup", "utf8").toLowerCase();
    if (cgroup.includes("docker") || cgroup.includes("kubepods")) return "tracking_postgres";
  } catch {}
  return "127.0.0.1";
}

export const pgPool = new Pool({
  host: getPgHost(),
  port: Number(process.env.PG_PORT),
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  database: process.env.PG_DATABASE,
});

// =========================
// 2️⃣ MariaDB Pool
// =========================
console.log("Postgres host:", process.env.PG_HOST);
console.log("Postgres user:", process.env.PG_USER);
console.log("Postgres password:", typeof process.env.PG_PASSWORD);
console.log("Postgres db:", process.env.PG_DATABASE);

const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST,
  port: Number(process.env.MARIA_DB_PORT || 3306),
  user: process.env.MARIA_DB_USER,
  password: process.env.MARIA_DB_PASSWORD,
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 20,
  acquireTimeout: 30000
});

// =========================
// 3️⃣ Config
// =========================
const FETCH_LIMIT = parseInt(process.env.FETCH_LIMIT || "500", 10);
const INSERT_BATCH = parseInt(process.env.INSERT_BATCH || "500", 10);
const CRON_INTERVAL = parseInt(process.env.CRON_INTERVAL || "300000", 10);

// =========================
// 4️⃣ Vehicles Sync
// =========================
export async function syncVehicles() {
  const conn = await mariaPool.getConnection();
  try {
    const rows = await conn.query(`
      SELECT serial, reg_no, vmodel, dealer, install_date, pstatus
      FROM registration
    `);

    for (const r of rows) {
      const serialKey = r.serial ? `0${r.serial}` : null;
      if (!serialKey) continue;

      await pgPool.query(
        `INSERT INTO vehicles
          (serial, plate_number, unit_name, make, model, year, status, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (serial) DO UPDATE SET
           plate_number = EXCLUDED.plate_number,
           unit_name = EXCLUDED.unit_name,
           model = EXCLUDED.model,
           status = EXCLUDED.status`,
        [
          serialKey,
          r.reg_no || "",
          `Unit ${serialKey}`,
          null,
          r.vmodel || "",
          null,
          r.pstatus || "inactive",
          r.install_date || new Date(),
        ]
      );
    }

    console.log(`Vehicles synced: ${rows.length}`);
  } finally {
    conn.release();
  }
}

// =========================
// 5️⃣ Telemetry Sync (FIXED FLOW)
// =========================
export async function syncTelemetry() {
  const conn = await mariaPool.getConnection();

  try {
    // Load existing PostgreSQL devices into memory
    const pgDevicesRes = await pgPool.query(
      "SELECT id, uniqueid FROM devices"
    );
    const deviceMap = new Map(pgDevicesRes.rows.map(d => [d.uniqueid, d]));

    // STEP 1: Get all serials from registration
    const registrations = await conn.query(`
      SELECT serial FROM registration
    `);

    for (const r of registrations) {
      if (!r.serial) continue;

      const serialKey = `0${r.serial}`;

      // STEP 2: get uniqueid from device table in MariaDB
      const deviceRows = await conn.query(
        "SELECT id, uniqueid FROM device WHERE uniqueid = ?",
        [serialKey]
      );
      if (!deviceRows.length) continue;

      const mariaDevice = deviceRows[0];
      const uniqueId = mariaDevice.uniqueid;

      // STEP 3: create device in PostgreSQL if it does NOT exist
      let pgDevice = deviceMap.get(uniqueId);
      if (!pgDevice) {
        const res = await pgPool.query(
          `INSERT INTO devices (uniqueid)
           VALUES ($1)
           RETURNING id, uniqueid`,
          [uniqueId]
        );
        pgDevice = res.rows[0];
        deviceMap.set(uniqueId, pgDevice);
        console.log(`➕ Created device ${uniqueId}`);
      }

      // STEP 4: get last synced telemetry time
      const lastRes = await pgPool.query(
        "SELECT MAX(signal_time) AS lasttime FROM telemetry WHERE device_id=$1",
        [pgDevice.id]
      );
      const lastTime = lastRes.rows[0].lasttime || "2000-01-01 00:00:00";

      // STEP 5: fetch telemetry from MariaDB in batches
      let offset = 0;
      while (true) {
        const events = await conn.query(
          `SELECT latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid = ? AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ? OFFSET ?`,
          [mariaDevice.id, lastTime, FETCH_LIMIT, offset]
        );
        if (!events.length) break;

        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);

          const values = [];
          const placeholders = batch
            .map((e, idx) => {
              const off = idx * 5;
              values.push(
                pgDevice.id,
                e.servertime,
                e.latitude,
                e.longitude,
                e.speed || 0
              );
              return `($${off + 1},$${off + 2},$${off + 3},$${off + 4},$${off + 5})`;
            })
            .join(",");

          await pgPool.query(
            `INSERT INTO telemetry
             (device_id, signal_time, latitude, longitude, speed)
             VALUES ${placeholders}
             ON CONFLICT DO NOTHING`,
            values
          );
        }

        offset += events.length;
        console.log(`📦 Telemetry synced: ${uniqueId} - ${events.length}`);
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
  console.log("🚀 Production Maria Sync Started");
  await syncVehicles();
  await syncTelemetry();
  console.log("✅ Maria Sync completed");
}

// =========================
// 7️⃣ Cron
// =========================
export function startMariaSyncCron(interval = CRON_INTERVAL) {
  runMariaSync().catch(console.error);
  setInterval(() => runMariaSync().catch(console.error), interval);
}