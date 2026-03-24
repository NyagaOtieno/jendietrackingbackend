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
    console.log("🔄 Starting telemetry sync using uniqueid...");

    // Step 1: fetch all serials from registration
    const registrations = await conn.query("SELECT serial FROM registration");

    for (const r of registrations) {
      const serialKey = `0${r.serial}`;

      // Step 2: fetch device info from device table using serialKey
      const devices = await conn.query(
        "SELECT id, uniqueid FROM device WHERE name = ?",
        [serialKey]
      );
      if (!devices.length) continue;

      const device = devices[0]; // device.id and device.uniqueid

      // Step 3: fetch all eventData rows for this device
      const telemetryRows = await conn.query(
        `SELECT
            servertime,
            devicetime,
            fixtime,
            valid,
            latitude,
            longitude,
            altitude,
            speed,
            course,
            address,
            attributes,
            accuracy,
            network,
            statuscode,
            alarmcode,
            speedlimit,
            odometer,
            isRead,
            signalwireconnected,
            powerwireconnected,
            eactime
         FROM eventData
         WHERE deviceid = ?
         ORDER BY servertime ASC`,
        [device.id]
      );
      if (!telemetryRows.length) continue;

      // Step 4: Insert into Postgres in batches
      for (let i = 0; i < telemetryRows.length; i += INSERT_BATCH) {
        const batch = telemetryRows.slice(i, i + INSERT_BATCH);
        const values = [];
        const placeholders = batch
          .map((e, idx) => {
            const off = idx * 22; // 22 columns for telemetry
            values.push(
              device.uniqueid,          // uniqueid
              e.servertime,             // signal_time
              e.devicetime,             // devicetime
              e.fixtime,                // fixtime
              e.valid,                  // valid
              e.latitude,
              e.longitude,
              e.altitude,
              e.speed,
              e.course,
              e.address,
              e.attributes,
              e.accuracy,
              e.network,
              e.statuscode,
              e.alarmcode,
              e.speedlimit,
              e.odometer,
              e.isRead,
              e.signalwireconnected,
              e.powerwireconnected,
              e.eactime || new Date()   // created_at fallback
            );
            return `($${off + 1},$${off + 2},$${off + 3},$${off + 4},$${off + 5},$${off + 6},$${off + 7},$${off + 8},$${off + 9},$${off + 10},$${off + 11},$${off + 12},$${off + 13},$${off + 14},$${off + 15},$${off + 16},$${off + 17},$${off + 18},$${off + 19},$${off + 20},$${off + 21},$${off + 22})`;
          })
          .join(",");

        await pgPool.query(
          `INSERT INTO telemetry
            (uniqueid, signal_time, devicetime, fixtime, valid, latitude, longitude, altitude,
             speed, course, address, attributes, accuracy, network, statuscode, alarmcode,
             speedlimit, odometer, isread, signalwireconnected, powerwireconnected, created_at)
           VALUES ${placeholders}
           ON CONFLICT (uniqueid, signal_time) DO NOTHING`,
          values
        );
      }
      console.log(`📦 Telemetry synced for device ${device.uniqueid} - ${telemetryRows.length} rows`);
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