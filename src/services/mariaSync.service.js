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

// =========================
// 5️⃣ Telemetry Sync (Full Columns)
// =========================
export async function syncTelemetry() {
  const conn = await mariaPool.getConnection();
  try {
    console.log("🔄 Starting telemetry sync using uniqueid...");

    // Step 0: Fetch all vehicles from Postgres to map serial -> id
    const pgVehiclesRes = await pgPool.query("SELECT id, serial, plate_number FROM vehicles");
    const vehicleMap = new Map(pgVehiclesRes.rows.map((v) => [v.serial, v]));

    // Step 1: fetch all serials from registration
    const registrations = await conn.query("SELECT serial FROM registration");
    for (const r of registrations) {
      const serialKey = `0${r.serial}`;

      // Step 2: fetch device uniqueid from device table
      const devices = await conn.query("SELECT uniqueid FROM device WHERE uniqueid = ?", [serialKey]);
      if (!devices.length) continue;
      const uniqueId = devices[0].uniqueid;

      // Step 3: fetch full eventData for this uniqueId
      const telemetryRows = await conn.query(
        `SELECT
            deviceid,
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
        [uniqueId]
      );
      if (!telemetryRows.length) continue;

      // Step 4: ensure vehicle exists in Postgres
      let vehicle = vehicleMap.get(serialKey);
      if (!vehicle) {
        const res = await pgPool.query(
          `INSERT INTO vehicles (serial, unit_name, status, created_at)
           VALUES ($1,$2,$3,$4)
           RETURNING id, serial, plate_number`,
          [serialKey, `Unit ${serialKey}`, "active", new Date()]
        );
        vehicle = res.rows[0];
        vehicleMap.set(serialKey, vehicle);
        console.log(`➕ Creating missing vehicle ${serialKey}`);
      }

      // Step 5: insert telemetry in batches
      for (let i = 0; i < telemetryRows.length; i += INSERT_BATCH) {
        const batch = telemetryRows.slice(i, i + INSERT_BATCH);
        const values = [];
        const placeholders = batch
          .map((e, idx) => {
            const off = idx * 22; // 22 columns in Postgres telemetry table now
            values.push(
              vehicle.id,                 // device_id FK
              serialKey,                  // serial
              uniqueId,                   // uniqueid
              e.latitude,
              e.longitude,
              e.speed || 0,
              e.servertime || new Date(), // signal_time
              e.devicetime || null,
              e.fixtime || null,
              e.valid ?? true,
              e.altitude || null,
              e.course || null,
              e.address || null,
              e.attributes || null,
              e.accuracy || null,
              e.network || null,
              e.statuscode ?? false,
              e.alarmcode || null,
              e.speedlimit || null,
              e.odometer || null,
              e.isRead ?? false,
              e.signalwireconnected ?? true,
              e.powerwireconnected ?? true,
              e.eactime || null,
              new Date()                  // created_at
            );
            return `(${Array.from({ length: 24 }, (_, j) => `$${off + j + 1}`).join(",")})`;
          })
          .join(",");

        await pgPool.query(
          `INSERT INTO telemetry
            (device_id, serial, uniqueid, latitude, longitude, speed, signal_time,
             devicetime, fixtime, valid, altitude, course, address, attributes, accuracy,
             network, statuscode, alarmcode, speedlimit, odometer, isread,
             signalwireconnected, powerwireconnected, eactime, created_at)
           VALUES ${placeholders}
           ON CONFLICT (device_id, signal_time) DO NOTHING`,
          values
        );
      }
      console.log(`📦 Telemetry synced: ${serialKey} - ${telemetryRows.length} rows`);
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