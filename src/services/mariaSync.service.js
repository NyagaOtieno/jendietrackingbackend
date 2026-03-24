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
    const pgVehiclesRes = await pgPool.query("SELECT id, serial FROM vehicles");
    const vehicleMap = new Map(pgVehiclesRes.rows.map(v => [v.serial, v]));

    // ✅ STEP 1: Start from registration (correct)
    const registrations = await conn.query(`
      SELECT serial, reg_no
      FROM registration
    `);

    for (const r of registrations) {
      const serialKey = `0${r.serial}`;

      // ✅ STEP 2: map to device using uniqueid
      const device = await conn.query(
        "SELECT id, uniqueid FROM device WHERE uniqueid = ?",
        [serialKey]
      );

      if (!device.length) {
        console.log(`⚠️ No device for serial ${r.serial}`);
        continue;
      }

      const deviceId = device[0].id;
      const uniqueId = device[0].uniqueid;

      // ✅ STEP 3: ensure vehicle exists
      let vehicle = vehicleMap.get(serialKey);

      if (!vehicle) {
        const res = await pgPool.query(
          `INSERT INTO vehicles (serial, plate_number, unit_name, status, created_at)
           VALUES ($1,$2,$3,$4,$5)
           RETURNING id, serial`,
          [serialKey, r.reg_no || "", `Unit ${serialKey}`, "active", new Date()]
        );
        vehicle = res.rows[0];
        vehicleMap.set(serialKey, vehicle);
      }

      // ✅ STEP 4: incremental sync
      const lastRes = await pgPool.query(
        "SELECT MAX(device_time) AS lasttime FROM telemetry WHERE vehicle_id=$1",
        [vehicle.id]
      );

      const lastTime = lastRes.rows[0].lasttime || "2000-01-01 00:00:00";

      let offset = 0;

      while (true) {
        const events = await conn.query(
          `SELECT latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid = ? AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ? OFFSET ?`,
          [deviceId, lastTime, FETCH_LIMIT, offset]
        );

        if (!events.length) break;

        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const values = [];

          const placeholders = batch.map((e, idx) => {
            const off = idx * 5;
            values.push(
              vehicle.id,
              e.latitude,
              e.longitude,
              e.speed || 0,
              e.servertime
            );
            return `($${off + 1},$${off + 2},$${off + 3},$${off + 4},$${off + 5})`;
          }).join(",");

          await pgPool.query(
            `INSERT INTO telemetry
             (vehicle_id, latitude, longitude, speed_kph, device_time)
             VALUES ${placeholders}
             ON CONFLICT (vehicle_id, device_time) DO NOTHING`,
            values
          );
        }

        console.log(`📦 Telemetry synced: ${uniqueId} - ${events.length}`);
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