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
// 5️⃣ Telemetry Sync (Full Columns)
// =========================
// =========================
// 5️⃣ Telemetry Sync (Incremental)
// =========================
export async function syncTelemetry() {
  const conn = await mariaPool.getConnection();

  try {
    console.log("🔄 Starting telemetry sync using device.id...");

    // Step 1: get registration serials
    const registrations = await conn.query("SELECT serial FROM registration");

    for (const r of registrations) {
      const serialKey = `0${r.serial}`;

      // Step 2: get device from MariaDB
      const deviceRes = await conn.query(
        "SELECT id, uniqueid FROM device WHERE uniqueid = ?",
        [serialKey]
      );

      if (!deviceRes.length) continue;

      const deviceId = deviceRes[0].id;
      const uniqueId = deviceRes[0].uniqueid;

      // Step 3: get last synced time for this device from Postgres
      const lastSyncedRes = await pgPool.query(
        `SELECT MAX(signal_time) as last_synced FROM telemetry WHERE device_id = $1`,
        [deviceId]
      );
      const lastSynced = lastSyncedRes.rows[0]?.last_synced || new Date(0); // default to epoch if never synced

      // Step 4: fetch only new telemetry from MariaDB
      const telemetryRows = await conn.query(
        `SELECT 
          protocol,
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
          AND servertime > ?
        ORDER BY servertime ASC`,
        [deviceId, lastSynced]
      );

      if (!telemetryRows.length) continue;

      // Step 5: insert into Postgres in batches
      for (let i = 0; i < telemetryRows.length; i += INSERT_BATCH) {
        const batch = telemetryRows.slice(i, i + INSERT_BATCH);
        const values = [];

        const placeholders = batch
          .map((e, idx) => {
            const off = idx * 24;

            values.push(
              deviceId,
              e.protocol || null,
              new Date(e.servertime),
              e.devicetime ? new Date(e.devicetime) : null,
              e.fixtime ? new Date(e.fixtime) : null,
              e.valid === 1 || e.valid === true,
              Number(e.latitude) || 0,
              Number(e.longitude) || 0,
              e.altitude != null ? Number(e.altitude) : null,
              e.speed != null ? Number(e.speed) : 0,
              e.course != null ? Number(e.course) : null,
              e.address || null,
              e.attributes || null,
              e.accuracy != null ? Number(e.accuracy) : null,
              e.network || null,
              e.statuscode === 1 || e.statuscode === true,
              e.alarmcode || null,
              e.speedlimit != null ? Number(e.speedlimit) : null,
              e.odometer != null ? Number(e.odometer) : null,
              e.isRead === 1 || e.isRead === true,
              e.signalwireconnected === 1 || e.signalwireconnected === true,
              e.powerwireconnected === 1 || e.powerwireconnected === true,
              e.eactime ? new Date(e.eactime) : null,
              new Date()
            );

            return `(${Array.from({ length: 24 }, (_, j) => `$${off + j + 1}`).join(",")})`;
          })
          .join(",");

        await pgPool.query(
          `INSERT INTO telemetry (
            device_id,
            protocol,
            signal_time,
            device_time,
            fix_time,
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
            isread,
            signalwireconnected,
            powerwireconnected,
            eactime,
            created_at
          ) VALUES ${placeholders}
          ON CONFLICT (device_id, signal_time) DO NOTHING`,
          values
        );
      }

      console.log(`📦 Telemetry synced: ${uniqueId} → ${telemetryRows.length} new rows`);
    }
  } catch (err) {
    console.error("❌ Telemetry sync error:", err);
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