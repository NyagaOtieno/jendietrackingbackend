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

      console.log(`⏳ Syncing telemetry for device ${uniqueId}...`);

      // Step 3: fetch telemetry in batches directly from MariaDB
      const BATCH_SIZE = 1000; // adjust as needed
      let lastId = 0;
      let rowsFetched;

      do {
        const telemetryRows = await conn.query(
          `SELECT
            id,
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
          WHERE deviceid = ? AND id > ?
          ORDER BY id ASC
          LIMIT ?`,
          [deviceId, lastId, BATCH_SIZE]
        );

        rowsFetched = telemetryRows.length;
        if (!rowsFetched) break;

        // Step 4: insert into Postgres
        const values = [];
        const placeholders = telemetryRows
          .map((e, idx) => {
            const off = idx * 24;
            values.push(
              deviceId,                                  // 1
              e.protocol || null,                        // 2
              new Date(e.servertime),                     // 3
              e.devicetime ? new Date(e.devicetime) : null, // 4
              e.fixtime ? new Date(e.fixtime) : null,    // 5
              e.valid === 1 || e.valid === true,         // 6
              Number(e.latitude) || 0,                   // 7
              Number(e.longitude) || 0,                  // 8
              e.altitude != null ? Number(e.altitude) : null, // 9
              e.speed != null ? Number(e.speed) : 0,     // 10
              e.course != null ? Number(e.course) : null,// 11
              e.address || null,                         // 12
              e.attributes || null,                      // 13
              e.accuracy != null ? Number(e.accuracy) : null, // 14
              e.network || null,                         // 15
              e.statuscode === 1 || e.statuscode === true, // 16
              e.alarmcode || null,                       // 17
              e.speedlimit != null ? Number(e.speedlimit) : null, // 18
              e.odometer != null ? Number(e.odometer) : null, // 19
              e.isRead === 1 || e.isRead === true,       // 20
              e.signalwireconnected === 1 || e.signalwireconnected === true, // 21
              e.powerwireconnected === 1 || e.powerwireconnected === true,   // 22
              e.eactime ? new Date(e.eactime) : null,    // 23
              new Date()                                 // 24
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

        lastId = telemetryRows[telemetryRows.length - 1].id;
        console.log(`📦 Synced ${rowsFetched} rows for ${uniqueId} (last id ${lastId})`);

      } while (rowsFetched === BATCH_SIZE);
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