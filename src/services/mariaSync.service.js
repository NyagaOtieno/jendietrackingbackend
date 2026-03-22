// src/services/mariaSync.service.js
import * as mariadb from "mariadb";
import pkg from "pg";
import fs from "fs";
import os from "os";
const { Pool } = pkg;

// 🔵 MariaDB Pool
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,
});

// 🟢 PostgreSQL — Auto detect host
function getPgHost() {
  if (process.env.PG_HOST) return process.env.PG_HOST;

  try {
    const cgroup = fs.readFileSync("/proc/1/cgroup", "utf8").toLowerCase();
    if (cgroup.includes("docker") || cgroup.includes("kubepods")) {
      return "tracking_postgres"; // Docker network
    }
  } catch {
    // fallback if file doesn't exist
  }
  return "127.0.0.1"; // local host
}

const pgHost = getPgHost();
console.log("📝 PostgreSQL host selected:", pgHost);

const pgPool = new Pool({
  host: pgHost,
  port: Number(process.env.PG_PORT) || 5432,
  user: process.env.PG_USER || "postgres",
  password: String(process.env.PG_PASSWORD || "postgres"),
  database: process.env.PG_DATABASE || "tracking_platform",
  ssl: process.env.PG_SSL === "true" ? { rejectUnauthorized: false } : false,
});

// ⚡ CONFIG
const FETCH_LIMIT = 500;
const INSERT_BATCH = 500;

export async function runMariaSync() {
  let conn;
  let totalInserted = 0;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Production Maria Sync Started");

    // 1️⃣ Get serials from registration table
    const registrations = await conn.query(
      "SELECT serial, reg_no FROM registration LIMIT 200"
    );

    const serials = registrations.map((r) =>
      r.serial.startsWith("0") ? r.serial : "0" + r.serial
    );

    if (!serials.length) {
      console.log("⚠️ No serials found");
      return { success: true, totalInserted: 0 };
    }

    // Map serial → reg_no for label
    const serialToLabelMap = new Map();
    registrations.forEach((r) => {
      const key = r.serial.startsWith("0") ? r.serial : "0" + r.serial;
      serialToLabelMap.set(key, r.reg_no);
    });

    // 2️⃣ Fetch devices from MariaDB
    const mariaDeviceMap = new Map();
    const SERIAL_CHUNK = 50;

    for (let i = 0; i < serials.length; i += SERIAL_CHUNK) {
      const chunk = serials.slice(i, i + SERIAL_CHUNK);
      console.log(`🔎 Matching devices chunk ${i} - ${i + chunk.length}`);

      const devices = await conn.query(
        `SELECT id, uniqueid, phone, model, createdat FROM device WHERE uniqueid IN (${chunk
          .map(() => "?")
          .join(",")})`,
        chunk
      );

      devices.forEach((d) => mariaDeviceMap.set(d.uniqueid, d));
    }

    console.log(`✅ Matched Maria devices: ${mariaDeviceMap.size}`);

    // 3️⃣ Load PostgreSQL devices
    const pgDevicesRes = await pgPool.query(`SELECT id, device_uid FROM devices`);
    const pgDeviceMap = new Map();
    pgDevicesRes.rows.forEach((d) => pgDeviceMap.set(d.device_uid, d.id));

    // 4️⃣ Process each device
    for (const [uniqueid, device] of mariaDeviceMap.entries()) {
      console.log(`\n🔄 Processing device ${uniqueid}`);

      let pgDeviceId = pgDeviceMap.get(uniqueid);

      // 🔥 Auto-create device if missing
      if (!pgDeviceId) {
        const label = serialToLabelMap.get(uniqueid) || null;

        console.log(`➕ Creating missing device ${uniqueid} with label ${label || ""}`);

        const insertRes = await pgPool.query(
          `INSERT INTO devices
           (device_uid, label, sim_number, protocol_type, created_at)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (device_uid) DO UPDATE
           SET label = EXCLUDED.label,
               sim_number = EXCLUDED.sim_number,
               protocol_type = EXCLUDED.protocol_type
           RETURNING id`,
          [uniqueid, label, device.phone || null, device.model || null, device.createdat]
        );

        if (insertRes.rows.length > 0) {
          pgDeviceId = insertRes.rows[0].id;
        } else {
          const fetchRes = await pgPool.query(
            `SELECT id FROM devices WHERE device_uid = $1`,
            [uniqueid]
          );
          pgDeviceId = fetchRes.rows[0]?.id;
        }

        pgDeviceMap.set(uniqueid, pgDeviceId);
      }

      if (!pgDeviceId) {
        console.log(`⏭️ Skipping ${uniqueid} (failed to create)`);
        continue;
      }

      // 🔁 Get last sync time
      const lastSyncRes = await pgPool.query(
        `SELECT MAX(device_time) AS lasttime FROM telemetry WHERE device_id = $1`,
        [pgDeviceId]
      );

      let lastSync = lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";
      let hasMore = true;

      while (hasMore) {
        console.log(`📡 Fetching after ${lastSync}`);

        // ✅ Fetch from MariaDB eventData
        const events = await conn.query(
          `SELECT deviceid, latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid = ? AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ${FETCH_LIMIT}`,
          [device.id, lastSync]
        );

        if (!events.length) {
          console.log(`⏭️ No more data for ${uniqueid}`);
          break;
        }

        console.log(`📦 Batch: ${events.length} rows`);

        // 5️⃣ Insert telemetry into PostgreSQL
        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const values = [];
          const placeholders = [];

          batch.forEach((e, idx) => {
            const base = idx * 5;
            placeholders.push(
              `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`
            );
            values.push(pgDeviceId, e.latitude, e.longitude, e.speed, e.servertime);
          });

          await pgPool.query(
            `INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, device_time)
             VALUES ${placeholders.join(",")}
             ON CONFLICT (device_id, device_time) DO NOTHING`,
            values
          );

          totalInserted += batch.length;
        }

        lastSync = events[events.length - 1].servertime;
        if (events.length < FETCH_LIMIT) hasMore = false;
      }

      console.log(`✅ ${uniqueid} fully synced`);
    }

    console.log(`🎯 Total inserted: ${totalInserted}`);
    return { success: true, totalInserted };
  } catch (error) {
    console.error("❌ Sync error:", error);
    return { success: false, message: error.message };
  } finally {
    if (conn) conn.release();
  }
}