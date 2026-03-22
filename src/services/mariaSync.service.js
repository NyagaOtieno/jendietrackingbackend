// src/services/mariaSync.service.js
import * as mariadb from "mariadb";
import pkg from "pg";
const { Pool } = pkg;

// 🔵 MariaDB
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,
});

/// 🟢 PostgreSQL — AUTO DETECT HOST
import os from "os";

function getPgHost() {
  // 1️⃣ If environment variable is set, always use it
  if (process.env.PG_HOST) return process.env.PG_HOST;

  // 2️⃣ If inside Docker, use Docker service name
  // Docker usually has /proc/1/cgroup containing 'docker' or hostname as container ID
  const cgroup = os.readFileSync("/proc/1/cgroup", "utf8").toLowerCase();
  if (cgroup.includes("docker") || cgroup.includes("kubepods")) {
    return "tracking_postgres"; // ✅ Docker network host
  }

  // 3️⃣ Default for local host / Ubuntu
  return "127.0.0.1"; // ✅ local Postgres host
}

const pgPool = new Pool({
  host: getPgHost(),
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

    // 1️⃣ Get serials
    const registrations = await conn.query(
      "SELECT serial FROM registration LIMIT 200"
    );

    const serials = registrations.map((r) =>
      r.serial.startsWith("0") ? r.serial : "0" + r.serial
    );

    if (!serials.length) {
      console.log("⚠️ No serials found");
      return { success: true, totalInserted: 0 };
    }

    // 2️⃣ Match MariaDB devices
    const mariaDeviceMap = new Map();
    const SERIAL_CHUNK = 50;

    for (let i = 0; i < serials.length; i += SERIAL_CHUNK) {
      const chunk = serials.slice(i, i + SERIAL_CHUNK);

      console.log(`🔎 Matching devices chunk ${i} - ${i + chunk.length}`);

      const devices = await conn.query(
        `SELECT id, uniqueid FROM device WHERE uniqueid IN (${chunk
          .map(() => "?")
          .join(",")})`,
        chunk
      );

      devices.forEach((d) => mariaDeviceMap.set(d.uniqueid, d.id));
    }

    console.log(`✅ Matched Maria devices: ${mariaDeviceMap.size}`);

    // 3️⃣ Load PostgreSQL devices
    const pgDevicesRes = await pgPool.query(
      `SELECT id, device_uid FROM devices`
    );

    const pgDeviceMap = new Map();
   pgDevicesRes.rows.forEach((d) => {
  pgDeviceMap.set(d.device_uid, d.id);
});
   
    // 4️⃣ PROCESS DEVICES
    for (const [uniqueid, mariaDeviceId] of mariaDeviceMap.entries()) {
      console.log(`\n🔄 Processing device ${uniqueid}`);

      let pgDeviceId = pgDeviceMap.get(uniqueid);

      // 🔥 AUTO-CREATE DEVICE IF MISSING
      if (!pgDeviceId) {
        console.log(`➕ Creating missing device ${uniqueid}`);

        const insertRes = await pgPool.query(
          `INSERT INTO devices (device_uid)
           VALUES ($1)
           ON CONFLICT (device_uid) DO NOTHING
           RETURNING id`,
          [uniqueid]
        );

        if (insertRes.rows.length > 0) {
          pgDeviceId = insertRes.rows[0].id;
        } else {
          // fetch existing
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

      // 🔁 Get last sync
      const lastSyncRes = await pgPool.query(
        `SELECT MAX(device_time) AS lasttime FROM telemetry WHERE device_id = $1`,
        [pgDeviceId]
      );

      let lastSync =
        lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";

      let hasMore = true;

      while (hasMore) {
        console.log(`📡 Fetching after ${lastSync}`);

        // ✅ Fetch from MariaDB
        const events = await conn.query(
          `SELECT deviceid, latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid = ? AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ${FETCH_LIMIT}`,
          [mariaDeviceId, lastSync]
        );

        if (!events.length) {
          console.log(`⏭️ No more data for ${uniqueid}`);
          break;
        }

        console.log(`📦 Batch: ${events.length} rows`);

        // 5️⃣ Insert into Postgres
        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);

          const values = [];
          const placeholders = [];

          batch.forEach((e, idx) => {
            const base = idx * 5;

            placeholders.push(
              `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`
            );

            values.push(
              pgDeviceId,      // ✅ correct ID
              e.latitude,
              e.longitude,
              e.speed,         // → speed_kph
              e.servertime     // → device_time
            );
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

        if (events.length < FETCH_LIMIT) {
          hasMore = false;
        }
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