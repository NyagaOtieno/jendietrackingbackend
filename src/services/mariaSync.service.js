// src/services/mariaSync.service.js
import * as mariadb from "mariadb";
import pkg from "pg";
const { Pool } = pkg;

let totalInserted = 0;

// 🔵 MariaDB connection pool
const mariaPool = mariadb.createPool({
  host: "18.218.110.222",
  user: "root",
  password: "nairobiyetu",
  database: "uradi",
  connectionLimit: 5,
});

// 🟢 PostgreSQL connection pool
const pgPool = new Pool({
  host: process.env.PG_HOST || "127.0.0.1",
  port: Number(process.env.PG_PORT) || 5432,
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "postgres",
  database: process.env.PG_DATABASE || "tracking_platform",
  ssl: process.env.PG_SSL === "true" ? { rejectUnauthorized: false } : false,
});

// ⚡ CONFIG
const FETCH_LIMIT = 500;   // Max rows per fetch from MariaDB
const INSERT_BATCH = 500;  // Max rows per insert to Postgres

export async function runMariaSync() {
  let conn;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Production Maria Sync Started");

    // 1️⃣ Fetch serials from MariaDB
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

    // 2️⃣ MATCH DEVICES in MariaDB
    const deviceMap = new Map();
    const SERIAL_CHUNK = 50; // safe chunk size

    for (let i = 0; i < serials.length; i += SERIAL_CHUNK) {
      const chunk = serials.slice(i, i + SERIAL_CHUNK);

      console.log(`🔎 Matching devices chunk ${i} - ${i + chunk.length}`);

      const devices = await conn.query(
        `SELECT id, uniqueid FROM device WHERE uniqueid IN (${chunk
          .map(() => "?")
          .join(",")})`,
        chunk
      );

      devices.forEach((d) => deviceMap.set(d.uniqueid, d.id));
    }

    console.log(`✅ Matched devices: ${deviceMap.size}`);

    // 3️⃣ PROCESS DEVICES
    for (const [uniqueid, deviceId] of deviceMap.entries()) {
      console.log(`\n🔄 Processing device ${uniqueid}`);

      // Get last sync timestamp from Postgres
      const lastSyncRes = await pgPool.query(
        `SELECT MAX(device_time) AS lasttime FROM telemetry WHERE device_id = $1`,
        [deviceId]
      );

      let lastSync =
        lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";

      let hasMore = true;

      while (hasMore) {
        console.log(`📡 Fetching after ${lastSync}`);

        const events = await conn.query(
          `SELECT deviceid, latitude, longitude, speed AS speed_kph, servertime AS device_time
           FROM eventData
           WHERE deviceid = ? AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ${FETCH_LIMIT}`,
          [deviceId, lastSync]
        );

        if (!events.length) {
          console.log(`⏭️ No more data for ${uniqueid}`);
          break;
        }

        console.log(`📦 Batch: ${events.length} rows`);

        // Insert into Postgres in smaller batches
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
              deviceId,
              e.latitude,
              e.longitude,
              e.speed_kph,
              e.device_time
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

        // Move forward cursor
        lastSync = events[events.length - 1].device_time;

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