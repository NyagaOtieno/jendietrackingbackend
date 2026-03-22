// src/services/mariaSync.service.js
import * as mariadb from "mariadb";
import pkg from "pg";
import dotenv from "dotenv";

dotenv.config(); // load .env first
const { Pool } = pkg;

let totalInserted = 0;

// 🔵 MariaDB Pool
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASS || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,
});

// 🟢 PostgreSQL Pool (Docker-ready)
const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,  // uses full connection string
  ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false,
});

// ⚡ Config
const FETCH_LIMIT = 500;
const INSERT_BATCH = 500;

export async function runMariaSync() {
  let conn;
  totalInserted = 0;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Production Maria Sync Started");

    // 1️⃣ Get serials
    const registrations = await conn.query(
      'SELECT serial FROM registration LIMIT 200'
    );

    const serials = registrations.map((r) =>
      r.serial.startsWith("0") ? r.serial : "0" + r.serial
    );

    if (!serials.length) {
      console.log("⚠️ No serials found");
      return { success: true, totalInserted: 0 };
    }

    // 2️⃣ MATCH DEVICES
    const deviceMap = new Map();
    const SERIAL_CHUNK = 50;

    for (let i = 0; i < serials.length; i += SERIAL_CHUNK) {
      const chunk = serials.slice(i, i + SERIAL_CHUNK);
      console.log(`🔎 Matching devices chunk ${i} - ${i + chunk.length}`);

      const deviceRows = await conn.query(
        `SELECT id, uniqueid FROM device WHERE uniqueid IN (${chunk.map(() => "?").join(",")})`,
        chunk
      );

      deviceRows.forEach((d) => deviceMap.set(d.uniqueid, d.id));
    }

    console.log(`✅ Matched devices: ${deviceMap.size}`);

    // 3️⃣ PROCESS DEVICES
    for (const [uniqueid, deviceId] of deviceMap.entries()) {
      console.log(`\n🔄 Processing device ${uniqueid}`);

      // last synced time
      const lastSyncRes = await pgPool.query(
        `SELECT MAX(servertime) as lasttime FROM telemetry WHERE device_id = $1`,
        [deviceId]
      );

      let lastSync = lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";
      let hasMore = true;

      while (hasMore) {
        console.log(`📡 Fetching after ${lastSync}`);

        const events = await conn.query(
          `SELECT deviceid, latitude, longitude, speed, servertime
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

        // INSERT IN BATCHES
        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const values = [];
          const placeholders = [];

          batch.forEach((e, idx) => {
            const base = idx * 5;
            placeholders.push(
              `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`
            );
            values.push(deviceId, e.latitude, e.longitude, e.speed, e.servertime);
          });

          await pgPool.query(
            `INSERT INTO telemetry (device_id, latitude, longitude, speed, servertime)
             VALUES ${placeholders.join(",")}
             ON CONFLICT (device_id, servertime) DO NOTHING`,
            values
          );

          totalInserted += batch.length;
        }

        lastSync = events[events.length - 1].servertime;
        if (events.length < FETCH_LIMIT) hasMore = false;
      }

      console.log(`✅ Device ${uniqueid} fully synced`);
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