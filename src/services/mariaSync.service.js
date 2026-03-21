import * as mariadb from "mariadb";
import pkg from "pg";
const { Pool } = pkg;

// 🔵 MariaDB
const mariaPool = mariadb.createPool({
  host: "18.218.110.222",
  user: "root",
  password: "nairobiyetu",
  database: "uradi",
  connectionLimit: 5,
});

// 🟢 PostgreSQL
const pgPool = new Pool({
  host: "db",
  user: "postgres",
  password: "postgres",
  database: "tracking_platform",
  port: 5432,
});

// ⚡ CONFIG
const FETCH_LIMIT = 500;   // 🔴 VERY IMPORTANT (prevents crash)
const INSERT_BATCH = 500;  // insert size

export async function runMariaSync() {
  let conn;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Production Maria Sync Started");

    // 1️⃣ Get serials
    const registrations = await conn.query("SELECT serial FROM registration");

    const serials = registrations.map((r) =>
      r.serial.startsWith("0") ? r.serial : "0" + r.serial
    );

    if (!serials.length) {
      console.log("⚠️ No serials found");
      return;
    }

    // 2️⃣ MATCH DEVICES
    const deviceRows = await conn.query(
      `SELECT id, uniqueid FROM device 
       WHERE uniqueid IN (${serials.map(() => "?").join(",")})`,
      serials
    );

    const deviceMap = new Map();
    deviceRows.forEach((d) => deviceMap.set(d.uniqueid, d.id));

    console.log(`✅ Matched devices: ${deviceMap.size}`);

    let totalInserted = 0;

    // 3️⃣ PROCESS DEVICES
    for (const [uniqueid, deviceId] of deviceMap.entries()) {
      console.log(`\n🔄 Processing ${uniqueid}`);

      // 🔁 last sync
      const lastSyncRes = await pgPool.query(
        `SELECT MAX(servertime) as lasttime 
         FROM telemetry WHERE device_id = $1`,
        [deviceId]
      );

      let lastSync =
        lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";

      let hasMore = true;

      while (hasMore) {
        console.log(`📡 Fetching after ${lastSync}`);

        // ✅ SAFE FETCH (LIMITED)
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

        // 4️⃣ INSERT IN SMALLER BATCHES
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
              e.speed,
              e.servertime
            );
          });

          await pgPool.query(
            `INSERT INTO telemetry (device_id, latitude, longitude, speed, servertime)
             VALUES ${placeholders.join(",")}
             ON CONFLICT (device_id, servertime) DO NOTHING`,
            values
          );

          totalInserted += batch.length;
        }

        // ✅ move forward cursor
        lastSync = events[events.length - 1].servertime;

        // stop if less than limit (no more data)
        if (events.length < FETCH_LIMIT) {
          hasMore = false;
        }
      }

      console.log(`✅ ${uniqueid} fully synced`);
    }

    console.log(`🎯 Total inserted: ${totalInserted}`);

    return {
      success: true,
      totalInserted,
    };
  } catch (error) {
    console.error("❌ Sync error:", error);
    return { success: false, message: error.message };
  } finally {
    if (conn) conn.release();
  }
}