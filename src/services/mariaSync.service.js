// src/services/mariaSync.service.js
import * as mariadb from "mariadb";
import pkg from "pg";
import os from "os";

const { Pool } = pkg;

// ----------------------------
// 1️⃣ MariaDB Pool
// ----------------------------
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,
});

// ----------------------------
// 2️⃣ PostgreSQL Pool — Docker-aware
// ----------------------------
function getPgHost() {
  if (process.env.PG_HOST) return process.env.PG_HOST;
  try {
    const cgroup = os.readFileSync("/proc/1/cgroup", "utf8").toLowerCase();
    if (cgroup.includes("docker") || cgroup.includes("kubepods")) return "tracking_postgres";
  } catch {}
  return "127.0.0.1";
}

const pgHost = getPgHost();
console.log("📝 PostgreSQL host selected:", pgHost);

const pgPool = new Pool({
  host: pgHost,
  port: Number(process.env.PG_PORT || process.env.DB_PORT || 5432),
  user: process.env.PG_USER || process.env.DB_USER || "postgres",
  password: process.env.PG_PASSWORD || process.env.DB_PASS || "postgres",
  database: process.env.PG_DATABASE || process.env.DB_NAME || "tracking_platform",
  ssl: process.env.PG_SSL === "true" ? { rejectUnauthorized: false } : false,
});

// ----------------------------
// 3️⃣ Sync Config
// ----------------------------
const FETCH_LIMIT = parseInt(process.env.FETCH_LIMIT || "500", 10);
const INSERT_BATCH = parseInt(process.env.INSERT_BATCH || "500", 10);

// ----------------------------
// 4️⃣ Main sync function
// ----------------------------
export async function runMariaSync() {
  let conn;
  let totalInserted = 0;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Production Maria Sync Started");

    // 1️⃣ Get all serials from registration
    const registrations = await conn.query("SELECT serial, reg_no FROM registration");
    if (!registrations.length) {
      console.log("⚠️ No registrations found");
      return { success: true, totalInserted: 0 };
    }

    const serials = registrations.map(r => (r.serial.startsWith("0") ? r.serial : "0" + r.serial));

    // 2️⃣ Fetch MariaDB devices in chunks
    const mariaDeviceMap = new Map();
    const SERIAL_CHUNK = 50;

    for (let i = 0; i < serials.length; i += SERIAL_CHUNK) {
      const chunk = serials.slice(i, i + SERIAL_CHUNK);
      console.log(`🔎 Matching devices chunk ${i} - ${i + chunk.length}`);

      const devices = await conn.query(
        `SELECT d.id, d.uniqueid, d.phone, d.category, r.reg_no
         FROM device d
         LEFT JOIN registration r ON r.serial = d.uniqueid
         WHERE d.uniqueid IN (${chunk.map(() => "?").join(",")})`,
        chunk
      );

      devices.forEach(d => {
        mariaDeviceMap.set(d.uniqueid, {
          mariaId: d.id,
          sim_number: d.phone || "",
          protocol_type: d.category || "",
          label: d.reg_no || "",
        });
      });
    }

    console.log(`✅ Matched Maria devices: ${mariaDeviceMap.size}`);

    // 3️⃣ Load Postgres devices
    const pgDevicesRes = await pgPool.query("SELECT id, device_uid FROM devices");
    const pgDeviceMap = new Map();
    pgDevicesRes.rows.forEach(d => pgDeviceMap.set(d.device_uid, d.id));

    // 4️⃣ Sync devices & telemetry
    for (const [uniqueid, mariaData] of mariaDeviceMap.entries()) {
      console.log(`\n🔄 Processing device ${uniqueid}`);
      let pgDeviceId = pgDeviceMap.get(uniqueid);

      // Auto-create device if missing
      if (!pgDeviceId) {
        console.log(`➕ Creating missing device ${uniqueid}`);
        const insertRes = await pgPool.query(
          `INSERT INTO devices
           (device_uid, label, sim_number, protocol_type)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (device_uid) DO UPDATE
           SET label = EXCLUDED.label,
               sim_number = EXCLUDED.sim_number,
               protocol_type = EXCLUDED.protocol_type
           RETURNING id`,
          [uniqueid, mariaData.label, mariaData.sim_number, mariaData.protocol_type]
        );
        pgDeviceId = insertRes.rows[0]?.id || (await pgPool.query("SELECT id FROM devices WHERE device_uid=$1", [uniqueid])).rows[0]?.id;
        pgDeviceMap.set(uniqueid, pgDeviceId);
      }

      if (!pgDeviceId) {
        console.log(`⏭️ Skipping ${uniqueid} (failed to create)`);
        continue;
      }

      // 5️⃣ Fetch last telemetry sync
      const lastSyncRes = await pgPool.query(
        "SELECT MAX(device_time) AS lasttime FROM telemetry WHERE device_id = $1",
        [pgDeviceId]
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
          [mariaData.mariaId, lastSync]
        );

        if (!events.length) break;
        console.log(`📦 Batch: ${events.length} rows`);

        // Insert telemetry in batches
        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const values = [];
          const placeholders = [];

          batch.forEach((e, idx) => {
            const base = idx * 5;
            placeholders.push(`($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5})`);
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

    console.log(`🎯 Total telemetry inserted: ${totalInserted}`);
    return { success: true, totalInserted };
  } catch (error) {
    console.error("❌ Sync error:", error);
    return { success: false, message: error.message };
  } finally {
    if (conn) conn.release();
  }
}

// ----------------------------
// 5️⃣ Continuous loop
// ----------------------------
export async function startMariaSyncLoop(intervalMs = 60000) {
  while (true) {
    await runMariaSync();
    await new Promise(res => setTimeout(res, intervalMs));
  }
}

// ----------------------------
// 6️⃣ Standalone run
// ----------------------------
if (require.main === module) {
  startMariaSyncLoop(Number(process.env.SYNC_INTERVAL || 60000))
    .catch(err => console.error("❌ Fatal sync loop error:", err));
}