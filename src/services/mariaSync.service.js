// src/services/mariaSync.service.js
import mariadb from "mariadb";
import pkg from "pg";
import fs from "fs";

const { Pool } = pkg;

// =========================
// 1️⃣ MariaDB Pool
// =========================
const mariaPool = mariadb.createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",
  connectionLimit: 5,
});

// =========================
// 2️⃣ PostgreSQL Pool
// =========================
function getPgHost() {
  if (process.env.PG_HOST) return process.env.PG_HOST;
  try {
    const cgroup = fs.readFileSync("/proc/1/cgroup", "utf8").toLowerCase();
    if (cgroup.includes("docker")) return "tracking_postgres";
  } catch {}
  return "127.0.0.1";
}

const pgPool = new Pool({
  host: getPgHost(),
  port: Number(process.env.PG_PORT || 5432),
  user: process.env.PG_USER || "postgres",
  password: process.env.PG_PASSWORD || "postgres",
  database: process.env.PG_DATABASE || "tracking_platform",
});

// =========================
// 3️⃣ CONFIG
// =========================
const FETCH_LIMIT = 500;
const INSERT_BATCH = 500;

// =========================
// 4️⃣ MAIN SYNC
// =========================
export async function runMariaSync() {
  let conn;
  let totalInserted = 0;

  try {
    conn = await mariaPool.getConnection();
    console.log("🚀 Maria Sync Started");

    // =========================
    // 🔥 LOAD ALL DEVICES (CORRECT JOIN)
    // =========================
    const devices = await conn.query(`
      SELECT 
        d.id,
        d.uniqueid,
        d.phone,
        d.category,
        r.reg_no,
        r.simno
      FROM device d
      LEFT JOIN registration r
        ON d.uniqueid = CONCAT('0', r.serial)
    `);

    console.log(`✅ Maria devices loaded: ${devices.length}`);

    // =========================
    // 🔥 UPSERT DEVICES (BULK)
    // =========================
    const values = [];
    const placeholders = [];

    devices.forEach((d, i) => {
      const idx = i * 4;
      placeholders.push(
        `($${idx + 1}, $${idx + 2}, $${idx + 3}, $${idx + 4})`
      );

      values.push(
        d.uniqueid,
        d.reg_no || "",
        d.simno || d.phone || "",
        d.category || ""
      );
    });

    await pgPool.query(`
      INSERT INTO devices (device_uid, label, sim_number, protocol_type)
      VALUES ${placeholders.join(",")}
      ON CONFLICT (device_uid) DO UPDATE
      SET 
        label = EXCLUDED.label,
        sim_number = EXCLUDED.sim_number,
        protocol_type = EXCLUDED.protocol_type
    `, values);

    console.log("✅ Devices synced");

    // =========================
    // 🔥 LOAD PG DEVICE MAP
    // =========================
    const pgDevicesRes = await pgPool.query(
      "SELECT id, device_uid FROM devices"
    );

    const pgDeviceMap = new Map();
    pgDevicesRes.rows.forEach(d =>
      pgDeviceMap.set(d.device_uid, d.id)
    );

    // =========================
    // 🔥 TELEMETRY SYNC
    // =========================
    for (const d of devices) {
      const pgDeviceId = pgDeviceMap.get(d.uniqueid);
      if (!pgDeviceId) continue;

      const lastSyncRes = await pgPool.query(
        "SELECT MAX(device_time) AS lasttime FROM telemetry WHERE device_id = $1",
        [pgDeviceId]
      );

      let lastSync =
        lastSyncRes.rows[0].lasttime || "2000-01-01 00:00:00";

      let hasMore = true;

      while (hasMore) {
        const events = await conn.query(
          `SELECT latitude, longitude, speed, servertime
           FROM eventData
           WHERE deviceid = ?
           AND servertime > ?
           ORDER BY servertime ASC
           LIMIT ${FETCH_LIMIT}`,
          [d.id, lastSync]
        );

        if (!events.length) break;

        for (let i = 0; i < events.length; i += INSERT_BATCH) {
          const batch = events.slice(i, i + INSERT_BATCH);
          const vals = [];
          const ph = [];

          batch.forEach((e, idx) => {
            const base = idx * 5;
            ph.push(
              `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5})`
            );
            vals.push(
              pgDeviceId,
              e.latitude,
              e.longitude,
              e.speed,
              e.servertime
            );
          });

          await pgPool.query(
            `INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, device_time)
             VALUES ${ph.join(",")}
             ON CONFLICT (device_id, device_time) DO NOTHING`,
            vals
          );

          totalInserted += batch.length;
        }

        lastSync = events[events.length - 1].servertime;
        if (events.length < FETCH_LIMIT) hasMore = false;
      }
    }

    console.log(`🎯 Total telemetry inserted: ${totalInserted}`);

    return { success: true, totalInserted };
  } catch (err) {
    console.error("❌ Sync error:", err);
    return { success: false, message: err.message };
  } finally {
    if (conn) conn.release();
  }
}

// =========================
// 5️⃣ LOOP
// =========================
export async function startMariaSyncLoop(interval = 60000) {
  while (true) {
    await runMariaSync();
    await new Promise(r => setTimeout(r, interval));
  }
}

// =========================
// 6️⃣ RUN DIRECTLY
// =========================
if (process.argv[1] === new URL(import.meta.url).pathname) {
  startMariaSyncLoop().catch(console.error);
}