// src/services/mariaSync.service.js

import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

/* ────────────────────────────────
   LOGGER
──────────────────────────────── */
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    level,
    msg,
    ...meta
  }));

const N = (v) => {
  if (v == null) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
};

/* ────────────────────────────────
   MARIA DB POOL (HARDENED FOR VPS)
──────────────────────────────── */
export const mariaPool = createPool({
  host: process.env.MARIA_HOST || "18.218.110.222",
  port: Number(process.env.MARIA_PORT || 3306),
  user: process.env.MARIA_USER || "root",
  password: process.env.MARIA_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB || "uradi",

  connectionLimit: 5,          // 🔥 reduced for 1GB VPS stability
  connectTimeout: 15000,
  acquireTimeout: 20000,
  multipleStatements: false,
});

/* ────────────────────────────────
   DEVICE CACHE
──────────────────────────────── */
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(`
    SELECT id, device_uid, vehicle_id
    FROM devices
    WHERE device_uid IS NOT NULL
  `);

  deviceMapCache.clear();

  for (const r of res.rows || []) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
      vehicleId: r.vehicle_id
    });
  }

  log("info", "Device cache loaded", {
    count: deviceMapCache.size
  });
}

/* ────────────────────────────────
   VEHICLE SYNC (SAFE UPSERT)
──────────────────────────────── */
export async function syncVehicles() {
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const rows = await conn.query(`
      SELECT uniqueid, name
      FROM device
      LIMIT 5000
    `);

    let count = 0;

    for (const r of rows) {
      const uid = String(r.uniqueid || "").trim();
      if (!uid) continue;

      await pgPool.query(`
        INSERT INTO devices (device_uid, name)
        VALUES ($1, $2)
        ON CONFLICT (device_uid)
        DO UPDATE SET name = EXCLUDED.name,
                      updated_at = NOW()
      `, [uid, r.name || uid]);

      count++;
    }

    log("info", "Vehicle sync complete", { vehicles: count });

  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });

  } finally {
    if (conn) {
      try { conn.release(); } catch {}
    }
  }
}

/* ────────────────────────────────
   CHECKPOINT
──────────────────────────────── */
const CHECKPOINT_KEY = "mariasync:lastEventId";
let lastCheckpoint = 0;

async function getCheckpoint() {
  if (lastCheckpoint) return lastCheckpoint;

  try {
    const r = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = $1",
      [CHECKPOINT_KEY]
    );

    lastCheckpoint = r.rows?.[0]
      ? Number(r.rows[0].value)
      : 0;

  } catch {
    lastCheckpoint = 0;
  }

  return lastCheckpoint;
}

async function saveCheckpoint(id) {
  lastCheckpoint = id;

  try {
    await pgPool.query(`
      INSERT INTO sync_checkpoints (key, value)
      VALUES ($1, $2)
      ON CONFLICT (key)
      DO UPDATE SET value = EXCLUDED.value,
                    updated_at = NOW()
    `, [CHECKPOINT_KEY, String(id)]);
  } catch {}
}

/* ────────────────────────────────
   REDIS CACHE (SAFE)
──────────────────────────────── */
async function cacheLatest(rows) {
  try {
    if (!redis?.pipeline) return;

    const pipe = redis.pipeline();

    for (const r of rows) {
      pipe.set(
        `pos:${r.device_uid}`,
        JSON.stringify(r),
        "EX",
        3600
      );
    }

    await pipe.exec();

  } catch (e) {
    log("warn", "Redis cache error", { error: e.message });
  }
}

/* ────────────────────────────────
   TELEMETRY SYNC (MAIN)
──────────────────────────────── */
export async function runMariaSync() {
  const lastId = await getCheckpoint();
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.id AS event_id,
        e.latitude,
        e.longitude,
        e.speed,
        e.course,
        e.devicetime,
        e.servertime
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.id > ?
      LIMIT 1500
    `, [lastId]);

    if (!rows.length) return;

    let maxId = lastId;
    const BATCH = 200;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);

      const vals = [];
      const params = [];
      let p = 1;

      for (const r of chunk) {
        const dev = deviceMapCache.get(String(r.device_uid));
        if (!dev) continue;

        const lat = N(r.latitude);
        const lon = N(r.longitude);
        if (lat == null || lon == null) continue;

        vals.push(
          `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`
        );

        params.push(
          dev.pgDeviceId,
          lat,
          lon,
          N(r.speed) || 0,
          N(r.course) || 0,
          r.devicetime,
          r.servertime
        );

        if (Number(r.event_id) > maxId) {
          maxId = Number(r.event_id);
        }
      }

      if (!vals.length) continue;

      await pgPool.query(`
        INSERT INTO telemetry
        (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
        VALUES ${vals.join(",")}
        ON CONFLICT DO NOTHING
      `, params);
    }

    await saveCheckpoint(maxId);
    await cacheLatest(rows);

    log("info", "Maria sync done", { count: rows.length });

  } catch (e) {
    log("error", "runMariaSync error", { error: e.message });

  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

/* ────────────────────────────────
   QUICK SYNC (LATEST POSITIONS)
──────────────────────────────── */
export async function runQuickSync() {
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const rows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed,
        e.course,
        e.devicetime
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      ORDER BY e.id DESC
      LIMIT 800
    `);

    if (!rows.length) return;

    const vals = [];
    const params = [];
    let p = 1;

    for (const r of rows) {
      const dev = deviceMapCache.get(String(r.device_uid));
      if (!dev) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);
      if (lat == null || lon == null) continue;

      vals.push(
        `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`
      );

      params.push(
        dev.pgDeviceId,
        lat,
        lon,
        N(r.speed) || 0,
        N(r.course) || 0,
        r.devicetime
      );
    }

    if (!vals.length) return;

    await pgPool.query(`
      INSERT INTO latest_positions
      (device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at)
      VALUES ${vals.join(",")}
      ON CONFLICT (device_id)
      DO UPDATE SET
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        speed_kph = EXCLUDED.speed_kph,
        heading = EXCLUDED.heading,
        device_time = EXCLUDED.device_time,
        updated_at = NOW()
    `, params);

    log("info", "quickSync done", { rows: rows.length });

  } catch (e) {
    log("error", "runQuickSync error", { error: e.message });

  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}