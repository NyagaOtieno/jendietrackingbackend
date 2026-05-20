// src/services/mariaSync.service.js
import dotenv from "dotenv";
dotenv.config();

import { createPool } from "mariadb";
import { pgPool }     from "../config/db.js";
import { redis }      from "../config/redis.js";

// ─────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────
const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

// ─────────────────────────────────────────────
// STATE
// lastEventId is seeded from PG on startup so restarts don't lose progress
// ─────────────────────────────────────────────
export let isSyncRunning = false;
let lastEventId = 0;

// ─────────────────────────────────────────────
// MARIA POOL
// ─────────────────────────────────────────────
export const mariaPool = createPool({
  host:            process.env.MARIADB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIADB_PORT     || 3306),
  user:            process.env.MARIADB_USER     || "root",
  password:        process.env.MARIADB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIADB_DATABASE || "uradi",
  connectionLimit: 10,
  connectTimeout:  20000,
  acquireTimeout:  20000,
});

// ─────────────────────────────────────────────
// SAFE NUMBER
// ─────────────────────────────────────────────
function N(v) {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ─────────────────────────────────────────────
// MARIA CONNECTION WITH RETRY
// ─────────────────────────────────────────────
async function getMariaConn(retries = 3) {
  for (let i = 0; i < retries; i++) {
    try { return await mariaPool.getConnection(); }
    catch (e) {
      if (i === retries - 1) throw e;
      await new Promise(r => setTimeout(r, 2000 * (i + 1)));
    }
  }
}

// ─────────────────────────────────────────────
// DEVICE MAP CACHE
// ─────────────────────────────────────────────
let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const { rows } = await pgPool.query(
    "SELECT id, device_uid, vehicle_id FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceMapCache = new Map(
    rows.map(r => [r.device_uid, { pgDeviceId: r.id, pgVehicleId: r.vehicle_id }])
  );
  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─────────────────────────────────────────────
// PERSIST CHECKPOINT IN PG
// Uses MAX(positionid) from devices table — already updated each sync cycle.
// Survives worker restarts without a separate table.
// ─────────────────────────────────────────────
async function loadCheckpoint() {
  try {
    const { rows } = await pgPool.query(
      "SELECT COALESCE(MAX(positionid), 0) AS last_id FROM devices WHERE positionid > 0"
    );
    const saved = Number(rows[0]?.last_id || 0);
    if (saved > 0) {
      lastEventId = saved;
      log("info", "Checkpoint restored from PG", { lastEventId });
    }
  } catch (e) {
    log("warn", "Checkpoint load failed", { error: e.message });
  }
}

// ─────────────────────────────────────────────
// REDIS CACHE
// ─────────────────────────────────────────────
async function cacheLatestPositions(rows) {
  if (!rows.length) return;
  try {
    const pipeline = redis.pipeline();
    for (const row of rows) {
      const uid = String(row.device_uid);
      pipeline.hset(`pos:${uid}`, {
        lat:      String(row.latitude  ?? 0),
        lon:      String(row.longitude ?? 0),
        speed:    String(row.speed_kph ?? 0),
        heading:  String(row.heading   ?? 0),
        time:     row.device_time ? new Date(row.device_time).toISOString() : "",
        received: row.received_at ? new Date(row.received_at).toISOString() : "",
      });
      pipeline.expire(`pos:${uid}`, 300);
    }
    await pipeline.exec();
  } catch (e) {
    log("warn", "Redis cache skipped", { error: e.message });
  }
}

// ─────────────────────────────────────────────
// VEHICLE SYNC — batched bulk upsert
// FIX: was 5000 individual PG queries → crashed PG connection → worker restart.
// Now uses batches of 100 rows = 50 queries total, 100× fewer round trips.
// ─────────────────────────────────────────────
export async function syncVehicles() {
  const conn = await getMariaConn();
  try {
    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus, d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL AND r.serial != ''
      LIMIT 5000
    `);
    conn.release();

    const BATCH = 100;
    let upserted = 0;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH).filter(r => String(r.serial || "").trim());

      if (!chunk.length) continue;

      // ── Bulk vehicle upsert ───────────────────────────────────────────
      const vVals = [], vParams = [];
      let p = 1;
      for (const row of chunk) {
        const serial = String(row.serial).trim();
        const plate  = String(row.reg_no || serial).substring(0, 30);
        vVals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        vParams.push(serial, plate, `Unit ${serial}`,
          String(row.vmodel || ""), String(row.pstatus || "inactive"));
      }
      try {
        await pgPool.query(
          `INSERT INTO vehicles (serial,plate_number,unit_name,model,status,created_at)
           VALUES ${vVals.join(",")}
           ON CONFLICT (serial) DO UPDATE SET
             plate_number = EXCLUDED.plate_number,
             unit_name    = EXCLUDED.unit_name,
             model        = EXCLUDED.model,
             status       = EXCLUDED.status`,
          vParams
        );
      } catch (e) {
        log("warn", "Vehicle batch failed", { batch: i, error: e.message });
        continue;
      }

      // ── Bulk device upsert (only rows with device_uid) ───────────────
      const withDevice = chunk.filter(r => r.device_uid);
      if (withDevice.length) {
        const dVals = [], dParams = [];
        let dp = 1;
        for (const row of withDevice) {
          dVals.push(`($${dp++},$${dp++},0)`);
          dParams.push(String(row.device_uid), String(row.serial).trim());
        }
        try {
          await pgPool.query(
            `INSERT INTO devices (device_uid,serial,positionid)
             VALUES ${dVals.join(",")}
             ON CONFLICT (device_uid) DO NOTHING`,
            dParams
          );
          // Link vehicle_id in bulk
          await pgPool.query(`
            UPDATE devices d
            SET vehicle_id = v.id
            FROM vehicles v
            WHERE v.serial = d.serial
              AND d.vehicle_id IS NULL
              AND d.device_uid = ANY($1::text[])
          `, [withDevice.map(r => String(r.device_uid))]);
        } catch (e) {
          log("warn", "Device batch failed", { error: e.message });
        }
      }

      upserted += chunk.length;
    }

    log("info", "Vehicle sync complete", { vehicles: upserted, total: rows.length });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
    try { conn?.release(); } catch {}
  }
}

// ─────────────────────────────────────────────
// QUICK SYNC — near-real-time, every 5s
//
// FIX: seeds lastEventId from PG positionid on startup so restarts
// don't go 10 minutes back. Uses id > ? (PRIMARY KEY) not servertime.
// Seeds to MAX(id) - 2000 on first run = ~2 min window, not 50 min.
// ─────────────────────────────────────────────
let isQuickRunning = false;

export async function runQuickSync() {
  if (isQuickRunning)       return;
  if (!deviceMapCache.size) { log("warn","quickSync skipped — device map empty"); return; }
  isQuickRunning = true;
  let conn;
  try {
    conn = await getMariaConn();

    // Seed checkpoint — use 2000 events back (≈2 min), not 50000
    if (lastEventId === 0) {
      await loadCheckpoint();
    }
    if (lastEventId === 0) {
      const r = await conn.query("SELECT COALESCE(MAX(id),0)+0 AS m FROM eventData");
      lastEventId = Math.max(0, Number(r[0].m) - 2000);
      log("info", "Checkpoint seeded from MariaDB MAX(id)", { lastEventId });
    }

    const sinceId = Math.max(0, lastEventId - 200); // small overlap to avoid gaps

    const rows = await conn.query(
      `SELECT
         d.uniqueid    AS device_uid,
         e.id+0        AS event_id,
         e.latitude+0  AS latitude,
         e.longitude+0 AS longitude,
         e.speed+0     AS speed_kph,
         e.course+0    AS heading,
         e.devicetime  AS device_time,
         e.servertime  AS received_at
       FROM eventData e
       INNER JOIN device d ON d.id = e.deviceid
       INNER JOIN (
         SELECT deviceid, MAX(id) AS max_id
         FROM eventData
         WHERE id > ?
           AND latitude  BETWEEN -90  AND 90
           AND longitude BETWEEN -180 AND 180
           AND NOT (latitude = 0 AND longitude = 0)
         GROUP BY deviceid
       ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
       LIMIT 5000`,
      [sinceId]
    );
    conn.release(); conn = null;

    if (!rows.length) {
      log("info", "quickSync — no new rows", { sinceId });
      return;
    }

    // ── Bulk upsert in chunks of 200 ─────────────────────────────────
    let upserted = 0;
    let maxId = lastEventId;

    for (let i = 0; i < rows.length; i += 200) {
      const chunk  = rows.slice(i, i + 200);
      const vals   = [], params = [];
      let p = 1;
      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;
        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(cached.pgDeviceId, lat, lon,
          N(r.speed_kph) ?? 0, N(r.heading) ?? 0,
          r.device_time, r.received_at);
        const eid = Number(r.event_id);
        if (eid > maxId) maxId = eid;
        upserted++;
      }
      if (!vals.length) continue;
      await pgPool.query(
        `INSERT INTO latest_positions
           (device_id,latitude,longitude,speed_kph,heading,device_time,received_at,updated_at)
         VALUES ${vals.join(",")}
         ON CONFLICT (device_id) DO UPDATE SET
           latitude    = EXCLUDED.latitude,
           longitude   = EXCLUDED.longitude,
           speed_kph   = EXCLUDED.speed_kph,
           heading     = EXCLUDED.heading,
           device_time = EXCLUDED.device_time,
           received_at = EXCLUDED.received_at,
           updated_at  = NOW()`,
        params
      );
    }

    // Advance checkpoint
    if (maxId > lastEventId) lastEventId = maxId;

    // Persist checkpoint to PG so restarts don't lose it
    await pgPool.query(
      `UPDATE devices SET positionid = GREATEST(positionid, $1)
       WHERE positionid < $1 AND positionid > 0
       LIMIT 1`,
      [lastEventId]
    ).catch(() => {}); // fire-and-forget, non-critical

    await cacheLatestPositions(rows);
    log("info", "quickSync complete", { upserted, sinceId, lastEventId });

  } catch (e) {
    log("error", "quickSync error", { error: e.message });
  } finally {
    try { conn?.release(); } catch {}
    isQuickRunning = false;
  }
}

// ─────────────────────────────────────────────
// FULL MARIA SYNC — history telemetry
// ─────────────────────────────────────────────
let isRunning = false;

export async function runMariaSync() {
  if (isRunning) return;
  isRunning = true;
  const start = Date.now();
  log("info", "MariaSync started");
  try {
    await loadDeviceMap();

    if (lastEventId === 0) await loadCheckpoint();

    const conn = await getMariaConn();
    let sinceId = lastEventId > 0 ? lastEventId : 0;

    if (sinceId === 0) {
      const r = await conn.query("SELECT COALESCE(MAX(id),0)+0 AS m FROM eventData");
      sinceId = Math.max(0, Number(r[0].m) - 5000);
    }

    const allRows = await conn.query(
      `SELECT
         e.id+0        AS event_id,
         d.uniqueid    AS device_uid,
         e.latitude+0  AS latitude,
         e.longitude+0 AS longitude,
         e.speed+0     AS speed_kph,
         e.course+0    AS heading,
         e.devicetime  AS device_time,
         e.servertime  AS received_at
       FROM eventData e
       INNER JOIN device d ON d.id = e.deviceid
       WHERE e.id > ?
         AND e.latitude  BETWEEN -90  AND 90
         AND e.longitude BETWEEN -180 AND 180
         AND NOT (e.latitude = 0 AND e.longitude = 0)
       ORDER BY e.id ASC
       LIMIT 500`,
      [sinceId]
    );
    conn.release();

    log("info", "Full sync rows", { count: allRows.length, sinceId });

    let inserted = 0;
    let maxId = lastEventId;

    for (const row of allRows) {
      const cached = deviceMapCache.get(String(row.device_uid));
      if (!cached) continue;
      const lat = N(row.latitude), lon = N(row.longitude);
      if (lat == null || lon == null) continue;
      try {
        await pgPool.query(
          `INSERT INTO telemetry
             (device_id,latitude,longitude,speed_kph,heading,device_time,received_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT DO NOTHING`,
          [cached.pgDeviceId, lat, lon,
           N(row.speed_kph) ?? 0, N(row.heading) ?? 0,
           row.device_time, row.received_at]
        );
        const eid = Number(row.event_id);
        if (eid > maxId) maxId = eid;
        inserted++;
      } catch {}
    }

    if (maxId > lastEventId) lastEventId = maxId;
    log("info", "MariaSync completed", { telemetry: inserted, ms: Date.now() - start });
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isRunning = false;
  }
}

export async function initMariaSync() {
  await loadCheckpoint();
  await loadDeviceMap();
  log("info", "MariaSync initialized", { lastEventId });
}