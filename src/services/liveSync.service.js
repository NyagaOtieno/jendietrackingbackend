// src/services/liveSync.service.js
//
// Lightweight "heartbeat" sync — runs every 15 s.
// ONLY fetches the single most-recent position per active device
// (devices that reported in the last 2 minutes) and upserts into
// latest_positions.  Does NOT write to the telemetry history table.
//
// This is what makes vehicles move on the map in near-real-time.
// The full runMariaSync() still handles history every 30 min.

import { mariaPool, pgPool } from "./mariaSync.service.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// Device uid → postgres device_id cache (shared with mariaSync via module scope)
let deviceCache = null;
let deviceCacheAt = 0;
const CACHE_TTL = 5 * 60_000; // rebuild cache every 5 min

async function getDeviceCache() {
  if (deviceCache && Date.now() - deviceCacheAt < CACHE_TTL) return deviceCache;
  const res = await pgPool.query(
    "SELECT id, device_uid FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceCache = new Map(res.rows.map((r) => [String(r.device_uid), r.id]));
  deviceCacheAt = Date.now();
  return deviceCache;
}

let isRunning = false;

export async function runLiveSync() {
  if (isRunning) return; // skip if previous tick still running
  isRunning = true;

  let conn;
  try {
    const cache   = await getDeviceCache();
    const sinceMs = Date.now() - 2 * 60_000; // last 2 minutes
    const since   = new Date(sinceMs).toISOString().slice(0, 19).replace("T", " ");

    conn = await mariaPool.getConnection();

    // One latest row per device — only devices active in the last 2 minutes.
    // Uses MAX(id) subquery which hits the primary key index efficiently.
    const rows = await conn.query(`
      SELECT
        d.uniqueid      AS device_uid,
        e.latitude,
        e.longitude,
        e.speed         AS speed_kph,
        e.course        AS heading,
        e.devicetime    AS device_time,
        e.servertime    AS received_at
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE servertime > ?
          AND latitude  BETWEEN -90  AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest ON e.deviceid = latest.deviceid
                AND e.id      = latest.max_id
      LIMIT 5000
    `, [since]);

    conn.release();
    conn = null;

    if (!rows.length) {
      log("info", "liveSync: no active devices", { since });
      return;
    }

    let upserted = 0;
    let skipped  = 0;

    // Bulk upsert in batches of 200 to avoid overwhelming PG
    const BATCH = 200;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);

      // Build a single multi-row upsert for the batch
      const values  = [];
      const params  = [];
      let   pIdx    = 1;

      for (const row of chunk) {
        const pgId = cache.get(String(row.device_uid));
        if (!pgId) { skipped++; continue; }

        const lat = N(row.latitude);
        const lon = N(row.longitude);
        if (lat === null || lon === null) { skipped++; continue; }

        values.push(
          `($${pIdx++}, $${pIdx++}, $${pIdx++}, $${pIdx++}, $${pIdx++}, $${pIdx++}, NOW())`
        );
        params.push(
          pgId,
          lat,
          lon,
          N(row.speed_kph)   ?? 0,
          N(row.heading)     ?? 0,
          row.device_time    ?? row.received_at ?? null
        );
        upserted++;
      }

      if (!values.length) continue;

      await pgPool.query(`
        INSERT INTO latest_positions
          (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
        VALUES ${values.join(",")}
        ON CONFLICT (device_id) DO UPDATE SET
          latitude    = EXCLUDED.latitude,
          longitude   = EXCLUDED.longitude,
          speed_kph   = EXCLUDED.speed_kph,
          heading     = EXCLUDED.heading,
          device_time = EXCLUDED.device_time,
          received_at = EXCLUDED.received_at
        WHERE EXCLUDED.device_time > latest_positions.device_time
           OR latest_positions.device_time IS NULL
      `, params);
    }

    log("info", "liveSync complete", { active: rows.length, upserted, skipped });

  } catch (err) {
    log("error", "liveSync error", { error: err.message });
  } finally {
    if (conn) conn.release();
    isRunning = false;
  }
}