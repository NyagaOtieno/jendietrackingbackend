// src/services/liveSync.service.js
import { mariaPool }      from "./mariaSync.service.js";
import { pgPool }         from "../config/db.js";          // ← was wrong before

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v === null || v === undefined) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// Device uid → postgres device_id  (rebuilt every 5 min)
let deviceCache   = null;
let deviceCacheAt = 0;
const CACHE_TTL   = 5 * 60_000;

async function getDeviceCache() {
  if (deviceCache && Date.now() - deviceCacheAt < CACHE_TTL) return deviceCache;
  const res = await pgPool.query(
    "SELECT id, device_uid FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceCache   = new Map(res.rows.map((r) => [String(r.device_uid), r.id]));
  deviceCacheAt = Date.now();
  return deviceCache;
}

let isRunning = false;

export async function runLiveSync() {
  if (isRunning) return;
  isRunning = true;

  let conn;
  try {
    const cache = await getDeviceCache();
    const since = new Date(Date.now() - 2 * 60_000)
      .toISOString().slice(0, 19).replace("T", " ");

    conn = await mariaPool.getConnection();

    // One latest row per device active in the last 2 minutes
    const rows = await conn.query(`
      SELECT
        d.uniqueid   AS device_uid,
        e.latitude,
        e.longitude,
        e.speed      AS speed_kph,
        e.course     AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
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
              AND e.id       = latest.max_id
      LIMIT 5000
    `, [since]);

    conn.release();
    conn = null;

    if (!rows.length) {
      log("info", "liveSync: no active devices", { since });
      return;
    }

    // Bulk upsert in chunks of 200
    let upserted = 0, skipped = 0;
    const BATCH  = 200;

    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk  = rows.slice(i, i + BATCH);
      const values = [];
      const params = [];
      let   p      = 1;

      for (const row of chunk) {
        const pgId = cache.get(String(row.device_uid));
        if (!pgId) { skipped++; continue; }
        const lat  = N(row.latitude);
        const lon  = N(row.longitude);
        if (lat === null || lon === null) { skipped++; continue; }

        values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(pgId, lat, lon, N(row.speed_kph) ?? 0, N(row.heading) ?? 0,
                    row.device_time ?? row.received_at ?? null);
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
    if (conn) try { conn.release(); } catch {}
    isRunning = false;
  }
}