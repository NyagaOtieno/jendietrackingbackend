// src/services/liveSync.service.js
import { mariaPool } from "./mariaSync.service.js";
import { pgPool }    from "../config/db.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function toNum(v) {
  if (v == null) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// Device uid → postgres device_id (refreshed every 5 min)
let _deviceCache   = null;
let _deviceCacheAt = 0;

async function deviceCache() {
  if (_deviceCache && Date.now() - _deviceCacheAt < 5 * 60_000) return _deviceCache;
  const res = await pgPool.query(
    "SELECT id, device_uid FROM devices WHERE device_uid IS NOT NULL"
  );
  _deviceCache   = new Map(res.rows.map((r) => [String(r.device_uid), r.id]));
  _deviceCacheAt = Date.now();
  log("info", "liveSync: device cache built", { count: _deviceCache.size });
  return _deviceCache;
}

let _running = false;

export async function runLiveSync() {
  if (_running) return;
  _running = true;
  let conn;

  try {
    const cache = await deviceCache();

    // Devices that reported in the last 2 minutes
    const since = new Date(Date.now() - 2 * 60_000)
      .toISOString().slice(0, 19).replace("T", " ");

    conn = await mariaPool.getConnection();

    // NO LIMIT — fetch ALL active devices
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
    `, [since]);

    conn.release(); conn = null;

    if (!rows.length) return;

    // Batch upsert into latest_positions — 200 rows at a time
    let upserted = 0, skipped = 0;

    for (let i = 0; i < rows.length; i += 200) {
      const chunk  = rows.slice(i, i + 200);
      const vals   = [];
      const params = [];
      let   p      = 1;

      for (const r of chunk) {
        const pgId = cache.get(String(r.device_uid));
        if (!pgId) { skipped++; continue; }
        const lat = toNum(r.latitude), lon = toNum(r.longitude);
        if (lat == null || lon == null) { skipped++; continue; }

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW())`);
        params.push(pgId, lat, lon,
                    toNum(r.speed_kph) ?? 0,
                    toNum(r.heading)   ?? 0,
                    r.device_time ?? r.received_at ?? null);
        upserted++;
      }

      if (!vals.length) continue;

      await pgPool.query(`
        INSERT INTO latest_positions
          (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
        VALUES ${vals.join(",")}
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
    _running = false;
  }
}