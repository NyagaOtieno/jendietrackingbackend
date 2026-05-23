// src/services/mariaSync.service.js
import { createPool } from "mariadb";
import { pgPool } from "../config/db.js";
import { redis } from "../config/redis.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v == null) return null;
  const n = typeof v === "bigint" ? Number(v) : Number(v);
  return Number.isFinite(n) ? n : null;
}

// ─── MariaDB pool ─────────────────────────────────────────────
export const mariaPool = createPool({
  host: process.env.MARIA_DB_HOST || "18.218.110.222",
  port: Number(process.env.MARIA_DB_PORT) || 3306,
  user: process.env.MARIA_DB_USER || "root",
  password: process.env.MARIA_DB_PASSWORD || "nairobiyetu",
  database: process.env.MARIA_DB_NAME || "uradi",
  connectionLimit: 3,
  connectTimeout: 15000,
  acquireTimeout: 20000,
  resetAfterUse: true,
});

export const getMariaConn = () => mariaPool.getConnection();

// ─── Device cache ─────────────────────────────────────────────
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(
    "SELECT d.id, d.device_uid, d.vehicle_id FROM devices d WHERE d.device_uid IS NOT NULL"
  );

  deviceMapCache.clear();

  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid), {
      pgDeviceId: r.id,
      pgVehicleId: r.vehicle_id,
    });
  }

  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─── FIXED VEHICLE SYNC (NO position table) ─────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await getMariaConn();
    const limit = Number(process.env.VEHICLE_BATCH || 5000);

    const rows = await conn.query(`
      SELECT
        r.serial AS serial,
        d.id AS device_id,
        d.uniqueid AS device_uid,
        d.name AS device_name
      FROM registration r
      JOIN device d
        ON d.uniqueid = CONCAT(r.serial, '0')
      LIMIT ?
    `, [limit]);

    conn.release();
    conn = null;

    let count = 0;

    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
      if (!uid) continue;

   await pgPool.query(`
  INSERT INTO devices (device_uid, label)
  VALUES ($1, $2)
  ON CONFLICT (device_uid)
  DO UPDATE SET label = EXCLUDED.label
`, [uid, r.device_name || uid]);

      count++;
    }

    log("info", "Vehicle sync complete", {
      vehicles: count,
      total: rows.length,
    });

  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─── Checkpoint ───────────────────────────────────────────────
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = 0;

async function getCheckpoint() {
  if (_lastEventId) return _lastEventId;

  try {
    const r = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = $1",
      [CHECKPOINT_KEY]
    );

    _lastEventId = r.rows[0] ? Number(r.rows[0].value) : 0;
  } catch {
    _lastEventId = 0;
  }

  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;

  try {
    await pgPool.query(`
      INSERT INTO sync_checkpoints (key, value)
      VALUES ($1, $2)
      ON CONFLICT (key)
      DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    `, [CHECKPOINT_KEY, String(id)]);
  } catch {}
}

// ─── Redis cache ───────────────────────────────────────────────
async function cacheLatestPositions(rows) {
  try {
    if (!redis?.pipeline) return;

    const pipe = redis.pipeline();

    for (const r of rows) {
      pipe.set(`pos:${r.device_uid}`, JSON.stringify(r), "EX", 3600);
    }

    await pipe.exec();
  } catch (e) {
    log("warn", "Redis cache error", { error: e.message });
  }
}

// ─── TELEMETRY SYNC (FIXED) ────────────────────────────────────
export async function syncTelemetry() {
  const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 300);
  const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 500);
  const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 2);

  const lastEventId = await getCheckpoint();

  const sinceMs = Date.now() - HISTORY_HOURS * 3600000;
  const sinceStr = new Date(sinceMs)
    .toISOString()
    .slice(0, 19)
    .replace("T", " ");

  let conn;

  try {
    conn = await getMariaConn();

    // ── Latest per device ──
    const latestRows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at,
        e.id AS event_id
      FROM eventData e
      JOIN device d ON d.id = e.deviceid
      JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE id > ?
          AND servertime > ?
          AND latitude BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest
      ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      LIMIT ?
    `, [lastEventId, sinceStr, DEVICE_BATCH]);

    // ── History ──
    const allRows = await conn.query(`
      SELECT
        d.uniqueid AS device_uid,
        e.id AS event_id,
        e.latitude,
        e.longitude,
        e.speed AS speed_kph,
        e.course AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      JOIN device d ON d.id = e.deviceid
      WHERE e.id > ?
        AND e.servertime > ?
        AND e.latitude BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `, [lastEventId, sinceStr, EVENTS_BATCH]);

    conn.release();
    conn = null;

    // ── Latest positions upsert ──
    {
      const BATCH = 200;
      let count = 0;

      for (let i = 0; i < latestRows.length; i += BATCH) {
        const chunk = latestRows.slice(i, i + BATCH);

        const vals = [];
        const params = [];
        let p = 1;

        for (const row of chunk) {
          const cached = deviceMapCache.get(String(row.device_uid));
          if (!cached) continue;

          const lat = N(row.latitude);
          const lon = N(row.longitude);
          if (lat == null || lon == null) continue;

          vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`);

          params.push(
            cached.pgDeviceId,
            lat,
            lon,
            N(row.speed_kph) ?? 0,
            N(row.heading) ?? 0,
            row.device_time
          );

          count++;
        }

        if (!vals.length) continue;

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
            received_at = NOW(),
            updated_at = NOW()
        `, params);
      }

      log("info", "latest_positions updated", { count });
    }

    await cacheLatestPositions(latestRows);

    // ── Telemetry history ──
    let maxId = lastEventId;
    let inserted = 0;

    for (let i = 0; i < allRows.length; i += 200) {
      const chunk = allRows.slice(i, i + 200);

      const vals = [];
      const params = [];
      let p = 1;

      for (const row of chunk) {
        const cached = deviceMapCache.get(String(row.device_uid));
        if (!cached) continue;

        const lat = N(row.latitude);
        const lon = N(row.longitude);
        if (lat == null || lon == null) continue;

        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);

        params.push(
          cached.pgDeviceId,
          lat,
          lon,
          N(row.speed_kph) ?? 0,
          N(row.heading) ?? 0,
          row.device_time,
          row.received_at
        );

        inserted++;

        if (Number(row.event_id) > maxId) maxId = Number(row.event_id);
      }

      if (!vals.length) continue;

      await pgPool.query(`
        INSERT INTO telemetry
        (device_id, latitude, longitude, speed_kph, heading, device_time, received_at)
        VALUES ${vals.join(",")}
        ON CONFLICT DO NOTHING
      `, params);
    }

    if (maxId > lastEventId) await saveCheckpoint(maxId);

    log("info", "Telemetry sync done", {
      inserted,
      latest: latestRows.length,
    });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}
