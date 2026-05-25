// src/services/mariaSync.service.js
import { createPool } from "mariadb";
import { pgPool }     from "../config/db.js";
import { setLatestPositionsBatch } from "./redisLatestPosition.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

function N(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// ─── MariaDB pool ─────────────────────────────────────────────────────────────
export const mariaPool = createPool({
  host:            process.env.MARIA_DB_HOST     || "18.218.110.222",
  port:     Number(process.env.MARIA_DB_PORT)    || 3306,
  user:            process.env.MARIA_DB_USER     || "root",
  password:        process.env.MARIA_DB_PASSWORD || "nairobiyetu",
  database:        process.env.MARIA_DB_NAME     || "uradi",
  connectionLimit: 3,
  connectTimeout:  15000,
  acquireTimeout:  20000,
});

export const getMariaConn = () => mariaPool.getConnection();

// ─── Device cache: device.uniqueid → { pgDeviceId } ──────────────────────────
export let deviceMapCache = new Map();

export async function loadDeviceMap() {
  const res = await pgPool.query(
    "SELECT id, device_uid FROM devices WHERE device_uid IS NOT NULL"
  );
  deviceMapCache.clear();
  for (const r of res.rows) {
    deviceMapCache.set(String(r.device_uid), { pgDeviceId: r.id });
  }
  log("info", "Device cache loaded", { count: deviceMapCache.size });
}

// ─── Checkpoint ───────────────────────────────────────────────────────────────
const CHECKPOINT_KEY = "mariasync:lastEventId";
let _lastEventId = 0;

async function getCheckpoint() {
  if (_lastEventId) return _lastEventId;
  try {
    const r = await pgPool.query(
      "SELECT value FROM sync_checkpoints WHERE key = $1", [CHECKPOINT_KEY]
    );
    _lastEventId = r.rows[0] ? Number(r.rows[0].value) : 0;
    if (!_lastEventId) log("info", "Checkpoint init", { lastEventId: 0 });
  } catch { _lastEventId = 0; }
  return _lastEventId;
}

async function saveCheckpoint(id) {
  _lastEventId = id;
  try {
    await pgPool.query(`
      INSERT INTO sync_checkpoints (key, value)
      VALUES ($1, $2)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
    `, [CHECKPOINT_KEY, String(id)]);
  } catch {}
}

// ─── Vehicle sync ─────────────────────────────────────────────────────────────
// registration.serial + '0' = device.uniqueid (Traccar convention)
export async function syncVehicles() {
  let conn;
  try {
    conn = await getMariaConn();
    const limit = Number(process.env.VEHICLE_BATCH || 5000);

    const rows = await conn.query(`
      SELECT d.uniqueid AS device_uid, d.name AS device_name
      FROM device d
      WHERE d.uniqueid IS NOT NULL AND d.uniqueid != ''
      LIMIT ?
    `, [limit]);

    conn.release(); conn = null;

    let count = 0;
    for (const r of rows) {
      const uid = String(r.device_uid || "").trim();
      if (!uid) continue;
      await pgPool.query(`
        INSERT INTO devices (device_uid, label)
        VALUES ($1, $2)
        ON CONFLICT (device_uid) DO UPDATE SET label = EXCLUDED.label, updated_at = NOW()
      `, [uid, r.device_name || uid]);
      count++;
    }
    log("info", "Vehicle sync complete", { vehicles: count, total: rows.length });
  } catch (e) {
    log("error", "Vehicle sync error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─── Telemetry sync ───────────────────────────────────────────────────────────
//
// BUG FIXED: old code used `e.deviceid AS device_uid` (numeric MariaDB internal ID)
// as the cache key, but deviceMapCache is keyed by device.uniqueid (GPS serial).
// Result: cached was always undefined → nothing ever inserted.
//
// FIX: JOIN device d ON d.id = e.deviceid → use d.uniqueid as the lookup key.
// Also added bulk upsert to latest_positions (was missing entirely).
//
export async function syncTelemetry() {
  const DEVICE_BATCH  = Number(process.env.DEVICE_BATCH  || 300);
  const EVENTS_BATCH  = Number(process.env.EVENTS_BATCH  || 500);
  const HISTORY_HOURS = Number(process.env.HISTORY_HOURS || 2);

  const lastEventId = await getCheckpoint();
  const sinceStr = new Date(Date.now() - HISTORY_HOURS * 3_600_000)
    .toISOString().slice(0, 19).replace("T", " ");

  let conn;
  try {
    conn = await getMariaConn();
    log("info", "Fetching events since id", { lastEventId });

    // QUERY A: one latest row per device (for latest_positions upsert)
    const latestRows = await conn.query(`
      SELECT
        d.uniqueid   AS device_uid,
        e.id         AS event_id,
        e.latitude,
        e.longitude,
        e.speed      AS speed_kph,
        e.course     AS heading,
        e.devicetime AS device_time
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      INNER JOIN (
        SELECT deviceid, MAX(id) AS max_id
        FROM eventData
        WHERE id > ? AND servertime > ?
          AND latitude  BETWEEN -90 AND 90
          AND longitude BETWEEN -180 AND 180
          AND NOT (latitude = 0 AND longitude = 0)
        GROUP BY deviceid
      ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      LIMIT ?
    `, [lastEventId, sinceStr, DEVICE_BATCH]);

    log("info", "Latest rows from MariaDB", { count: latestRows.length });

    // QUERY B: all rows since checkpoint (for telemetry history)
    const allRows = await conn.query(`
      SELECT
        d.uniqueid   AS device_uid,
        e.id         AS event_id,
        e.latitude,
        e.longitude,
        e.speed      AS speed_kph,
        e.course     AS heading,
        e.devicetime AS device_time,
        e.servertime AS received_at
      FROM eventData e
      INNER JOIN device d ON d.id = e.deviceid
      WHERE e.id > ? AND e.servertime > ?
        AND e.latitude  BETWEEN -90 AND 90
        AND e.longitude BETWEEN -180 AND 180
        AND NOT (e.latitude = 0 AND e.longitude = 0)
      ORDER BY e.id ASC
      LIMIT ?
    `, [lastEventId, sinceStr, EVENTS_BATCH]);

    log("info", "All telemetry rows from MariaDB", { count: allRows.length });
    conn.release(); conn = null;

    // ── Bulk upsert latest_positions ─────────────────────────────────────────
    // received_at = NOW() avoids timezone issues with MariaDB servertime
    {
      let count = 0;
      for (let i = 0; i < latestRows.length; i += 200) {
        const chunk = latestRows.slice(i, i + 200);
        const vals = [], params = [];
        let p = 1;
        for (const r of chunk) {
          const cached = deviceMapCache.get(String(r.device_uid));
          if (!cached) continue;
          const lat = N(r.latitude), lon = N(r.longitude);
          if (lat == null || lon == null) continue;
          vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`);
          params.push(cached.pgDeviceId, lat, lon,
            N(r.speed_kph) ?? 0, N(r.heading) ?? 0, r.device_time);
          count++;
        }
        if (!vals.length) continue;
        await pgPool.query(`
          INSERT INTO latest_positions
            (device_id,latitude,longitude,speed_kph,heading,device_time,received_at,updated_at)
          VALUES ${vals.join(",")}
          ON CONFLICT (device_id) DO UPDATE SET
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            speed_kph   = EXCLUDED.speed_kph,
            heading     = EXCLUDED.heading,
            device_time = EXCLUDED.device_time,
            received_at = NOW(),
            updated_at  = NOW()
        `, params);
      }
      log("info", "latest_positions upserted", { count });
    }
// ── Bulk upsert latest_positions + Redis realtime ─────────────
{
  let count = 0;
  const redisPositions = [];

  for (let i = 0; i < latestRows.length; i += 200) {
    const chunk = latestRows.slice(i, i + 200);

    const vals = [];
    const params = [];
    let p = 1;

    for (const r of chunk) {
      const cached = deviceMapCache.get(String(r.device_uid));

      if (!cached) continue;

      const lat = N(r.latitude);
      const lon = N(r.longitude);

      if (lat == null || lon == null) continue;

      vals.push(
        `($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`
      );

      params.push(
        cached.pgDeviceId,
        lat,
        lon,
        N(r.speed_kph) ?? 0,
        N(r.heading) ?? 0,
        r.device_time
      );

      redisPositions.push({
        deviceId: cached.pgDeviceId,
        lat,
        lon,
        speed: N(r.speed_kph) ?? 0,
        heading: N(r.heading) ?? 0,
        dt: new Date(r.device_time)
      });

      count++;
    }

    if (!vals.length) continue;

    await pgPool.query(`
      INSERT INTO latest_positions
      (
        device_id,
        latitude,
        longitude,
        speed_kph,
        heading,
        device_time,
        received_at,
        updated_at
      )
      VALUES ${vals.join(",")}
      ON CONFLICT(device_id)
      DO UPDATE SET
        latitude=EXCLUDED.latitude,
        longitude=EXCLUDED.longitude,
        speed_kph=EXCLUDED.speed_kph,
        heading=EXCLUDED.heading,
        device_time=EXCLUDED.device_time,
        received_at=NOW(),
        updated_at=NOW()
    `, params);
  }

  // Redis batch update
  if (redisPositions.length) {
      await setLatestPositionsBatch(redisPositions);

      log(
        "info",
        "Redis realtime updated",
        {count:redisPositions.length}
      );
  }

  log(
      "info",
      "latest_positions upserted",
      {count}
  );
}
    // ── Bulk insert telemetry history ─────────────────────────────────────────
    let maxId = lastEventId, inserted = 0;
    for (let i = 0; i < allRows.length; i += 200) {
      const chunk = allRows.slice(i, i + 200);
      const vals = [], params = [];
      let p = 1;
      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;
        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++})`);
        params.push(cached.pgDeviceId, lat, lon,
          N(r.speed_kph) ?? 0, N(r.heading) ?? 0,
          r.device_time, r.received_at);
        inserted++;
        if (Number(r.event_id) > maxId) maxId = Number(r.event_id);
      }
      if (!vals.length) continue;
      await pgPool.query(`
        INSERT INTO telemetry
          (device_id,latitude,longitude,speed_kph,heading,device_time,received_at)
        VALUES ${vals.join(",")}
        ON CONFLICT DO NOTHING
      `, params);
    }

    if (maxId > lastEventId) await saveCheckpoint(maxId);
    log("info", "Telemetry sync done", { inserted, latest: latestRows.length });

  } catch (e) {
    log("error", "syncTelemetry error", { error: e.message });
  } finally {
    if (conn) try { conn.release(); } catch {}
  }
}

// ─── runMariaSync ─────────────────────────────────────────────────────────────
let _syncRunning = false;
export async function runMariaSync() {
  if (_syncRunning) { log("warn", "MariaSync skipped"); return; }
  _syncRunning = true;
  const t0 = Date.now();
  log("info", "MariaSync started");
  try {
    await loadDeviceMap();
    await syncTelemetry();
    log("info", "MariaSync completed", { ms: Date.now() - t0 });
  } catch (e) { log("error", "MariaSync failed", { error: e.message }); }
  finally { _syncRunning = false; }
}

// ─── runQuickSync ─────────────────────────────────────────────────────────────
// Only fetches devices active in last 2 min → bulk upsert latest_positions in ~1s
// JOIN device d ON d.id = e.deviceid → d.uniqueid is the correct cache key
let _quickRunning = false;
export async function runQuickSync() {
  if (_quickRunning) return;
  if (!deviceMapCache.size) return;
  _quickRunning = true;
  let conn;
  try {
    conn = await getMariaConn();
    const since = new Date(Date.now() - 2 * 60_000)
      .toISOString().slice(0, 19).replace("T", " ");

    const rows = await conn.query(`
      SELECT
        d.uniqueid   AS device_uid,
        e.latitude,
        e.longitude,
        e.speed      AS speed_kph,
        e.course     AS heading,
        e.devicetime AS device_time
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
      ) latest ON e.deviceid = latest.deviceid AND e.id = latest.max_id
      LIMIT 5000
    `, [since]);

    conn.release(); conn = null;
    if (!rows.length) { log("info", "quickSync: 0 active"); return; }

    let upserted = 0;
    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);
      const vals = [], params = [];
      let p = 1;
      for (const r of chunk) {
        const cached = deviceMapCache.get(String(r.device_uid));
        if (!cached) continue;
        const lat = N(r.latitude), lon = N(r.longitude);
        if (lat == null || lon == null) continue;
        vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NOW(),NOW())`);
        params.push(cached.pgDeviceId, lat, lon,
          N(r.speed_kph) ?? 0, N(r.heading) ?? 0, r.device_time);
        upserted++;
      }
      if (!vals.length) continue;
      await pgPool.query(`
        INSERT INTO latest_positions
          (device_id,latitude,longitude,speed_kph,heading,device_time,received_at,updated_at)
        VALUES ${vals.join(",")}
        ON CONFLICT (device_id) DO UPDATE SET
          latitude    = EXCLUDED.latitude,
          longitude   = EXCLUDED.longitude,
          speed_kph   = EXCLUDED.speed_kph,
          heading     = EXCLUDED.heading,
          device_time = EXCLUDED.device_time,
          received_at = NOW(),
          updated_at  = NOW()
      `, params);
    }
    log("info", "quickSync complete", { active: rows.length, upserted });
  } catch (e) { log("error", "quickSync error", { error: e.message }); }
  finally {
    if (conn) try { conn.release(); } catch {}
    _quickRunning = false;
  }
}