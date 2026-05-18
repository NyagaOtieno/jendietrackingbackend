// src/controllers/positions.controller.js
import { query } from "../config/db.js";
import * as geo from "../services/reverseGeocode.js";
import { normalizeLimit } from "../utils/sql.js";
import { isPrivilegedRole } from "../middleware/auth.js";

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
async function safeLocationName(lat, lon) {
  try {
    if (!lat || !lon) return "Unknown location";
    return (await geo.getLocationName(lat, lon)) || "Unknown location";
  } catch {
    return "Unknown location";
  }
}

function isPrivileged(req) {
  return isPrivilegedRole(req?.user?.role || "guest");
}

// ─────────────────────────────────────────────
// BATCH: latest positions (paginated)
// ─────────────────────────────────────────────
async function loadLatestFromDb(req, { limit = 5000, offset = 0 } = {}) {
  const params = [];

  let sql = `
    SELECT
      d.device_uid        AS "deviceUid",
      d.id                AS "deviceId",
      lp.latitude         AS lat,
      lp.longitude        AS lon,
      COALESCE(lp.speed_kph, 0) AS "speedKph",
      lp.heading,
      lp.device_time      AS "deviceTime",
      lp.received_at      AS "receivedAt",
      COALESCE(v.id, 0)           AS "vehicleId",
      COALESCE(v.plate_number,'') AS "plateNumber",
      COALESCE(v.unit_name,'')    AS "unitName",
      COALESCE(v.account_id, 0)   AS "accountId"
    FROM latest_positions lp
    LEFT JOIN devices  d ON d.id = lp.device_id
    LEFT JOIN vehicles v ON v.id = d.vehicle_id
  `;

  if (!isPrivileged(req)) {
    const accId = Number(req?.user?.accountId || 0);
    sql += ` WHERE (v.account_id = $1 OR v.account_id IS NULL) `;
    params.push(accId);
  }

  const safeLimit  = Math.min(Math.max(parseInt(limit)  || 5000, 1), 10_000);
  const safeOffset = Math.max(parseInt(offset) || 0, 0);

  sql += ` ORDER BY lp.received_at DESC
           LIMIT  $${params.length + 1}
           OFFSET $${params.length + 2}`;

  params.push(safeLimit, safeOffset);

  const result = await query(sql, params);
  return result.rows || [];
}

// ─────────────────────────────────────────────
// SINGLE VEHICLE: latest position on-demand
// FIX: DO NOT CAST vehicleId TO Number (prevents int overflow 22003)
// ─────────────────────────────────────────────
async function loadVehicleLatestFromDb(req, vehicleId) {
  try {
    const vId = String(vehicleId || "").trim(); // SAFE BIGINT

    if (!vId) return null;

    const lpSql = `
      SELECT
        d.device_uid AS "deviceUid",
        d.id AS "deviceId",
        lp.latitude AS lat,
        lp.longitude AS lon,
        COALESCE(lp.speed_kph, 0) AS "speedKph",
        lp.heading,
        lp.device_time AS "deviceTime",
        lp.received_at AS "receivedAt",
        v.id AS "vehicleId",
        COALESCE(v.plate_number,'') AS "plateNumber",
        COALESCE(v.unit_name,'') AS "unitName",
        COALESCE(v.account_id, 0) AS "accountId"
      FROM latest_positions lp
      INNER JOIN devices d ON d.id = lp.device_id
      INNER JOIN vehicles v ON v.id = d.vehicle_id
      WHERE v.id = $1
      LIMIT 1
    `;

    const lpResult = await query(lpSql, [vId]);
    if (lpResult.rows.length) return lpResult.rows[0];

    const telSql = `
      SELECT
        d.device_uid AS "deviceUid",
        d.id AS "deviceId",
        t.latitude AS lat,
        t.longitude AS lon,
        COALESCE(t.speed_kph, 0) AS "speedKph",
        t.heading,
        t.device_time AS "deviceTime",
        t.received_at AS "receivedAt",
        v.id AS "vehicleId",
        COALESCE(v.plate_number,'') AS "plateNumber",
        COALESCE(v.unit_name,'') AS "unitName",
        COALESCE(v.account_id, 0) AS "accountId"
      FROM telemetry t
      INNER JOIN devices d ON d.id = t.device_id
      INNER JOIN vehicles v ON v.id = d.vehicle_id
      WHERE v.id = $1
      ORDER BY t.received_at DESC
      LIMIT 1
    `;

    const telResult = await query(telSql, [vId]);
    return telResult.rows[0] || null;

  } catch (err) {
    // ─────────────────────────────────────────────
    // IMPROVED ERROR HANDLING
    // ─────────────────────────────────────────────
    console.error("loadVehicleLatestFromDb error:", err);

    if (err.code === "22003") {
      return {
        error: true,
        message: "Vehicle ID is too large or invalid for database integer type",
        hint: "Backend expected BIGINT-compatible ID. No schema change required."
      };
    }

    return null;
  }
}

// ─────────────────────────────────────────────
// HISTORY
// ─────────────────────────────────────────────
async function loadHistoryFromDb(req, deviceUid, limit, from, to) {
  const clauses = [`d.device_uid = $1`];
  const params  = [deviceUid];
  let index = 2;

  if (!isPrivileged(req)) {
    clauses.push(`v.account_id = $${index++}`);
    params.push(Number(req?.user?.accountId || 0));
  }
  if (from) { clauses.push(`t.received_at >= $${index++}`); params.push(from); }
  if (to)   { clauses.push(`t.received_at <= $${index++}`); params.push(to); }

  params.push(limit);

  const sql = `
    SELECT
      t.id::text          AS id,
      d.device_uid        AS "deviceUid",
      t.latitude          AS lat,
      t.longitude         AS lon,
      COALESCE(t.speed_kph, 0) AS "speedKph",
      t.heading,
      t.device_time       AS "deviceTime",
      t.received_at       AS "receivedAt",
      v.id                        AS "vehicleId",
      v.plate_number              AS "plateNumber",
      COALESCE(v.unit_name,'')    AS "unitName",
      COALESCE(v.account_id, 0)   AS "accountId"
    FROM telemetry t
    INNER JOIN devices  d ON d.id = t.device_id
    INNER JOIN vehicles v ON v.id = d.vehicle_id
    WHERE ${clauses.join(" AND ")}
    ORDER BY t.received_at DESC
    LIMIT $${index}
  `;

  const result = await query(sql, params);
  return result.rows || [];
}

// ─────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────
export async function getLatestPositions(req, res) {
  try {
    const { limit = 5000, offset = 0 } = req.query;
    const rows = await loadLatestFromDb(req, { limit, offset });
    return res.json({ success: true, debugCount: rows.length, data: rows });
  } catch (err) {
    console.error("getLatestPositions error:", err);
    return res.status(500).json({ success: false, message: "Failed to load latest positions" });
  }
}

export async function getVehicleLatestPosition(req, res) {
  try {
    const vehicleId = String(req.params.vehicleId || "").trim();

    const result = await loadVehicleLatestFromDb(req, vehicleId);

    // 🔴 improved error handling response
    if (result?.error) {
      return res.status(400).json({
        success: false,
        message: result.message,
        hint: result.hint
      });
    }

    if (!result) {
      return res.status(404).json({
        success: false,
        message: "No position found for this vehicle"
      });
    }

    return res.json({ success: true, data: result });

  } catch (err) {
    console.error("getVehicleLatestPosition error:", err);

    if (err.code === "22003") {
      return res.status(400).json({
        success: false,
        message: "Vehicle ID overflow error (invalid numeric range)"
      });
    }

    return res.status(500).json({
      success: false,
      message: "Failed to load vehicle position"
    });
  }
}

export async function getHistory(req, res) {
  try {
    const { deviceUid, from, to } = req.query;
    const limit = normalizeLimit(req.query.limit, 200, 2000);
    if (!deviceUid) {
      return res.status(400).json({ success: false, message: "deviceUid is required" });
    }
    const rows = await loadHistoryFromDb(req, deviceUid, limit, from, to);
    const enriched = [];
    for (const pos of rows) {
      enriched.push({ ...pos, locationName: await safeLocationName(pos.lat, pos.lon) });
    }
    return res.json({ success: true, data: enriched });
  } catch (error) {
    console.error("getHistory error:", error);
    return res.status(500).json({ success: false, message: "Failed to load history" });
  }
}

export async function createPosition(req, res) {
  try {
    const { deviceUid, lat, lon, speedKph = 0, heading = 0, deviceTime = null } = req.body;
    if (!deviceUid || lat == null || lon == null) {
      return res.status(400).json({ success: false, message: "deviceUid, lat and lon are required" });
    }
    const result = await query(
      `INSERT INTO telemetry (device_id, latitude, longitude, speed_kph, heading, device_time)
       SELECT d.id, $2, $3, $4, $5, $6 FROM devices d WHERE d.device_uid = $1
       RETURNING id::text AS id, latitude AS lat, longitude AS lon,
                 speed_kph AS "speedKph", heading,
                 device_time AS "deviceTime", received_at AS "receivedAt"`,
      [deviceUid, lat, lon, speedKph, heading, deviceTime]
    );
    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: "Device not found" });
    }
    return res.status(201).json({ success: true, data: { deviceUid, ...result.rows[0] } });
  } catch (error) {
    console.error("createPosition error:", error);
    return res.status(500).json({ success: false, message: "Failed to create position" });
  }
}

export async function getPositionById(req, res) {
  try {
    const { id } = req.params;
    let sql = `
      SELECT t.id::text AS id, d.device_uid AS "deviceUid",
             t.latitude AS lat, t.longitude AS lon,
             COALESCE(t.speed_kph,0) AS "speedKph", t.heading,
             t.device_time AS "deviceTime", t.received_at AS "receivedAt",
             v.id AS "vehicleId", v.plate_number AS "plateNumber",
             COALESCE(v.unit_name,'') AS "unitName", COALESCE(v.account_id,0) AS "accountId"
      FROM telemetry t
      INNER JOIN devices  d ON d.id = t.device_id
      INNER JOIN vehicles v ON v.id = d.vehicle_id
      WHERE t.id = $1
    `;
    const params = [id];
    if (!isPrivileged(req)) { sql += ` AND v.account_id = $2`; params.push(String(req?.user?.accountId || "0")); }
    const result = await query(sql, params);
    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: "Position not found" });
    }
    const row = result.rows[0];
    return res.json({ success: true, data: { ...row, locationName: await safeLocationName(row.lat, row.lon) } });
  } catch (error) {
    console.error("getPositionById error:", error);
    return res.status(500).json({ success: false, message: "Failed to load position" });
  }
}

export async function updatePosition(req, res) {
  try {
    const { id } = req.params;
    const { lat, lon, speedKph, heading, deviceTime } = req.body;
    const existing = await query(`SELECT * FROM telemetry WHERE id = $1`, [id]);
    if (!existing.rows.length) {
      return res.status(404).json({ success: false, message: "Position not found" });
    }
    const cur = existing.rows[0];
    const result = await query(
      `UPDATE telemetry SET latitude=$1, longitude=$2, speed_kph=$3, heading=$4, device_time=$5
       WHERE id=$6
       RETURNING id::text AS id, latitude AS lat, longitude AS lon,
                 speed_kph AS "speedKph", heading,
                 device_time AS "deviceTime", received_at AS "receivedAt"`,
      [lat ?? cur.latitude, lon ?? cur.longitude, speedKph ?? cur.speed_kph,
       heading ?? cur.heading, deviceTime ?? cur.device_time, id]
    );
    return res.json({ success: true, data: result.rows[0] });
  } catch (error) {
    console.error("updatePosition error:", error);
    return res.status(500).json({ success: false, message: "Failed to update position" });
  }
}

export async function deletePosition(req, res) {
  try {
    const { id } = req.params;
    const result = await query(`DELETE FROM telemetry WHERE id = $1 RETURNING id`, [id]);
    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: "Position not found" });
    }
    return res.json({ success: true, message: "Position deleted" });
  } catch (error) {
    console.error("deletePosition error:", error);
    return res.status(500).json({ success: false, message: "Failed to delete position" });
  }
}