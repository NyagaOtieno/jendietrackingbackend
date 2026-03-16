import { query } from "../config/db.js";
import * as geo from "../services/reverseGeocode.js";
import { normalizeLimit } from "../utils/sql.js";

const mockPositions = [
  {
    deviceUid: "VEH001",
    lat: -1.2921,
    lon: 36.8219,
    speedKph: 45,
    heading: 90,
    deviceTime: null,
    receivedAt: new Date().toISOString(),
  },
  {
    deviceUid: "VEH002",
    lat: -1.2864,
    lon: 36.8172,
    speedKph: 12,
    heading: 180,
    deviceTime: null,
    receivedAt: new Date().toISOString(),
  },
];

const mockHistory = {
  VEH001: [
    {
      id: "1",
      deviceUid: "VEH001",
      lat: -1.295,
      lon: 36.818,
      speedKph: 20,
      heading: 85,
      receivedAt: new Date(Date.now() - 15 * 60000).toISOString(),
    },
    {
      id: "2",
      deviceUid: "VEH001",
      lat: -1.2935,
      lon: 36.8202,
      speedKph: 32,
      heading: 88,
      receivedAt: new Date(Date.now() - 10 * 60000).toISOString(),
    },
    {
      id: "3",
      deviceUid: "VEH001",
      lat: -1.2921,
      lon: 36.8219,
      speedKph: 45,
      heading: 90,
      receivedAt: new Date().toISOString(),
    },
  ],
};

async function safeLocationName(lat, lon) {
  try {
    return (await geo.getLocationName(lat, lon)) || "Unknown location";
  } catch {
    return "Unknown location";
  }
}

async function loadLatestFromDb() {
  const result = await query(`
    SELECT
      d.device_uid AS "deviceUid",
      lp.latitude AS lat,
      lp.longitude AS lon,
      lp.speed_kph AS "speedKph",
      lp.heading,
      lp.device_time AS "deviceTime",
      lp.received_at AS "receivedAt"
    FROM latest_positions lp
    INNER JOIN devices d ON d.id = lp.device_id
    ORDER BY lp.received_at DESC
  `);

  return result.rows;
}

async function loadHistoryFromDb(deviceUid, limit, from, to) {
  const clauses = [`d.device_uid = $1`];
  const params = [deviceUid];
  let index = 2;

  if (from) {
    clauses.push(`t.received_at >= $${index++}`);
    params.push(from);
  }

  if (to) {
    clauses.push(`t.received_at <= $${index++}`);
    params.push(to);
  }

  params.push(limit);

  const sql = `
    SELECT
      t.id::text AS id,
      d.device_uid AS "deviceUid",
      t.latitude AS lat,
      t.longitude AS lon,
      t.speed_kph AS "speedKph",
      t.heading,
      t.device_time AS "deviceTime",
      t.received_at AS "receivedAt"
    FROM telemetry t
    INNER JOIN devices d ON d.id = t.device_id
    WHERE ${clauses.join(" AND ")}
    ORDER BY t.received_at DESC
    LIMIT $${index}
  `;

  const result = await query(sql, params);
  return result.rows;
}

export async function getLatestPositions(_req, res) {
  try {
    let rows = [];

    try {
      rows = await loadLatestFromDb();
    } catch {
      rows = mockPositions;
    }

    const enriched = await Promise.all(
      rows.map(async (pos) => ({
        ...pos,
        locationName: await safeLocationName(pos.lat, pos.lon),
      }))
    );

    return res.json({
      success: true,
      data: enriched,
    });
  } catch (error) {
    console.error("getLatestPositions error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load latest positions",
    });
  }
}

export async function getHistory(req, res) {
  try {
    const { deviceUid, from, to } = req.query;
    const limit = normalizeLimit(req.query.limit, 200, 2000);

    if (!deviceUid) {
      return res.status(400).json({
        success: false,
        message: "deviceUid is required",
      });
    }

    let rows = [];

    try {
      rows = await loadHistoryFromDb(deviceUid, limit, from, to);
    } catch {
      rows = (mockHistory[deviceUid] || []).slice(0, limit);
    }

    const enriched = await Promise.all(
      rows.map(async (pos) => ({
        ...pos,
        locationName: await safeLocationName(pos.lat, pos.lon),
      }))
    );

    return res.json({
      success: true,
      data: enriched,
    });
  } catch (error) {
    console.error("getHistory error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load history",
    });
  }
}

export async function createPosition(req, res) {
  try {
    const { deviceUid, lat, lon, speedKph = 0, heading = 0, deviceTime = null } = req.body;

    if (!deviceUid || lat == null || lon == null) {
      return res.status(400).json({
        success: false,
        message: "deviceUid, lat and lon are required",
      });
    }

    const result = await query(
      `
      INSERT INTO telemetry (
        device_id,
        latitude,
        longitude,
        speed_kph,
        heading,
        device_time
      )
      SELECT
        d.id,
        $2,
        $3,
        $4,
        $5,
        $6
      FROM devices d
      WHERE d.device_uid = $1
      RETURNING
        id::text AS id,
        latitude AS lat,
        longitude AS lon,
        speed_kph AS "speedKph",
        heading,
        device_time AS "deviceTime",
        received_at AS "receivedAt"
      `,
      [deviceUid, lat, lon, speedKph, heading, deviceTime]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Device not found",
      });
    }

    return res.status(201).json({
      success: true,
      data: {
        deviceUid,
        ...result.rows[0],
      },
    });
  } catch (error) {
    console.error("createPosition error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to create position",
    });
  }
}

export async function getPositionById(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `
      SELECT
        t.id::text AS id,
        d.device_uid AS "deviceUid",
        t.latitude AS lat,
        t.longitude AS lon,
        t.speed_kph AS "speedKph",
        t.heading,
        t.device_time AS "deviceTime",
        t.received_at AS "receivedAt"
      FROM telemetry t
      INNER JOIN devices d ON d.id = t.device_id
      WHERE t.id = $1
      `,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Position not found",
      });
    }

    const row = result.rows[0];

    return res.json({
      success: true,
      data: {
        ...row,
        locationName: await safeLocationName(row.lat, row.lon),
      },
    });
  } catch (error) {
    console.error("getPositionById error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load position",
    });
  }
}

export async function updatePosition(req, res) {
  try {
    const { id } = req.params;
    const { lat, lon, speedKph, heading, deviceTime } = req.body;

    const existing = await query(`SELECT * FROM telemetry WHERE id = $1`, [id]);

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Position not found",
      });
    }

    const current = existing.rows[0];

    const result = await query(
      `
      UPDATE telemetry
      SET
        latitude = $1,
        longitude = $2,
        speed_kph = $3,
        heading = $4,
        device_time = $5
      WHERE id = $6
      RETURNING
        id::text AS id,
        latitude AS lat,
        longitude AS lon,
        speed_kph AS "speedKph",
        heading,
        device_time AS "deviceTime",
        received_at AS "receivedAt"
      `,
      [
        lat ?? current.latitude,
        lon ?? current.longitude,
        speedKph ?? current.speed_kph,
        heading ?? current.heading,
        deviceTime ?? current.device_time,
        id,
      ]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("updatePosition error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update position",
    });
  }
}

export async function deletePosition(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `DELETE FROM telemetry WHERE id = $1 RETURNING id`,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Position not found",
      });
    }

    return res.json({
      success: true,
      message: "Position deleted",
    });
  } catch (error) {
    console.error("deletePosition error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to delete position",
    });
  }
}