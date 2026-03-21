import { getMariaConnection } from "../config/mariadb.js";

export async function fetchMariaTrackingData(limit = 5) {
  let conn;

  try {
    conn = await getMariaConnection();

    const safeLimit = Number.isFinite(Number(limit)) ? Number(limit) : 5;

    const query = `
      SELECT
        r.reg_no,
        d.id AS source_device_id,
        d.uniqueid,
        DATE_FORMAT(d.lastupdate, '%Y-%m-%d %H:%i:%s') AS lastupdate,
        CAST(e.id AS CHAR) AS event_id,
        e.protocol,
        e.deviceid,
        DATE_FORMAT(e.servertime, '%Y-%m-%d %H:%i:%s') AS servertime,
        DATE_FORMAT(e.devicetime, '%Y-%m-%d %H:%i:%s') AS devicetime,
        DATE_FORMAT(e.fixtime, '%Y-%m-%d %H:%i:%s') AS fixtime,
        CAST(e.valid AS UNSIGNED) AS valid,
        CAST(e.latitude AS DECIMAL(12,8)) AS latitude,
        CAST(e.longitude AS DECIMAL(12,8)) AS longitude,
        CAST(e.altitude AS DECIMAL(12,2)) AS altitude,
        CAST(e.speed AS DECIMAL(12,2)) AS speed,
        CAST(e.course AS DECIMAL(12,2)) AS course,
        e.alarmcode,
        CAST(e.statuscode AS UNSIGNED) AS statuscode,
        CAST(e.odometer AS DECIMAL(18,2)) AS odometer
      FROM registration r
      JOIN device d
        ON d.uniqueid = CONCAT('0', r.serial)
      JOIN eventData e
        ON e.deviceid = d.id
      WHERE e.id = (
        SELECT MAX(e2.id)
        FROM eventData e2
        WHERE e2.deviceid = d.id
      )
      ORDER BY e.id DESC
      LIMIT ${safeLimit}
    `;

    const [rows] = await conn.query(query);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    console.error("Maria tracking fetch failed:", error);
    throw error;
  } finally {
    if (conn) await conn.end();
  }
}