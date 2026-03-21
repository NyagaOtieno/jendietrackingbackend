import mariadbPool from "../config/mariadb.js";

export async function fetchMariaTrackingData(limit = 50) {
  let conn;

  try {
    conn = await mariadbPool.getConnection();

    const query = `
      SELECT
        r.reg_no,
        d.id AS source_device_id,
        d.uniqueid,
        d.lastupdate,
        e.id AS event_id,
        e.protocol,
        e.deviceid,
        e.servertime,
        e.devicetime,
        e.fixtime,
        e.valid,
        e.latitude,
        e.longitude,
        e.altitude,
        e.speed,
        e.course,
        e.alarmcode,
        e.statuscode,
        e.odometer
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
      LIMIT ?
    `;

    const [rows] = await conn.query(query, [limit]);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    console.error("Maria tracking fetch failed:", error);
    throw error;
  } finally {
    if (conn) conn.release();
  }
}