import mariadbPool from "../config/mariadb.js";

export async function fetchMariaTrackingData(limit = 500) {
  let conn;

  try {
    conn = await mariadbPool.getConnection();

    const query = `
      SELECT
        r.reg_no,
        r.serial AS registration_serial,
        CONCAT('0', r.serial) AS lookup_uniqueid,
        r.upd_time AS registration_updated_at,

        d.id AS source_device_id,
        d.name AS device_name,
        d.uniqueid,
        d.lastupdate,
        d.positionid,

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
        e.address,
        e.attributes,
        e.accuracy,
        e.network,
        e.statuscode,
        e.alarmcode,
        e.speedlimit,
        e.odometer,
        e.isRead,
        e.signalwireconnected,
        e.powerwireconnected,
        e.eactime
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
    console.error("Maria tracking fetch failed:", error?.message || error);
    throw error;
  } finally {
    if (conn) conn.release();
  }
}