import mariadbPool from "../config/mariadb.js";

export async function fetchMariaTrackingData() {
  let conn;

  try {
    conn = await mariadbPool.getConnection();

    const query = `
      SELECT
        imei AS device_uid,
        vehicle_reg AS plate_number,
        latitude,
        longitude,
        speed,
        ignition,
        gps_time AS tracked_at,
        'online' AS status
      FROM tracking_positions
      WHERE gps_time >= NOW() - INTERVAL 10 MINUTE
      ORDER BY gps_time DESC
      LIMIT 500
    `;

    const rows = await conn.query(query);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    console.error("Maria tracking fetch failed:", error?.message || error);
    throw error;
  } finally {
    if (conn) conn.release();
  }
}