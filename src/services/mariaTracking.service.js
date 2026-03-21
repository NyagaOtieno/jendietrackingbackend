import { getMariaConnection } from "../config/mariadb.js";

export async function fetchMariaTrackingData(limit = 1) {
  let conn;

  try {
    console.log("Connecting to MariaDB...");
    conn = await getMariaConnection();

    console.log("Running query...");

    const [rows] = await conn.query(
      `
      SELECT 
        id,
        deviceid,
        latitude,
        longitude,
        servertime,
        devicetime
      FROM eventData
      ORDER BY id DESC
      LIMIT ?
      `,
      [limit]
    );

    console.log("Query completed");

    return rows;
  } catch (error) {
    console.error("Maria fetch error:", error);
    throw error;
  } finally {
    if (conn) {
      await conn.end();
      console.log("Maria connection closed");
    }
  }
}