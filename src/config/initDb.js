import { mariaPool } from "./mariadb.pool.js";

export async function initMariaDB() {
  try {
    const conn = await mariaPool.getConnection();
    console.log("✅ MariaDB/MySQL connected");
    conn.release();
  } catch (err) {
    console.error("❌ MariaDB/MySQL connection failed", err);
  }
}