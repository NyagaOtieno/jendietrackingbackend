import { pgPool } from "./db.js";

export async function initDb() {
  try {
    await pgPool.query("SELECT 1");
    console.log("✅ PostgreSQL connected");
  } catch (err) {
    console.error("❌ PostgreSQL connection failed", err.message);
  }
}

export async function initMariaDB() {
  console.log("✅ MariaDB init — handled by mariaSync");
}
