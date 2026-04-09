import dotenv from "dotenv";
import pkg from "pg";

dotenv.config();

const { Pool } = pkg;

export const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pgPool.on("connect", () => {
  console.log("✅ PostgreSQL connected");
});

pgPool.on("error", (err) => {
  console.error("❌ Unexpected DB error:", err);
});

// Query helper
export async function query(text, params = []) {
  return pgPool.query(text, params);
}

// Health check
export async function testDbConnection() {
  try {
    const result = await pgPool.query("SELECT NOW() AS now");
    return result.rows[0];
  } catch (err) {
    console.error("DB connection test failed:", err.message);
    throw err;
  }
}

// Retry logic
export async function waitForDb(retries = 5) {
  while (retries) {
    try {
      await pgPool.query("SELECT 1");
      console.log("✅ DB ready");
      return;
    } catch {
      console.log("⏳ Waiting for DB...");
      await new Promise((res) => setTimeout(res, 3000));
      retries--;
    }
  }
  throw new Error("❌ DB connection failed after retries");
}