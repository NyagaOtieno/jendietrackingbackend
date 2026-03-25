import dotenv from "dotenv";
import pkg from "pg";

dotenv.config();

const { Pool } = pkg;

// Use individual DB vars or fallback to DATABASE_URL
export const pgPool = new Pool({
  host: process.env.DB_HOST || "localhost",
  port: process.env.DB_PORT ? parseInt(process.env.DB_PORT) : 5432,
  user: process.env.DB_USER || "postgres",
  password: process.env.DB_PASS || "postgres",
  database: process.env.DB_NAME || "tracking_platform",
  ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false,
});

// Keep your query function
export async function query(text, params = []) {
  return pgPool.query(text, params);
}

// Keep your DB test function
export async function testDbConnection() {
  const result = await pgPool.query("SELECT NOW() AS now");
  return result.rows[0];
}