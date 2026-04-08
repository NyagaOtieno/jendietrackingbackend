import dotenv from "dotenv";
import pkg from "pg";

dotenv.config();

const { Pool } = pkg;

// Use individual DB vars or fallback to DATABASE_URL
export const pgPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
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