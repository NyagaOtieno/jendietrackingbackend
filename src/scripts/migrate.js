import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { pgPool } from "../config/db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// IMPORTANT: correct path
const schemaPath = path.join(__dirname, "../db/schema.sql");

async function migrate() {
  try {
    console.log("🚀 Running migrations...");

    const sql = fs.readFileSync(schemaPath, "utf8");

    await pgPool.query(sql);

    console.log("✅ Migration completed successfully");
    process.exit(0);
  } catch (err) {
    console.error("❌ Migration failed:", err.message);
    process.exit(1);
  }
}

migrate();