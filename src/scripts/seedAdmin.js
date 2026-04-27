import dotenv from "dotenv";
dotenv.config();

import bcrypt from "bcryptjs";
import { pgPool } from "../config/db.js"; // ✅ FIXED

async function seedAdmin() {
  try {
    const email = process.env.ADMIN_EMAIL || "admin@tracking.local";
    const password = process.env.ADMIN_PASSWORD || "Admin@123";

    const existing = await pgPool.query(
      "SELECT id FROM users WHERE email = $1",
      [email]
    );

    if (existing.rows.length > 0) {
      console.log("⚠️ Admin already exists");
      process.exit(0);
    }

    const hashed = await bcrypt.hash(password, 10);

    const result = await pgPool.query(
      `INSERT INTO users (
        full_name, email, phone, password_hash, role, status
      ) VALUES ($1,$2,$3,$4,$5,$6)
      RETURNING id,email,role`,
      ["System Admin", email, "0700000000", hashed, "super_admin", "active"]
    );

    console.log("✅ Admin created:", result.rows[0]);

    process.exit(0);
  } catch (err) {
    console.error("❌ Seed failed:", err.message);
    process.exit(1);
  }
}

seedAdmin();