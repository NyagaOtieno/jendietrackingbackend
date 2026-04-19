import dotenv from "dotenv";
dotenv.config();

import bcrypt from "bcryptjs";
import { pgPool } from "../config/db.js";

const seedAdmin = async () => {
  try {
    const email = process.env.ADMIN_EMAIL || "admin@tracking.local";
    const password = process.env.ADMIN_PASSWORD || "Admin@123";

    // 1. check existing admin
    const existing = await pgPool.query(
      "SELECT id FROM users WHERE email = $1 LIMIT 1",
      [email]
    );

    if (existing.rows.length > 0) {
      console.log("⚠️ Admin already exists:", email);
      process.exit(0);
    }

    // 2. hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // 3. insert admin (MATCH YOUR LOGIN CONTROLLER)
    const result = await pgPool.query(
      `INSERT INTO users 
        (full_name, email, phone, password_hash, role, status)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, full_name, email, role, status`,
      [
        "System Admin",
        email,
        "0700000000",
        hashedPassword,
        "super_admin",   // ✅ FIXED ROLE
        "active",
      ]
    );

    console.log("✅ Admin created successfully:");
    console.log(result.rows[0]);

    console.log("\nLOGIN CREDENTIALS:");
    console.log("Email:", email);
    console.log("Password:", password);

    process.exit(0);
  } catch (err) {
    console.error("❌ Seed failed:", err);
    process.exit(1);
  }
};

seedAdmin();