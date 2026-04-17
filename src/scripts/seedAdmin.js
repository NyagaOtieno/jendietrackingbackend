import dotenv from "dotenv";
dotenv.config();

import bcrypt from "bcryptjs";
import { pgPool } from "../config/db.js";

const seedAdmin = async () => {
  try {
    const email = process.env.ADMIN_EMAIL;
    const password = process.env.ADMIN_PASSWORD;

    // check existing admin
    const existing = await pgPool.query(
      "SELECT * FROM users WHERE email = $1",
      [email]
    );

    if (existing.rows.length > 0) {
      console.log("⚠️ Admin already exists");
      process.exit(0);
    }

    // hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // insert admin (MATCH YOUR SCHEMA)
    const result = await pgPool.query(
      `INSERT INTO users 
        (full_name, email, phone, password_hash, role, status)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, email, role`,
      [
        "System Admin",
        email,
        "0700000000",
        hashedPassword,
        "admin",
        "active",
      ]
    );

    console.log("✅ Admin created:", result.rows[0]);

    process.exit(0);
  } catch (err) {
    console.error("❌ Seed failed:", err.message);
    process.exit(1);
  }
};

seedAdmin();