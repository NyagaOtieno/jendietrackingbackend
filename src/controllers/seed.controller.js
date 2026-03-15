import bcrypt from "bcryptjs";
import { query } from "../config/db.js";

export async function seedAdmin(_req, res) {
  try {
    const existing = await query(
      `SELECT id FROM users WHERE email = 'admin@tracking.local' LIMIT 1`
    );

    if (existing.rows.length > 0) {
      return res.json({
        success: true,
        message: "Admin already exists",
        data: {
          email: "admin@tracking.local",
          password: "Admin@123",
        },
      });
    }

    const passwordHash = await bcrypt.hash("Admin@123", 10);

    const result = await query(
      `
      INSERT INTO users (full_name, email, phone, password_hash, role, status)
      VALUES ('System Admin', 'admin@tracking.local', '0700000000', $1, 'super_admin', 'active')
      RETURNING id, full_name, email, role, status
      `,
      [passwordHash]
    );

    return res.status(201).json({
      success: true,
      message: "Seed admin created",
      data: {
        ...result.rows[0],
        defaultPassword: "Admin@123",
      },
    });
  } catch (error) {
    console.error("seedAdmin error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to seed admin",
    });
  }
}