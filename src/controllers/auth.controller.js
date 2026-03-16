import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { query } from "../config/db.js";

function signUser(user) {
  return jwt.sign(
    {
      id: user.id,
      email: user.email,
      role: user.role,
      fullName: user.full_name,
      status: user.status,
    },
    process.env.JWT_SECRET,
    { expiresIn: "7d" }
  );
}

export async function register(req, res) {
  try {
    const { fullName, email, phone, password, role = "staff" } = req.body;

    if (!fullName || !email || !password) {
      return res.status(400).json({
        success: false,
        message: "fullName, email and password are required",
      });
    }

    const existing = await query(
      `SELECT id FROM users WHERE email = $1 LIMIT 1`,
      [email]
    );

    if (existing.rows.length > 0) {
      return res.status(409).json({
        success: false,
        message: "Email already exists",
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const result = await query(
      `
      INSERT INTO users (full_name, email, phone, password_hash, role, status)
      VALUES ($1, $2, $3, $4, $5, 'active')
      RETURNING id, full_name, email, phone, role, status, created_at
      `,
      [fullName, email, phone || null, passwordHash, role]
    );

    const user = result.rows[0];
    const token = signUser(user);

    return res.status(201).json({
      success: true,
      message: "User registered successfully",
      data: {
        token,
        user: {
          id: user.id,
          fullName: user.full_name,
          email: user.email,
          phone: user.phone,
          role: user.role,
          status: user.status,
          createdAt: user.created_at,
        },
      },
    });
  } catch (error) {
    console.error("register error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to register user",
    });
  }
}

export async function login(req, res) {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({
        success: false,
        message: "email and password are required",
      });
    }

    const result = await query(
      `SELECT * FROM users WHERE email = $1 LIMIT 1`,
      [email]
    );

    const user = result.rows[0];

    if (!user) {
      return res.status(401).json({
        success: false,
        message: "Invalid credentials",
      });
    }

    const valid = await bcrypt.compare(password, user.password_hash);

    if (!valid) {
      return res.status(401).json({
        success: false,
        message: "Invalid credentials",
      });
    }

    if (user.status !== "active") {
      return res.status(403).json({
        success: false,
        message: "User is not active",
      });
    }

    const token = signUser(user);

    return res.json({
      success: true,
      message: "Login successful",
      data: {
        token,
        user: {
          id: user.id,
          fullName: user.full_name,
          email: user.email,
          phone: user.phone,
          role: user.role,
          status: user.status,
          createdAt: user.created_at,
        },
      },
    });
  } catch (error) {
    console.error("login error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to login",
    });
  }
}

export async function getMe(req, res) {
  try {
    const result = await query(
      `
      SELECT id, full_name, email, phone, role, status, created_at
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
      [req.user.id]
    );

    const user = result.rows[0];

    if (!user) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    return res.json({
      success: true,
      data: {
        id: user.id,
        fullName: user.full_name,
        email: user.email,
        phone: user.phone,
        role: user.role,
        status: user.status,
        createdAt: user.created_at,
      },
    });
  } catch (error) {
    console.error("getMe error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load profile",
    });
  }
}