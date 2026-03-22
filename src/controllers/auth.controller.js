import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import { query } from "../config/db.js";

function signUser(user) {
  return jwt.sign(
    {
      id: user.id,
      email: user.email,
      username: user.username,
      role: user.role,
      fullName: user.full_name,
      status: user.status,
      accountId: user.account_id || null,
    },
    process.env.JWT_SECRET,
    { expiresIn: "7d" }
  );
}

function mapUser(user) {
  return {
    id: user.id,
    fullName: user.full_name,
    email: user.email,
    username: user.username,
    phone: user.phone,
    role: user.role,
    status: user.status,
    accountId: user.account_id || null,
    createdAt: user.created_at,
  };
}

export async function register(req, res) {
  try {
    const {
      fullName,
      username,
      email,
      phone,
      password,
      role = "staff",
      accountId = null,
    } = req.body;

    if (!fullName || !username || !email || !password) {
      return res.status(400).json({
        success: false,
        message: "fullName, username, email and password are required",
      });
    }

    const existing = await query(
      `
      SELECT id
      FROM users
      WHERE email = $1 OR username = $2
      LIMIT 1
      `,
      [email, username]
    );

    if (existing.rows.length > 0) {
      return res.status(409).json({
        success: false,
        message: "Email or username already exists",
      });
    }

    if (accountId) {
      const accountCheck = await query(
        `SELECT id FROM accounts WHERE id = $1 LIMIT 1`,
        [accountId]
      );

      if (!accountCheck.rows.length) {
        return res.status(404).json({
          success: false,
          message: "Account not found",
        });
      }
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const result = await query(
      `
      INSERT INTO users (
        full_name,
        username,
        email,
        phone,
        password_hash,
        role,
        account_id,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
      RETURNING id, full_name, username, email, phone, role, account_id, status, created_at
      `,
      [fullName, username, email, phone || null, passwordHash, role, accountId]
    );

    const user = result.rows[0];
    const token = signUser(user);

    return res.status(201).json({
      success: true,
      message: "User registered successfully",
      data: {
        token,
        user: mapUser(user),
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
    const { login, password } = req.body;

    if (!login || !password) {
      return res.status(400).json({
        success: false,
        message: "login and password are required",
      });
    }

    const result = await query(
      `
      SELECT *
      FROM users
      WHERE email = $1 OR username = $1
      LIMIT 1
      `,
      [login]
    );

    const user = result.rows[0];

    if (!user) {
      return res.status(401).json({
        success: false,
        message: "Invalid credentials",
      });
    }

    const valid = await bcrypt.compare(password, user.password);

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
        user: mapUser(user),
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
      SELECT
        id,
        full_name,
        username,
        email,
        phone,
        role,
        account_id,
        status,
        created_at
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
      data: mapUser(user),
    });
  } catch (error) {
    console.error("getMe error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load profile",
    });
  }
}