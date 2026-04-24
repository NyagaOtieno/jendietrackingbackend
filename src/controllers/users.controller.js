import { query } from "../config/db.js";
import bcrypt from "bcryptjs";
import { isPrivilegedRole } from "../middleware/auth.js";

// ======================================================
// HELPERS
// ======================================================
function getAccountId(req) {
  return req?.user?.accountId || null;
}

function canCreate(role) {
  return ["super_admin", "admin", "staff"].includes(role);
}

function canViewAll(role) {
  return ["super_admin", "admin"].includes(role);
}

// ======================================================
// CREATE USER
// ======================================================
export async function createUser(req, res) {
  try {
    const { full_name, email, phone, password, role, account_id } = req.body;

    const creatorRole = req.user.role;

    if (!full_name || !email || !password || !role) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields",
      });
    }

    // ROLE RULES
    if (!canCreate(creatorRole)) {
      return res.status(403).json({
        success: false,
        message: "Not allowed to create users",
      });
    }

    if (role === "admin" && creatorRole !== "super_admin") {
      return res.status(403).json({
        success: false,
        message: "Only super admin can create admin",
      });
    }

    if (role === "staff" && !["super_admin", "admin"].includes(creatorRole)) {
      return res.status(403).json({
        success: false,
        message: "Not allowed to create staff",
      });
    }

    // CLIENT MUST HAVE ACCOUNT
    let finalAccountId = null;

    if (role === "client") {
      finalAccountId = account_id || getAccountId(req);

      if (!finalAccountId) {
        return res.status(400).json({
          success: false,
          message: "Client must have account_id",
        });
      }
    }

    const password_hash = await bcrypt.hash(password, 10);

    const result = await query(
      `
      INSERT INTO users (
        full_name,
        email,
        phone,
        password_hash,
        role,
        account_id,
        status
      )
      VALUES ($1,$2,$3,$4,$5,$6,'active')
      RETURNING id, full_name, email, role, status, account_id
      `,
      [
        full_name,
        email,
        phone,
        password_hash,
        role,
        finalAccountId, // ✅ FIXED HERE
      ]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("createUser error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to create user",
    });
  }
}

// ======================================================
// READ USERS
// ======================================================
export async function getUsers(req, res) {
  try {
    const role = req.user.role;
    const accountId = req.user.accountId;

    let sql = `
      SELECT id, full_name, email, phone, role, status, created_at
      FROM users
    `;

    let params = [];

    // CLIENTS ONLY SEE THEMSELVES
    if (role === "client") {
  sql += ` WHERE account_id = $1 `;
  params.push(req.user.accountId);
}

    const result = await query(sql, params);

    return res.json({
      success: true,
      data: result.rows,
    });

  } catch (error) {
    console.error("getUsers error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to fetch users",
    });
  }
}

// ======================================================
// UPDATE USER
// ======================================================
export async function updateUser(req, res) {
  try {
    const { id } = req.params;
    const { full_name, email, phone, role, status } = req.body;

    const existing = await query(
      `SELECT * FROM users WHERE id = $1`,
      [id]
    );

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    const result = await query(
      `
      UPDATE users
      SET
        full_name = COALESCE($1, full_name),
        email = COALESCE($2, email),
        phone = COALESCE($3, phone),
        role = COALESCE($4, role),
        status = COALESCE($5, status)
      WHERE id = $6
      RETURNING id, full_name, email, role, status
      `,
      [full_name, email, phone, role, status, id]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("updateUser error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update user",
    });
  }
}

// ======================================================
// DELETE USER
// ======================================================
export async function deleteUser(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `
      DELETE FROM users
      WHERE id = $1
      RETURNING id
      `,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "User not found",
      });
    }

    return res.json({
      success: true,
      message: "User deleted",
    });

  } catch (error) {
    console.error("deleteUser error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to delete user",
    });
  }
}