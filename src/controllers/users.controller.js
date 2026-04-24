import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";
import bcrypt from "bcryptjs";

// ======================================================
// CREATE USER (ROLE-AWARE RULE ENGINE)
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

    // ===============================
    // ROLE CREATION RULES
    // ===============================
    const isSystemAdmin = creatorRole === "super_admin";
    const isAdmin = creatorRole === "admin";
    const isStaff = creatorRole === "staff";

    if (role === "admin" && !isSystemAdmin) {
      return res.status(403).json({
        success: false,
        message: "Only system admin can create admin",
      });
    }

    if (role === "staff" && !(isSystemAdmin || isAdmin)) {
      return res.status(403).json({
        success: false,
        message: "Only admin or system admin can create staff",
      });
    }

    if (role === "client" && !(isSystemAdmin || isAdmin || isStaff)) {
      return res.status(403).json({
        success: false,
        message: "Not allowed to create client",
      });
    }

    // ===============================
    // ACCOUNT RULE
    // ===============================
    let finalAccountId = null;

    if (role === "client") {
      // client MUST belong to account
      finalAccountId = account_id;

      if (!finalAccountId) {
        return res.status(400).json({
          success: false,
          message: "Client must have account_id",
        });
      }
    }

    // ===============================
    // HASH PASSWORD
    // ===============================
    const password_hash = await bcrypt.hash(password, 10);

    const result = await query(
      `
      INSERT INTO users (
        full_name,
        email,
        phone,
        password_hash,
        role,
        status
      )
      VALUES ($1,$2,$3,$4,$5,'active')
      RETURNING id, full_name, email, role, status
      `,
      [full_name, email, phone, password_hash, role]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
      account_id: finalAccountId,
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
// GET USERS (ROLE FILTERED)
// ======================================================
export async function getUsers(req, res) {
  try {
    const role = req.user.role;

    let sql = `
      SELECT id, full_name, email, phone, role, status, created_at
      FROM users
    `;

    let params = [];

    // clients only see themselves OR their account users later
    if (role === "client") {
      sql += ` WHERE id = $1 `;
      params.push(req.user.id);
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