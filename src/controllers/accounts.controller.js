import bcrypt from "bcryptjs";
import { query } from "../config/db.js";
import { ROLES } from "../utils/roles.js";

// ======================================================
// HELPERS
// ======================================================

function getDefaultRoleForAccountType(accountType) {
  if (accountType === "sacco") return "sacco_user";
  if (accountType === "company") return "company_user";
  return "individual_user";
}

// ONLY clients are account-bound
function isClientRole(role) {
  return role === ROLES.CLIENT;
}

// ======================================================
// GET ACCOUNTS (SCOPED SAFELY)
// ======================================================
export async function getAccounts(req, res) {
  try {
    const { role, accountId } = req.user;

    let sql = `
      SELECT
        id,
        account_type,
        account_name,
        client_code,
        company_code,
        sacco_code,
        status,
        created_at
      FROM accounts
    `;

    const params = [];

    // Clients only see their own account
    if (role === ROLES.CLIENT) {
      if (!accountId) {
        return res.status(403).json({
          success: false,
          message: "Client account context missing",
        });
      }

      sql += ` WHERE id = $1 `;
      params.push(accountId);
    }

    sql += ` ORDER BY created_at DESC`;

    const result = await query(sql, params);

    return res.json({
      success: true,
      data: result.rows,
    });

  } catch (error) {
    console.error("getAccounts error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load accounts",
    });
  }
}

// ======================================================
// GET ACCOUNT BY ID
// ======================================================
export async function getAccountById(req, res) {
  try {
    const { id } = req.params;
    const { role, accountId } = req.user;

    // CLIENT SAFETY CHECK
    if (role === ROLES.CLIENT && accountId !== id) {
      return res.status(403).json({
        success: false,
        message: "Forbidden: cannot access this account",
      });
    }

    const result = await query(
      `
      SELECT
        id,
        account_type,
        account_name,
        client_code,
        company_code,
        sacco_code,
        status,
        created_at
      FROM accounts
      WHERE id = $1
      `,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Account not found",
      });
    }

    return res.json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("getAccountById error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load account",
    });
  }
}

// ======================================================
// CREATE ACCOUNT (FIXED ROLE CONTROL)
// ======================================================
export async function createAccount(req, res) {
  try {
    const { role } = req.user;

    const {
      account_type = "individual",
      account_name,
      client_code = null,
      company_code = null,
      sacco_code = null,
      status = "active",
    } = req.body;

    if (!account_name) {
      return res.status(400).json({
        success: false,
        message: "account_name is required",
      });
    }

    // Only non-clients can create accounts
    if (role === ROLES.CLIENT) {
      return res.status(403).json({
        success: false,
        message: "Clients cannot create accounts",
      });
    }

    const result = await query(
      `
      INSERT INTO accounts (
        account_type,
        account_name,
        client_code,
        company_code,
        sacco_code,
        status
      )
      VALUES ($1,$2,$3,$4,$5,$6)
      RETURNING *
      `,
      [account_type, account_name, client_code, company_code, sacco_code, status]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("createAccount error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to create account",
    });
  }
}

// ======================================================
// UPDATE ACCOUNT
// ======================================================
export async function updateAccount(req, res) {
  try {
    const { id } = req.params;
    const { role, accountId } = req.user;

    // CLIENT SAFETY
    if (role === ROLES.CLIENT && accountId !== id) {
      return res.status(403).json({
        success: false,
        message: "Forbidden",
      });
    }

    const existing = await query(`SELECT * FROM accounts WHERE id = $1`, [id]);

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Account not found",
      });
    }

    const current = existing.rows[0];
    const {
      account_type,
      account_name,
      client_code,
      company_code,
      sacco_code,
      status,
    } = req.body;

    const result = await query(
      `
      UPDATE accounts
      SET
        account_type = $1,
        account_name = $2,
        client_code = $3,
        company_code = $4,
        sacco_code = $5,
        status = $6
      WHERE id = $7
      RETURNING *
      `,
      [
        account_type ?? current.account_type,
        account_name ?? current.account_name,
        client_code ?? current.client_code,
        company_code ?? current.company_code,
        sacco_code ?? current.sacco_code,
        status ?? current.status,
        id,
      ]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("updateAccount error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update account",
    });
  }
}

// ======================================================
// DELETE ACCOUNT
// ======================================================
export async function deleteAccount(req, res) {
  try {
    const { id } = req.params;
    const { role } = req.user;

    if (role === ROLES.CLIENT) {
      return res.status(403).json({
        success: false,
        message: "Clients cannot delete accounts",
      });
    }

    const result = await query(
      `DELETE FROM accounts WHERE id = $1 RETURNING id`,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Account not found",
      });
    }

    return res.json({
      success: true,
      message: "Account deleted",
    });

  } catch (error) {
    console.error("deleteAccount error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to delete account",
    });
  }
}

// ======================================================
// ADD USER TO ACCOUNT (UNCHANGED LOGIC, SAFER ROLE HANDLING)
// ======================================================
export async function addUserToAccount(req, res) {
  try {
    const { id } = req.params;
    const {
      fullName,
      username,
      email,
      phone,
      password,
      role,
      status = "active",
    } = req.body;

    if (!fullName || !username || !email || !password) {
      return res.status(400).json({
        success: false,
        message: "fullName, username, email and password are required",
      });
    }

    const accountResult = await query(
      `SELECT * FROM accounts WHERE id = $1 LIMIT 1`,
      [id]
    );

    if (!accountResult.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Account not found",
      });
    }

    const existingUser = await query(
      `
      SELECT id
      FROM users
      WHERE email = $1 OR username = $2
      LIMIT 1
      `,
      [email, username]
    );

    if (existingUser.rows.length > 0) {
      return res.status(409).json({
        success: false,
        message: "Email or username already exists",
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);
    const finalRole = role || getDefaultRoleForAccountType(accountResult.rows[0].account_type);

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
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING id, full_name, username, email, phone, role, account_id, status, created_at
      `,
      [
        fullName,
        username,
        email,
        phone || null,
        passwordHash,
        finalRole,
        id,
        status,
      ]
    );

    return res.status(201).json({
      success: true,
      message: "User added to account successfully",
      data: result.rows[0],
    });

  } catch (error) {
    console.error("addUserToAccount error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to add user to account",
    });
  }
}

// ======================================================
// GET ACCOUNT USERS (CLIENT SAFE NOT REQUIRED)
// ======================================================
export async function getAccountUsers(req, res) {
  try {
    const { id } = req.params;

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
      WHERE account_id = $1
      ORDER BY created_at DESC
      `,
      [id]
    );

    return res.json({
      success: true,
      data: result.rows,
    });

  } catch (error) {
    console.error("getAccountUsers error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load account users",
    });
  }
}