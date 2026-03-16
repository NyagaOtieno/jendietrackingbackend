import { query } from "../config/db.js";

export async function getAccounts(_req, res) {
  try {
    const result = await query(`
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
      ORDER BY created_at DESC
    `);

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

export async function getAccountById(req, res) {
  try {
    const { id } = req.params;

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

export async function createAccount(req, res) {
  try {
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
      VALUES ($1, $2, $3, $4, $5, $6)
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

export async function updateAccount(req, res) {
  try {
    const { id } = req.params;

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

export async function deleteAccount(req, res) {
  try {
    const { id } = req.params;

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