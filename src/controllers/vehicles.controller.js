import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

// ======================================================
// HELPERS
// ======================================================
function getUser(req) {
  return req?.user || {};
}

function isPrivileged(req) {
  return isPrivilegedRole(req?.user?.role);
}

function getAccountId(req) {
  return req?.user?.accountId ?? null;
}

function requireAccount(res, accountId) {
  if (!accountId) {
    res.status(401).json({
      success: false,
      message: "Missing account context",
    });
    return false;
  }
  return true;
}

function isValidId(id) {
  return id !== undefined && id !== null && id !== "";
}

// ======================================================
// GET ALL VEHICLES
// ======================================================
export async function getVehicles(req, res) {
  try {
    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `
      SELECT
        id,
        plate_number,
        COALESCE(unit_name,'') AS unit_name,
        COALESCE(make,'') AS make,
        COALESCE(model,'') AS model,
        COALESCE(year,0) AS year,
        status,
        created_at,
        serial,
        account_id
      FROM vehicles
    `;

    const params = [];

    // ONLY CLIENT SCOPING
    if (!privileged) {
      if (!requireAccount(res, accountId)) return;

      sql += ` WHERE account_id = $1 `;
      params.push(accountId);
    }

    sql += ` ORDER BY id DESC`;

    const result = await query(sql, params);

    return res.json({
      success: true,
      data: result.rows,
    });

  } catch (error) {
    console.error("getVehicles error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load vehicles",
    });
  }
}

// ======================================================
// GET VEHICLE BY ID
// ======================================================
export async function getVehicleById(req, res) {
  try {
    const { id } = req.params;

    if (!isValidId(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle id",
      });
    }

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `SELECT * FROM vehicles WHERE id = $1`;
    const params = [id];

    if (!privileged) {
      if (!requireAccount(res, accountId)) return;

      sql += ` AND account_id = $2`;
      params.push(accountId);
    }

    const result = await query(sql, params);

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Vehicle not found",
      });
    }

    return res.json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("getVehicleById error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load vehicle",
    });
  }
}

// ======================================================
// CREATE VEHICLE (STRICT OWNERSHIP)
// ======================================================
export async function createVehicle(req, res) {
  try {
    const {
      plate_number,
      unit_name,
      make,
      model,
      year,
      account_id,
      status = "active",
      serial,
    } = req.body;

    if (!plate_number) {
      return res.status(400).json({
        success: false,
        message: "plate_number is required",
      });
    }

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    // 🚨 STRICT RULE: CLIENTS NEVER OVERRIDE ACCOUNT
    const finalAccountId = privileged ? (account_id || accountId) : accountId;

    if (!finalAccountId) {
      return res.status(400).json({
        success: false,
        message: "Account context missing for vehicle creation",
      });
    }

    const result = await query(
      `
      INSERT INTO vehicles (
        plate_number,
        unit_name,
        make,
        model,
        year,
        account_id,
        status,
        serial
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING *
      `,
      [
        plate_number,
        unit_name,
        make,
        model,
        year,
        finalAccountId,
        status,
        serial,
      ]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("createVehicle error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to create vehicle",
    });
  }
}

// ======================================================
// UPDATE VEHICLE (NO ACCOUNT OVERRIDE BY CLIENTS)
// ======================================================
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;
    const { make, model, year } = req.body;

    if (!isValidId(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle id",
      });
    }

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `
      UPDATE vehicles
      SET
        make = COALESCE($1, make),
        model = COALESCE($2, model),
        year = COALESCE($3, year)
    `;

    const params = [make, model, year];

    // only privileged can reassign ownership (optional rule)
    if (privileged) {
      sql += `, account_id = COALESCE($4, account_id)`;
      params.push(req.body.account_id ?? null);
    }

    sql += ` WHERE id = $${params.length + 1}`;
    params.push(id);

    const result = await query(sql + " RETURNING *", params);

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Vehicle not found",
      });
    }

    return res.json({
      success: true,
      data: result.rows[0],
    });

  } catch (error) {
    console.error("updateVehicle error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update vehicle",
    });
  }
}

// ======================================================
// DELETE VEHICLE (FIXED SAFE LOGIC)
// ======================================================
export async function deleteVehicle(req, res) {
  try {
    const { id } = req.params;

    if (!isValidId(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle id",
      });
    }

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `DELETE FROM vehicles WHERE id = $1`;
    const params = [id];

    if (!privileged) {
      if (!requireAccount(res, accountId)) return;

      sql += ` AND account_id = $2`;
      params.push(accountId);
    }

    const result = await query(sql + " RETURNING id", params);

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Vehicle not found",
      });
    }

    return res.json({
      success: true,
      message: "Vehicle deleted",
    });

  } catch (error) {
    console.error("deleteVehicle error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to delete vehicle",
    });
  }
}