import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

// ======================================================
// HELPERS
// ======================================================
function isPrivileged(req) {
  return isPrivilegedRole(req?.user?.role);
}

function getAccountId(req) {
  return req?.user?.accountId ?? null;
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
        v.id,
        v.plate_number,
        COALESCE(v.unit_name, '') AS unit_name,
        COALESCE(v.make, '') AS make,
        COALESCE(v.model, '') AS model,
        COALESCE(v.year, 0) AS year,
        v.status,
        v.created_at,
        v.updated_at,
        v.device_uid,
        v.account_id
      FROM vehicles v
    `;

    const params = [];

    // 🔐 ONLY clients are restricted
    if (!privileged) {
      if (!accountId) {
        return res.status(400).json({
          success: false,
          message: "Client user missing accountId in token",
        });
      }

      sql += ` WHERE v.account_id = $1 `;
      params.push(accountId);
    }

    sql += ` ORDER BY v.created_at DESC`;

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

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `
      SELECT *
      FROM vehicles
      WHERE id = $1
    `;

    const params = [id];

    if (!privileged) {
      sql += ` AND account_id = $2 `;
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
// CREATE VEHICLE
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
      device_uid,
    } = req.body;

    if (!plate_number) {
      return res.status(400).json({
        success: false,
        message: "plate_number is required",
      });
    }

    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    // 🔐 enforce tenant ownership
    const finalAccountId = privileged ? (account_id ?? null) : accountId;

    if (!finalAccountId) {
      return res.status(400).json({
        success: false,
        message: "Missing account_id for vehicle assignment",
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
        device_uid
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
        device_uid,
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
// UPDATE VEHICLE (ONLY SAFE FIELDS)
// ======================================================
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;
    const { make, model, year } = req.body;

    const existing = await query(
      `SELECT * FROM vehicles WHERE id = $1`,
      [id]
    );

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Vehicle not found",
      });
    }

    const result = await query(
      `
      UPDATE vehicles
      SET
        make = COALESCE($1, make),
        model = COALESCE($2, model),
        year = COALESCE($3, year),
        updated_at = NOW()
      WHERE id = $4
      RETURNING *
      `,
      [make, model, year, id]
    );

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
// DELETE VEHICLE
// ======================================================
export async function deleteVehicle(req, res) {
  try {
    const privileged = isPrivileged(req);
    const accountId = getAccountId(req);

    let sql = `DELETE FROM vehicles WHERE id = $1`;
    const params = [req.params.id];

    if (!privileged) {
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