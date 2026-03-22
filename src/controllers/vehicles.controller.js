import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

/**
 * Returns true if the user can access all accounts
 */
function canAccessAccountData(req) {
  return isPrivilegedRole(req.user.role);
}

/**
 * GET /vehicles
 * Returns all vehicles, optionally filtered by account_id
 */
export async function getVehicles(req, res) {
  try {
    // Ensure accountId is integer or null
    const userAccountId = parseInt(req.user.accountId, 10) || null;

    let sql = `
      SELECT
        v.id,
        v.plate_number,
        COALESCE(v.unit_name, '') AS unit_name,
        COALESCE(v.make, '') AS make,
        COALESCE(v.model, '') AS model,
        COALESCE(v.year, 0) AS year,
        COALESCE(v.account_id, 0) AS account_id,
        a.account_name,
        a.account_type,
        COALESCE(v.status, 'active') AS status,
        v.created_at,
        v.updated_at
      FROM vehicles v
      LEFT JOIN accounts a ON a.id = v.account_id
    `;

    const params = [];

    if (!canAccessAccountData(req)) {
      sql += ` WHERE v.account_id = $1 `;
      params.push(userAccountId);
    }

    sql += ` ORDER BY v.created_at DESC`;

    const result = await query(sql, params);

    return res.json({
      success: true,
      data: result.rows,
    });
  } catch (error) {
    console.error("getVehicles error:", error);
    console.error("Check if req.user.accountId is a valid integer:", req.user.accountId);
    return res.status(500).json({
      success: false,
      message: "Failed to load vehicles. Check server logs for guidance.",
    });
  }
}

/**
 * GET /vehicles/:id
 * Returns a single vehicle by ID
 */
export async function getVehicleById(req, res) {
  try {
    const { id } = req.params;
    const vehicleId = parseInt(id, 10);

    if (isNaN(vehicleId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle ID",
      });
    }

    const userAccountId = parseInt(req.user.accountId, 10) || null;

    let sql = `
      SELECT
        v.id,
        v.plate_number,
        COALESCE(v.unit_name, '') AS unit_name,
        COALESCE(v.make, '') AS make,
        COALESCE(v.model, '') AS model,
        COALESCE(v.year, 0) AS year,
        COALESCE(v.account_id, 0) AS account_id,
        a.account_name,
        a.account_type,
        COALESCE(v.status, 'active') AS status,
        v.created_at,
        v.updated_at
      FROM vehicles v
      LEFT JOIN accounts a ON a.id = v.account_id
      WHERE v.id = $1
    `;

    const params = [vehicleId];

    if (!canAccessAccountData(req)) {
      sql += ` AND v.account_id = $2 `;
      params.push(userAccountId);
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
    console.error("Check if req.user.accountId is valid:", req.user.accountId);
    return res.status(500).json({
      success: false,
      message: "Failed to load vehicle. Check server logs for guidance.",
    });
  }
}

/**
 * POST /vehicles
 * Creates a new vehicle
 */
export async function createVehicle(req, res) {
  try {
    const {
      plate_number,
      unit_name = null,
      make = null,
      model = null,
      year = null,
      account_id = null,
      status = "active",
    } = req.body;

    if (!plate_number) {
      return res.status(400).json({
        success: false,
        message: "plate_number is required",
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
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
      `,
      [plate_number, unit_name, make, model, year, account_id, status]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("createVehicle error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to create vehicle. Check server logs for guidance.",
    });
  }
}

/**
 * PUT /vehicles/:id
 * Updates an existing vehicle
 */
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;
    const vehicleId = parseInt(id, 10);

    if (isNaN(vehicleId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle ID",
      });
    }

    const existing = await query(`SELECT * FROM vehicles WHERE id = $1`, [vehicleId]);

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Vehicle not found",
      });
    }

    const current = existing.rows[0];
    const {
      plate_number,
      unit_name,
      make,
      model,
      year,
      account_id,
      status,
    } = req.body;

    const result = await query(
      `
      UPDATE vehicles
      SET
        plate_number = $1,
        unit_name = $2,
        make = $3,
        model = $4,
        year = $5,
        account_id = $6,
        status = $7,
        updated_at = NOW()
      WHERE id = $8
      RETURNING *
      `,
      [
        plate_number ?? current.plate_number,
        unit_name ?? current.unit_name,
        make ?? current.make,
        model ?? current.model,
        year ?? current.year,
        account_id ?? current.account_id,
        status ?? current.status,
        vehicleId,
      ]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("updateVehicle error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update vehicle. Check server logs for guidance.",
    });
  }
}

/**
 * DELETE /vehicles/:id
 * Deletes a vehicle
 */
export async function deleteVehicle(req, res) {
  try {
    const { id } = req.params;
    const vehicleId = parseInt(id, 10);

    if (isNaN(vehicleId)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle ID",
      });
    }

    const result = await query(
      `DELETE FROM vehicles WHERE id = $1 RETURNING id`,
      [vehicleId]
    );

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
      message: "Failed to delete vehicle. Check server logs for guidance.",
    });
  }
}