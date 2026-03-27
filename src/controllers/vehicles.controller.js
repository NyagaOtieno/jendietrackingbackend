// src/controllers/vehicles.controller.js
import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

/**
 * Check if the user can access all accounts (admin/privileged)
 */
function canAccessAccountData(req) {
  return isPrivilegedRole(req.user.role);
}

/**
 * GET all vehicles
 */
export async function getVehicles(req, res) {
  try {
    let sql = `
      SELECT
        v.id,
        v.plate_number,
        v.unit_name,
        v.make,
        v.model,
        v.year,
        v.status,
        v.created_at,
        v.serial AS device_uid, -- map serial as device UID
        v.account_id,
        a.account_name,
        a.account_type
      FROM vehicles v
      LEFT JOIN accounts a ON a.id = v.account_id
    `;

    const params = [];

    // 🔹 Restrict to user's account if not privileged
    if (!canAccessAccountData(req)) {
      const accountId = req.user.accountId; // should be string UUID

      if (!accountId) {
        return res.status(400).json({
          success: false,
          message: "Invalid accountId on user",
        });
      }

      sql += ` WHERE v.account_id = $1::uuid `;
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
      message: "Failed to load vehicles. Check server logs for guidance.",
    });
  }
}

/**
 * GET vehicle by ID
 */
export async function getVehicleById(req, res) {
  try {
    const { id } = req.params;

    let sql = `
      SELECT
        v.id,
        v.plate_number,
        COALESCE(v.unit_name, '') AS unit_name,
        COALESCE(v.make, '') AS make,
        COALESCE(v.model, '') AS model,
        COALESCE(v.year, 0) AS year,
        COALESCE(v.account_id, '') AS account_id,
        a.account_name,
        a.account_type,
        COALESCE(v.status, 'active') AS status,
        v.created_at,
        v.updated_at,
        v.serial AS device_uid
      FROM vehicles v
      LEFT JOIN accounts a ON a.id = v.account_id
      WHERE v.id = $1
    `;

    const params = [parseInt(id, 10)];

    // 🔹 Restrict to user's account if not privileged
    if (!canAccessAccountData(req)) {
      sql += ` AND v.account_id = $2::uuid `;
      params.push(req.user.accountId);
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

/**
 * CREATE a new vehicle
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

    const accId = account_id ?? null; // should be UUID string or null

    const result = await query(
      `
      INSERT INTO vehicles (plate_number, unit_name, make, model, year, account_id, status)
      VALUES ($1, $2, $3, $4, $5, $6::uuid, $7)
      RETURNING *
      `,
      [plate_number, unit_name, make, model, year, accId, status]
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

/**
 * UPDATE a vehicle
 */
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;

    const existing = await query(`SELECT * FROM vehicles WHERE id = $1`, [parseInt(id, 10)]);

    if (!existing.rows.length) {
      return res.status(404).json({ success: false, message: "Vehicle not found" });
    }

    const current = existing.rows[0];
    const { plate_number, unit_name, make, model, year, account_id, status } = req.body;

    const accId = account_id ?? current.account_id; // keep current if null

    const result = await query(
      `
      UPDATE vehicles
      SET
        plate_number = $1,
        unit_name = $2,
        make = $3,
        model = $4,
        year = $5,
        account_id = $6::uuid,
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
        accId,
        status ?? current.status,
        id,
      ]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("updateVehicle error:", error);
    return res.status(500).json({ success: false, message: "Failed to update vehicle" });
  }
}

/**
 * DELETE a vehicle
 */
export async function deleteVehicle(req, res) {
  try {
    const { id } = req.params;

    const result = await query(`DELETE FROM vehicles WHERE id = $1 RETURNING id`, [parseInt(id, 10)]);

    if (!result.rows.length) {
      return res.status(404).json({ success: false, message: "Vehicle not found" });
    }

    return res.json({ success: true, message: "Vehicle deleted" });
  } catch (error) {
    console.error("deleteVehicle error:", error);
    return res.status(500).json({ success: false, message: "Failed to delete vehicle" });
  }
}