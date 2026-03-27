// src/controllers/vehicles.controller.js
import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

function canAccessAccountData(req) {
  return isPrivilegedRole(req.user.role);
}

// ✅ Get all vehicles
export async function getVehicles(req, res) {
  try {
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
        v.device_uid
      FROM vehicles v
    `;

    const params = [];

    // Filter by account if user is not privileged
    if (!canAccessAccountData(req)) {
      const accountId = req.user.accountId; // must be UUID string

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

// ✅ Get single vehicle by ID
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
        COALESCE(v.account_id::text, '') AS account_id,
        v.status,
        v.created_at,
        v.updated_at,
        v.device_uid
      FROM vehicles v
      WHERE v.id = $1
    `;

    const params = [parseInt(id, 10)];

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

// ✅ Create new vehicle
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
      device_uid = null
    } = req.body;

    if (!plate_number) {
      return res.status(400).json({
        success: false,
        message: "plate_number is required",
      });
    }

    const result = await query(
      `
      INSERT INTO vehicles (plate_number, unit_name, make, model, year, account_id, status, device_uid)
      VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $8)
      RETURNING *
      `,
      [plate_number, unit_name, make, model, year, account_id, status, device_uid]
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

// ✅ Update existing vehicle
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;
    const { plate_number, unit_name, make, model, year, account_id, status, device_uid } = req.body;

    const existing = await query(`SELECT * FROM vehicles WHERE id = $1`, [parseInt(id, 10)]);

    if (!existing.rows.length) {
      return res.status(404).json({ success: false, message: "Vehicle not found" });
    }

    const current = existing.rows[0];

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
        device_uid = $8,
        updated_at = NOW()
      WHERE id = $9
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
        device_uid ?? current.device_uid,
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

// ✅ Delete vehicle
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