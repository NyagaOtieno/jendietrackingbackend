// src/controllers/vehicles.controller.js
import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

function canAccessAccountData(req) {
  return isPrivilegedRole(req.user.role);
}

// 🔥 Helper: check if column exists
async function hasColumn(column) {
  const res = await query(
    `
    SELECT 1 
    FROM information_schema.columns 
    WHERE table_name='vehicles' AND column_name=$1
    `,
    [column]
  );
  return res.rows.length > 0;
}

export async function getVehicles(req, res) {
  try {
    const hasAccountId = await hasColumn("account_id");

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
        v.serial AS device_uid
        ${hasAccountId ? ", v.account_id, a.account_name, a.account_type" : ""}
      FROM vehicles v
      ${hasAccountId ? "LEFT JOIN accounts a ON a.id = v.account_id" : ""}
    `;

    const params = [];

    // ✅ Only filter if column exists
    if (hasAccountId && !canAccessAccountData(req)) {
      const accountId = Number(req.user.accountId);

      if (!accountId) {
        return res.status(400).json({
          success: false,
          message: "Invalid accountId on user",
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

export async function getVehicleById(req, res) {
  try {
    const { id } = req.params;
    const hasAccountId = await hasColumn("account_id");

    let sql = `
      SELECT
        v.id,
        v.plate_number,
        COALESCE(v.unit_name, '') AS unit_name,
        COALESCE(v.make, '') AS make,
        COALESCE(v.model, '') AS model,
        COALESCE(v.year, 0) AS year,
        COALESCE(v.status, 'active') AS status,
        v.created_at,
        v.updated_at,
        v.serial AS device_uid
        ${
          hasAccountId
            ? ", COALESCE(v.account_id, 0) AS account_id, a.account_name, a.account_type"
            : ""
        }
      FROM vehicles v
      ${hasAccountId ? "LEFT JOIN accounts a ON a.id = v.account_id" : ""}
      WHERE v.id = $1
    `;

    const params = [parseInt(id, 10)];

    if (hasAccountId && !canAccessAccountData(req)) {
      sql += ` AND v.account_id = $2 `;
      const accId = parseInt(req.user.accountId, 10);
      params.push(isNaN(accId) ? 0 : accId);
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

    const hasAccountId = await hasColumn("account_id");

    let sql;
    let values;

    if (hasAccountId) {
      const accId = account_id != null ? parseInt(account_id, 10) : null;

      sql = `
        INSERT INTO vehicles (plate_number, unit_name, make, model, year, account_id, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
      `;
      values = [plate_number, unit_name, make, model, year, accId, status];
    } else {
      sql = `
        INSERT INTO vehicles (plate_number, unit_name, make, model, year, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
      `;
      values = [plate_number, unit_name, make, model, year, status];
    }

    const result = await query(sql, values);

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

export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;

    const existing = await query(`SELECT * FROM vehicles WHERE id = $1`, [
      parseInt(id, 10),
    ]);

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

    const hasAccountId = await hasColumn("account_id");

    let sql;
    let values;

    if (hasAccountId) {
      const accId =
        account_id != null
          ? parseInt(account_id, 10)
          : current.account_id;

      sql = `
        UPDATE vehicles
        SET plate_number = $1,
            unit_name = $2,
            make = $3,
            model = $4,
            year = $5,
            account_id = $6,
            status = $7,
            updated_at = NOW()
        WHERE id = $8
        RETURNING *
      `;

      values = [
        plate_number ?? current.plate_number,
        unit_name ?? current.unit_name,
        make ?? current.make,
        model ?? current.model,
        year ?? current.year,
        accId,
        status ?? current.status,
        id,
      ];
    } else {
      sql = `
        UPDATE vehicles
        SET plate_number = $1,
            unit_name = $2,
            make = $3,
            model = $4,
            year = $5,
            status = $6,
            updated_at = NOW()
        WHERE id = $7
        RETURNING *
      `;

      values = [
        plate_number ?? current.plate_number,
        unit_name ?? current.unit_name,
        make ?? current.make,
        model ?? current.model,
        year ?? current.year,
        status ?? current.status,
        id,
      ];
    }

    const result = await query(sql, values);

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

export async function deleteVehicle(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `DELETE FROM vehicles WHERE id = $1 RETURNING id`,
      [parseInt(id, 10)]
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
      message: "Failed to delete vehicle",
    });
  }
}