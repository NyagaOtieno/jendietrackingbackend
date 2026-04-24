import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

// ======================================================
// HELPERS
// ======================================================
function getUser(req) {
  return req?.user || {};
}

function canAccessAllVehicles(req) {
  return isPrivilegedRole(req?.user?.role);
}

function getAccountId(req) {
  return req?.user?.accountId ?? null;
}

function isValidId(id) {
  return id !== undefined && id !== null && id !== "";
}

function isValidYear(year) {
  return (
    typeof year === "number" &&
    year >= 1900 &&
    year <= new Date().getFullYear() + 1
  );
}

// ======================================================
// GET ALL VEHICLES (OPTIMIZED + SEARCH + PAGINATION)
// ======================================================
export async function getVehicles(req, res) {
  try {
    const isPrivileged = canAccessAllVehicles(req);
    const accountId = getAccountId(req);

    // =========================
    // PAGINATION
    // =========================
    const limit = Math.min(parseInt(req.query.limit || "200"), 500);
    const offset = Math.max(parseInt(req.query.offset || "0"), 0);

    // =========================
    // SEARCH (NEW FIX)
    // =========================
    const search = req.query.search?.trim()?.toLowerCase() || null;

    // =========================
    // BASE QUERY
    // =========================
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
        v.serial,
        v.account_id
      FROM vehicles v
    `;

    let countSql = `SELECT COUNT(*) FROM vehicles v`;

    const params = [];
    const countParams = [];

    let where = [];

    // =========================
    // ACCOUNT FILTER
    // =========================
    if (!isPrivileged) {
      if (!accountId) {
        return res.status(401).json({
          success: false,
          message: "Missing account context for user",
        });
      }

      params.push(accountId);
      countParams.push(accountId);

      where.push(`v.account_id = $${params.length}`);
    }

    // =========================
    // SEARCH FILTER (IMPORTANT FIX)
    // =========================
    if (search) {
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);

      const searchParamIndex = params.length;

      where.push(`
        (
          LOWER(v.plate_number) LIKE $${searchParamIndex}
          OR LOWER(v.unit_name) LIKE $${searchParamIndex}
          OR LOWER(v.make) LIKE $${searchParamIndex}
          OR LOWER(v.model) LIKE $${searchParamIndex}
        )
      `);
    }

    // =========================
    // APPLY WHERE
    // =========================
    if (where.length > 0) {
      sql += ` WHERE ` + where.join(" AND ");
      countSql += ` WHERE ` + where.join(" AND ");
    }

    // =========================
    // FINAL ORDER + PAGINATION
    // =========================
    sql += ` ORDER BY v.id DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;

    params.push(limit, offset);

    const [result, countResult] = await Promise.all([
      query(sql, params),
      query(countSql, countParams),
    ]);

    return res.json({
      success: true,
      data: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit,
      offset,
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

    const isPrivileged = canAccessAllVehicles(req);
    const accountId = getAccountId(req);

    let sql = `
      SELECT *
      FROM vehicles
      WHERE id = $1
    `;

    const params = [id];

    if (!isPrivileged) {
      if (!accountId) {
        return res.status(401).json({
          success: false,
          message: "Missing account context",
        });
      }

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
      serial,
    } = req.body;

    if (!plate_number) {
      return res.status(400).json({
        success: false,
        message: "plate_number is required",
      });
    }

    if (year !== undefined && !isValidYear(year)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle year",
      });
    }

    const isPrivileged = canAccessAllVehicles(req);
    const userAccountId = getAccountId(req);

    let finalAccountId;

    if (isPrivileged) {
      if (!account_id) {
        return res.status(400).json({
          success: false,
          message: "account_id is required for privileged users",
        });
      }

      const accCheck = await query(
        `SELECT id FROM accounts WHERE id = $1 LIMIT 1`,
        [account_id]
      );

      if (!accCheck.rows.length) {
        return res.status(404).json({
          success: false,
          message: "Account not found",
        });
      }

      finalAccountId = account_id;
    } else {
      if (!userAccountId) {
        return res.status(400).json({
          success: false,
          message: "Account context missing",
        });
      }

      finalAccountId = userAccountId;
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
// UPDATE VEHICLE
// ======================================================
export async function updateVehicle(req, res) {
  try {
    const { id } = req.params;
    const { make, model, year, account_id } = req.body;

    if (!isValidId(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle id",
      });
    }

    const numericYear =
      year !== undefined && year !== null ? Number(year) : null;

    if (numericYear !== null && Number.isNaN(numericYear)) {
      return res.status(400).json({
        success: false,
        message: "Year must be a number",
      });
    }

    if (
      numericYear !== null &&
      (numericYear < 1900 ||
        numericYear > new Date().getFullYear() + 1)
    ) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle year",
      });
    }

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

    const vehicle = existing.rows[0];

    const finalAccountId = account_id ?? vehicle.account_id;

    const result = await query(
      `
      UPDATE vehicles
      SET
        make = COALESCE($1, make),
        model = COALESCE($2, model),
        year = COALESCE($3, year),
        account_id = COALESCE($4, account_id)
      WHERE id = $5
      RETURNING *
      `,
      [
        make ?? null,
        model ?? null,
        numericYear ?? null,
        finalAccountId ?? null,
        id,
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
      message: "Failed to update vehicle",
    });
  }
}

// ======================================================
// DELETE VEHICLE
// ======================================================
export async function deleteVehicle(req, res) {
  try {
    const { id } = req.params;

    const isPrivileged = canAccessAllVehicles(req);
    const accountId = getAccountId(req);

    if (!isValidId(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid vehicle id",
      });
    }

    let sql = `
      DELETE FROM vehicles
      WHERE id = $1
    `;

    const params = [id];

    if (!isPrivileged) {
      if (!accountId) {
        return res.status(401).json({
          success: false,
          message: "Missing account context",
        });
      }

      sql += ` AND account_id = $2 `;
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