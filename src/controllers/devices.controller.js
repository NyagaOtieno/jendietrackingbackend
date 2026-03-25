import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";
import crypto from "crypto";

function generateApiKey() {
  return crypto.randomBytes(32).toString("hex");
}

function isPrivileged(req) {
  return isPrivilegedRole(req.user.role);
}

export async function getDevices(req, res) {
  try {
    let sql = `
      SELECT
        d.id,
        d.device_uid,
        d.label,
        d.imei,
        d.sim_number,
        d.protocol_type,
        d.vehicle_id,
        d.expires_at,
        d.created_at,
        v.plate_number,
        v.unit_name,
        v.account_id,
        a.account_name,
        a.account_type
      FROM devices d
      LEFT JOIN vehicles v ON v.id = d.vehicle_id
      LEFT JOIN accounts a ON a.id = v.account_id
    `;

    const params = [];

    if (!isPrivileged(req)) {
      sql += ` WHERE v.account_id = $1 `;
      params.push(req.user.accountId || null);
    }

    sql += ` ORDER BY d.created_at DESC `;

    const result = await query(sql, params);

    return res.json({
      success: true,
      data: result.rows,
    });
  } catch (error) {
    console.error("getDevices error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load devices",
    });
  }
}

export async function getDeviceById(req, res) {
  try {
    const { id } = req.params;

    let sql = `
      SELECT
        d.id,
        d.device_uid,
        d.label,
        d.imei,
        d.sim_number,
        d.protocol_type,
        d.vehicle_id,
        d.expires_at,
        d.created_at,
        v.plate_number,
        v.unit_name,
        v.account_id,
        a.account_name,
        a.account_type
      FROM devices d
      LEFT JOIN vehicles v ON v.id = d.vehicle_id
      LEFT JOIN accounts a ON a.id = v.account_id
      WHERE d.id = $1
    `;

    const params = [id];

    if (!isPrivileged(req)) {
      sql += ` AND v.account_id = $2 `;
      params.push(req.user.accountId || null);
    }

    const result = await query(sql, params);

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Device not found",
      });
    }

    return res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("getDeviceById error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load device",
    });
  }
}


export async function createDevice(req, res) {
  try {
    const {
      label = null,
      imei,
      sim_number = null,
      protocol_type = null,
      vehicle_id = null,
      expires_at = null,
    } = req.body;

    // ✅ validation
    if (!imei) {
      return res.status(400).json({
        success: false,
        message: "imei is required",
      });
    }

    // ✅ prevent duplicates
    const existing = await query(
      `SELECT id FROM devices WHERE imei = $1 LIMIT 1`,
      [imei]
    );

    if (existing.rows.length) {
      return res.status(409).json({
        success: false,
        message: "Device with this IMEI already exists",
      });
    }

    const apiKey = generateApiKey();

    const result = await query(
      `
      INSERT INTO devices (
        label,
        imei,
        sim_number,
        protocol_type,
        vehicle_id,
        expires_at,
        api_key,
        status
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
      RETURNING *
      `,
      [
        label,
        imei,
        sim_number,
        protocol_type,
        vehicle_id,
        expires_at,
        apiKey,
      ]
    );

    return res.status(201).json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("createDevice error:", error);

    return res.status(500).json({
      success: false,
      message: "Failed to create device",
      error: error.message, // TEMP DEBUG
    });
  }
}

export async function updateDevice(req, res) {
  try {
    const { id } = req.params;

    const existing = await query(`SELECT * FROM devices WHERE id = $1`, [id]);

    if (!existing.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Device not found",
      });
    }

    const current = existing.rows[0];
    const {
      device_uid,
      label,
      imei,
      sim_number,
      protocol_type,
      vehicle_id,
      expires_at,
    } = req.body;

    const result = await query(
      `
      UPDATE devices
      SET
        device_uid = $1,
        label = $2,
        imei = $3,
        sim_number = $4,
        protocol_type = $5,
        vehicle_id = $6,
        expires_at = $7
      WHERE id = $8
      RETURNING *
      `,
      [
        device_uid ?? current.device_uid,
        label ?? current.label,
        imei ?? current.imei,
        sim_number ?? current.sim_number,
        protocol_type ?? current.protocol_type,
        vehicle_id ?? current.vehicle_id,
        expires_at ?? current.expires_at,
        id,
      ]
    );

    return res.json({
      success: true,
      data: result.rows[0],
    });
  } catch (error) {
    console.error("updateDevice error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to update device",
    });
  }
}

export async function deleteDevice(req, res) {
  try {
    const { id } = req.params;

    const result = await query(
      `DELETE FROM devices WHERE id = $1 RETURNING id`,
      [id]
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Device not found",
      });
    }

    return res.json({
      success: true,
      message: "Device deleted",
    });
  } catch (error) {
    console.error("deleteDevice error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to delete device",
    });
  }
}