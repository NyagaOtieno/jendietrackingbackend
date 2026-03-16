
import { query } from "../config/db.js";

export async function getDevices(_req, res) {
  try {
    const result = await query(`
      SELECT
        id,
        device_uid,
        label,
        imei,
        sim_number,
        protocol_type,
        vehicle_id,
        expires_at,
        created_at
      FROM devices
      ORDER BY created_at DESC
    `);

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

    const result = await query(
      `
      SELECT
        id,
        device_uid,
        label,
        imei,
        sim_number,
        protocol_type,
        vehicle_id,
        expires_at,
        created_at
      FROM devices
      WHERE id = $1
      `,
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
      device_uid,
      label = null,
      imei = null,
      sim_number = null,
      protocol_type = null,
      vehicle_id = null,
      expires_at = null,
    } = req.body;

    if (!device_uid) {
      return res.status(400).json({
        success: false,
        message: "device_uid is required",
      });
    }

    const result = await query(
      `
      INSERT INTO devices (
        device_uid,
        label,
        imei,
        sim_number,
        protocol_type,
        vehicle_id,
        expires_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *
      `,
      [device_uid, label, imei, sim_number, protocol_type, vehicle_id, expires_at]
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
