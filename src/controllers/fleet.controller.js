import { query } from "../config/db.js";
import { isPrivilegedRole } from "../middleware/auth.js";

export async function getFleets(req, res) {
  try {
    const params = [];
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

    if (!isPrivilegedRole(req.user.role)) {
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
    console.error("getFleets error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load fleet devices",
    });
  }
}