import { query } from "../config/db.js";

const mockDevices = [
  {
    deviceUid: "VEH001",
    label: "Tracker 001",
    vehicleReg: "KDA 190Z",
    clientId: "CLIENT001",
    companyId: null,
    saccoId: "SACCO001",
    expiresAt: null,
    isExpired: false,
    alertCount: 1,
  },
  {
    deviceUid: "VEH002",
    label: "Tracker 002",
    vehicleReg: "KBT 742X",
    clientId: "CLIENT002",
    companyId: "COMP001",
    saccoId: null,
    expiresAt: null,
    isExpired: false,
    alertCount: 0,
  },
];

export async function getFleetDevices(_req, res) {
  try {
    let devices = [];

    try {
      const result = await query(`
        SELECT
          d.device_uid AS "deviceUid",
          COALESCE(d.label, d.device_uid) AS label,
          COALESCE(v.plate_number, '') AS "vehicleReg",
          a.client_code AS "clientId",
          a.company_code AS "companyId",
          a.sacco_code AS "saccoId",
          d.expires_at AS "expiresAt",
          CASE
            WHEN d.expires_at IS NOT NULL AND d.expires_at < NOW() THEN true
            ELSE false
          END AS "isExpired",
          0::int AS "alertCount"
        FROM devices d
        LEFT JOIN vehicles v ON v.id = d.vehicle_id
        LEFT JOIN accounts a ON a.id = v.account_id
        ORDER BY d.created_at DESC
      `);

      devices = result.rows;
    } catch {
      devices = mockDevices;
    }

    return res.json({
      success: true,
      data: devices,
    });
  } catch (error) {
    console.error("getFleetDevices error:", error);
    return res.status(500).json({
      success: false,
      message: "Failed to load fleet devices",
    });
  }
}