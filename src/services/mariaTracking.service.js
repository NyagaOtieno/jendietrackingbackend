import { mariaPool } from "../config/mariadb.js";

/**
 * Assumptions:
 * - registration.serial_number exists
 * - device.serial_number exists
 * - device.id is the device id
 * - device.last_update exists
 * - event_data.device_id links to device.id
 */

export async function getTrackingDataFromRegistration(registrationValue) {
  const conn = await mariaPool.getConnection();

  try {
    // 1) find registration row
    const [registrations] = await conn.query(
      `
      SELECT *
      FROM registration
      WHERE registration = ?
      LIMIT 1
      `,
      [registrationValue]
    );

    if (!registrations.length) {
      return {
        success: false,
        message: "Registration not found",
      };
    }

    const reg = registrations[0];
    const rawSerial = String(reg.serial_number || "").trim();

    if (!rawSerial) {
      return {
        success: false,
        message: "Serial number not found in registration table",
      };
    }

    // 2) add leading 0
    const serialWithZero = `0${rawSerial}`;

    // 3) find device using serial number
    const [devices] = await conn.query(
      `
      SELECT *
      FROM device
      WHERE serial_number = ?
      ORDER BY last_update DESC
      LIMIT 1
      `,
      [serialWithZero]
    );

    if (!devices.length) {
      return {
        success: false,
        message: "Matching device not found",
        data: {
          registration: reg.registration,
          serial_number: rawSerial,
          serial_with_zero: serialWithZero,
        },
      };
    }

    const device = devices[0];
    const deviceId = device.id;

    // 4) get all event data for device
    const [events] = await conn.query(
      `
      SELECT *
      FROM event_data
      WHERE device_id = ?
      ORDER BY id DESC
      `,
      [deviceId]
    );

    return {
      success: true,
      data: {
        registration: reg,
        serial_number_original: rawSerial,
        serial_number_lookup: serialWithZero,
        device,
        events,
      },
    };
  } finally {
    conn.release();
  }
}