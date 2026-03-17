import { pool } from "../config/db.js";
import { fetchMariaTrackingData } from "./mariaTracking.service.js";

export async function runMariaSync() {
  let client;

  try {
    client = await pool.connect();

    const remoteRows = await fetchMariaTrackingData();

    if (!Array.isArray(remoteRows) || remoteRows.length === 0) {
      return {
        success: true,
        fetched: 0,
        inserted: 0,
        updated: 0,
        message: "No rows returned from MariaDB source",
      };
    }

    let inserted = 0;
    let updated = 0;
    let skipped = 0;

    for (const row of remoteRows) {
      const {
        device_uid,
        plate_number,
        speed,
        latitude,
        longitude,
        ignition,
        status,
        tracked_at,
      } = normalizeTrackingRow(row);

      if (!device_uid) {
        skipped++;
        continue;
      }

      const existing = await client.query(
        `SELECT id FROM devices WHERE device_uid = $1 LIMIT 1`,
        [device_uid]
      );

      let deviceId = null;

      if (existing.rows.length > 0) {
        deviceId = existing.rows[0].id;

        await client.query(
          `
          UPDATE devices
          SET
            plate_number = COALESCE($2, plate_number),
            status = COALESCE($3, status),
            updated_at = NOW()
          WHERE id = $1
          `,
          [deviceId, plate_number, status || "online"]
        );

        updated++;
      } else {
        const insertedDevice = await client.query(
          `
          INSERT INTO devices (device_uid, plate_number, status, created_at, updated_at)
          VALUES ($1, $2, $3, NOW(), NOW())
          RETURNING id
          `,
          [device_uid, plate_number, status || "online"]
        );

        deviceId = insertedDevice.rows[0].id;
        inserted++;
      }

      if (latitude != null && longitude != null) {
        await client.query(
          `
          INSERT INTO positions (
            device_id,
            latitude,
            longitude,
            speed,
            ignition,
            tracked_at,
            created_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, NOW())
          `,
          [
            deviceId,
            latitude,
            longitude,
            speed ?? 0,
            ignition ?? false,
            tracked_at || new Date(),
          ]
        );
      }
    }

    return {
      success: true,
      fetched: remoteRows.length,
      inserted,
      updated,
      skipped,
    };
  } finally {
    if (client) client.release();
  }
}

function normalizeTrackingRow(row) {
  return {
    device_uid:
      row.device_uid ||
      row.deviceUid ||
      row.imei ||
      row.device_id ||
      null,

    plate_number:
      row.plate_number ||
      row.plateNumber ||
      row.vehicle_reg ||
      row.registration ||
      null,

    speed: toNumber(row.speed),
    latitude: toNumber(row.latitude),
    longitude: toNumber(row.longitude),

    ignition:
      row.ignition === true ||
      row.ignition === 1 ||
      row.ignition === "1" ||
      row.ignition === "true",

    status: row.status || "online",

    tracked_at:
      row.tracked_at ||
      row.trackedAt ||
      row.gps_time ||
      row.created_at ||
      new Date(),
  };
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}