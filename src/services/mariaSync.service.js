import { pool } from "../config/db.js";
import { fetchMariaTrackingData } from "./mariaTracking.service.js";

export async function runMariaSync() {
  let client;

  try {
    client = await pool.connect();

    const remoteRows = await fetchMariaTrackingData(500);

    if (!Array.isArray(remoteRows) || remoteRows.length === 0) {
      return {
        success: true,
        fetched: 0,
        insertedVehicles: 0,
        insertedDevices: 0,
        telemetryInserted: 0,
        latestUpdated: 0,
        skipped: 0,
        message: "No rows returned from MariaDB source",
      };
    }

    let insertedVehicles = 0;
    let insertedDevices = 0;
    let telemetryInserted = 0;
    let latestUpdated = 0;
    let skipped = 0;

    for (const row of remoteRows) {
      const normalized = normalizeTrackingRow(row);

      if (!normalized.device_uid || !normalized.plate_number) {
        skipped++;
        continue;
      }

      if (normalized.latitude == null || normalized.longitude == null) {
        skipped++;
        continue;
      }

      await client.query("BEGIN");

      try {
        let vehicleId;
        const existingVehicle = await client.query(
          `SELECT id FROM vehicles WHERE plate_number = $1 LIMIT 1`,
          [normalized.plate_number]
        );

        if (existingVehicle.rows.length > 0) {
          vehicleId = existingVehicle.rows[0].id;
        } else {
          const insertedVehicle = await client.query(
            `
            INSERT INTO vehicles (plate_number, unit_name, status, created_at)
            VALUES ($1, $2, $3, NOW())
            RETURNING id
            `,
            [
              normalized.plate_number,
              normalized.plate_number,
              normalized.vehicle_status || "active",
            ]
          );
          vehicleId = insertedVehicle.rows[0].id;
          insertedVehicles++;
        }

        let deviceId;
        const existingDevice = await client.query(
          `SELECT id FROM devices WHERE device_uid = $1 LIMIT 1`,
          [normalized.device_uid]
        );

        if (existingDevice.rows.length > 0) {
          deviceId = existingDevice.rows[0].id;

          await client.query(
            `
            UPDATE devices
            SET
              label = COALESCE($2, label),
              vehicle_id = $3
            WHERE id = $1
            `,
            [deviceId, normalized.plate_number, vehicleId]
          );
        } else {
          const insertedDevice = await client.query(
            `
            INSERT INTO devices (
              device_uid,
              label,
              protocol_type,
              vehicle_id,
              created_at
            )
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING id
            `,
            [
              normalized.device_uid,
              normalized.plate_number,
              normalized.protocol || "jendie",
              vehicleId,
            ]
          );

          deviceId = insertedDevice.rows[0].id;
          insertedDevices++;
        }

        await client.query(
          `
          INSERT INTO telemetry (
            device_id,
            latitude,
            longitude,
            speed_kph,
            heading,
            device_time,
            received_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [
            deviceId,
            normalized.latitude,
            normalized.longitude,
            normalized.speed,
            normalized.heading,
            normalized.device_time,
            normalized.received_at,
          ]
        );

        telemetryInserted++;

        await client.query(
          `
          INSERT INTO latest_positions (
            device_id,
            latitude,
            longitude,
            speed_kph,
            heading,
            device_time,
            received_at,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
          ON CONFLICT (device_id)
          DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            speed_kph = EXCLUDED.speed_kph,
            heading = EXCLUDED.heading,
            device_time = EXCLUDED.device_time,
            received_at = EXCLUDED.received_at,
            updated_at = NOW()
          `,
          [
            deviceId,
            normalized.latitude,
            normalized.longitude,
            normalized.speed,
            normalized.heading,
            normalized.device_time,
            normalized.received_at,
          ]
        );

        latestUpdated++;
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        console.error(
          `Sync failed for ${normalized.plate_number}/${normalized.device_uid}:`,
          error.message
        );
        skipped++;
      }
    }

    return {
      success: true,
      fetched: remoteRows.length,
      insertedVehicles,
      insertedDevices,
      telemetryInserted,
      latestUpdated,
      skipped,
    };
  } finally {
    if (client) client.release();
  }
}

function normalizeTrackingRow(row) {
  const attributes = safeJsonParse(row.attributes);

  return {
    device_uid: row.uniqueid || null,
    source_device_id: row.source_device_id || row.deviceid || null,
    plate_number: row.reg_no || null,
    protocol: row.protocol || "jendie",

    latitude: toNumber(row.latitude),
    longitude: toNumber(row.longitude),
    altitude: toNumber(row.altitude),
    speed: toNumber(row.speed, 0),
    heading: toNumber(row.course, 0),

    gps_valid:
      row.valid === 1 ||
      row.valid === true ||
      row.valid === "1",

    ignition: extractIgnition(attributes, row),

    vehicle_status:
      row.lastupdate || row.servertime ? "active" : "inactive",

    device_time:
      normalizeTimestamp(row.devicetime) ||
      normalizeTimestamp(row.fixtime) ||
      null,

    received_at:
      normalizeTimestamp(row.servertime) ||
      normalizeTimestamp(row.lastupdate) ||
      new Date().toISOString(),

    attributes,
    alarmcode: row.alarmcode || null,
    statuscode: row.statuscode || null,
    odometer: toNumber(row.odometer),
  };
}

function safeJsonParse(value) {
  if (!value) return {};
  if (typeof value === "object") return value;

  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function extractIgnition(attributes, row) {
  if (typeof attributes.ignition === "boolean") return attributes.ignition;
  if (typeof attributes.ignitionOn === "boolean") return attributes.ignitionOn;
  if (typeof attributes.motion === "boolean") return attributes.motion;
  if (toNumber(row.speed, 0) > 0) return true;
  return false;
}

function normalizeTimestamp(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function toNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isNaN(n) ? fallback : n;
}