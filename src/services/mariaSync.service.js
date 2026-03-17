import { mariaPool } from "../config/mariadb.js";
import { query } from "../config/db.js";

function toNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeTimestamp(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

async function writeSyncLog({ jobName, status, recordsProcessed, message, startedAt }) {
  await query(
    `
    INSERT INTO sync_logs (job_name, status, records_processed, message, started_at, finished_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    `,
    [jobName, status, recordsProcessed, message || null, startedAt]
  );
}

async function upsertVehicle(registration) {
  const existing = await query(
    `SELECT id FROM vehicles WHERE plate_number = $1 LIMIT 1`,
    [registration]
  );

  if (existing.rows.length) return existing.rows[0].id;

  const created = await query(
    `
    INSERT INTO vehicles (plate_number, status)
    VALUES ($1, 'active')
    RETURNING id
    `,
    [registration]
  );

  return created.rows[0].id;
}

async function upsertDevice({ registration, lookupSerial, sourceDeviceId }) {
  const vehicleId = await upsertVehicle(registration);
  const deviceUid = String(sourceDeviceId || lookupSerial || registration).trim();

  const existing = await query(
    `SELECT id FROM devices WHERE device_uid = $1 LIMIT 1`,
    [deviceUid]
  );

  if (existing.rows.length) {
    await query(
      `
      UPDATE devices
      SET label = COALESCE(label, $2),
          vehicle_id = COALESCE(vehicle_id, $3)
      WHERE id = $1::bigint OR device_uid = $1
      `,
      [deviceUid, registration, vehicleId]
    );

    const row = await query(
      `SELECT id FROM devices WHERE device_uid = $1 LIMIT 1`,
      [deviceUid]
    );

    return row.rows[0].id;
  }

  const created = await query(
    `
    INSERT INTO devices (device_uid, label, serial_number, vehicle_id, created_at)
    VALUES ($1, $2, $3, $4, NOW())
    RETURNING id
    `,
    [deviceUid, registration, lookupSerial, vehicleId]
  );

  return created.rows[0].id;
}

async function upsertLatestPosition({
  deviceId,
  lat,
  lon,
  speedKph,
  heading,
  deviceTime,
  receivedAt,
}) {
  await query(
    `
    INSERT INTO latest_positions (
      device_id, latitude, longitude, speed_kph, heading, device_time, received_at, updated_at
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
    [deviceId, lat, lon, speedKph, heading, deviceTime, receivedAt]
  );
}

async function insertTelemetry({
  deviceId,
  lat,
  lon,
  speedKph,
  heading,
  deviceTime,
  receivedAt,
}) {
  await query(
    `
    INSERT INTO telemetry (
      device_id, latitude, longitude, speed_kph, heading, device_time, received_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    `,
    [deviceId, lat, lon, speedKph, heading, deviceTime, receivedAt]
  );
}

async function saveExternalLink({ registration, sourceSerial, lookupSerial, sourceDeviceId }) {
  await query(
    `
    INSERT INTO external_device_links (
      registration, source_serial_number, lookup_serial_number, source_device_id, updated_at
    )
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (registration)
    DO UPDATE SET
      source_serial_number = EXCLUDED.source_serial_number,
      lookup_serial_number = EXCLUDED.lookup_serial_number,
      source_device_id = EXCLUDED.source_device_id,
      updated_at = NOW()
    `,
    [registration, sourceSerial, lookupSerial, String(sourceDeviceId || "")]
  );
}

export async function runMariaSync() {
  const startedAt = new Date().toISOString();
  let processed = 0;

  try {
    const conn = await mariaPool.getConnection();

    try {
      // Adjust column names once third party confirms schema
      const [registrations] = await conn.query(`
        SELECT registration, serial_number
        FROM registration
        WHERE serial_number IS NOT NULL
        LIMIT 500
      `);

      for (const reg of registrations) {
        const registration = String(reg.registration || "").trim();
        const rawSerial = String(reg.serial_number || "").trim();

        if (!registration || !rawSerial) continue;

        const lookupSerial = `0${rawSerial}`;

        const [devices] = await conn.query(
          `
          SELECT id, serial_number, last_update
          FROM device
          WHERE serial_number = ?
          ORDER BY last_update DESC
          LIMIT 1
          `,
          [lookupSerial]
        );

        if (!devices.length) continue;

        const sourceDevice = devices[0];
        const sourceDeviceId = sourceDevice.id;

        const [events] = await conn.query(
          `
          SELECT *
          FROM event_data
          WHERE device_id = ?
          ORDER BY id DESC
          LIMIT 1
          `,
          [sourceDeviceId]
        );

        if (!events.length) continue;

        const event = events[0];

        // Replace these field names with actual third-party schema names
        const lat = toNumber(event.latitude ?? event.lat);
        const lon = toNumber(event.longitude ?? event.lon);
        const speedKph = toNumber(event.speed ?? event.speed_kph, 0);
        const heading = toNumber(event.heading, 0);
        const deviceTime = normalizeTimestamp(event.device_time ?? event.gps_time ?? event.event_time);
        const receivedAt =
          normalizeTimestamp(event.received_at ?? event.created_at ?? sourceDevice.last_update) ||
          new Date().toISOString();

        if (lat === null || lon === null) continue;

        const deviceId = await upsertDevice({
          registration,
          lookupSerial,
          sourceDeviceId,
        });

        await saveExternalLink({
          registration,
          sourceSerial: rawSerial,
          lookupSerial,
          sourceDeviceId,
        });

        await insertTelemetry({
          deviceId,
          lat,
          lon,
          speedKph,
          heading,
          deviceTime,
          receivedAt,
        });

        await upsertLatestPosition({
          deviceId,
          lat,
          lon,
          speedKph,
          heading,
          deviceTime,
          receivedAt,
        });

        processed += 1;
      }
    } finally {
      conn.release();
    }

    await writeSyncLog({
      jobName: "maria_sync",
      status: "success",
      recordsProcessed: processed,
      message: "Sync completed successfully",
      startedAt,
    });

    return {
      success: true,
      processed,
    };
  } catch (error) {
    await writeSyncLog({
      jobName: "maria_sync",
      status: "failed",
      recordsProcessed: processed,
      message: error.message,
      startedAt,
    });

    throw error;
  }
}