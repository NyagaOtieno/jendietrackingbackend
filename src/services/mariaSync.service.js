import dotenv from "dotenv";
dotenv.config();

import { mariaPool } from "../config/mariadb.js";
import { pgPool } from "../config/db.js";

// ─────────────────────────────
// LOGGER
// ─────────────────────────────
const log = (level, msg, meta = {}) => {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      level,
      msg,
      ...meta,
    })
  );
};

// ─────────────────────────────
// LOCKING STATE
// ─────────────────────────────
let isSyncRunning = false;
export { isSyncRunning };

// ─────────────────────────────
// CONFIG
// ─────────────────────────────
const DEVICE_BATCH = Number(process.env.DEVICE_BATCH || 200);
const EVENTS_BATCH = Number(process.env.EVENTS_BATCH || 200);

// ─────────────────────────────
// LOCK (POSTGRES SAFE)
// ─────────────────────────────
async function acquireLock() {
  const res = await pgPool.query(
    "SELECT pg_try_advisory_lock(778899) AS locked"
  );
  return res.rows[0].locked;
}

async function releaseLock() {
  await pgPool.query("SELECT pg_advisory_unlock(778899)");
}

// ─────────────────────────────
// VEHICLE SYNC
// ─────────────────────────────
export async function syncVehicles() {
  let conn;

  try {
    conn = await mariaPool.getConnection();
    log("info", "MariaDB connected for vehicle sync");

    const rows = await conn.query(`
      SELECT r.serial, r.reg_no, r.vmodel, r.pstatus,
             d.uniqueid AS device_uid
      FROM registration r
      LEFT JOIN device d ON d.uniqueid = CONCAT('0', r.serial)
      WHERE r.serial IS NOT NULL
      LIMIT 5000
    `);

    let count = 0;

    for (const r of rows) {
      const serial = String(r.serial || "").trim();
      if (!serial) continue;

      await pgPool.query(
        `
        INSERT INTO vehicles (serial, plate_number, unit_name, model, status, created_at)
        VALUES ($1,$2,$3,$4,$5,NOW())
        ON CONFLICT (serial) DO UPDATE SET
          plate_number = EXCLUDED.plate_number,
          unit_name = EXCLUDED.unit_name,
          model = EXCLUDED.model,
          status = EXCLUDED.status
        `,
        [
          serial,
          r.reg_no || serial,
          `Unit ${serial}`,
          r.vmodel || "",
          r.pstatus || "inactive",
        ]
      );

      count++;
    }

    log("info", "Vehicle sync complete", { vehicles: count });
  } catch (e) {
    log("error", "Vehicle sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────
// TELEMETRY SYNC
// ─────────────────────────────
export async function syncTelemetry() {
  let conn;

  try {
    conn = await mariaPool.getConnection();

    const { rows: devices } = await pgPool.query(
      "SELECT device_uid, positionid FROM devices"
    );

    let total = 0;

    for (const d of devices.slice(0, DEVICE_BATCH)) {
      const mariaDev = await conn.query(
        "SELECT id FROM device WHERE uniqueid=? LIMIT 1",
        [d.device_uid]
      );

      if (!mariaDev.length) continue;

      const mariaDeviceId = mariaDev[0].id;

      const rows = await conn.query(
        `
        SELECT id, devicetime, latitude, longitude, speed, course
        FROM eventData
        WHERE deviceid=? AND id > ?
        ORDER BY id ASC
        LIMIT ?
        `,
        [mariaDeviceId, d.positionid || 0, EVENTS_BATCH]
      );

      if (!rows.length) continue;

      const values = [];
      const ids = [];

      for (const r of rows) {
        const lat = Number(r.latitude);
        const lon = Number(r.longitude);
        if (!lat || !lon) continue;

        values.push([
          d.device_uid,
          lat,
          lon,
          Number(r.speed) || 0,
          Number(r.course) || 0,
          new Date(r.devicetime),
        ]);

        ids.push(r.id);
      }

      if (!values.length) continue;

      for (const v of values) {
        await pgPool.query(
          `
          INSERT INTO telemetry
          (device_uid, latitude, longitude, speed_kph, heading, device_time)
          VALUES ($1,$2,$3,$4,$5,$6)
          ON CONFLICT DO NOTHING
          `,
          v
        );
      }

      await pgPool.query(
        `
        UPDATE devices
        SET positionid = GREATEST(positionid,$1)
        WHERE device_uid=$2
        `,
        [Math.max(...ids), d.device_uid]
      );

      total += values.length;
    }

    log("info", "Telemetry sync complete", { total });
  } catch (e) {
    log("error", "Telemetry sync failed", { error: e.message });
  } finally {
    conn?.release();
  }
}

// ─────────────────────────────
// MAIN SYNC
// ─────────────────────────────
export async function runMariaSync() {
  if (isSyncRunning) return;

  const locked = await acquireLock();
  if (!locked) return;

  isSyncRunning = true;

  try {
    log("info", "MariaSync started");

    await syncVehicles();
    await syncTelemetry();

    log("info", "MariaSync completed");
  } catch (e) {
    log("error", "MariaSync failed", { error: e.message });
  } finally {
    isSyncRunning = false;
    await releaseLock();
  }
}