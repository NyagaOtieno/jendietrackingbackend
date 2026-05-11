import dotenv from "dotenv";
dotenv.config();

import { initDb, initMariaDB } from "../config/initDb.js";
import { runMariaSync, syncVehicles } from "../services/mariaSync.service.js";

const SYNC_INTERVAL = Number(process.env.SYNC_INTERVAL || 60_000); // 1 minute
const VEHICLE_SYNC_INTERVAL = 30 * 60_000; // 30 minutes

let isRunning = false;
let vehicleSyncRunning = false;

/**
 * =========================
 * TELEMETRY SYNC (SAFE)
 * =========================
 */
async function runSafe() {
  if (isRunning) return;

  isRunning = true;
  try {
    await runMariaSync();
  } catch (e) {
    console.error("[Worker] MariaSync error:", e.message);
  } finally {
    isRunning = false;
  }
}

/**
 * =========================
 * VEHICLE SYNC (SAFE)
 * =========================
 */
async function runVehicleSync() {
  if (vehicleSyncRunning) return;

  vehicleSyncRunning = true;
  try {
    await syncVehicles();
  } catch (e) {
    console.error("[Worker] Vehicle sync failed:", e.message);
  } finally {
    vehicleSyncRunning = false;
  }
}

/**
 * =========================
 * START WORKER
 * =========================
 */
async function start() {
  await initDb().catch(e =>
    console.warn("[Worker] initDb warning:", e.message)
  );

  await initMariaDB().catch(() => {});

  console.log("[Worker] ✅ Telemetry worker started");
  console.log(`[Worker] ⚡ Sync every ${SYNC_INTERVAL / 1000}s`);

  /**
   * Vehicle sync on startup
   */
  try {
    await runVehicleSync();
  } catch (e) {
    console.error("[Worker] Initial vehicle sync failed:", e.message);
  }

  /**
   * Vehicle sync interval
   */
  setInterval(runVehicleSync, VEHICLE_SYNC_INTERVAL);

  /**
   * Telemetry sync (first run immediate)
   */
  await runSafe();

  /**
   * Telemetry interval
   */
  setInterval(runSafe, SYNC_INTERVAL);
}

/**
 * =========================
 * BOOT
 * =========================
 */
start().catch(e => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});