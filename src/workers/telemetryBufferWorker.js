import dotenv from "dotenv";
dotenv.config();

import { initDb, initMariaDB } from "../config/initDb.js";
import {
  runMariaSync,
  syncVehicles,
  loadDeviceMap,
} from "../services/mariaSync.service.js";

const SYNC_INTERVAL = Number(process.env.SYNC_INTERVAL || 60_000); // 1 min
const VEHICLE_SYNC_INTERVAL = 30 * 60_000; // 30 min

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
    await loadDeviceMap(); // 🔥 CRITICAL
  } catch (e) {
    console.error("[Worker] Vehicle sync failed:", e.message);
  } finally {
    vehicleSyncRunning = false;
  }
}

/**
 * =========================
 * LOOP HELPERS (NO setInterval)
 * =========================
 */
async function telemetryLoop() {
  while (true) {
    await runSafe();
    await new Promise(r => setTimeout(r, SYNC_INTERVAL));
  }
}

async function vehicleLoop() {
  while (true) {
    await runVehicleSync();
    await new Promise(r => setTimeout(r, VEHICLE_SYNC_INTERVAL));
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
  console.log(`[Worker] ⚡ Telemetry every ${SYNC_INTERVAL / 1000}s`);
  console.log(`[Worker] 🚗 Vehicle sync every ${VEHICLE_SYNC_INTERVAL / 60000} min`);

  /**
   * INITIAL LOAD (VERY IMPORTANT)
   */
  try {
    await syncVehicles();
    await loadDeviceMap(); // 🔥 ensures telemetry works immediately
  } catch (e) {
    console.error("[Worker] Initial vehicle sync failed:", e.message);
  }

  /**
   * FIRST TELEMETRY RUN
   */
  await runSafe();

  /**
   * START LOOPS (NON-BLOCKING)
   */
  telemetryLoop();
  vehicleLoop();
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