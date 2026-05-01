import dotenv from "dotenv";
dotenv.config();

import { initDb, initMariaDB } from "../config/initDb.js";
import { runMariaSync, syncVehicles } from "../services/mariaSync.service.js";

const SYNC_INTERVAL = Number(process.env.SYNC_INTERVAL || 60_000); // 1 minute default
const VEHICLE_SYNC_INTERVAL = 30 * 60_000; // 30 minutes

let isRunning = false;

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

async function start() {
  await initDb().catch(e => console.warn("[Worker] initDb warning:", e.message));
  await initMariaDB().catch(() => {});

  console.log("[Worker] ✅ Telemetry worker started");
  console.log(`[Worker] ⚡ Sync every ${SYNC_INTERVAL / 1000}s`);

  // Sync vehicles once on startup, then every 30 min
  try { await syncVehicles(); } catch (e) {
    console.error("[Worker] Initial vehicle sync failed:", e.message);
  }
  setInterval(async () => {
    try { await syncVehicles(); } catch (e) {
      console.error("[Worker] Vehicle sync failed:", e.message);
    }
  }, VEHICLE_SYNC_INTERVAL);

  // Immediate first telemetry sync
  await runSafe();

  // Then on interval
  setInterval(runSafe, SYNC_INTERVAL);
}

start().catch(e => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});