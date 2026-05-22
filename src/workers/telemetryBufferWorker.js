import dotenv from "dotenv";
dotenv.config();

import {
  runMariaSync,
  runQuickSync,
  mariaPool,
  syncVehicles,
  loadDeviceMap
} from "../services/mariaSync.service.js";

import { pgPool } from "../config/db.js";

const QUICK_INTERVAL = 15000;
const FULL_INTERVAL = 60000;
const VEHICLE_INTERVAL = 1800000;

let globalLock = false;

const safe = (name, fn) => async () => {
  if (globalLock) {
    console.log(`[Worker] Skipped ${name} (lock active)`);
    return;
  }

  try {
    globalLock = true;
    await fn();
  } catch (e) {
    console.error(`[Worker] ${name}:`, e.message);
  } finally {
    globalLock = false;
  }
};

const safeVehicle = safe("vehicleSync", syncVehicles);
const safeFull = safe("MariaSync", runMariaSync);
const safeQuick = safe("quickSync", runQuickSync);

async function start() {
  try {
    await pgPool.connect();
    console.log("✅ PostgreSQL connected");

    const mc = await mariaPool.getConnection();
    mc.release();
    console.log("✅ MariaDB connected");

    await loadDeviceMap();

    setInterval(safeVehicle, VEHICLE_INTERVAL);
    setTimeout(() => {
      console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
      safeFull();
      setInterval(safeFull, FULL_INTERVAL);
    }, 5000);

    setTimeout(() => {
      console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
      safeQuick();
      setInterval(safeQuick, QUICK_INTERVAL);
    }, 10000);

  } catch (e) {
    console.error("[Worker] Fatal startup error:", e.message);
    setTimeout(start, 30000);
  }
}

start();