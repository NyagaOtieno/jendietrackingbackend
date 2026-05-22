// src/workers/telemetryBufferWorker.js

import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,
  mariaPool,
  loadDeviceMap
} from "../services/mariaSync.service.js";

import { pgPool } from "../config/db.js";

/* ────────────────────────────────
   INTERVALS (1GB VPS SAFE)
──────────────────────────────── */
const QUICK_INTERVAL   = 15000;   // 15s
const FULL_INTERVAL    = 60000;   // 60s
const VEHICLE_INTERVAL = 1800000; // 30min

let running = {
  quick: false,
  full: false,
  vehicle: false
};

/* ────────────────────────────────
   SAFE RUNNER
──────────────────────────────── */
const safeRun = (name, flag, fn) => async () => {
  if (running[flag]) return;
  running[flag] = true;

  try {
    await fn();
  } catch (e) {
    console.error(`[Worker] ${name} error:`, e.message);
  } finally {
    running[flag] = false;
  }
};

/* ────────────────────────────────
   TASKS
──────────────────────────────── */
const vehicleTask = safeRun("vehicleSync", "vehicle", syncVehicles);
const fullTask    = safeRun("fullSync", "full", runMariaSync);
const quickTask   = safeRun("quickSync", "quick", runQuickSync);

/* ────────────────────────────────
   START
──────────────────────────────── */
async function start() {
  try {
    await pgPool.query(`SELECT 1`);
    const mc = await mariaPool.getConnection();
    mc.release();

    console.log("✅ DBs connected");

    await loadDeviceMap();

    await vehicleTask();
    setInterval(vehicleTask, VEHICLE_INTERVAL);

    setTimeout(() => {
      console.log(`[Worker] Full sync every ${FULL_INTERVAL / 1000}s`);
      fullTask();
      setInterval(fullTask, FULL_INTERVAL);
    }, 5000);

    setTimeout(() => {
      console.log(`[Worker] Quick sync every ${QUICK_INTERVAL / 1000}s`);
      quickTask();
      setInterval(quickTask, QUICK_INTERVAL);
    }, 15000);

    setInterval(() => {
      console.log("[POOL STATUS]", {
        maria_total: mariaPool.totalConnections?.(),
        maria_active: mariaPool.activeConnections?.(),
        maria_idle: mariaPool.idleConnections?.(),
      });
    }, 30000);

  } catch (e) {
    console.error("[Worker] Fatal startup error:", e.message);
    setTimeout(start, 20000);
  }
}

process.on("uncaughtException", (e) =>
  console.error("[Worker] Uncaught:", e.message)
);

process.on("unhandledRejection", (e) =>
  console.error("[Worker] Rejection:", e)
);

start();