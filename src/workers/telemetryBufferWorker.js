// src/workers/telemetryBufferWorker.js

import { runQuickSync, runMariaSync } from "../services/mariaSync.service.js";

const log = (level, msg, meta = {}) =>
  console.log(JSON.stringify({ time: new Date().toISOString(), level, msg, ...meta }));

let quickInterval = null;
let fullInterval = null;

let shuttingDown = false;

// ─────────────────────────────────────────────────────────────
// CONFIG (safe defaults for 512MB VPS)
// ─────────────────────────────────────────────────────────────
const QUICK_SYNC_MS = Number(process.env.QUICK_SYNC_MS || 10000); // 10s
const FULL_SYNC_MS  = Number(process.env.FULL_SYNC_MS  || 60000); // 60s

// ─────────────────────────────────────────────────────────────
// START WORKER
// ─────────────────────────────────────────────────────────────
export function startTelemetryBufferWorker() {
  if (quickInterval || fullInterval) {
    log("warn", "Worker already running");
    return;
  }

  log("info", "Telemetry Buffer Worker starting...");

  // QUICK SYNC LOOP (latest positions)
  quickInterval = setInterval(async () => {
    if (shuttingDown) return;

    try {
      await runQuickSync();
    } catch (e) {
      log("error", "QuickSync failed", { error: e.message });
    }
  }, QUICK_SYNC_MS);

  // FULL SYNC LOOP (telemetry history + checkpoint)
  fullInterval = setInterval(async () => {
    if (shuttingDown) return;

    try {
      await runMariaSync();
    } catch (e) {
      log("error", "MariaSync failed", { error: e.message });
    }
  }, FULL_SYNC_MS);

  log("info", "Telemetry Buffer Worker started", {
    quickInterval: QUICK_SYNC_MS,
    fullInterval: FULL_SYNC_MS
  });
}

// ─────────────────────────────────────────────────────────────
// STOP WORKER (GRACEFUL)
// ─────────────────────────────────────────────────────────────
export function stopTelemetryBufferWorker() {
  shuttingDown = true;

  if (quickInterval) clearInterval(quickInterval);
  if (fullInterval) clearInterval(fullInterval);

  quickInterval = null;
  fullInterval = null;

  log("info", "Telemetry Buffer Worker stopped");
}

// ─────────────────────────────────────────────────────────────
// HANDLE PROCESS SIGNALS (PM2 SAFE SHUTDOWN)
// ─────────────────────────────────────────────────────────────
process.on("SIGINT", () => {
  log("warn", "SIGINT received");
  stopTelemetryBufferWorker();
  process.exit(0);
});

process.on("SIGTERM", () => {
  log("warn", "SIGTERM received");
  stopTelemetryBufferWorker();
  process.exit(0);
});