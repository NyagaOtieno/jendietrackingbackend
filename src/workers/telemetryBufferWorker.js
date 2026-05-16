// src/workers/telemetryBufferWorker.js
import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, mariaPool } from "../services/mariaSync.service.js";
import { pgPool }                                from "../config/db.js";

// ── Intervals ─────────────────────────────────────────────────────────────────
const LIVE_INTERVAL      = Number(process.env.LIVE_SYNC_INTERVAL   ||  15_000); // 15 s
const VEHICLE_INTERVAL   = Number(process.env.VEHICLE_SYNC_INTERVAL || 1_800_000); // 30 min
const TELEMETRY_INTERVAL = Number(process.env.SYNC_INTERVAL         ||    30_000); // 30 s (keep existing)

// ── Guard flags (prevent overlap) ────────────────────────────────────────────
let liveBusy = false;
let vehBusy  = false;
let telBusy  = false;

async function start() {

  // ── Verify connections ────────────────────────────────────────────────────
  const pg = await pgPool.connect();
  pg.release();
  console.log("✅ PostgreSQL connected");

  const mc = await mariaPool.getConnection();
  mc.release();
  console.log("MariaDB connected");

  // ── Load liveSync dynamically — if the import fails, worker keeps going ───
  let runLiveSync = null;
  try {
    const mod = await import("../services/liveSync.service.js");
    runLiveSync = mod.runLiveSync;
    console.log(`[Worker] Live sync enabled — every ${LIVE_INTERVAL / 1000}s`);
  } catch (e) {
    console.warn("[Worker] Live sync unavailable (will use MariaSync only):", e.message);
  }

  // ── Vehicle registry — once on boot, then every 30 min ───────────────────
  try { await syncVehicles(); } catch (e) { console.error("[Worker] Vehicle sync error:", e.message); }
  setInterval(async () => {
    if (vehBusy) return;
    vehBusy = true;
    try   { await syncVehicles(); }
    catch (e) { console.error("[Worker] Vehicle sync error:", e.message); }
    finally   { vehBusy = false; }
  }, VEHICLE_INTERVAL);

  // ── Live position sync — every 15 s (fast, only active devices) ──────────
  if (runLiveSync) {
    // First run after 3 s so DB is warm
    setTimeout(async () => {
      if (!liveBusy) { liveBusy = true; try { await runLiveSync(); } catch {} finally { liveBusy = false; } }
      setInterval(async () => {
        if (liveBusy) return;
        liveBusy = true;
        try   { await runLiveSync(); }
        catch (e) { console.error("[Worker] Live sync error:", e.message); }
        finally   { liveBusy = false; }
      }, LIVE_INTERVAL);
    }, 3_000);
  }

  // ── Full MariaSync — every 30 s (history + latest_positions fallback) ─────
  // Starts 10 s after boot to let live sync run first
  setTimeout(async () => {
    if (!telBusy) {
      telBusy = true;
      try   { await runMariaSync(); }
      catch (e) { console.error("[Worker] MariaSync error:", e.message); }
      finally   { telBusy = false; }
    }
    setInterval(async () => {
      if (telBusy) return;
      telBusy = true;
      try   { await runMariaSync(); }
      catch (e) { console.error("[Worker] MariaSync error:", e.message); }
      finally   { telBusy = false; }
    }, TELEMETRY_INTERVAL);
  }, 10_000);
}

start().catch((e) => {
  console.error("[Worker] Fatal start error:", e.message);
  process.exit(1);
});