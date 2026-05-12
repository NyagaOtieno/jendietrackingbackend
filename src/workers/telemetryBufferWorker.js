import dotenv from "dotenv";
dotenv.config();

import { syncVehicles, runMariaSync, mariaPool } from "../services/mariaSync.service.js";
import { pgPool } from "../config/db.js";

const TELEMETRY_INTERVAL = Number(process.env.SYNC_INTERVAL || 30000);
const VEHICLE_INTERVAL   = Number(process.env.VEHICLE_SYNC_INTERVAL || 1800000);

let telBusy = false;
let vehBusy = false;

async function vehicleTick() {
  if (vehBusy) return;
  vehBusy = true;
  try { await syncVehicles(); }
  catch (e) { console.error("[Worker] Vehicle sync error:", e.message); }
  finally { vehBusy = false; }
}

async function telemetryTick() {
  if (telBusy) return;
  telBusy = true;
  try { await runMariaSync(); }
  catch (e) { console.error("[Worker] Telemetry sync error:", e.message); }
  finally { telBusy = false; }
}

async function start() {
  const pg = await pgPool.connect();
  pg.release();
  console.log("PostgreSQL connected");

  const mc = await mariaPool.getConnection();
  mc.release();
  console.log("MariaDB connected");

  await vehicleTick();
  setInterval(vehicleTick, VEHICLE_INTERVAL);

  setTimeout(async () => {
    console.log("[Worker] Telemetry sync every " + (TELEMETRY_INTERVAL / 1000) + "s");
    await telemetryTick();
    setInterval(telemetryTick, TELEMETRY_INTERVAL);
  }, 5000);
}

start().catch(e => {
  console.error("[Worker] Fatal:", e.message);
  process.exit(1);
});