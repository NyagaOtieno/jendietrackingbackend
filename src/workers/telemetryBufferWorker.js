import dotenv from "dotenv";
dotenv.config();
import { pgPool } from "../config/db.js";
import { syncVehicles, runQuickSync, loadDeviceMap, mariaPool } from "../services/mariaSync.service.js";

const QUICK_INTERVAL   = Number(process.env.QUICK_INTERVAL   || 15000);
const VEHICLE_INTERVAL = Number(process.env.VEHICLE_INTERVAL || 1800000);
let quickBusy = false, vehicleBusy = false;

async function quickTick() {
  if (quickBusy) return;
  quickBusy = true;
  try { await runQuickSync(); }
  catch(e) { console.error("quickTick error:", e.message); }
  finally { quickBusy = false; }
}

async function vehicleTick() {
  if (vehicleBusy) return;
  vehicleBusy = true;
  try { await syncVehicles(); await loadDeviceMap(); }
  catch(e) { console.error("vehicleTick error:", e.message); }
  finally { vehicleBusy = false; }
}

async function start() {
  const pg = await pgPool.connect(); pg.release();
  console.log("PostgreSQL connected");
  const mc = await mariaPool.getConnection(); mc.release();
  console.log("MariaDB connected");
  await loadDeviceMap();
  console.log("Device map loaded, starting quickSync loop");
  await quickTick();
  setInterval(quickTick, QUICK_INTERVAL);
  vehicleTick();
  setInterval(vehicleTick, VEHICLE_INTERVAL);
}

start().catch(e => { console.error("Fatal:", e.message); process.exit(1); });