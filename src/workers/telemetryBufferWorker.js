// src/workers/telemetryBufferWorker.js

import dotenv from "dotenv";
dotenv.config();

import {
  syncVehicles,
  runMariaSync,
  runQuickSync,
  mariaPool
} from "../services/mariaSync.service.js";

import { pgPool } from "../config/db.js";

const QUICK_INTERVAL =
  Number(process.env.LIVE_SYNC_INTERVAL || 4000);

const FULL_INTERVAL =
  Number(process.env.SYNC_INTERVAL || 30000);

const VEHICLE_INTERVAL =
  Number(process.env.VEHICLE_SYNC_INTERVAL || 1800000);

let quickBusy=false;
let fullBusy=false;
let vehBusy=false;

async function safeQuickSync(){
    if(quickBusy) return;
    quickBusy=true;

    try{
        await runQuickSync();
    }
    catch(e){
        console.error("[Worker] quickSync:",e.message);
    }
    finally{
        quickBusy=false;
    }
}

async function safeFullSync(){
    if(fullBusy) return;
    fullBusy=true;

    try{
        await runMariaSync();
    }
    catch(e){
        console.error("[Worker] fullSync:",e.message);
    }
    finally{
        fullBusy=false;
    }
}

async function safeVehicleSync(){
    if(vehBusy) return;
    vehBusy=true;

    try{
        await syncVehicles();
    }
    catch(e){
        console.error("[Worker] vehicleSync:",e.message);
    }
    finally{
        vehBusy=false;
    }
}

async function start(){

    const pg=await pgPool.connect();
    pg.release();

    console.log("✅ PostgreSQL connected");

    const mc=await mariaPool.getConnection();
    mc.release();

    console.log("MariaDB connected");

    await safeVehicleSync();

    console.log(
        `[Worker] Vehicle sync every ${VEHICLE_INTERVAL/1000}s`
    );

    setInterval(
        safeVehicleSync,
        VEHICLE_INTERVAL
    );

    console.log(
        `[Worker] Full sync every ${FULL_INTERVAL/1000}s`
    );

    await safeFullSync();

    setInterval(
        safeFullSync,
        FULL_INTERVAL
    );

    console.log(
        `[Worker] Quick sync every ${QUICK_INTERVAL/1000}s`
    );

    await safeQuickSync();

    setInterval(
        safeQuickSync,
        QUICK_INTERVAL
    );

    setInterval(()=>{
        console.log("🟢 Worker heartbeat OK");
    },10000);
}

start().catch((e)=>{
    console.error(
        "[Worker] Fatal:",
        e
    );
    process.exit(1);
});