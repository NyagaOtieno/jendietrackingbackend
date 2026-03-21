import { pool } from "../config/db.js";
import { fetchMariaTrackingData } from "./mariaTracking.service.js";

export async function runMariaSync() {
  let client;

  try {
    client = await pool.connect();

    console.log("Starting Maria sync...");
    const remoteRows = await fetchMariaTrackingData(1);
    console.log("Fetched rows:", remoteRows.length);
    console.log("First row:", remoteRows[0]);

    return {
      success: true,
      fetched: remoteRows.length,
      message: "Fetch-only sync test passed",
      sample: remoteRows[0] || null,
    };
  } catch (error) {
    console.error("runMariaSync failed:", error);
    throw error;
  } finally {
    if (client) client.release();
  }
}