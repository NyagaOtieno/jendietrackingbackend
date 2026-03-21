import { fetchMariaTrackingData } from "./mariaTracking.service.js";

export async function runMariaSync() {
  console.log("runMariaSync called");

  const remoteRows = await fetchMariaTrackingData(1);

  console.log("Fetched rows:", remoteRows.length);
  console.log("First row:", remoteRows[0]);

  return {
    success: true,
    fetched: remoteRows.length,
    message: "Fetch-only sync test passed",
    sample: remoteRows[0] || null,
  };
}