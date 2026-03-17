import cron from "node-cron";
import { runMariaSync } from "../services/mariaSync.service.js";

let isRunning = false;

export function startMariaSyncJob() {
  if (process.env.SYNC_ENABLED !== "true") {
    console.log("Maria sync job disabled");
    return;
  }

  const schedule = process.env.SYNC_CRON || "*/2 * * * *";

  cron.schedule(schedule, async () => {
    if (isRunning) {
      console.log("Maria sync skipped: previous run still in progress");
      return;
    }

    isRunning = true;
    const startedAt = Date.now();

    try {
      console.log("Maria sync job tick");

      const result = await runMariaSync();

      const durationMs = Date.now() - startedAt;

      console.log(
        `Maria sync completed in ${durationMs}ms`,
        result || {}
      );
    } catch (error) {
      console.error("Maria sync failed:", error?.message || error);
    } finally {
      isRunning = false;
    }
  });

  console.log(`Maria sync job scheduled: ${schedule}`);
}