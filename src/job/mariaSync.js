import cron from "node-cron";

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

    try {
      console.log("Maria sync job tick");
      // temporary placeholder until full sync service is wired
    } catch (error) {
      console.error("Maria sync failed:", error.message);
    } finally {
      isRunning = false;
    }
  });

  console.log(`Maria sync job scheduled: ${schedule}`);
}