// ecosystem.config.cjs  ← rename to .cjs if your package is "type":"module"
// Usage:  pm2 start ecosystem.config.cjs
//         fly.io: set CMD in Dockerfile or Procfile to run both

module.exports = {
  apps: [
    // ── 1. Main HTTP + WebSocket API ──────────────────────────────────────
    {
      name:   "jendie-api",
      script: "./src/server.js",
      interpreter: "node",
      // ✅ NOTE: server.js now starts the sync loop itself,
      //    so running only this process is sufficient for single-dyno deploys.
      env: {
        NODE_ENV: "production",
      },
      // Auto-restart on crash
      restart_delay: 3000,
      max_restarts:  10,
    },

    // ── 2. Dedicated sync worker (optional — for multi-process setups) ────
    // Useful if you want to separate the HTTP server from the heavy sync.
    // On Fly.io free tier, run only jendie-api (it includes the sync loop).
    // Uncomment on Railway/EC2 where you can afford two processes.
    //
    // {
    //   name:   "jendie-sync-worker",
    //   script: "./src/workers/telemetryBufferWorker.js",
    //   interpreter: "node",
    //   env: {
    //     NODE_ENV: "production",
    //     SYNC_INTERVAL: "60000",
    //   },
    //   restart_delay: 5000,
    //   max_restarts:  10,
    // },
  ],
};
