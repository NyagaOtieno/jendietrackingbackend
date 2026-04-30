const { createClient } = require("redis");

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

const redis = createClient({
  url: REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("❌ Redis max retries reached");
        return new Error("Retry limit reached");
      }
      return Math.min(retries * 200, 2000);
    },
  },
});

redis.on("connect", () => console.log("🔌 Redis connecting..."));
redis.on("ready", () => console.log("✅ Redis ready"));
redis.on("error", (err) => console.error("❌ Redis error:", err.message));
redis.on("end", () => console.warn("⚠️ Redis disconnected"));

async function initRedis() {
  if (!redis.isOpen) await redis.connect();
}

module.exports = {
  redis,
  initRedis,
};