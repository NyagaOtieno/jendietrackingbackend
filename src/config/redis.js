// config/redisClient.js
import { createClient } from "redis";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

export const redis = createClient({
  url: REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 10) {
        console.error("❌ Redis max retries reached");
        return new Error("Retry limit reached");
      }
      return Math.min(retries * 200, 2000); // exponential backoff
    }
  }
});

// Events
redis.on("connect", () => {
  console.log("🔌 Redis connecting...");
});

redis.on("ready", () => {
  console.log("✅ Redis ready");
});

redis.on("error", (err) => {
  console.error("❌ Redis error:", err.message);
});

redis.on("end", () => {
  console.warn("⚠️ Redis connection closed");
});

// Safe connect (no top-level await issues in some setups)
export async function initRedis() {
  if (!redis.isOpen) {
    await redis.connect();
  }
}