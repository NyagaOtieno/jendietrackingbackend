import { createClient } from "redis";

export const redis = createClient({
  url: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  socket: {
    reconnectStrategy: (retries) => {
      // exponential backoff (prevents CPU spike)
      return Math.min(retries * 50, 2000);
    },
  },
});

redis.on("error", (err) => {
  console.error("❌ Redis Error:", err.message);
});

redis.on("connect", () => {
  console.log("⚡ Redis connecting...");
});

redis.on("ready", () => {
  console.log("✅ Redis ready");
});

redis.on("end", () => {
  console.log("⚠️ Redis connection closed");
});

// ─────────────────────────────
// INIT SAFE CONNECTION
// ─────────────────────────────
export async function initRedis() {
  try {
    if (!redis.isOpen) {
      await redis.connect();
    }
  } catch (err) {
    console.error("❌ Redis init failed:", err.message);
    throw err;
  }
}

// ─────────────────────────────
// SAFE SHUTDOWN HOOK (IMPORTANT)
// ─────────────────────────────
export async function closeRedis() {
  try {
    if (redis.isOpen) {
      await redis.quit();
    }
  } catch (err) {
    console.error("❌ Redis close error:", err.message);
  }
}