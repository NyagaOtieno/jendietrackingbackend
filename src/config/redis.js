import Redis from "ioredis";

export const redis = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),

  retryStrategy: (times) => Math.min(times * 200, 5000),

  reconnectOnError: () => true,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
});

redis.on("connect", () => console.log("⚡ Redis connected"));
redis.on("ready", () => console.log("🚀 Redis ready"));
redis.on("error", (e) => console.error("❌ Redis error:", e.message));
redis.on("reconnecting", () => console.warn("🔄 Redis reconnecting"));