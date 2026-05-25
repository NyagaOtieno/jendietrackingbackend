import { createClient } from "redis";

export const redis = createClient({
  url: process.env.REDIS_URL || "redis://127.0.0.1:6379",

  socket: {
    reconnectStrategy(retries) {
      return Math.min(retries * 100, 3000);
    }
  }
});

export const redisPub = redis.duplicate();
export const redisSub = redis.duplicate();

function register(client, name) {
  client.on("connect", () =>
    console.log(`⚡ Redis ${name} connecting`)
  );

  client.on("ready", () =>
    console.log(`✅ Redis ${name} ready`)
  );

  client.on("error", err =>
    console.error(`❌ Redis ${name}:`, err.message)
  );
}

register(redis, "main");
register(redisPub, "pub");
register(redisSub, "sub");

export async function initRedis() {
  const clients = [redis, redisPub, redisSub];

  for (const client of clients) {
    if (!client.isOpen) {
      await client.connect();
    }
  }
}

export async function closeRedis() {
  await Promise.all(
    [redis, redisPub, redisSub]
      .filter(c => c.isOpen)
      .map(c => c.quit())
  );
}