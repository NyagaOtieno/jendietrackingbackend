import { redis } from '../config/redis.js';

const TTL = 10; // seconds (real-time data)

export async function setLatestPosition(deviceId, data) {
  await redis.setex(`latest:${deviceId}`, TTL, JSON.stringify(data));
}

export async function getLatestPosition(deviceId) {
  const data = await redis.get(`latest:${deviceId}`);
  return data ? JSON.parse(data) : null;
}

export async function setFleetSnapshot(accountId, data) {
  await redis.setex(`fleet:${accountId}`, 15, JSON.stringify(data));
}

export async function getFleetSnapshot(accountId) {
  const data = await redis.get(`fleet:${accountId}`);
  return data ? JSON.parse(data) : null;
}