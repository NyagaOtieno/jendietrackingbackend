// src/utils/safe-json.js

/**
 * Safely serializes objects containing BigInt
 * Used for Socket.IO, API responses, and queue payloads
 */
export function safeJSON(obj) {
  return JSON.parse(
    JSON.stringify(obj, (_, value) =>
      typeof value === "bigint" ? Number(value) : value
    )
  );
}