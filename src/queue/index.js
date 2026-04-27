import { startConsumers } from "./consumers.js";

export async function initQueue() {
  await startConsumers();
}

export default { initQueue };