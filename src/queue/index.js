// src/queue/index.js
import { connect } from './connection.js';
import { startAllConsumers } from './consumers.js';

export async function initQueue() {
  await connect();
  await startAllConsumers();
}
