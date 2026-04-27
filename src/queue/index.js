import { connect } from './connection.js';
import { startConsumers } from './consumers.js';

export async function initQueue() {
  await connect();
  await startConsumers();
}