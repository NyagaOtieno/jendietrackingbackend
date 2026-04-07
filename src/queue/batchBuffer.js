// src/queue/batchBuffer.js
import { BATCH_SIZE, BATCH_FLUSH_INTERVAL_MS } from './config.js';

/**
 * Accumulates messages and flushes them in a single bulk DB insert.
 * Dramatically reduces round trips at high vehicle throughput.
 *
 * @param {(rows: object[]) => Promise<void>} flushFn
 * @param {{ batchSize?: number, flushInterval?: number }} options
 */
export class BatchBuffer {
  constructor(flushFn, options = {}) {
    this.flushFn = flushFn;
    this.batchSize = options.batchSize || BATCH_SIZE;
    this.flushInterval = options.flushInterval || BATCH_FLUSH_INTERVAL_MS;
    this.buffer = [];
    this.pendingAcks = [];
    this.timer = setInterval(() => {
      if (this.buffer.length > 0) this._flush();
    }, this.flushInterval);
  }

  add(row, ackFn) {
    this.buffer.push(row);
    this.pendingAcks.push(ackFn);
    if (this.buffer.length >= this.batchSize) this._flush();
  }

  async _flush() {
    if (this.buffer.length === 0) return;
    const rows = this.buffer.splice(0);
    const acks = this.pendingAcks.splice(0);
    try {
      await this.flushFn(rows);
      acks.forEach(fn => fn(true));
    } catch (err) {
      console.error('[BatchBuffer] Flush failed:', err.message);
      acks.forEach(fn => fn(false));
    }
  }

  destroy() {
    clearInterval(this.timer);
  }
}
