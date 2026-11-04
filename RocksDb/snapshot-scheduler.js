import { debug, error, info } from '../FileSystem/logger.js';
import { writeSnapshots } from './RocksDbmanager.js';

export class SnapshotScheduler {
  constructor({ intervalMs = Number(process.env.SNAPSHOT_INTERVAL_MS ?? 1000), persist = writeSnapshots } = {}) {
    if (!Number.isInteger(intervalMs) || intervalMs < 1) throw new TypeError('intervalMs must be a positive integer.');
    this.intervalMs = intervalMs;
    this.persist = persist;
    this.pending = new Map();
    this.timer = undefined;
    this.flushing = false;
  }

  enqueue(events) {
    for (const event of events) if (event?.eventId) this.pending.set(event.eventId, event);
    debug('snapshot', 'events.queued', { pendingCount: this.pending.size });
  }

  async flush() {
    if (this.flushing || this.pending.size === 0) return 0;
    this.flushing = true;
    const batch = [...this.pending.values()];
    try {
      await this.persist(batch);
      for (const event of batch) this.pending.delete(event.eventId);
      info('snapshot', 'batch.persisted', { count: batch.length });
      return batch.length;
    } catch (cause) {
      error('snapshot', 'batch.persist.failed', { count: batch.length, cause });
      return 0;
    } finally {
      this.flushing = false;
    }
  }

  start() {
    if (this.timer) return this;
    this.timer = setInterval(() => void this.flush(), this.intervalMs);
    info('snapshot', 'scheduler.started', { intervalMs: this.intervalMs });
    return this;
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    await this.flush();
    info('snapshot', 'scheduler.stopped');
  }
}
