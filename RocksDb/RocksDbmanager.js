import { RocksDatabase } from '@harperfast/rocksdb-js';
import { join } from 'node:path';
import { error, info } from '../FileSystem/logger.js';

const service = 'SNAPSHOT';
const databasePath = process.env.ROCKSDB_PATH ?? join(process.cwd(), 'rocks_data');

function ensureUserId(userId) {
  if (typeof userId !== 'string' || !/^[A-Za-z][A-Za-z0-9_]*$/.test(userId)) {
    throw new TypeError('userId must start with a letter and contain only letters, numbers, or underscores.');
  }
}

function timestampOf(event) {
  const timestamp = Date.parse(event.occurredAt);
  if (!Number.isFinite(timestamp)) throw new TypeError(`Invalid event timestamp: ${event.occurredAt}`);
  return timestamp;
}

function keyFor(event, timestamp) {
  return `${event.entity}|${event.eventId}`;
}

/** Repository for immutable, event-derived snapshots. */
export class RocksDbManager {
  constructor({ path = databasePath, readOnly = false } = {}) {
    this.path = path;
    this.readOnly = readOnly;
    this.database = undefined;
    this.opening = undefined;
  }

  async initialize() {
    if (this.database) return this;
    if (this.opening) return this.opening;
    this.opening = Promise.resolve().then(() => {
      this.database = RocksDatabase.open(this.path, {
        compression: { algorithm: 'zstd', level: 3 },
        readOnly: this.readOnly,
      });
      info(service, 'database.initialized', { path: this.path });
      return this;
    }).catch((cause) => {
      error(service, 'database.initialization.failed', { path: this.path, cause });
      throw cause;
    });
    return this.opening;
  }

  async putSnapshot(event) {
    await this.initialize();
    if (!event || typeof event !== 'object') throw new TypeError('Snapshot event must be an object.');
    ensureUserId(event.entity);
    if (typeof event.eventId !== 'string' || event.eventId.length === 0) throw new TypeError('Snapshot event must contain eventId.');
    const timestamp = timestampOf(event);
    const value = { userId: event.entity, snapshotAt: new Date(timestamp).toISOString(), storedAt: new Date().toISOString(), event };
    const key = keyFor(event, timestamp);
    await this.database.put(key, value);
    return { key, value };
  }

  async putSnapshots(events) {
    await this.initialize();
    if (!Array.isArray(events)) throw new TypeError('Snapshots must be an array.');
    if (events.length === 0) return 0;
    await this.database.transaction(async (transaction) => {
      for (const event of events) {
        if (!event || typeof event !== 'object') throw new TypeError('Snapshot event must be an object.');
        ensureUserId(event.entity);
        if (typeof event.eventId !== 'string' || event.eventId.length === 0) throw new TypeError('Snapshot event must contain eventId.');
        const timestamp = timestampOf(event);
        await transaction.put(keyFor(event, timestamp), {
          userId: event.entity,
          snapshotAt: new Date(timestamp).toISOString(),
          storedAt: new Date().toISOString(),
          event,
        });
      }
    });
    info(service, 'snapshots.persisted', { count: events.length });
    return events.length;
  }

  async getSnapshot(key) {
    await this.initialize();
    return this.database.get(key);
  }

  async getSnapshots(userId, { from, to, limit = 100 } = {}) {
    await this.initialize();
    ensureUserId(userId);
    if (!Number.isInteger(limit) || limit < 1) throw new TypeError('limit must be a positive integer.');
    const results = [];
    for await (const { key, value } of this.database.getRange({ start: `${userId}|`, end: `${userId}|\uffff` })) {
      const timestamp = Date.parse(value.snapshotAt);
      if (from !== undefined && timestamp < from) continue;
      if (to !== undefined && timestamp > to) continue;
      results.push({ key, ...value });
    }
    results.sort((left, right) => Date.parse(left.snapshotAt) - Date.parse(right.snapshotAt));
    return results.slice(0, limit);
  }

  async getLatestSnapshot(userId) {
    const snapshots = await this.getSnapshots(userId, { limit: Number.MAX_SAFE_INTEGER });
    return snapshots.at(-1);
  }

  close() {
    if (!this.database) return;
    this.database.close();
    this.database = undefined;
    this.opening = undefined;
    info(service, 'database.closed', { path: this.path });
  }
}

export const rocksDbManager = new RocksDbManager();
export const writeToDb = (event) => rocksDbManager.putSnapshot(event);
export const writeSnapshots = (events) => rocksDbManager.putSnapshots(events);

async function withFreshReadOnlyManager(operation) {
  const manager = new RocksDbManager({ readOnly: true });
  try {
    return await operation(manager);
  } finally {
    manager.close();
  }
}

export const readFromDb = (key) => withFreshReadOnlyManager((manager) => manager.getSnapshot(key));
export const readSnapshotsForUser = (userId, options) => withFreshReadOnlyManager((manager) => manager.getSnapshots(userId, options));
export const readLatestSnapshot = (userId) => withFreshReadOnlyManager((manager) => manager.getLatestSnapshot(userId));

process.once('exit', () => rocksDbManager.close());
