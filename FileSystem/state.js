// Applies the append-only HDFS event log to PostgreSQL user balances.
import { HdfsReaderWriter } from './hdfs-reader-writer.js';
import { PostgresReaderWriter } from './postgres-reader-writer.js';
import { debug, error, info, warn } from './logger.js';

const eventsPath = '/system/v1/events';
const checkpointPath = '/system/v1/state-offset';
const pollIntervalMs = Number(process.env.STATE_POLL_INTERVAL_MS ?? 5000);
const hdfs = new HdfsReaderWriter();
const postgres = new PostgresReaderWriter();
let isPolling = false;
let processedEventCount = 0;

export function validateEvent(event) {
  const valid = Boolean(
    event &&
    typeof event.eventId === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(event.eventId) &&
    typeof event.entity === 'string' &&
    /^[A-Za-z][A-Za-z0-9_]*$/.test(event.entity) &&
    (event.operation === '+' || event.operation === '-') &&
    Number.isSafeInteger(event.amount) &&
    event.amount > 0 &&
    typeof event.occurredAt === 'string' &&
    Number.isFinite(Date.parse(event.occurredAt))
  );
  if (!valid) {
    warn('state', 'event.validation.failed', {
      eventId: event?.eventId,
      entity: event?.entity,
      operation: event?.operation,
    });
  }
  return valid;
}

/** Read, validate, and apply every event currently in the HDFS log. */
export async function processEvents() {
  if (isPolling) return;
  isPolling = true;
  let eventLogRead = false;
  debug('state', 'poll.started', { eventsPath, processedEventCount });

  try {
    const contents = await hdfs.readFile(eventsPath, { encoding: 'utf8' });
    eventLogRead = true;
    const events = contents
      .split(/\r?\n/)
      .filter((line) => line.trim().length > 0)
      .map((line) => JSON.parse(line));
    info('state', 'events.read', {
      path: eventsPath,
      eventCount: events.length,
      bytes: Buffer.byteLength(contents),
      checkpoint: processedEventCount,
    });

    // Validate the full log, including entries already covered by the checkpoint.
    for (const event of events) {
      if (!validateEvent(event)) {
        throw new TypeError(`Invalid event in ${eventsPath}: ${JSON.stringify(event)}`);
      }
    }

    if (processedEventCount > events.length) {
      throw new Error('The event log is shorter than its saved HDFS checkpoint.');
    }

    for (let index = processedEventCount; index < events.length; index += 1) {
      info('state', 'event.apply.started', { index, eventId: events[index].eventId });
      await postgres.applyEvent(events[index]);
      processedEventCount = index + 1;
      info('state', 'checkpoint.write.started', { path: checkpointPath, processedEventCount });
      await hdfs.writeFile(checkpointPath, String(processedEventCount));
      info('state', 'event.apply.completed', {
        index,
        eventId: events[index].eventId,
        checkpoint: processedEventCount,
      });
    }
  } catch (cause) {
    // The log file is created by the event processor; it may not exist yet.
    if (/not found|does not exist|file not found/i.test(cause.message)) {
      debug('state', 'event_log.not_ready', { path: eventsPath, message: cause.message });
    } else {
      error('state', 'poll.failed', { eventsPath, checkpoint: processedEventCount, cause });
    }
  } finally {
    if (eventLogRead) {
      try {
        await hdfs.deleteFile(eventsPath);
        info('state', 'events.deleted', { path: eventsPath, reason: 'read_acknowledged' });
        processedEventCount = 0;
        await hdfs.writeFile(checkpointPath, '0');
        info('state', 'checkpoint.reset', { path: checkpointPath });
      } catch (cause) {
        error('state', 'events.delete.failed', { path: eventsPath, cause });
      }
    }
    isPolling = false;
  }
}

async function startStateProcessor() {
  info('state', 'service.starting', { eventsPath, checkpointPath, pollIntervalMs });
  await postgres.initialize();
  info('state', 'database.ready');

  try {
    const savedOffset = await hdfs.readFile(checkpointPath, { encoding: 'utf8' });
    if (!/^\d+$/.test(savedOffset.trim())) {
      throw new Error(`Invalid state checkpoint at ${checkpointPath}`);
    }
    processedEventCount = Number(savedOffset.trim());
    info('state', 'checkpoint.loaded', { path: checkpointPath, processedEventCount });
  } catch (cause) {
    if (/not found|does not exist|file not found/i.test(cause.message)) {
      processedEventCount = 0;
      info('state', 'checkpoint.initialized', { path: checkpointPath, processedEventCount });
    } else {
      error('state', 'checkpoint.load.failed', { path: checkpointPath, cause });
    }
  }

  await processEvents();
  const timer = setInterval(() => void processEvents(), pollIntervalMs);
  info('state', 'poller.started', { pollIntervalMs });

  const shutdown = async () => {
    info('state', 'service.shutdown.started');
    clearInterval(timer);
    await postgres.close();
    info('state', 'service.shutdown.completed');
    process.exit(0);
  };

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}

startStateProcessor().catch((cause) => {
  error('state', 'service.start.failed', { cause });
  process.exitCode = 1;
});
