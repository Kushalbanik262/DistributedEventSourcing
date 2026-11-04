import express from 'express';
import { randomUUID } from 'node:crypto';
import { HdfsReaderWriter } from './hdfs-reader-writer.js';
import { error as logError, info, warn } from './logger.js';
import { readSnapshotsForUser } from '../RocksDb/RocksDbmanager.js';

const app = express();
const port = Number(process.env.PORT ?? 3000);

const service = "COMMAND";

export const commandPath = '/system/v1/command';

app.use(express.json());

function parseSnapshotTime(value, name) {
  if (value === undefined) return undefined;
  if (Array.isArray(value) || String(value).trim() === '') throw new TypeError(`${name} must be an epoch millisecond or ISO timestamp.`);
  const text = String(value).trim();
  const timestamp = /^\d+$/.test(text) ? Number(text) : Date.parse(text);
  if (!Number.isSafeInteger(timestamp) || timestamp < 0) throw new TypeError(`${name} must be an epoch millisecond or ISO timestamp.`);
  return timestamp;
}

function parseSnapshotLimit(value) {
  if (value === undefined) return 100;
  if (Array.isArray(value) || !/^\d+$/.test(String(value))) throw new TypeError('limit must be a positive integer.');
  const limit = Number(value);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new TypeError('limit must be between 1 and 1000.');
  return limit;
}

app.get('/snapshots/:userId', async (request, response) => {
  const requestId = randomUUID();
  const { userId } = request.params;
  try {
    const from = parseSnapshotTime(request.query.from, 'from');
    const to = parseSnapshotTime(request.query.to, 'to');
    const limit = parseSnapshotLimit(request.query.limit);
    if (from !== undefined && to !== undefined && from > to) {
      throw new TypeError('from must be earlier than or equal to to.');
    }
    const snapshots = await readSnapshotsForUser(userId, { from, to, limit });
    info('command', 'snapshots.read.completed', { requestId, userId, count: snapshots.length });
    return response.json({ userId, count: snapshots.length, snapshots });
  } catch (cause) {
    const isBadRequest = cause instanceof TypeError || /invalid argument|invalid key/i.test(cause.message ?? '');
    logError('command', 'snapshots.read.failed', { requestId, userId, cause });
    return response.status(isBadRequest ? 400 : 503).json({
      error: isBadRequest ? cause.message : 'Could not read snapshots from RocksDB.',
    });
  }
});

app.post('/command', async (request, response) => {
  const requestId = randomUUID();
  const startedAt = Date.now();
  info('command', 'request.received', {
    requestId,
    method: request.method,
    path: request.path,
    remoteAddress: request.socket.remoteAddress,
  });
  response.on('finish', () => info('command', 'request.completed', {
    requestId,
    statusCode: response.statusCode,
    durationMs: Date.now() - startedAt,
  }));

  const { command } = request.body ?? {};

  if (typeof command !== 'string' || command.trim().length === 0) {
    warn('command', 'request.rejected', { requestId, reason: 'empty_or_non_string_command' });
    return response.status(400).json({ error: 'A non-empty command string is required.' });
  }

  try{
    info('command', 'store.started', { requestId, commandLength: command.trim().length, destination: commandPath });
    const commandId = randomUUID();
    await writeCommandToFile(command.trim(), commandId);
    info('command', 'store.completed', { requestId, destination: commandPath });
    return response.status(201).json({
      message: 'Command stored.',
      commandId,
      command: command.trim(),
    });
  }
  catch(error){
    logError('command', 'store.failed', { requestId, cause: error });
    return response.status(502).json({
      error: 'Could not store command in HDFS.',
    });
  }
});

app.use((request, response) => {
  warn('command', 'route.not_found', { method: request.method, path: request.path });
  return response.status(404).json({ error: 'Not found.' });
});

app.use((cause, request, response, next) => {
  if (response.headersSent) return next(cause);
  const statusCode = cause.status === 400 ? 400 : 500;
  logError('command', 'request.middleware.failed', {
    method: request.method,
    path: request.path,
    statusCode,
    cause,
  });
  return response.status(statusCode).json({ error: statusCode === 400 ? 'Invalid JSON request body.' : 'Internal server error.' });
});

const server = app.listen(port, () => {
  info('command', 'server.started', { host: '0.0.0.0', port });
});
server.on('error', (cause) => logError('command', 'server.failed', { port, cause }));


// Store each submitted command as a line for the event processor to consume.
async function writeCommandToFile(command, commandId){
  const writer = new HdfsReaderWriter();
  info(service,"WRITE",`Writing to Command File:${command}`);
  await writer.appendFile(commandPath, `${JSON.stringify({ commandId, command })}\n`);
}

