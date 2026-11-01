// Polls the HDFS command file, expands commands into events, then removes processed commands.
import { randomUUID } from 'node:crypto';
import { HdfsReaderWriter } from './hdfs-reader-writer.js';
import { debug, error, info, warn } from './logger.js';

const commandPath = '/system/v1/command';
export const eventsPath = '/system/v1/events';
const pollingInterval = 5000;
const hdfs = new HdfsReaderWriter();
let isPolling = false;

/** Convert a transfer command such as "A-$100-b" into debit and credit events. */
export function parseCommandEvents(command) {
  if (typeof command !== 'string' || command.trim().length === 0) {
    throw new TypeError('Command must be a non-empty string.');
  }

  const normalizedCommand = command.trim();
  const match = normalizedCommand.match(/^([A-Za-z][A-Za-z0-9_]*)-\$([1-9][0-9]*)-([A-Za-z][A-Za-z0-9_]*)$/);
  if (!match || match[1].toLowerCase() === match[3].toLowerCase()) {
    warn('event', 'command.invalid', { commandLength: normalizedCommand.length });
    throw new TypeError('Malformed transfer command. Expected A-$amount-B.');
  }

  const [, sender, amountText, receiver] = match;
  const amount = Number(amountText);
  const transferId = randomUUID();
  const occurredAt = new Date().toISOString();
  const parsedEvents = [
    {
      eventId: randomUUID(),
      transferId,
      entity: sender,
      operation: '-',
      amount,
      counterpart: receiver,
      occurredAt,
    },
    {
      eventId: randomUUID(),
      transferId,
      entity: receiver,
      operation: '+',
      amount,
      counterpart: sender,
      occurredAt,
    },
  ];

  for (const event of parsedEvents) {
    debug('event', 'event.created', {
      eventId: event.eventId,
      transferId,
      entity: event.entity,
      operation: event.operation,
      amount: event.amount,
    });
  }

  info('event', 'command.parsed', { sender, receiver, amount, eventCount: parsedEvents.length, transferId });
  return parsedEvents;
}


async function readCommand() {
  if (isPolling) return;
  isPolling = true;
  let commandFileRead = false;
  debug('event', 'poll.started', { commandPath });

  try {
    const commandFile = await hdfs.readFile(commandPath, { encoding: 'utf8' });
    commandFileRead = true;
    const commands = commandFile.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    info('event', 'commands.read', { path: commandPath, commandCount: commands.length, bytes: Buffer.byteLength(commandFile), command: commandFile });

    if (commands.length === 0) {
      debug('event', 'empty_command_file.deleted', { path: commandPath });
      return;
    }

    const events = commands.flatMap(parseCommandEvents);
    info('event', 'events.append.started', { path: eventsPath, commandCount: commands.length, eventCount: events.length });
    const jsonLines = `${events.map((event) => JSON.stringify(event)).join('\n')}\n`;
    await hdfs.appendFile(eventsPath, jsonLines);
    info('event', 'events.append.completed', { path: eventsPath, eventCount: events.length });

  } catch (cause) {
    // A missing command file is normal; a malformed command is acknowledged and deleted.
    if (/not found|does not exist|file not found/i.test(cause.message)) {
      debug('event', 'command_file.not_ready', { path: commandPath, message: cause.message });
    } else {
      error('event', 'poll.failed', { commandPath, cause });
    }
  } finally {
    if (commandFileRead) {
      try {
        await hdfs.deleteFile(commandPath);
        info('event', 'commands.deleted', { path: commandPath, reason: 'read_acknowledged' });
      } catch (cause) {
        error('event', 'commands.delete.failed', { path: commandPath, cause });
      }
    }
    isPolling = false;
  }
}

export function filePoller() {
  info('event', 'poller.started', { intervalMs: pollingInterval, commandPath, eventsPath });
  void readCommand();
  return setInterval(() => void readCommand(), pollingInterval);
}

filePoller();
