# Distributed Event Sourcing

A small Node.js event-sourcing example that uses HDFS as the file-backed command/event transport and PostgreSQL as the materialized balance store.

## Flow

```text
HTTP client
    | POST /command { "command": "A-$100-B" }
    v
Command service (port 3000)
    | appends one command per line
    v
HDFS: /system/v1/command
    | event service polls every 5 seconds
    | converts one transfer into debit + credit events
    | appends JSON Lines, then deletes the command file
    v
HDFS: /system/v1/events
    | state service polls every 5 seconds
    | validates and applies new events in order
    v
PostgreSQL: users(user_id, amount)
    | writes the processed-event offset to HDFS
    v
HDFS: /system/v1/state-offset

Event service snapshot scheduler
    | flushes event-derived snapshots every SNAPSHOT_INTERVAL_MS
    v
RocksDB: rocks_data (userId|timestamp|eventId -> snapshot)
```

The services are independent processes:

- `FileSystem/command.js` accepts commands and appends them to HDFS.
- `FileSystem/event.js` reads pending commands and expands each transfer into events.
- `FileSystem/state.js` replays events into PostgreSQL and maintains the current balances.
- `RocksDb/RocksDbmanager.js` stores immutable event-derived snapshots and supports per-user range queries.
- `RocksDb/snapshot-scheduler.js` batches snapshots and persists them on a configurable interval.

## Transfer format

Commands must use this format:

```text
<sender>-$<positive integer amount>-<receiver>
```

For example, `A-$100-B` creates two events sharing a `transferId`:

```json
{"entity":"A","operation":"-","amount":100,"counterpart":"B"}
{"entity":"B","operation":"+","amount":100,"counterpart":"A"}
```

Each event also contains a unique `eventId` and an ISO timestamp. Sender and receiver must be different; user names may contain letters, numbers, and underscores, but must start with a letter.

## Running locally

### 1. Start HDFS and PostgreSQL

Docker Compose starts a NameNode, DataNode, and PostgreSQL instance:

```bash
docker compose up -d
```

The default local connections are:

| Dependency | Address |
|---|---|
| HDFS WebHDFS | `http://localhost:9870/webhdfs/v1` |
| PostgreSQL | `localhost:5432` |

The default PostgreSQL database is `event_sourcing`, with user `event_user` and password `event_password`.

### 2. Start the three application services

Run each command in a separate terminal:

```bash
npm start
npm run start:events
npm run start:state
```

The command service listens on port `3000`. The event and state services are pollers and do not expose HTTP endpoints.

### 3. Submit a command

```bash
curl -X POST http://localhost:3000/command \
  -H "Content-Type: application/json" \
  -d '{"command":"A-$100-B"}'
```

The command service returns `201` after the command has been appended to HDFS. Within the next polling cycle, the event service creates the debit and credit events, and the state service applies them to PostgreSQL.

## State processing details

- PostgreSQL creates the `users` table on startup and seeds `A`, `B`, and `C` with random starting balances when they do not already exist.
- A debit is applied only when the user has sufficient balance. The database transaction is rolled back otherwise.
- Events are processed in file order. The state service records the number of applied events in `/system/v1/state-offset` so a restart can continue from the saved position.
- After the current event file has been read, it is deleted and the checkpoint is reset to `0`; the next event batch starts a fresh file.
- Missing command/event files are normal while the system is idle.

## RocksDB snapshots

The event service queues each event after its HDFS append succeeds. The snapshot scheduler flushes
the queue periodically to RocksDB in one transaction. The default interval is one second and can be
changed with `SNAPSHOT_INTERVAL_MS`.

Snapshots use this composite key:

```text
<userId>|<eventId>
```

This keeps snapshots immutable, permits multiple snapshots for one user, and makes retries
idempotent because the event ID is part of the key. The database path defaults to `./rocks_data`
and can be changed with `ROCKSDB_PATH`.

Commands are assigned a stable `commandId` before being written to HDFS. If the event service
retries a command after an append/delete failure, it regenerates the same event IDs instead of
creating another transfer. Duplicate-looking records already present from earlier retries are
historical data and are not automatically deleted.

The manager exports `writeToDb` for one event, `writeSnapshots` for a batch, plus `readFromDb`,
`readSnapshotsForUser`, and `readLatestSnapshot` for application code.

Read snapshots over HTTP with:

```bash
curl "http://localhost:3000/snapshots/A?limit=100&from=2026-01-01T00:00:00Z"
```

The endpoint is `GET /snapshots/:userId`. Optional `from` and `to` values accept ISO timestamps
or epoch milliseconds. `limit` defaults to `100` and is capped at `1000`.

## Configuration

The services use these environment variables when provided:

| Variable | Default | Used by |
|---|---|---|
| `PORT` | `3000` | command service |
| `HDFS_HOST` | `localhost` | all HDFS access |
| `HDFS_PORT` | `9870` | all HDFS access |
| `HDFS_USER` | `root` | all HDFS access |
| `PGHOST` | `localhost` | state service |
| `PGPORT` | `5432` | state service |
| `PGDATABASE` | `event_sourcing` | state service |
| `PGUSER` | `event_user` | state service |
| `PGPASSWORD` | `event_password` | state service |
| `STATE_POLL_INTERVAL_MS` | `5000` | state service |
| `SNAPSHOT_INTERVAL_MS` | `1000` | event service snapshot scheduler |
| `ROCKSDB_PATH` | `./rocks_data` | event service snapshot store |
| `LOG_LEVEL` | `INFO` | all services |

Set `LOG_LEVEL=DEBUG` to see polling, HDFS, checkpoint, and database transaction details.

## Project layout

```text
RocksDb/
├── RocksDbmanager.js         # RocksDB snapshot repository
└── snapshot-scheduler.js     # periodic snapshot batcher

FileSystem/
├── command.js                # HTTP command intake
├── event.js                  # command-to-event processor
├── state.js                  # event replay and checkpointing
├── hdfs-reader-writer.js     # WebHDFS read/write/append/delete helper
├── postgres-reader-writer.js # PostgreSQL schema and balance updates
└── logger.js                 # structured console logging
compose.yaml                  # HDFS and PostgreSQL dependencies
```

To stop the dependencies:

```bash
docker compose down
```

Add `-v` only when you intentionally want to remove the persisted HDFS and PostgreSQL volumes.
