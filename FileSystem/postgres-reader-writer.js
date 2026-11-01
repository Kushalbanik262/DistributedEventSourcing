import pg from 'pg';
import { debug, error, info, warn } from './logger.js';

const { Pool } = pg;

/** PostgreSQL access for user balances. */
export class PostgresReaderWriter {
  constructor(config = {}) {
    this.pool = new Pool({
      host: process.env.PGHOST ?? 'localhost',
      port: Number(process.env.PGPORT ?? 5432),
      database: process.env.PGDATABASE ?? 'event_sourcing',
      user: process.env.PGUSER ?? 'event_user',
      password: process.env.PGPASSWORD ?? 'event_password',
      ...config,
    });
    info('postgres', 'pool.created', {
      host: this.pool.options.host,
      port: this.pool.options.port,
      database: this.pool.options.database,
      user: this.pool.options.user,
    });
    this.pool.on('error', (cause) => error('postgres', 'pool.error', { cause }));
  }

  /** Create the balance table if it does not exist. */
  async initialize() {
    info('postgres', 'schema.initialize.started', { table: 'users' });
    try {
      await this.pool.query(`
        CREATE TABLE IF NOT EXISTS users (
          user_id TEXT PRIMARY KEY,
          amount BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0)
        )
      `);
      info('postgres', 'schema.users.ready', { columns: ['user_id', 'amount'] });

      const seeded = await this.pool.query(`
        INSERT INTO users (user_id, amount)
        VALUES
          ('A', floor(random() * 901 + 100)::BIGINT),
          ('B', floor(random() * 901 + 100)::BIGINT),
          ('C', floor(random() * 901 + 100)::BIGINT)
        ON CONFLICT (user_id) DO NOTHING
      `);
      info('postgres', 'users.seeded', { inserted: seeded.rowCount, userIds: ['A', 'B', 'C'] });
    } catch (cause) {
      error('postgres', 'schema.initialize.failed', { cause });
      throw cause;
    }
  }

  /** Apply one balance change atomically. */
  async applyEvent(event) {
    info('postgres', 'balance.apply.started', {
      eventId: event.eventId,
      userId: event.entity,
      operation: event.operation,
      amount: event.amount,
    });
    let client;
    try {
      client = await this.pool.connect();
      debug('postgres', 'connection.acquired', { eventId: event.eventId });
      await client.query('BEGIN');
      debug('postgres', 'transaction.started', { eventId: event.eventId });

      const createdUser = await client.query(
        'INSERT INTO users (user_id, amount) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING',
        [event.entity],
      );
      if (createdUser.rowCount > 0) {
        info('postgres', 'user.created', { userId: event.entity, initialAmount: 0 });
      }

      let balanceUpdate;
      if (event.operation === '+') {
        balanceUpdate = await client.query(
          'UPDATE users SET amount = amount + $1 WHERE user_id = $2 RETURNING amount',
          [event.amount, event.entity],
        );
      } else {
        balanceUpdate = await client.query(
          'UPDATE users SET amount = amount - $1 WHERE user_id = $2 AND amount >= $1 RETURNING amount',
          [event.amount, event.entity],
        );
        if (balanceUpdate.rowCount === 0) {
          warn('postgres', 'balance.deduction.rejected', {
            eventId: event.eventId,
            userId: event.entity,
            requestedAmount: event.amount,
            reason: 'insufficient_balance',
          });
          throw new Error(`Insufficient balance for user ${event.entity}`);
        }
      }

      await client.query('COMMIT');
      info('postgres', 'transaction.committed', {
        eventId: event.eventId,
        userId: event.entity,
        operation: event.operation,
        amount: event.amount,
        balance: balanceUpdate.rows[0].amount,
      });
      return { applied: true, balance: balanceUpdate.rows[0].amount };
    } catch (cause) {
      if (client) {
        try {
          await client.query('ROLLBACK');
          warn('postgres', 'transaction.rolled_back', { eventId: event.eventId, reason: cause.message });
        } catch (rollbackCause) {
          error('postgres', 'transaction.rollback_failed', {
            eventId: event.eventId,
            cause,
            rollbackCause,
          });
        }
      }
      error('postgres', 'balance.apply.failed', { eventId: event.eventId, userId: event.entity, cause });
      throw cause;
    } finally {
      if (client) {
        client.release();
        debug('postgres', 'connection.released', { eventId: event.eventId });
      }
    }
  }

  async close() {
    info('postgres', 'pool.close.started');
    await this.pool.end();
    info('postgres', 'pool.closed');
  }
}
