import { Pool } from 'pg';
import { log } from '../utils';

const MIGRATIONS = [
  {
    version: 1,
    name: 'create_tasks_table',
    sql: `
      CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING',
        priority TEXT NOT NULL DEFAULT 'NORMAL',
        payload JSONB NOT NULL DEFAULT '{}',
        result JSONB,
        error TEXT,
        retries INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        CONSTRAINT valid_status CHECK (status IN ('QUEUED','PROCESSING','COMPLETED','FAILED','CANCELLED')),
        CONSTRAINT valid_priority CHECK (priority IN ('LOW','NORMAL','HIGH','CRITICAL'))
      );

      CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
      CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
    `,
  },
  {
    version: 2,
    name: 'create_outbox_events_table',
    sql: `
      CREATE TABLE IF NOT EXISTS outbox_events (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL REFERENCES tasks(id),
        event_type TEXT NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'PENDING',
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ,
        claimed_by TEXT,
        claimed_at TIMESTAMPTZ,
        CONSTRAINT valid_outbox_status CHECK (status IN ('PENDING','DELIVERED','FAILED'))
      );

      CREATE INDEX IF NOT EXISTS idx_outbox_events_status ON outbox_events(status);
      CREATE INDEX IF NOT EXISTS idx_outbox_events_created_at ON outbox_events(created_at);
    `,
  },
  {
    version: 3,
    name: 'create_schema_migrations_table',
    sql: `
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `,
  },
  {
    version: 4,
    name: 'remove_pending_status',
    sql: `
      ALTER TABLE tasks DROP CONSTRAINT IF EXISTS valid_status;
      ALTER TABLE tasks ADD CONSTRAINT valid_status CHECK (status IN ('QUEUED','PROCESSING','COMPLETED','FAILED','CANCELLED'));
    `,
  },
];

export async function runMigrations(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock(42)');

    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
      `);

      const { rows } = await client.query('SELECT version FROM schema_migrations ORDER BY version');
      const applied = new Set(rows.map((r: { version: number }) => r.version));

      for (const migration of MIGRATIONS) {
        if (migration.name === 'create_schema_migrations_table') continue;
        if (applied.has(migration.version)) continue;

        log('INFO', `Running migration ${migration.version}: ${migration.name}`);
        await client.query('BEGIN');
        try {
          await client.query(migration.sql);
          await client.query(
            'INSERT INTO schema_migrations (version, name) VALUES ($1, $2)',
            [migration.version, migration.name],
          );
          await client.query('COMMIT');
          log('INFO', `Migration ${migration.version} applied successfully`);
        } catch (error) {
          await client.query('ROLLBACK');
          throw error;
        }
      }
    } finally {
      await client.query('SELECT pg_advisory_unlock(42)');
    }
  } finally {
    client.release();
  }
}
