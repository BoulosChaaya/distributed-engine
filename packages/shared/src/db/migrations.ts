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
        status TEXT NOT NULL DEFAULT 'QUEUED',
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
  {
    version: 5,
    name: 'add_task_claimed_by',
    sql: `
      ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_by TEXT;
    `,
  },
  {
    version: 6,
    name: 'add_task_claim_token',
    sql: `
      ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claim_token TEXT;
      ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;
    `,
  },
  {
    version: 7,
    name: 'add_outbox_trace_context',
    sql: `
      ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS trace_context JSONB;
    `,
  },
  {
    version: 8,
    name: 'add_scheduled_status_and_task_scheduling_columns',
    sql: `
      ALTER TABLE tasks DROP CONSTRAINT IF EXISTS valid_status;
      ALTER TABLE tasks ADD CONSTRAINT valid_status CHECK (status IN ('SCHEDULED','QUEUED','PROCESSING','COMPLETED','FAILED','CANCELLED'));
      ALTER TABLE tasks ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ;
      ALTER TABLE tasks ADD COLUMN IF NOT EXISTS schedule_id TEXT;
      CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_for ON tasks(scheduled_for) WHERE status = 'SCHEDULED';
    `,
  },
  {
    version: 9,
    name: 'create_recurring_schedules_table',
    sql: `
      CREATE TABLE IF NOT EXISTS recurring_schedules (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        task_name TEXT NOT NULL,
        task_priority TEXT NOT NULL DEFAULT 'NORMAL',
        task_payload JSONB NOT NULL DEFAULT '{}',
        task_max_retries INTEGER NOT NULL DEFAULT 3,
        cron_expression TEXT NOT NULL,
        timezone TEXT NOT NULL DEFAULT 'UTC',
        next_run_at TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL DEFAULT 'ACTIVE',
        misfire_policy TEXT NOT NULL DEFAULT 'SKIP_MISSED',
        overlap_policy TEXT NOT NULL DEFAULT 'ALLOW_OVERLAP',
        execution_lease_token TEXT,
        execution_lease_expires_at TIMESTAMPTZ,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT valid_schedule_status CHECK (status IN ('ACTIVE','PAUSED','DISABLED')),
        CONSTRAINT valid_misfire_policy CHECK (misfire_policy IN ('CATCH_UP_ALL','SKIP_MISSED','RUN_ONCE')),
        CONSTRAINT valid_overlap_policy CHECK (overlap_policy IN ('ALLOW_OVERLAP','FORBID_OVERLAP')),
        CONSTRAINT valid_task_priority_sched CHECK (task_priority IN ('LOW','NORMAL','HIGH','CRITICAL'))
      );

      CREATE INDEX IF NOT EXISTS idx_recurring_schedules_status_next_run
        ON recurring_schedules(next_run_at) WHERE status = 'ACTIVE';

      ALTER TABLE tasks ADD CONSTRAINT fk_tasks_schedule_id
        FOREIGN KEY (schedule_id) REFERENCES recurring_schedules(id);
    `,
  },
  {
    version: 10,
    name: 'add_occurrence_uniqueness_constraint',
    sql: `
      CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_occurrence
        ON tasks(schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL;
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
