import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { Pool } from 'pg';
import { TaskRepository } from '../db/task-repository';
import { TaskReconciler, ReconcilerQueue } from '../db/task-reconciler';
import { runMigrations } from '../db/migrations';

// DELIVERED outbox events are pruned after the retention period; nothing
// else in the outbox is ever touched. Real PostgreSQL.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const silent: any = { info() {}, warn() {}, error() {}, debug() {}, child() { return silent; } };
const noJobs: ReconcilerQueue = { getJob: async () => null };
const DAY = 86_400_000;

let pool: Pool;
let repo: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  repo = new TaskRepository(pool);
});
afterAll(async () => { await pool.end(); });

/** A task whose single outbox event is in `status`, processed `ageDays` ago. */
async function event(status: 'PENDING' | 'DELIVERED' | 'FAILED', ageDays: number): Promise<string> {
  const { task, outboxEvent } = await repo.createTaskWithOutbox({ name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 0 });
  await pool.query(`UPDATE tasks SET status = 'COMPLETED' WHERE id = $1`, [task.id]); // not a reconcile candidate
  await pool.query(
    `UPDATE outbox_events SET status = $2, processed_at = NOW() - $3 * INTERVAL '1 day' WHERE id = $1`,
    [outboxEvent.id, status, ageDays],
  );
  return outboxEvent.id;
}

async function exists(ids: string[]): Promise<boolean[]> {
  const r = await pool.query('SELECT id FROM outbox_events WHERE id = ANY($1)', [ids]);
  const found = new Set(r.rows.map((x) => x.id));
  return ids.map((id) => found.has(id));
}

function reconciler(opts: object) {
  return new TaskReconciler(pool, noJobs, { queuedGraceMs: 365 * DAY, logger: silent, ...opts });
}

describe('pruning delivered outbox events (requires PostgreSQL)', () => {
  it('deletes only DELIVERED events older than the retention', async () => {
    const oldDelivered = await event('DELIVERED', 8);
    const newDelivered = await event('DELIVERED', 1);
    const oldFailed = await event('FAILED', 30);
    const pending = await event('PENDING', 30);

    const result = await reconciler({ deliveredOutboxRetentionMs: 7 * DAY }).reconcile();

    expect(result.pruned).toBeGreaterThanOrEqual(1);
    expect(await exists([oldDelivered, newDelivered, oldFailed, pending])).toEqual([false, true, true, true]);
  });

  it('keeps everything when retention is 0 (disabled)', async () => {
    const old = await event('DELIVERED', 400);
    const result = await reconciler({ deliveredOutboxRetentionMs: 0 }).reconcile();
    expect(result.pruned).toBe(0);
    expect(await exists([old])).toEqual([true]);
  });

  it('works through a backlog in bounded batches within one cycle', async () => {
    const ids = [];
    for (let i = 0; i < 5; i++) ids.push(await event('DELIVERED', 10));
    await reconciler({ deliveredOutboxRetentionMs: 7 * DAY, pruneBatchSize: 2 }).reconcile();
    expect(await exists(ids)).toEqual([false, false, false, false, false]);
  });
});
