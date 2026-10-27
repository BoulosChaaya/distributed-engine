import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue, Worker, Job } from 'bullmq';
import IORedis from 'ioredis';
import { TaskRepository } from '../db/task-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { TaskReconciler, ReconcilerQueue } from '../db/task-reconciler';
import { runMigrations } from '../db/migrations';
import { submitTask } from './helpers/submit-task';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const TEST_REDIS_HOST = process.env.TEST_REDIS_HOST || 'localhost';
const TEST_REDIS_PORT = parseInt(process.env.TEST_REDIS_PORT || '6379');
const TEST_REDIS_PASSWORD = process.env.REDIS_PASSWORD;

const QUEUE_NAME = 'tasks-test-reconciler';
const silentLogger: any = {
  info: () => {}, warn: () => {}, error: () => {}, debug: () => {},
  child() { return silentLogger; },
};

let pool: Pool;
let repo: TaskRepository;
let redis: IORedis;
let queue: Queue;

const connections: IORedis[] = [];

// BullMQ does not close connections it was handed, so track and close them
// ourselves or Jest never exits.
function createRedisConn(): IORedis {
  const conn = new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, password: TEST_REDIS_PASSWORD, maxRetriesPerRequest: null });
  connections.push(conn);
  return conn;
}

/** Simulates Redis losing this queue's data (restart without persistence, FLUSHALL, failover). */
async function wipeQueueFromRedis(): Promise<number> {
  let cursor = '0';
  let deleted = 0;
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `bull:${QUEUE_NAME}:*`, 'COUNT', 500);
    cursor = next;
    if (keys.length > 0) deleted += await redis.del(...keys);
  } while (cursor !== '0');
  return deleted;
}

async function createTask(name = 'job', maxRetries = 3): Promise<string> {
  const { task } = await submitTask(repo, { name, priority: 'NORMAL', payload: { n: name }, maxRetries } as any);
  return task.id;
}

async function backdate(taskId: string, ms: number): Promise<void> {
  await pool.query(`UPDATE tasks SET updated_at = NOW() - $2 * INTERVAL '1 millisecond' WHERE id = $1`, [taskId, ms]);
}

async function markOutboxDelivered(): Promise<void> {
  await pool.query(`UPDATE outbox_events SET status = 'DELIVERED', processed_at = NOW() WHERE status = 'PENDING'`);
}

async function pendingRequeues(taskId: string): Promise<number> {
  const r = await pool.query(
    `SELECT COUNT(*)::int AS c FROM outbox_events WHERE task_id = $1 AND event_type = 'TASK_REQUEUED' AND status = 'PENDING'`,
    [taskId],
  );
  return r.rows[0].c;
}

function reconciler(q: ReconcilerQueue = queue as unknown as ReconcilerQueue, queuedGraceMs = 1000): TaskReconciler {
  return new TaskReconciler(pool, q, { intervalMs: 200, queuedGraceMs, batchSize: 100, logger: silentLogger });
}

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await pool.query('SELECT 1');
  redis = createRedisConn();
  await redis.ping();
  await runMigrations(pool);
  repo = new TaskRepository(pool, 2000);
  queue = new Queue(QUEUE_NAME, { connection: createRedisConn() });
});

afterAll(async () => {
  if (queue) {
    try { await queue.obliterate({ force: true }); } catch {}
    await queue.close();
  }
  for (const conn of connections) conn.disconnect();
  if (pool) {
    await pool.query('TRUNCATE outbox_events, tasks CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  await wipeQueueFromRedis();
});

describe('TaskReconciler: orphan detection (requires PostgreSQL + Redis)', () => {
  it('requeues a QUEUED task whose job is missing from Redis, exactly once', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    await backdate(id, 5000);
    const before = await repo.getTask(id);

    const r = reconciler();
    const first = await r.reconcile();
    expect(first.requeued).toBe(1);
    expect(await pendingRequeues(id)).toBe(1);

    const after = await repo.getTask(id);
    expect(after!.status).toBe('QUEUED');
    expect(after!.version).toBe(before!.version + 1);

    // A pending event already exists, so a second cycle must not stack another.
    await backdate(id, 5000);
    const second = await r.reconcile();
    expect(second.requeued).toBe(0);
    expect(await pendingRequeues(id)).toBe(1);
  });

  it('carries the original task data and trace context on the requeue event', async () => {
    const id = await createTask('render-report', 4);
    await pool.query(`UPDATE outbox_events SET trace_context = '{"traceparent":"00-abc-def-01"}'::jsonb WHERE task_id = $1`, [id]);
    await markOutboxDelivered();
    await backdate(id, 5000);

    await reconciler().reconcile();

    const ev = await pool.query(
      `SELECT payload, trace_context FROM outbox_events WHERE task_id = $1 AND event_type = 'TASK_REQUEUED'`,
      [id],
    );
    expect(ev.rows[0].payload).toEqual({
      taskId: id, taskName: 'render-report', priority: 'NORMAL', payload: { n: 'render-report' }, maxRetries: 4,
    });
    expect(ev.rows[0].trace_context).toEqual({ traceparent: '00-abc-def-01' });
  });

  it('leaves a QUEUED task alone while its job is still waiting in Redis', async () => {
    const id = await createTask();
    await queue.add('job', { taskId: id }, { jobId: id });
    await markOutboxDelivered();
    await backdate(id, 5000);

    const result = await reconciler().reconcile();
    expect(result.examined).toBe(1);
    expect(result.requeued).toBe(0);
    expect(await pendingRequeues(id)).toBe(0);
  });

  it('leaves a recently queued task alone (grace period)', async () => {
    const id = await createTask();
    await markOutboxDelivered();

    const result = await reconciler(undefined, 60_000).reconcile();
    expect(result.examined).toBe(0);
    expect(await pendingRequeues(id)).toBe(0);
  });

  it('ignores tasks whose outbox event is still pending (publisher has not run yet)', async () => {
    const id = await createTask();
    await backdate(id, 5000);

    const result = await reconciler().reconcile();
    expect(result.examined).toBe(0);
    expect(await pendingRequeues(id)).toBe(0);
  });

  it('removes a finished job that would block re-publishing, then requeues', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    const job = await queue.add('job', { taskId: id }, { jobId: id });
    // Park the job in "completed" while the database still says QUEUED.
    const token = 'test-token';
    const worker = new Worker(QUEUE_NAME, null as any, { connection: createRedisConn(), autorun: false });
    const fetched = await worker.getNextJob(token);
    expect(fetched!.id).toBe(id);
    await fetched!.moveToCompleted('done', token, false);
    await worker.close();
    expect(await (await queue.getJob(id))!.getState()).toBe('completed');
    await backdate(id, 5000);

    const result = await reconciler().reconcile();
    expect(result.requeued).toBe(1);
    expect(await queue.getJob(job.id!)).toBeUndefined();
    expect(await pendingRequeues(id)).toBe(1);
  });

  it('requeues a PROCESSING task with an expired lease and charges one retry', async () => {
    const id = await createTask('t', 3);
    await markOutboxDelivered();
    const t = await repo.getTask(id);
    await repo.transitionStatus(id, t!.version, 'PROCESSING', { claimedBy: 'dead-worker' });
    await pool.query(`UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, [id]);

    const result = await reconciler().reconcile();
    expect(result.requeued).toBe(1);

    const after = await repo.getTask(id);
    expect(after!.status).toBe('QUEUED');
    expect(after!.retries).toBe(1);
    expect(after!.claimedBy ?? null).toBeNull();
    expect(after!.claimToken ?? null).toBeNull();
    expect(await pendingRequeues(id)).toBe(1);
  });

  it('fails a PROCESSING orphan whose retry budget is spent instead of looping forever', async () => {
    const id = await createTask('poison', 2);
    await markOutboxDelivered();
    const t = await repo.getTask(id);
    await repo.transitionStatus(id, t!.version, 'PROCESSING', { claimedBy: 'dead-worker' });
    await pool.query(`UPDATE tasks SET retries = 2, claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, [id]);

    const result = await reconciler().reconcile();
    expect(result.failed).toBe(1);
    expect(result.requeued).toBe(0);

    const after = await repo.getTask(id);
    expect(after!.status).toBe('FAILED');
    expect(after!.retries).toBe(3);
    expect(after!.error).toMatch(/retry budget/);
    expect(await pendingRequeues(id)).toBe(0);
  });

  it('does not touch a PROCESSING task whose lease is still live', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    const t = await repo.getTask(id);
    await repo.transitionStatus(id, t!.version, 'PROCESSING', { claimedBy: 'live-worker' });

    const result = await reconciler().reconcile();
    expect(result.examined).toBe(0);
    expect((await repo.getTask(id))!.status).toBe('PROCESSING');
  });

  it('ignores terminal tasks', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    await pool.query(`UPDATE tasks SET status = 'COMPLETED', updated_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, [id]);

    const result = await reconciler().reconcile();
    expect(result.examined).toBe(0);
  });

  it('skips the whole cycle when Redis cannot answer, rather than guessing', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    await backdate(id, 5000);

    const brokenQueue: ReconcilerQueue = { getJob: async () => { throw new Error('ECONNREFUSED'); } };
    const result = await reconciler(brokenQueue).reconcile();
    expect(result.skipped).toBe('redis_unavailable');
    expect(await pendingRequeues(id)).toBe(0);
  });

  it('runs on only one replica at a time (advisory lock)', async () => {
    const id = await createTask();
    await markOutboxDelivered();
    await backdate(id, 5000);

    const holder = await pool.connect();
    try {
      await holder.query('SELECT pg_advisory_lock(7340021)');
      const result = await reconciler().reconcile();
      expect(result.skipped).toBe('locked');
      expect(await pendingRequeues(id)).toBe(0);
    } finally {
      await holder.query('SELECT pg_advisory_unlock(7340021)');
      holder.release();
    }
    expect((await reconciler().reconcile()).requeued).toBe(1);
  });
});

describe('TaskReconciler: Redis loses the queue mid-run (requires PostgreSQL + Redis)', () => {
  it('every task still completes, and none is executed twice', async () => {
    const TASKS = 30;
    const executions = new Map<string, number>();

    const publisher = new OutboxPublisher(pool, queue, 100, 50, 5, { logger: silentLogger });
    const rec = reconciler(undefined, 1000);

    // Mirrors the real worker: claim QUEUED -> PROCESSING in PostgreSQL, run,
    // then COMPLETED with the claim token. Anything not QUEUED is skipped.
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const taskId = job.data.taskId as string;
        const task = await repo.getTask(taskId);
        if (!task || task.status !== 'QUEUED') return { skipped: true };
        const claimed = await repo.transitionStatus(taskId, task.version, 'PROCESSING', { claimedBy: 'w1', startedAt: new Date() });
        executions.set(taskId, (executions.get(taskId) ?? 0) + 1);
        await new Promise((r) => setTimeout(r, 150));
        await repo.transitionStatus(taskId, claimed.version, 'COMPLETED', {
          claimToken: claimed.claimToken, completedAt: new Date(), result: { ok: true },
        });
        return { ok: true };
      },
      { connection: createRedisConn(), concurrency: 2 },
    );
    worker.on('error', () => {});

    const ids: string[] = [];
    for (let i = 0; i < TASKS; i++) ids.push(await createTask(`chaos-${i}`));

    publisher.start();
    try {
      // Let a few tasks finish, then wipe Redis while the rest are waiting/active.
      const deadlineFirst = Date.now() + 15_000;
      while (Date.now() < deadlineFirst) {
        const done = await pool.query(`SELECT COUNT(*)::int AS c FROM tasks WHERE status = 'COMPLETED'`);
        if (done.rows[0].c >= 3) break;
        await new Promise((r) => setTimeout(r, 50));
      }
      const deleted = await wipeQueueFromRedis();
      expect(deleted).toBeGreaterThan(0);

      const stuck = await pool.query(`SELECT COUNT(*)::int AS c FROM tasks WHERE status <> 'COMPLETED'`);
      expect(stuck.rows[0].c).toBeGreaterThan(0);

      rec.start();
      const deadline = Date.now() + 45_000;
      let remaining = TASKS;
      while (Date.now() < deadline) {
        const r = await pool.query(`SELECT COUNT(*)::int AS c FROM tasks WHERE status <> 'COMPLETED'`);
        remaining = r.rows[0].c;
        if (remaining === 0) break;
        await new Promise((r2) => setTimeout(r2, 200));
      }
      expect(remaining).toBe(0);
    } finally {
      await rec.stop();
      await publisher.stop();
      await worker.close();
    }

    for (const id of ids) {
      expect(executions.get(id)).toBe(1);
    }
    const requeued = await pool.query(`SELECT COUNT(DISTINCT task_id)::int AS c FROM outbox_events WHERE event_type = 'TASK_REQUEUED'`);
    expect(requeued.rows[0].c).toBeGreaterThan(0);
  }, 90_000);
});
