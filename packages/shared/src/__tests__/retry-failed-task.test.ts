import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import IORedis from 'ioredis';
import { Queue, Worker, UnrecoverableError } from 'bullmq';
import { runMigrations } from '../db/migrations';
import { TaskRepository, TaskNotRetryableError } from '../db/task-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { WorkflowRepository } from '../db/workflow-repository';

// Operator redrive of a FAILED task, against real PostgreSQL and Redis.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let repo: TaskRepository;
let redis: IORedis;
let queue: Queue;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  repo = new TaskRepository(pool);
  redis = new IORedis({
    host: process.env.TEST_REDIS_HOST || 'localhost',
    port: Number(process.env.TEST_REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: null,
  });
  queue = new Queue('tasks-test-retry', { connection: redis, defaultJobOptions: { removeOnFail: false } });
});
afterAll(async () => {
  await queue.obliterate({ force: true }).catch(() => {});
  await queue.close();
  redis.disconnect();
  await pool.query('TRUNCATE outbox_events, task_dependencies, tasks, workflows CASCADE').catch(() => {});
  await pool.end();
});
beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, task_dependencies, tasks, workflows CASCADE');
  await queue.obliterate({ force: true }).catch(() => {});
});

async function failedTask() {
  const { task } = await repo.createTaskWithOutbox({ name: 'http', priority: 'NORMAL', payload: {}, maxRetries: 2 });
  await pool.query(
    `UPDATE tasks SET status = 'FAILED', retries = 3, error = 'upstream responded 503', completed_at = NOW() WHERE id = $1`,
    [task.id],
  );
  return task.id;
}

describe('retryFailedTask (requires PostgreSQL)', () => {
  it('moves FAILED to QUEUED with a fresh budget and a TASK_REQUEUED event, in one transaction', async () => {
    const id = await failedTask();
    const before = (await pool.query('SELECT version FROM tasks WHERE id = $1', [id])).rows[0].version;

    const task = await repo.retryFailedTask(id);

    expect(task.status).toBe('QUEUED');
    expect(task.retries).toBe(0);
    expect(task.version).toBe(before + 1);
    expect(task.error).toBe('upstream responded 503'); // kept until the new run ends
    const events = (await pool.query(
      `SELECT event_type, status FROM outbox_events WHERE task_id = $1 ORDER BY created_at`, [id],
    )).rows;
    expect(events.at(-1)).toEqual({ event_type: 'TASK_REQUEUED', status: 'PENDING' });
  });

  it.each(['QUEUED', 'PROCESSING', 'COMPLETED', 'CANCELLED'])('refuses a %s task', async (status) => {
    const { task } = await repo.createTaskWithOutbox({ name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 2 });
    await pool.query('UPDATE tasks SET status = $2 WHERE id = $1', [task.id, status]);
    await expect(repo.retryFailedTask(task.id)).rejects.toBeInstanceOf(TaskNotRetryableError);
  });

  it('refuses a workflow step', async () => {
    const wf = await new WorkflowRepository(pool).createWorkflow({ name: 'wf', steps: [{ key: 'a', name: 'echo' }] });
    const stepId = wf.detail.steps[0].taskId;
    await pool.query(`UPDATE tasks SET status = 'FAILED' WHERE id = $1`, [stepId]);
    await expect(repo.retryFailedTask(stepId)).rejects.toThrow(/workflow step/);
  });

  it('only lets a tenant retry its own task', async () => {
    const id = await failedTask();
    await expect(repo.retryFailedTask(id, 'some-other-tenant')).rejects.toThrow(/not found/);
  });
});

describe('publishing a requeue over a dead job (requires PostgreSQL + Redis)', () => {
  it('removes the failed BullMQ job so the requeued task is really enqueued', async () => {
    const id = await failedTask();
    // What BullMQ keeps after the task failed: a job with the same id, in "failed".
    // Produce it the real way: a worker that fails the job for good.
    const workerConnection = redis.duplicate();
    const worker = new Worker('tasks-test-retry', async () => { throw new UnrecoverableError('upstream responded 503'); }, {
      connection: workerConnection,
    });
    const failed = new Promise((resolve) => worker.once('failed', resolve));
    await queue.add('http', { taskId: id }, { jobId: id, attempts: 1 });
    await failed;
    await worker.close();
    workerConnection.disconnect();
    expect(await (await queue.getJob(id))!.getState()).toBe('failed');

    await repo.retryFailedTask(id);
    await pool.query(`UPDATE outbox_events SET status = 'DELIVERED' WHERE task_id = $1 AND event_type = 'TASK_CREATED'`, [id]);
    const published = await new OutboxPublisher(pool, queue, 60000, 10, 5).processOutbox();

    expect(published).toBe(1);
    // Live again (a job with a priority waits in BullMQ's "prioritized" list).
    expect(['waiting', 'prioritized']).toContain(await (await queue.getJob(id))!.getState());
  });
});
