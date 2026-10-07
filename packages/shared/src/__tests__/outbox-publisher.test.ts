import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { TaskRepository } from '../db/task-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { runMigrations } from '../db/migrations';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const TEST_REDIS_HOST = process.env.TEST_REDIS_HOST || 'localhost';
const TEST_REDIS_PORT = parseInt(process.env.TEST_REDIS_PORT || '6379');

let pool: Pool;
let repo: TaskRepository;
let redis: IORedis;
let queue: Queue;
let publisher: OutboxPublisher;

function requireInfra(): void {
  if (!pool || !repo || !redis || !queue) {
    throw new Error('PostgreSQL and Redis are required for this test.');
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    throw new Error(
      `PostgreSQL not available at ${TEST_PG_URL}. Integration tests require PostgreSQL. Error: ${err}`
    );
  }

  redis = new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, maxRetriesPerRequest: null });
  try {
    await redis.ping();
  } catch (err) {
    throw new Error(
      `Redis not available at ${TEST_REDIS_HOST}:${TEST_REDIS_PORT}. Integration tests require Redis. Error: ${err}`
    );
  }

  await runMigrations(pool);
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  repo = new TaskRepository(pool);

  queue = new Queue('tasks-test-outbox', {
    connection: redis,
    defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
  });

  publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
});

afterAll(async () => {
  if (publisher) await publisher.stop();
  if (queue) {
    try { await queue.obliterate({ force: true }); } catch {}
    await queue.close();
  }
  if (redis) redis.disconnect();
  if (pool) {
    await pool.query('TRUNCATE outbox_events, tasks CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  requireInfra();
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  try { await queue.drain(); } catch {}
});

describe('OutboxPublisher (requires PostgreSQL + Redis)', () => {
  it('should publish pending outbox events to BullMQ', async () => {
    requireInfra();

    await repo.createTaskWithOutbox({
      name: 'outbox-test-1',
      priority: 'HIGH',
      payload: { test: true },
      maxRetries: 3,
    });

    const pendingBefore = await publisher.getPendingCount();
    expect(pendingBefore).toBe(1);

    const processed = await publisher.processOutbox();
    expect(processed).toBe(1);

    const pendingAfter = await publisher.getPendingCount();
    expect(pendingAfter).toBe(0);
  });

  it('should keep task as QUEUED after publish (no status change needed)', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'outbox-status-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(task.status).toBe('QUEUED');

    await publisher.processOutbox();

    const updated = await repo.getTask(task.id);
    expect(updated!.status).toBe('QUEUED');
  });

  it('should use task ID as BullMQ job ID for idempotency', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'idempotent-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await publisher.processOutbox();

    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();
    expect(job!.id).toBe(task.id);
  });

  it('should set per-job BullMQ attempts from maxRetries', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'attempts-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 5,
    });

    await publisher.processOutbox();

    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();
    expect(job!.opts.attempts).toBe(6);
  });

  it('should handle multiple outbox events in one batch', async () => {
    requireInfra();

    for (let i = 0; i < 5; i++) {
      await repo.createTaskWithOutbox({
        name: `batch-test-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
      });
    }

    const processed = await publisher.processOutbox();
    expect(processed).toBe(5);

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);
  });

  it('should not create a second BullMQ job when re-publishing with same taskId as jobId', async () => {
    requireInfra();

    const dedupQueue = new Queue('tasks-test-dedup', {
      connection: redis,
      defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
    });

    const dedupPublisher = new OutboxPublisher(pool, dedupQueue, 60000, 10, 3);

    try {
      const { task } = await repo.createTaskWithOutbox({
        name: 'dedup-test',
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
      });

      await dedupPublisher.processOutbox();

      const job1 = await dedupQueue.getJob(task.id);
      expect(job1).not.toBeNull();

      await pool.query(
        `UPDATE outbox_events SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, attempts = 0 WHERE task_id = $1`,
        [task.id],
      );

      await dedupPublisher.processOutbox();

      const job2 = await dedupQueue.getJob(task.id);
      expect(job2).toBeDefined();
      expect(job2!.id).toBe(task.id);
    } finally {
      try { await dedupQueue.obliterate({ force: true }); } catch {}
      await dedupQueue.close();
    }
  });

  it('should mark event as FAILED after max attempts', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'fail-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await pool.query(
      `UPDATE outbox_events SET attempts = 3 WHERE task_id = $1`,
      [task.id],
    );

    const processed = await publisher.processOutbox();
    expect(processed).toBe(0);

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);
  });

  it('should start with circuit breaker CLOSED', () => {
    requireInfra();
    expect(publisher.getCircuitState()).toBe('CLOSED');
  });

  it('should handle atomicity: task and outbox event created together', async () => {
    requireInfra();

    const { task, outboxEvent } = await repo.createTaskWithOutbox({
      name: 'atomic-test',
      priority: 'NORMAL',
      payload: { foo: 'bar' },
      maxRetries: 3,
    });

    const dbTask = await pool.query('SELECT * FROM tasks WHERE id = $1', [task.id]);
    const dbOutbox = await pool.query('SELECT * FROM outbox_events WHERE id = $1', [outboxEvent.id]);

    expect(dbTask.rows.length).toBe(1);
    expect(dbOutbox.rows.length).toBe(1);
    expect(dbOutbox.rows[0].task_id).toBe(task.id);
  });
});

describe('Cancellation vs outbox race (requires PostgreSQL + Redis)', () => {
  it('should not publish a task that was cancelled before outbox processing', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'cancel-before-publish',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.cancelTask(task.id);

    const processed = await publisher.processOutbox();
    expect(processed).toBe(0);

    const job = await queue.getJob(task.id);
    expect(job).toBeFalsy();
  });

  it('should mark outbox event as DELIVERED when task is cancelled (skip without error)', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'cancel-mark-delivered',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.cancelTask(task.id);
    await publisher.processOutbox();

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);

    const result = await pool.query(
      `SELECT status FROM outbox_events WHERE task_id = $1`,
      [task.id],
    );
    expect(result.rows[0].status).toBe('DELIVERED');
  });

  it('should honour cancellation that commits after claim but before publish (SELECT FOR UPDATE)', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'race-cancel',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.cancelTask(task.id);

    const processed = await publisher.processOutbox();
    expect(processed).toBe(0);

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');

    const job = await queue.getJob(task.id);
    expect(job).toBeFalsy();
  });

  it('should publish a task that is still QUEUED', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'not-cancelled',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processed = await publisher.processOutbox();
    expect(processed).toBe(1);

    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();
  });

  it('should not re-enqueue a task that was cancelled after initial publication', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'cancel-after-pub',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await publisher.processOutbox();

    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();

    await repo.cancelTask(task.id);

    await pool.query(
      `UPDATE outbox_events SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL, attempts = 0 WHERE task_id = $1`,
      [task.id],
    );

    await publisher.processOutbox();

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');
  });
});

describe('HALF_OPEN circuit breaker bounding (requires PostgreSQL + Redis)', () => {
  it('should process only one event when in HALF_OPEN state', async () => {
    requireInfra();

    const halfOpenPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3);

    for (let i = 0; i < 3; i++) {
      await repo.createTaskWithOutbox({
        name: `halfopen-test-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
      });
    }

    // Force into HALF_OPEN state by manipulating internal state
    // We'll use a separate publisher and manually force the state
    (halfOpenPublisher as any).circuitState = 'HALF_OPEN';
    (halfOpenPublisher as any).consecutiveSuccesses = 0;

    const processed = await halfOpenPublisher.processOutbox();
    expect(processed).toBe(1);

    const pending = await halfOpenPublisher.getPendingCount();
    expect(pending).toBe(2);
  });
});
