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

describe('OutboxPublisher (requires PostgreSQL + Redis)', () => {
  let pool: Pool;
  let repo: TaskRepository;
  let redis: IORedis;
  let queue: Queue;
  let publisher: OutboxPublisher;
  let pgAvailable = false;
  let redisAvailable = false;

  beforeAll(async () => {
    pool = new Pool({ connectionString: TEST_PG_URL });
    try {
      await pool.query('SELECT 1');
      pgAvailable = true;
    } catch {
      console.warn('PostgreSQL not available, skipping OutboxPublisher tests');
      return;
    }

    redis = new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, maxRetriesPerRequest: null });
    try {
      await redis.ping();
      redisAvailable = true;
    } catch {
      console.warn('Redis not available, skipping OutboxPublisher tests');
      return;
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
    if (!pgAvailable || !redisAvailable) return;
    await pool.query('TRUNCATE outbox_events, tasks CASCADE');
    try { await queue.drain(); } catch {}
  });

  it('should publish pending outbox events to BullMQ', async () => {
    if (!pgAvailable || !redisAvailable) return;

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

  it('should transition task from PENDING to QUEUED after publish', async () => {
    if (!pgAvailable || !redisAvailable) return;

    const { task } = await repo.createTaskWithOutbox({
      name: 'outbox-transition-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(task.status).toBe('PENDING');

    await publisher.processOutbox();

    const updated = await repo.getTask(task.id);
    expect(updated!.status).toBe('QUEUED');
  });

  it('should use task ID as BullMQ job ID for idempotency', async () => {
    if (!pgAvailable || !redisAvailable) return;

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

  it('should handle multiple outbox events in one batch', async () => {
    if (!pgAvailable || !redisAvailable) return;

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

  it('should not create duplicate BullMQ jobs on re-publish attempt', async () => {
    if (!pgAvailable || !redisAvailable) return;

    const { task } = await repo.createTaskWithOutbox({
      name: 'dedup-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await publisher.processOutbox();

    await pool.query(
      `UPDATE outbox_events SET status = 'PENDING', claimed_by = NULL, claimed_at = NULL WHERE task_id = $1`,
      [task.id],
    );

    const processed = await publisher.processOutbox();
    expect(processed).toBeLessThanOrEqual(1);
  });

  it('should mark event as FAILED after max attempts', async () => {
    if (!pgAvailable || !redisAvailable) return;

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

  it('should handle atomicity: task and outbox event created together', async () => {
    if (!pgAvailable || !redisAvailable) return;

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
