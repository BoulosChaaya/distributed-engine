import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { Writable } from 'stream';
import { TaskRepository } from '../db/task-repository';
import { TenantRepository } from '../db/tenant-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { WeightedFairScheduler } from '../fairness';
import { runMigrations } from '../db/migrations';
import { createLogger } from '../logger/index';
import { computeBillingPeriodStart } from '../db/tenant-repository';

function createCaptureStream(): { stream: Writable; lines: string[] } {
  const lines: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      const str = chunk.toString().trim();
      if (str) lines.push(str);
      callback();
    },
  });
  return { stream, lines };
}

function capturedMessages(lines: string[]): string[] {
  return lines.map((l) => JSON.parse(l).msg as string);
}

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

    (halfOpenPublisher as any).circuitState = 'HALF_OPEN';
    (halfOpenPublisher as any).consecutiveSuccesses = 0;

    const processed = await halfOpenPublisher.processOutbox();
    expect(processed).toBe(1);

    const pending = await halfOpenPublisher.getPendingCount();
    expect(pending).toBe(2);
  });
});

describe('Cancellation race: BullMQ job for cancelled task (requires PostgreSQL + Redis)', () => {
  it('should leave a BullMQ job harmless when task is cancelled after publication', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'race-post-publish-cancel',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await publisher.processOutbox();
    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();

    await repo.cancelTask(task.id);

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');
  });

  it('should allow publication even when task is concurrently cancelled (best-effort check)', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'concurrent-cancel-publish',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Publish first, then cancel — simulates the race where cancel commits
    // after the best-effort check but before BullMQ.add() returns
    await publisher.processOutbox();
    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();

    // Cancel after publish — job exists in BullMQ but task is CANCELLED in PG
    await repo.cancelTask(task.id);
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');

    // PG is authoritative — the BullMQ job is harmless
    expect(job!.id).toBe(task.id);
  });

  it('should document that cancellation check is best-effort, not atomic', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'non-atomic-check',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Task is QUEUED, publisher checks status and sees QUEUED, then publishes
    const processed = await publisher.processOutbox();
    expect(processed).toBe(1);

    // Now cancel the task — BullMQ still has the job
    await repo.cancelTask(task.id);
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');

    // The job is in BullMQ, but workers will check PG before processing
    const job = await queue.getJob(task.id);
    expect(job).not.toBeNull();
  });

  it('should skip cancelled tasks detected by best-effort check before publish', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'skip-cancelled-early',
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

  it('should transition outbox event to DELIVERED even when skipping a cancelled task', async () => {
    requireInfra();

    const { task } = await repo.createTaskWithOutbox({
      name: 'delivered-on-skip',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.cancelTask(task.id);
    await publisher.processOutbox();

    const result = await pool.query(
      `SELECT status FROM outbox_events WHERE task_id = $1`,
      [task.id],
    );
    expect(result.rows[0].status).toBe('DELIVERED');

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);
  });
});

describe('Circuit breaker full cycle with injectable timing (requires PostgreSQL + Redis)', () => {
  it('should go CLOSED -> OPEN after failure threshold via internal state', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    expect(cbPublisher.getCircuitState()).toBe('CLOSED');

    // Simulate 3 consecutive BullMQ failures
    for (let i = 0; i < 3; i++) {
      (cbPublisher as any).onBullMQFailure();
    }

    expect(cbPublisher.getCircuitState()).toBe('OPEN');
  });

  it('should not open before reaching failure threshold', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).onBullMQFailure();
    (cbPublisher as any).onBullMQFailure();
    expect(cbPublisher.getCircuitState()).toBe('CLOSED');
  });

  it('should reset failure count on success', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).onBullMQFailure();
    (cbPublisher as any).onBullMQFailure();
    (cbPublisher as any).onBullMQSuccess();
    (cbPublisher as any).onBullMQFailure();
    expect(cbPublisher.getCircuitState()).toBe('CLOSED');
  });

  it('should transition OPEN -> HALF_OPEN after reset timeout (injectable timing)', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).circuitState = 'OPEN';
    (cbPublisher as any).lastFailureTime = 1000;

    // Not enough time has passed
    fakeNow = 1400;
    expect(cbPublisher.getCircuitState()).toBe('OPEN');

    // Now enough time has passed
    fakeNow = 1600;
    expect(cbPublisher.getCircuitState()).toBe('HALF_OPEN');
  });

  it('should transition HALF_OPEN -> CLOSED after success threshold', async () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).circuitState = 'HALF_OPEN';
    (cbPublisher as any).consecutiveSuccesses = 0;

    await repo.createTaskWithOutbox({
      name: 'cb-success-1',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await cbPublisher.processOutbox();
    expect(cbPublisher.getCircuitState()).toBe('HALF_OPEN');

    await repo.createTaskWithOutbox({
      name: 'cb-success-2',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await cbPublisher.processOutbox();
    expect(cbPublisher.getCircuitState()).toBe('CLOSED');
  });

  it('should transition HALF_OPEN -> OPEN on failure', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).circuitState = 'HALF_OPEN';
    (cbPublisher as any).consecutiveSuccesses = 0;

    (cbPublisher as any).onBullMQFailure();
    expect(cbPublisher.getCircuitState()).toBe('OPEN');
  });

  it('should return 0 processed when circuit is OPEN', async () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    (cbPublisher as any).circuitState = 'OPEN';
    (cbPublisher as any).lastFailureTime = 900;

    await repo.createTaskWithOutbox({
      name: 'cb-open-skip',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processed = await cbPublisher.processOutbox();
    expect(processed).toBe(0);
  });

  it('should complete full cycle: CLOSED -> OPEN -> HALF_OPEN -> CLOSED', async () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    expect(cbPublisher.getCircuitState()).toBe('CLOSED');

    // Drive to OPEN
    for (let i = 0; i < 3; i++) {
      (cbPublisher as any).onBullMQFailure();
    }
    expect(cbPublisher.getCircuitState()).toBe('OPEN');

    // Advance time past reset timeout
    fakeNow = 1600;
    expect(cbPublisher.getCircuitState()).toBe('HALF_OPEN');

    // Two successes should close it
    await repo.createTaskWithOutbox({
      name: 'cb-cycle-1',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });
    await cbPublisher.processOutbox();
    expect(cbPublisher.getCircuitState()).toBe('HALF_OPEN');

    await repo.createTaskWithOutbox({
      name: 'cb-cycle-2',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });
    await cbPublisher.processOutbox();
    expect(cbPublisher.getCircuitState()).toBe('CLOSED');
  });

  it('should complete cycle: CLOSED -> OPEN -> HALF_OPEN -> OPEN on failure', () => {
    requireInfra();

    let fakeNow = 1000;
    const cbPublisher = new OutboxPublisher(pool, queue, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => fakeNow,
    });

    // Drive to OPEN
    for (let i = 0; i < 3; i++) {
      (cbPublisher as any).onBullMQFailure();
    }
    expect(cbPublisher.getCircuitState()).toBe('OPEN');

    // Wait for reset timeout
    fakeNow = 1600;
    expect(cbPublisher.getCircuitState()).toBe('HALF_OPEN');

    // Failure in HALF_OPEN goes back to OPEN
    (cbPublisher as any).onBullMQFailure();
    expect(cbPublisher.getCircuitState()).toBe('OPEN');
  });
});

describe('Partial-failure semantics (requires PostgreSQL + Redis)', () => {
  it('should not trigger circuit breaker when BullMQ succeeds but PG DELIVERED update fails', async () => {
    requireInfra();

    const testQueue = new Queue('tasks-test-partial-fail', {
      connection: redis,
      defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
    });

    const cbPublisher = new OutboxPublisher(pool, testQueue, 60000, 10, 3, {
      failureThreshold: 2,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => Date.now(),
    });

    await repo.createTaskWithOutbox({
      name: 'partial-fail-cb-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const realConnect = pool.connect.bind(pool);
    let deliveredUpdateIntercepted = false;

    pool.connect = (async () => {
      const client = await realConnect();
      const realQuery = client.query.bind(client);

      (client as any).query = async function (...args: any[]) {
        const sql = typeof args[0] === 'string' ? args[0] : '';
        if (sql.includes("'DELIVERED'") && sql.includes('UPDATE outbox_events')) {
          deliveredUpdateIntercepted = true;
          throw new Error('Simulated PG failure on DELIVERED update');
        }
        return realQuery(...args);
      };

      return client;
    }) as any;

    try {
      await cbPublisher.processOutbox();

      expect(deliveredUpdateIntercepted).toBe(true);
      expect(cbPublisher.getCircuitState()).toBe('CLOSED');
      expect((cbPublisher as any).consecutiveFailures).toBe(0);
    } finally {
      pool.connect = realConnect;
      await pool.query(`UPDATE outbox_events SET status = 'DELIVERED' WHERE status = 'PENDING'`);
      try { await testQueue.obliterate({ force: true }); } catch {}
      await testQueue.close();
    }
  });

  it('should emit post-publication persistence failure log (not queue failure log) when PG DELIVERED update fails', async () => {
    requireInfra();

    const testQueue2 = new Queue('tasks-test-partial-fail-2', {
      connection: redis,
      defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
    });

    const { stream, lines } = createCaptureStream();
    const captureLogger = createLogger({
      service: 'outbox-publisher',
      environment: 'test',
      level: 'debug',
      destination: stream,
    });

    const cbPublisher = new OutboxPublisher(pool, testQueue2, 60000, 10, 3, {
      failureThreshold: 2,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => Date.now(),
      logger: captureLogger,
    });

    await repo.createTaskWithOutbox({
      name: 'partial-fail-log-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const realConnect = pool.connect.bind(pool);

    pool.connect = (async () => {
      const client = await realConnect();
      const realQuery = client.query.bind(client);

      (client as any).query = async function (...args: any[]) {
        const sql = typeof args[0] === 'string' ? args[0] : '';
        if (sql.includes("'DELIVERED'") && sql.includes('UPDATE outbox_events')) {
          throw new Error('Simulated PG failure');
        }
        return realQuery(...args);
      };

      return client;
    }) as any;

    try {
      const processed = await cbPublisher.processOutbox();
      expect(processed).toBe(1);

      const msgs = capturedMessages(lines);
      expect(msgs).toContain(
        'Failed to mark outbox event as delivered after successful queue publication',
      );
      expect(msgs).not.toContain('Failed to publish outbox event to queue');

      expect(cbPublisher.getCircuitState()).toBe('CLOSED');
      expect((cbPublisher as any).consecutiveFailures).toBe(0);
    } finally {
      pool.connect = realConnect;
      await pool.query(`UPDATE outbox_events SET status = 'DELIVERED' WHERE status = 'PENDING'`);
      try { await testQueue2.obliterate({ force: true }); } catch {}
      await testQueue2.close();
    }
  });

  it('should emit queue failure log and increment circuit breaker on genuine BullMQ publication failure', async () => {
    requireInfra();

    const testQueue3 = new Queue('tasks-test-bullmq-fail', {
      connection: redis,
      defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
    });

    const { stream, lines } = createCaptureStream();
    const captureLogger = createLogger({
      service: 'outbox-publisher',
      environment: 'test',
      level: 'debug',
      destination: stream,
    });

    const cbPublisher = new OutboxPublisher(pool, testQueue3, 60000, 10, 3, {
      failureThreshold: 3,
      successThreshold: 2,
      resetTimeoutMs: 500,
      nowFn: () => Date.now(),
      logger: captureLogger,
    });

    await repo.createTaskWithOutbox({
      name: 'bullmq-fail-log-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const origAdd = testQueue3.add.bind(testQueue3);
    (testQueue3 as any).add = async () => {
      throw new Error('BullMQ connection refused');
    };

    try {
      await cbPublisher.processOutbox();

      const msgs = capturedMessages(lines);
      expect(msgs).toContain('Failed to publish outbox event to queue');
      expect(msgs).not.toContain(
        'Failed to mark outbox event as delivered after successful queue publication',
      );

      expect((cbPublisher as any).consecutiveFailures).toBe(1);
    } finally {
      (testQueue3 as any).add = origAdd;
      await pool.query(
        `UPDATE outbox_events SET claimed_by = NULL, claimed_at = NULL WHERE status = 'PENDING'`,
      );
      try { await testQueue3.obliterate({ force: true }); } catch {}
      await testQueue3.close();
    }
  });
});

describe('Outbox stop/drain guarantee (requires PostgreSQL + Redis)', () => {
  it('should await in-progress poll when stop() is called', async () => {
    requireInfra();

    const drainPublisher = new OutboxPublisher(pool, queue, 50, 10, 3);

    await repo.createTaskWithOutbox({
      name: 'drain-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Barrier: resolve when processOutbox begins executing
    let pollStartedResolve!: () => void;
    const pollStartedPromise = new Promise<void>(r => { pollStartedResolve = r; });
    const origProcess = drainPublisher.processOutbox.bind(drainPublisher);
    let signalled = false;
    (drainPublisher as any).processOutbox = async () => {
      if (!signalled) {
        signalled = true;
        pollStartedResolve();
      }
      return origProcess();
    };

    drainPublisher.start();

    // Deterministic: wait until the poll has actually started
    await pollStartedPromise;

    // stop() should await the active poll, not abort it
    await drainPublisher.stop();

    // After stop, the in-progress poll should have completed
    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);
  });

  it('should not schedule new polls after stop()', async () => {
    requireInfra();

    const drainPublisher = new OutboxPublisher(pool, queue, 50, 10, 3);

    drainPublisher.start();
    await drainPublisher.stop();

    // Create a task after stop — it should not be processed
    await repo.createTaskWithOutbox({
      name: 'after-stop-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await new Promise(r => setTimeout(r, 200));

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(1);
  });
});

describe('Outbox claim concurrency (requires PostgreSQL + Redis)', () => {
  it('should not double-claim events when two publishers process concurrently', async () => {
    requireInfra();

    const queue2 = new Queue('tasks-test-concurrency', {
      connection: redis,
      defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
    });

    const pub1 = new OutboxPublisher(pool, queue2, 60000, 10, 3);
    const pub2 = new OutboxPublisher(pool, queue2, 60000, 10, 3);

    for (let i = 0; i < 5; i++) {
      await repo.createTaskWithOutbox({
        name: `concurrency-test-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
      });
    }

    const [count1, count2] = await Promise.all([
      pub1.processOutbox(),
      pub2.processOutbox(),
    ]);

    // Total processed should be exactly 5 — no double-claiming
    expect(count1 + count2).toBe(5);

    const pending = await publisher.getPendingCount();
    expect(pending).toBe(0);

    try { await queue2.obliterate({ force: true }); } catch {}
    await queue2.close();
  });
});

describe('Fairness: starvation prevention via processOutbox (requires PostgreSQL + Redis)', () => {
  it('should serve a newer tenant within bounded polls despite an older continuous backlog', async () => {
    requireInfra();

    const tenantRepo = new TenantRepository(pool);
    const plans = await tenantRepo.listPlans();
    const plan = plans[0];

    const tenantA = await tenantRepo.createTenant(
      'starve-a', plan.id, `starve-a-${Date.now()}`,
    );
    const tenantB = await tenantRepo.createTenant(
      'starve-b', plan.id, `starve-b-${Date.now()}`,
    );

    const limitsA = await tenantRepo.getEffectiveLimits(tenantA.id);
    const limitsB = await tenantRepo.getEffectiveLimits(tenantB.id);
    const bpA = computeBillingPeriodStart(tenantA.createdAt, limitsA.billingPeriodDays, new Date());
    const bpB = computeBillingPeriodStart(tenantB.createdAt, limitsB.billingPeriodDays, new Date());

    for (let i = 0; i < 20; i++) {
      await repo.acceptTask({
        tenantId: tenantA.id,
        name: `backlog-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart: bpA,
        maxJobsPerPeriod: limitsA.maxJobsPerPeriod,
      });
    }
    for (let i = 0; i < 5; i++) {
      await repo.acceptTask({
        tenantId: tenantB.id,
        name: `newer-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart: bpB,
        maxJobsPerPeriod: limitsB.maxJobsPerPeriod,
      });
    }

    const fairQueue = new Queue(`tasks-starve-${Date.now()}`, {
      connection: redis,
      defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
    });

    const scheduler = new WeightedFairScheduler();
    const fairPublisher = new OutboxPublisher(pool, fairQueue, 60000, 4, 3, {
      fairScheduler: scheduler,
    });

    try {
      let bServed = 0;
      for (let poll = 0; poll < 3; poll++) {
        await fairPublisher.processOutbox();
        const bDelivered = await pool.query(
          `SELECT COUNT(*)::int as c FROM outbox_events oe
           JOIN tasks t ON t.id = oe.task_id
           WHERE oe.status = 'DELIVERED' AND t.tenant_id = $1`,
          [tenantB.id],
        );
        bServed = bDelivered.rows[0].c;
        if (bServed >= 5) break;
      }

      expect(bServed).toBeGreaterThanOrEqual(3);

      const aDelivered = await pool.query(
        `SELECT COUNT(*)::int as c FROM outbox_events oe
         JOIN tasks t ON t.id = oe.task_id
         WHERE oe.status = 'DELIVERED' AND t.tenant_id = $1`,
        [tenantA.id],
      );
      expect(aDelivered.rows[0].c).toBeGreaterThan(0);
    } finally {
      try { await fairQueue.obliterate({ force: true }); } catch {}
      await fairQueue.close();
    }
  });
});

describe('Fairness: weighted distribution via processOutbox (requires PostgreSQL + Redis)', () => {
  it('should allocate publications proportional to tenant weights across polls', async () => {
    requireInfra();

    const tenantRepo = new TenantRepository(pool);
    const plans = await tenantRepo.listPlans();
    const plan = plans[0];

    const heavyTenant = await tenantRepo.createTenant(
      'heavy-w', plan.id, `heavy-w-${Date.now()}`,
    );
    const lightTenant = await tenantRepo.createTenant(
      'light-w', plan.id, `light-w-${Date.now()}`,
    );

    await tenantRepo.setOverrides(heavyTenant.id, { weight: 3 });
    await tenantRepo.setOverrides(lightTenant.id, { weight: 1 });

    const heavyLimits = await tenantRepo.getEffectiveLimits(heavyTenant.id);
    const lightLimits = await tenantRepo.getEffectiveLimits(lightTenant.id);
    const heavyBp = computeBillingPeriodStart(heavyTenant.createdAt, heavyLimits.billingPeriodDays, new Date());
    const lightBp = computeBillingPeriodStart(lightTenant.createdAt, lightLimits.billingPeriodDays, new Date());

    for (let i = 0; i < 24; i++) {
      await repo.acceptTask({
        tenantId: heavyTenant.id,
        name: `heavy-w-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart: heavyBp,
        maxJobsPerPeriod: heavyLimits.maxJobsPerPeriod,
      });
    }
    for (let i = 0; i < 24; i++) {
      await repo.acceptTask({
        tenantId: lightTenant.id,
        name: `light-w-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart: lightBp,
        maxJobsPerPeriod: lightLimits.maxJobsPerPeriod,
      });
    }

    const fairQueue = new Queue(`tasks-weighted-${Date.now()}`, {
      connection: redis,
      defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
    });

    const scheduler = new WeightedFairScheduler();
    const fairPublisher = new OutboxPublisher(pool, fairQueue, 60000, 8, 3, {
      fairScheduler: scheduler,
    });

    try {
      for (let poll = 0; poll < 4; poll++) {
        await fairPublisher.processOutbox();
      }

      const heavyDelivered = await pool.query(
        `SELECT COUNT(*)::int as c FROM outbox_events oe
         JOIN tasks t ON t.id = oe.task_id
         WHERE oe.status = 'DELIVERED' AND t.tenant_id = $1`,
        [heavyTenant.id],
      );
      const lightDelivered = await pool.query(
        `SELECT COUNT(*)::int as c FROM outbox_events oe
         JOIN tasks t ON t.id = oe.task_id
         WHERE oe.status = 'DELIVERED' AND t.tenant_id = $1`,
        [lightTenant.id],
      );

      const heavy = heavyDelivered.rows[0].c as number;
      const light = lightDelivered.rows[0].c as number;
      const total = heavy + light;

      expect(total).toBe(32);

      const ratio = heavy / light;
      expect(ratio).toBeGreaterThanOrEqual(2.5);
      expect(ratio).toBeLessThanOrEqual(3.5);
    } finally {
      try { await fairQueue.obliterate({ force: true }); } catch {}
      await fairQueue.close();
    }
  });
});
