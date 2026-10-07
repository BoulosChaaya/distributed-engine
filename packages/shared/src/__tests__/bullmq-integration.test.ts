import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue, Worker, Job, DelayedError } from 'bullmq';
import IORedis from 'ioredis';
import { TaskRepository, ClaimNotExpiredError, StaleVersionError, ClaimTokenMismatchError } from '../db/task-repository';
import { runMigrations } from '../db/migrations';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const TEST_REDIS_HOST = process.env.TEST_REDIS_HOST || 'localhost';
const TEST_REDIS_PORT = parseInt(process.env.TEST_REDIS_PORT || '6379');

const QUEUE_NAME = 'tasks-test-bullmq-integration';

let pool: Pool;
let repo: TaskRepository;
let redis: IORedis;
let queue: Queue;

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
  repo = new TaskRepository(pool, 5000);

  queue = new Queue(QUEUE_NAME, {
    connection: redis,
    defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
  });
});

afterAll(async () => {
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

function createRedisConn(): IORedis {
  return new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, maxRetriesPerRequest: null });
}

describe('Section 5: Lease deferral does not consume execution retry budget (requires PostgreSQL + Redis)', () => {
  it('should defer via moveToDelayed without consuming an attempt when lease is active', async () => {
    requireInfra();

    const LEASE_TTL = 5000;
    const testRepo = new TaskRepository(pool, LEASE_TTL);
    const LEASE_DEFERRAL_MARGIN_MS = 500;

    const { task } = await testRepo.createTaskWithOutbox({
      name: 'lease-deferral-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 1,
    });

    const claimed = await testRepo.transitionStatus(task.id, task.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-original',
    });

    const MAX_ATTEMPTS = 2;
    let deferralCount = 0;
    let completedSuccessfully = false;

    const workerConn = createRedisConn();
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const taskId = job.data.taskId;
        const currentTask = await testRepo.getTask(taskId);
        if (!currentTask || currentTask.status !== 'PROCESSING') {
          return { status: 'SKIPPED' };
        }

        try {
          await testRepo.reclaimStalledTask(taskId, currentTask.version, 'worker-new');
          completedSuccessfully = true;
          return { status: 'RECLAIMED' };
        } catch (error) {
          if (error instanceof ClaimNotExpiredError) {
            deferralCount++;
            const deferUntil = error.expiresAt.getTime() + LEASE_DEFERRAL_MARGIN_MS;
            await job.moveToDelayed(deferUntil, job.token);
            throw new DelayedError();
          }
          throw error;
        }
      },
      {
        connection: workerConn,
        concurrency: 1,
        maxStalledCount: 2,
        stalledInterval: 30000,
      },
    );

    worker.on('error', () => {});

    await queue.add('task', { taskId: task.id, taskName: task.name }, {
      jobId: `deferral-${task.id}`,
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'exponential', delay: 1000 },
    });

    await new Promise((resolve) => setTimeout(resolve, LEASE_TTL + LEASE_DEFERRAL_MARGIN_MS + 2000));

    await worker.close();
    workerConn.disconnect();

    expect(deferralCount).toBeGreaterThanOrEqual(1);
    expect(completedSuccessfully).toBe(true);

    const job = await queue.getJob(`deferral-${task.id}`);
    if (job) {
      expect(job.attemptsMade).toBeLessThanOrEqual(1);
    }
  }, 20000);

  it('should keep job recoverable after deferral — task eventually completes', async () => {
    requireInfra();

    const LEASE_TTL = 3000;
    const testRepo = new TaskRepository(pool, LEASE_TTL);
    const LEASE_DEFERRAL_MARGIN_MS = 500;

    const { task } = await testRepo.createTaskWithOutbox({
      name: 'recoverable-deferral-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 0,
    });

    await testRepo.transitionStatus(task.id, task.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-original',
    });

    let taskCompleted = false;

    const workerConn = createRedisConn();
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const currentTask = await testRepo.getTask(task.id);
        if (!currentTask || currentTask.status !== 'PROCESSING') {
          return { status: 'SKIPPED' };
        }

        try {
          const reclaimed = await testRepo.reclaimStalledTask(task.id, currentTask.version, 'worker-recovery');
          await testRepo.transitionStatus(task.id, reclaimed.version, 'COMPLETED', {
            completedAt: new Date(),
            result: { processedBy: 'worker-recovery' },
            claimToken: reclaimed.claimToken,
          });
          taskCompleted = true;
          return { status: 'COMPLETED' };
        } catch (error) {
          if (error instanceof ClaimNotExpiredError) {
            const deferUntil = error.expiresAt.getTime() + LEASE_DEFERRAL_MARGIN_MS;
            await job.moveToDelayed(deferUntil, job.token);
            throw new DelayedError();
          }
          throw error;
        }
      },
      {
        connection: workerConn,
        concurrency: 1,
        maxStalledCount: 2,
        stalledInterval: 30000,
      },
    );

    worker.on('error', () => {});

    await queue.add('task', { taskId: task.id, taskName: task.name }, {
      jobId: `recover-${task.id}`,
      attempts: 1,
      backoff: { type: 'exponential', delay: 1000 },
    });

    await new Promise((resolve) => setTimeout(resolve, LEASE_TTL + LEASE_DEFERRAL_MARGIN_MS + 2000));

    await worker.close();
    workerConn.disconnect();

    expect(taskCompleted).toBe(true);
    const finalTask = await testRepo.getTask(task.id);
    expect(finalTask?.status).toBe('COMPLETED');
  }, 15000);
});

describe('Section 6: Real execution failure consumes retry budget (requires PostgreSQL + Redis)', () => {
  it('should consume BullMQ attempts on actual execution failure and transition PG state', async () => {
    requireInfra();

    const testRepo = new TaskRepository(pool, 30000);

    const { task } = await testRepo.createTaskWithOutbox({
      name: 'exec-failure-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 2,
    });

    let attemptCount = 0;
    const MAX_ATTEMPTS = 3;

    const workerConn = createRedisConn();
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const taskId = job.data.taskId;
        const currentTask = await testRepo.getTask(taskId);
        if (!currentTask) throw new Error('Task not found');

        if (currentTask.status === 'QUEUED') {
          const claimed = await testRepo.transitionStatus(taskId, currentTask.version, 'PROCESSING', {
            startedAt: new Date(),
            claimedBy: 'worker-fail-test',
          });

          attemptCount++;
          const isFinalAttempt = job.attemptsMade + 1 >= MAX_ATTEMPTS;

          try {
            if (isFinalAttempt) {
              await testRepo.transitionStatus(taskId, claimed.version, 'FAILED', {
                error: 'Simulated failure',
                retries: attemptCount,
                claimToken: claimed.claimToken,
              });
            } else {
              await testRepo.transitionStatus(taskId, claimed.version, 'QUEUED', {
                error: 'Simulated failure',
                retries: attemptCount,
                claimToken: claimed.claimToken,
              });
            }
          } catch {}

          throw new Error('Simulated execution failure');
        }

        if (currentTask.status === 'PROCESSING') {
          attemptCount++;
          const isFinalAttempt = job.attemptsMade + 1 >= MAX_ATTEMPTS;
          try {
            if (isFinalAttempt) {
              await testRepo.transitionStatus(taskId, currentTask.version, 'FAILED', {
                error: 'Simulated failure',
                retries: attemptCount,
                claimToken: currentTask.claimToken,
              });
            } else {
              await testRepo.transitionStatus(taskId, currentTask.version, 'QUEUED', {
                error: 'Simulated failure',
                retries: attemptCount,
                claimToken: currentTask.claimToken,
              });
            }
          } catch {}
          throw new Error('Simulated execution failure');
        }

        return { status: 'SKIPPED' };
      },
      {
        connection: workerConn,
        concurrency: 1,
        maxStalledCount: 2,
        stalledInterval: 30000,
      },
    );

    worker.on('error', () => {});

    await queue.add('task', { taskId: task.id, taskName: task.name }, {
      jobId: `fail-${task.id}`,
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'exponential', delay: 500 },
    });

    await new Promise((resolve) => setTimeout(resolve, 8000));

    await worker.close();
    workerConn.disconnect();

    expect(attemptCount).toBeGreaterThanOrEqual(2);

    const job = await queue.getJob(`fail-${task.id}`);
    if (job) {
      expect(job.attemptsMade).toBeGreaterThanOrEqual(2);
    }

    const finalTask = await testRepo.getTask(task.id);
    expect(finalTask).not.toBeNull();
    expect(['QUEUED', 'FAILED']).toContain(finalTask!.status);
    expect(finalTask!.error).toBe('Simulated failure');
  }, 15000);
});

describe('Section 7: Healthy long task with renewal — foreign worker cannot reclaim (requires PostgreSQL + Redis)', () => {
  it('should renew lease, block foreign reclaim, and complete without consuming extra attempts', async () => {
    requireInfra();

    const LEASE_TTL = 3000;
    const RENEWAL_INTERVAL = Math.floor(LEASE_TTL / 3);
    const testRepo = new TaskRepository(pool, LEASE_TTL);

    const { task } = await testRepo.createTaskWithOutbox({
      name: 'renewal-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 1,
    });

    let ownerCompleted = false;
    let foreignReclaimRejected = false;

    const workerConn = createRedisConn();
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const taskId = job.data.taskId;
        const currentTask = await testRepo.getTask(taskId);
        if (!currentTask) return { status: 'SKIPPED' };

        if (currentTask.status === 'QUEUED') {
          const claimed = await testRepo.transitionStatus(taskId, currentTask.version, 'PROCESSING', {
            startedAt: new Date(),
            claimedBy: 'worker-owner',
          });

          let claimToken = claimed.claimToken!;
          let renewalTimer: ReturnType<typeof setInterval> | undefined;

          renewalTimer = setInterval(async () => {
            try {
              await testRepo.renewClaim(taskId, claimToken);
            } catch {
              if (renewalTimer) clearInterval(renewalTimer);
            }
          }, RENEWAL_INTERVAL);

          await new Promise((resolve) => setTimeout(resolve, LEASE_TTL * 2));

          if (renewalTimer) clearInterval(renewalTimer);

          try {
            await testRepo.reclaimStalledTask(taskId, claimed.version, 'worker-foreign');
            foreignReclaimRejected = false;
          } catch (error) {
            if (error instanceof ClaimNotExpiredError) {
              foreignReclaimRejected = true;
            }
          }

          const freshTask = await testRepo.getTask(taskId);
          if (freshTask && freshTask.status === 'PROCESSING') {
            await testRepo.transitionStatus(taskId, freshTask.version, 'COMPLETED', {
              completedAt: new Date(),
              result: { processedBy: 'worker-owner' },
              claimToken: freshTask.claimToken,
            });
            ownerCompleted = true;
          }

          return { status: 'COMPLETED' };
        }
        return { status: 'SKIPPED' };
      },
      {
        connection: workerConn,
        concurrency: 1,
        maxStalledCount: 2,
        stalledInterval: 30000,
      },
    );

    worker.on('error', () => {});

    const MAX_ATTEMPTS = 2;
    await queue.add('task', { taskId: task.id, taskName: task.name }, {
      jobId: `renew-${task.id}`,
      attempts: MAX_ATTEMPTS,
      backoff: { type: 'exponential', delay: 1000 },
    });

    await new Promise((resolve) => setTimeout(resolve, LEASE_TTL * 2 + 3000));

    await worker.close();
    workerConn.disconnect();

    expect(ownerCompleted).toBe(true);
    expect(foreignReclaimRejected).toBe(true);

    const finalTask = await testRepo.getTask(task.id);
    expect(finalTask?.status).toBe('COMPLETED');

    const job = await queue.getJob(`renew-${task.id}`);
    if (job) {
      expect(job.attemptsMade).toBeLessThanOrEqual(1);
    }
  }, 20000);
});

describe('Section 8: Ownership loss — cooperative abort and stale token rejection (requires PostgreSQL + Redis)', () => {
  it('should set ownershipLost signal when renewal fails and abort without completing', async () => {
    requireInfra();

    const LEASE_TTL = 3000;
    const RENEWAL_INTERVAL = Math.floor(LEASE_TTL / 3);
    const testRepo = new TaskRepository(pool, LEASE_TTL);

    const { task } = await testRepo.createTaskWithOutbox({
      name: 'ownership-loss-test',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 1,
    });

    let ownershipLostDetected = false;
    let staleTransitionRejected = false;
    let workerAAttemptedComplete = false;

    const workerConn = createRedisConn();
    const worker = new Worker(
      QUEUE_NAME,
      async (job: Job) => {
        const taskId = job.data.taskId;
        const currentTask = await testRepo.getTask(taskId);
        if (!currentTask) return { status: 'SKIPPED' };

        if (currentTask.status === 'QUEUED') {
          const claimed = await testRepo.transitionStatus(taskId, currentTask.version, 'PROCESSING', {
            startedAt: new Date(),
            claimedBy: 'worker-A',
          });

          const workerAVersion = claimed.version;
          const workerAToken = claimed.claimToken!;
          let ownershipLost = false;

          await new Promise((resolve) => setTimeout(resolve, LEASE_TTL + 500));

          const reclaimed = await testRepo.reclaimStalledTask(taskId, claimed.version, 'worker-B');

          let renewalTimer: ReturnType<typeof setInterval> | undefined;
          renewalTimer = setInterval(async () => {
            try {
              await testRepo.renewClaim(taskId, workerAToken);
            } catch {
              ownershipLost = true;
              if (renewalTimer) {
                clearInterval(renewalTimer);
                renewalTimer = undefined;
              }
            }
          }, RENEWAL_INTERVAL);

          await new Promise((resolve) => setTimeout(resolve, RENEWAL_INTERVAL + 500));

          if (renewalTimer) clearInterval(renewalTimer);

          ownershipLostDetected = ownershipLost;

          if (ownershipLost) {
            workerAAttemptedComplete = true;
            try {
              await testRepo.transitionStatus(taskId, workerAVersion, 'COMPLETED', {
                completedAt: new Date(),
                result: { processedBy: 'worker-A' },
                claimToken: workerAToken,
              });
              staleTransitionRejected = false;
            } catch (error) {
              staleTransitionRejected = (
                error instanceof StaleVersionError ||
                error instanceof ClaimTokenMismatchError
              );
            }
          }

          await testRepo.transitionStatus(taskId, reclaimed.version, 'COMPLETED', {
            completedAt: new Date(),
            result: { processedBy: 'worker-B' },
            claimToken: reclaimed.claimToken,
          });

          return { status: 'COMPLETED' };
        }
        return { status: 'SKIPPED' };
      },
      {
        connection: workerConn,
        concurrency: 1,
        maxStalledCount: 2,
        stalledInterval: 30000,
      },
    );

    worker.on('error', () => {});

    await queue.add('task', { taskId: task.id, taskName: task.name }, {
      jobId: `ownership-${task.id}`,
      attempts: 2,
      backoff: { type: 'exponential', delay: 1000 },
    });

    await new Promise((resolve) => setTimeout(resolve, LEASE_TTL + RENEWAL_INTERVAL + 5000));

    await worker.close();
    workerConn.disconnect();

    expect(ownershipLostDetected).toBe(true);
    expect(workerAAttemptedComplete).toBe(true);
    expect(staleTransitionRejected).toBe(true);

    const finalTask = await testRepo.getTask(task.id);
    expect(finalTask?.status).toBe('COMPLETED');
    expect(finalTask?.result).toEqual({ processedBy: 'worker-B' });
  }, 20000);
});
