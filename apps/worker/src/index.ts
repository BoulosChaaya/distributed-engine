import { Worker, DelayedError } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import {
  TaskRepository,
  ClaimNotExpiredError,
  runMigrations,
  initTelemetry,
  shutdownTelemetry,
  tracing,
  extractTraceContext,
  TRACE_CONTEXT_KEY,
  createLogger,
} from '@repo/shared';
import { randomUUID } from 'crypto';

initTelemetry({
  serviceName: 'distributed-engine-worker',
});

const WORKER_ID = randomUUID().substring(0, 8);

const logger = createLogger({
  service: 'worker',
  environment: process.env.NODE_ENV ?? 'development',
  level: process.env.LOG_LEVEL,
}).child({ workerId: WORKER_ID });

const pgPool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'distributed_engine',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  max: 10,
});

pgPool.on('error', (err) => logger.error('PostgreSQL pool error', { reason: err.message }));

const redisClient = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
});

redisClient.on('error', (err) => logger.error('Redis connection error', { reason: err.message }));
redisClient.on('connect', () => logger.info('Redis connected'));

const taskRepo = new TaskRepository(pgPool);
const RENEWAL_INTERVAL = Math.floor(taskRepo.claimTtl / 3);
const LEASE_DEFERRAL_MARGIN_MS = 2000;

const workerMetrics = {
  id: WORKER_ID,
  startTime: Date.now(),
  jobsProcessed: 0,
  jobsFailed: 0,
  jobsCompleted: 0,
};

async function updateWorkerStatus() {
  try {
    const uptime = Math.floor((Date.now() - workerMetrics.startTime) / 1000);
    const status = {
      id: workerMetrics.id,
      uptime,
      jobsProcessed: workerMetrics.jobsProcessed,
      jobsCompleted: workerMetrics.jobsCompleted,
      jobsFailed: workerMetrics.jobsFailed,
      status: 'healthy',
      lastHeartbeat: new Date().toISOString(),
    };

    await redisClient.setex(
      `worker:${workerMetrics.id}`,
      30,
      JSON.stringify(status),
    );
  } catch (error) {
    logger.warn('Failed to update worker status', { reason: String(error) });
  }
}

import type { Job } from 'bullmq';
import type { Span } from '@opentelemetry/api';

async function processTask(
  job: Job,
  taskId: string,
  parentSpan: Span,
): Promise<Record<string, unknown>> {
    const currentTask = await taskRepo.getTask(taskId);
    if (!currentTask) {
      logger.warn('Task not found in database, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'not_found');
      return { status: 'SKIPPED', taskId, reason: 'not_found' };
    }

    if (currentTask.status === 'CANCELLED') {
      logger.info('Task was cancelled, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'cancelled');
      return { status: 'SKIPPED', taskId, reason: 'cancelled' };
    }

    if (currentTask.status === 'COMPLETED') {
      logger.info('Task already completed, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'already_completed');
      return { status: 'SKIPPED', taskId, reason: 'already_completed' };
    }

    if (currentTask.status === 'FAILED') {
      logger.info('Task already failed, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'already_failed');
      return { status: 'SKIPPED', taskId, reason: 'already_failed' };
    }

    let taskVersion = currentTask.version;
    let claimToken: string | undefined;
    let currentRetries = currentTask.retries;
    let renewalTimer: ReturnType<typeof setInterval> | undefined;
    let ownershipLost = false;

    if (currentTask.status === 'QUEUED') {
      const claimSpan = tracing.startTaskClaim(taskId, WORKER_ID, 'initial');
      try {
        const updated = await taskRepo.transitionStatus(taskId, taskVersion, 'PROCESSING', {
          startedAt: new Date(),
          claimedBy: WORKER_ID,
        });
        taskVersion = updated.version;
        claimToken = updated.claimToken;
        currentRetries = updated.retries;
        logger.info('Task claimed', { taskId });
        tracing.setSpanOk(claimSpan);
      } catch (error) {
        tracing.recordError(claimSpan, error);
        logger.warn('Failed to transition task to PROCESSING', {
          taskId,
          reason: String(error),
        });
        return { status: 'SKIPPED', taskId, reason: 'transition_failed' };
      } finally {
        tracing.endSpan(claimSpan);
      }
    } else if (currentTask.status === 'PROCESSING') {
      const reclaimSpan = tracing.startTaskClaim(taskId, WORKER_ID, 'reclaim');
      try {
        const reclaimed = await taskRepo.reclaimStalledTask(taskId, taskVersion, WORKER_ID);
        taskVersion = reclaimed.version;
        claimToken = reclaimed.claimToken;
        currentRetries = reclaimed.retries;
        logger.info('Reclaimed stalled task', { taskId });
        tracing.setSpanOk(reclaimSpan);
      } catch (error) {
        if (error instanceof ClaimNotExpiredError) {
          reclaimSpan.setAttribute('task.claim.deferred', true);
          tracing.endSpan(reclaimSpan);
          const deferUntil = error.expiresAt.getTime() + LEASE_DEFERRAL_MARGIN_MS;
          logger.info('Lease not expired, deferring job', {
            taskId,
            expiresAt: error.expiresAt.toISOString(),
            deferUntil: new Date(deferUntil).toISOString(),
          });
          await job.moveToDelayed(deferUntil, job.token);
          throw new DelayedError();
        }
        tracing.recordError(reclaimSpan, error);
        logger.warn('Failed to reclaim stalled task', {
          taskId,
          reason: String(error),
        });
        return { status: 'SKIPPED', taskId, reason: 'reclaim_failed' };
      } finally {
        tracing.endSpan(reclaimSpan);
      }
    }

    try {
      renewalTimer = setInterval(async () => {
        if (!claimToken) return;
        try {
          await taskRepo.renewClaim(taskId, claimToken);
          logger.debug('Lease renewed', { taskId });
        } catch (renewError) {
          logger.warn('Lease renewal failed, ownership lost', { taskId, reason: String(renewError) });
          ownershipLost = true;
          if (renewalTimer) {
            clearInterval(renewalTimer);
            renewalTimer = undefined;
          }
        }
      }, RENEWAL_INTERVAL);

      logger.info('Task execution started', { taskId });

      await new Promise((resolve) => setTimeout(resolve, 2000));

      if (ownershipLost) {
        logger.warn('Ownership lost during execution, aborting', { taskId });
        parentSpan.setAttribute('task.ownership_lost', true);
        return { status: 'SKIPPED', taskId, reason: 'ownership_lost' };
      }

      const updatedTask = await taskRepo.getTask(taskId);
      if (!updatedTask || updatedTask.status === 'CANCELLED' || updatedTask.status === 'FAILED') {
        logger.info('Task no longer processable, skipping completion', {
          taskId, status: updatedTask?.status,
        });
        return { status: 'SKIPPED', taskId, reason: updatedTask?.status?.toLowerCase() || 'not_found' };
      }

      if (ownershipLost) {
        logger.warn('Ownership lost before completion, aborting', { taskId });
        parentSpan.setAttribute('task.ownership_lost', true);
        return { status: 'SKIPPED', taskId, reason: 'ownership_lost' };
      }

      const completeSpan = tracing.startTaskComplete(taskId, WORKER_ID);
      try {
        await taskRepo.transitionStatus(taskId, taskVersion, 'COMPLETED', {
          completedAt: new Date(),
          result: { processedBy: WORKER_ID },
          claimToken,
        });
        tracing.setSpanOk(completeSpan);
      } catch (error) {
        tracing.recordError(completeSpan, error);
        throw error;
      } finally {
        tracing.endSpan(completeSpan);
      }

      workerMetrics.jobsCompleted++;
      logger.info('Task durably completed', { taskId });
      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      workerMetrics.jobsFailed++;

      const errorMessage = error instanceof Error ? error.message : String(error);
      const maxAttempts = job.opts.attempts ?? 1;
      const isFinalAttempt = job.attemptsMade + 1 >= maxAttempts;

      const failSpan = tracing.startTaskFail(taskId, WORKER_ID, isFinalAttempt);
      try {
        if (isFinalAttempt) {
          await taskRepo.transitionStatus(taskId, taskVersion, 'FAILED', {
            error: errorMessage,
            retries: currentRetries + 1,
            claimToken,
          });
        } else {
          await taskRepo.transitionStatus(taskId, taskVersion, 'QUEUED', {
            error: errorMessage,
            retries: currentRetries + 1,
            claimToken,
          });
        }
        failSpan.setAttribute('task.status', isFinalAttempt ? 'FAILED' : 'QUEUED');
      } catch (transitionError) {
        tracing.recordError(failSpan, transitionError);
        logger.error('Task failure persistence failed', {
          taskId,
          targetStatus: isFinalAttempt ? 'FAILED' : 'QUEUED',
          reason: String(transitionError),
        });
      } finally {
        tracing.endSpan(failSpan);
      }

      logger.error('Task execution failed', {
        taskId,
        reason: errorMessage,
        attempt: job.attemptsMade + 1,
        maxAttempts,
        retryable: !isFinalAttempt,
        errorType: error instanceof Error ? error.name : 'UnknownError',
      });

      throw error;
    } finally {
      if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = undefined;
      }
    }
}

const worker = new Worker(
  'tasks',
  async (job) => {
    const { taskId, taskName } = job.data;
    logger.info('Task received', { taskId, taskName, jobId: job.id, attempt: job.attemptsMade + 1 });
    workerMetrics.jobsProcessed++;

    const traceCarrier = job.data[TRACE_CONTEXT_KEY] as Record<string, string> | undefined;
    const parentCtx = extractTraceContext(traceCarrier);
    const processSpan = tracing.startTaskProcess(
      taskId,
      taskName ?? 'unknown',
      WORKER_ID,
      job.attemptsMade + 1,
      parentCtx,
    );

    if (job.data.publishedAt) {
      processSpan.setAttribute('task.queue_delay_ms',
        Date.now() - new Date(job.data.publishedAt as string).getTime(),
      );
    }

    return tracing.withActiveSpan(processSpan, async () => {
      try {
        const result = await processTask(job, taskId, processSpan);
        tracing.setSpanOk(processSpan);
        return result;
      } catch (error) {
        if (!(error instanceof DelayedError)) {
          tracing.recordError(processSpan, error);
        } else {
          processSpan.setAttribute('task.deferred', true);
        }
        throw error;
      } finally {
        tracing.endSpan(processSpan);
      }
    });
  },
  {
    connection: redisClient,
    concurrency: parseInt(process.env.QUEUE_CONCURRENCY || '5'),
    maxStalledCount: 2,
    stalledInterval: 5000,
  },
);

worker.on('completed', (job) => {
  logger.debug('BullMQ job completed', { jobId: job.id });
});

worker.on('failed', (job, err) => {
  logger.warn('BullMQ job failed', {
    jobId: job?.id,
    reason: err.message,
    attempt: job?.attemptsMade,
  });
});

worker.on('stalled', (jobId) => {
  logger.warn('BullMQ job stalled', { jobId });
});

worker.on('error', (err) => {
  logger.error('Worker error', { reason: err.message, errorType: err.name });
});

const heartbeatInterval = setInterval(() => {
  updateWorkerStatus();
}, 10000);

let isShuttingDown = false;

async function gracefulShutdown(signal: string) {
  if (isShuttingDown) {
    logger.warn('Shutdown already in progress, forcing exit', { signal });
    process.exit(1);
  }

  isShuttingDown = true;
  logger.info('Graceful shutdown initiated', { signal });

  clearInterval(heartbeatInterval);

  try {
    await redisClient.del(`worker:${WORKER_ID}`);
  } catch (error) {
    logger.warn('Failed to remove worker status', { reason: String(error) });
  }

  const shutdownTimeout = setTimeout(() => {
    logger.warn('Shutdown timeout exceeded, forcing exit');
    process.exit(1);
  }, 30000);

  try {
    await worker.close();
    logger.info('Worker closed');
  } catch (error) {
    logger.warn('Error closing worker', { reason: String(error) });
  }

  try {
    redisClient.disconnect();
    logger.info('Redis disconnected');
  } catch (error) {
    logger.warn('Error disconnecting Redis', { reason: String(error) });
  }

  try {
    await pgPool.end();
    logger.info('PostgreSQL pool closed');
  } catch (error) {
    logger.warn('Error closing PostgreSQL pool', { reason: String(error) });
  }

  await shutdownTelemetry();

  clearTimeout(shutdownTimeout);
  logger.info('Graceful shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

(async () => {
  await runMigrations(pgPool);
  logger.info('Database migrations complete');

  await updateWorkerStatus();

  logger.info('Worker initialized and listening for tasks', {
    concurrency: parseInt(process.env.QUEUE_CONCURRENCY || '5'),
  });
})();
