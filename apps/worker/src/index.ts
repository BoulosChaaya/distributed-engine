import { Worker, DelayedError } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import {
  TaskRepository,
  ClaimNotExpiredError,
  ScheduleRepository,
  runMigrations,
  initTelemetry,
  shutdownTelemetry,
  tracing,
  extractTraceContext,
  TRACE_CONTEXT_KEY,
  createLogger,
} from '@repo/shared';
import { randomUUID } from 'crypto';
import { createProcessTask, computeRenewalInterval, validateScheduleLeaseDuration } from './process-task';

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
const scheduleRepo = new ScheduleRepository(pgPool);
const LEASE_DEFERRAL_MARGIN_MS = 2000;
const SCHEDULE_LEASE_DURATION_MS = parseInt(process.env.SCHEDULE_LEASE_DURATION_MS || '300000');

validateScheduleLeaseDuration(SCHEDULE_LEASE_DURATION_MS);

const RENEWAL_INTERVAL = computeRenewalInterval(taskRepo.claimTtl, SCHEDULE_LEASE_DURATION_MS);

logger.info('Renewal interval computed', {
  taskClaimTtlMs: taskRepo.claimTtl,
  scheduleLeaseDurationMs: SCHEDULE_LEASE_DURATION_MS,
  renewalIntervalMs: RENEWAL_INTERVAL,
});

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

const processTask = createProcessTask({
  taskRepo,
  scheduleRepo,
  tracing,
  logger,
  workerId: WORKER_ID,
  scheduleLeaseDurationMs: SCHEDULE_LEASE_DURATION_MS,
  leaseDeferralMarginMs: LEASE_DEFERRAL_MARGIN_MS,
  renewalIntervalMs: RENEWAL_INTERVAL,
  workerMetrics,
  ClaimNotExpiredError,
  DelayedError,
});

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
