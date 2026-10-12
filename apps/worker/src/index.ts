import { Worker, DelayedError } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import http from 'http';
import {
  TaskRepository,
  ClaimNotExpiredError,
  ScheduleRepository,
  TenantRepository,
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
import { parseStrictIntInRange } from './config';

initTelemetry({
  serviceName: 'distributed-engine-worker',
});

const WORKER_ID = randomUUID().substring(0, 8);

const logger = createLogger({
  service: 'worker',
  environment: process.env.NODE_ENV ?? 'development',
  level: process.env.LOG_LEVEL,
}).child({ workerId: WORKER_ID });

const POSTGRES_PORT = parseStrictIntInRange(process.env.POSTGRES_PORT, 5432, 'POSTGRES_PORT', 1, 65535);
const REDIS_PORT = parseStrictIntInRange(process.env.REDIS_PORT, 6379, 'REDIS_PORT', 1, 65535);
const SCHEDULE_LEASE_DURATION_MS = parseStrictIntInRange(
  process.env.SCHEDULE_LEASE_DURATION_MS,
  300000,
  'SCHEDULE_LEASE_DURATION_MS',
  1000,
  86400000,
);
const QUEUE_CONCURRENCY = parseStrictIntInRange(process.env.QUEUE_CONCURRENCY, 5, 'QUEUE_CONCURRENCY', 1, 100);

let pgPool: Pool | null = null;
let redisClient: IORedis | null = null;
let worker: Worker | undefined;
let healthServer: http.Server | undefined;
let heartbeatInterval: NodeJS.Timeout | undefined;
let isShuttingDown = false;

async function gracefulShutdown(signal: string) {
  if (isShuttingDown) {
    logger.warn('Shutdown already in progress, forcing exit', { signal });
    process.exit(1);
  }

  isShuttingDown = true;
  logger.info('Graceful shutdown initiated', { signal });

  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
  }

  if (redisClient) {
    try {
      await redisClient.del(`worker:${WORKER_ID}`);
    } catch (error) {
      logger.warn('Failed to remove worker status', { reason: String(error) });
    }
  }

  const shutdownTimeout = setTimeout(() => {
    logger.warn('Shutdown timeout exceeded, forcing exit');
    process.exit(1);
  }, 30000);

  if (healthServer) {
    healthServer.close();
  }

  try {
    if (worker) {
      await worker.close();
      logger.info('Worker closed');
    }
  } catch (error) {
    logger.warn('Error closing worker', { reason: String(error) });
  }

  if (redisClient) {
    try {
      redisClient.disconnect();
      logger.info('Redis disconnected');
    } catch (error) {
      logger.warn('Error disconnecting Redis', { reason: String(error) });
    }
  }

  if (pgPool) {
    try {
      await pgPool.end();
      logger.info('PostgreSQL pool closed');
    } catch (error) {
      logger.warn('Error closing PostgreSQL pool', { reason: String(error) });
    }
  }

  await shutdownTelemetry();

  clearTimeout(shutdownTimeout);
  logger.info('Graceful shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

(async () => {
  pgPool = new Pool({
    host: process.env.POSTGRES_HOST || 'localhost',
    port: POSTGRES_PORT,
    database: process.env.POSTGRES_DB || 'distributed_engine',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD,
    max: 10,
  });

  pgPool.on('error', (err) => logger.error('PostgreSQL pool error', { reason: err.message }));

  redisClient = new IORedis({
    host: process.env.REDIS_HOST || 'localhost',
    port: REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: null,
  });

  redisClient.on('error', (err) => logger.error('Redis connection error', { reason: err.message }));
  redisClient.on('connect', () => logger.info('Redis connected'));

  const taskRepo = new TaskRepository(pgPool);
  const scheduleRepo = new ScheduleRepository(pgPool);
  const tenantRepo = new TenantRepository(pgPool);
  const LEASE_DEFERRAL_MARGIN_MS = 2000;

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

  const localRedisClient = redisClient;

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

      await localRedisClient.setex(
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
    tenantRepo,
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

  await runMigrations(pgPool);
  logger.info('Database migrations complete');

  healthServer = http.createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'alive' }));
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  await new Promise<void>((resolve) => {
    healthServer!.listen(9000, () => {
      logger.info('Health server listening', { port: 9000 });
      resolve();
    });
  });

  worker = new Worker(
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
      connection: localRedisClient,
      concurrency: QUEUE_CONCURRENCY,
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

  heartbeatInterval = setInterval(() => {
    updateWorkerStatus();
  }, 10000);

  await updateWorkerStatus();

  logger.info('Worker initialized and listening for tasks', {
    concurrency: QUEUE_CONCURRENCY,
  });
})().catch(async (err) => {
  logger.error('Fatal initialization error', { reason: String(err), stack: err?.stack });

  const fatalCleanupTimeout = setTimeout(() => {
    logger.error('Fatal cleanup timeout exceeded, forcing exit');
    process.exit(1);
  }, 10000);

  if (redisClient) {
    try {
      redisClient.disconnect();
    } catch (cleanupErr) {
      logger.warn('Fatal cleanup: error disconnecting Redis', { reason: String(cleanupErr) });
    }
  }

  if (pgPool) {
    try {
      await pgPool.end();
    } catch (cleanupErr) {
      logger.warn('Fatal cleanup: error closing PostgreSQL pool', { reason: String(cleanupErr) });
    }
  }

  await shutdownTelemetry();

  clearTimeout(fatalCleanupTimeout);
  process.exit(1);
});
