import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import { log, TaskRepository, runMigrations } from '@repo/shared';
import { randomUUID } from 'crypto';

const WORKER_ID = randomUUID().substring(0, 8);

const pgPool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'distributed_engine',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  max: 10,
});

pgPool.on('error', (err) => log('ERROR', 'PostgreSQL pool error', { error: err.message, workerId: WORKER_ID }));

const redisClient = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
});

redisClient.on('error', (err) => log('ERROR', 'Redis error', { error: err.message, workerId: WORKER_ID }));
redisClient.on('connect', () => log('INFO', 'Redis connected', { workerId: WORKER_ID }));

const taskRepo = new TaskRepository(pgPool);

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
    log('WARN', 'Failed to update worker status', { error: String(error), workerId: WORKER_ID });
  }
}

const worker = new Worker(
  'tasks',
  async (job) => {
    const { taskId, taskName } = job.data;
    log('INFO', 'Processing task', { taskId, name: taskName, jobId: job.id, attempt: job.attemptsMade + 1, workerId: WORKER_ID });
    workerMetrics.jobsProcessed++;

    const currentTask = await taskRepo.getTask(taskId);
    if (!currentTask) {
      log('WARN', 'Task not found in database, skipping', { taskId, workerId: WORKER_ID });
      return { status: 'SKIPPED', taskId, reason: 'not_found' };
    }

    if (currentTask.status === 'CANCELLED') {
      log('INFO', 'Task was cancelled, skipping', { taskId, workerId: WORKER_ID });
      return { status: 'SKIPPED', taskId, reason: 'cancelled' };
    }

    if (currentTask.status === 'COMPLETED') {
      log('INFO', 'Task already completed, skipping', { taskId, workerId: WORKER_ID });
      return { status: 'SKIPPED', taskId, reason: 'already_completed' };
    }

    if (currentTask.status === 'FAILED') {
      log('INFO', 'Task already failed, skipping', { taskId, workerId: WORKER_ID });
      return { status: 'SKIPPED', taskId, reason: 'already_failed' };
    }

    let taskVersion = currentTask.version;
    let claimToken: string | undefined;
    let currentRetries = currentTask.retries;

    if (currentTask.status === 'QUEUED') {
      try {
        const updated = await taskRepo.transitionStatus(taskId, taskVersion, 'PROCESSING', {
          startedAt: new Date(),
          claimedBy: WORKER_ID,
        });
        taskVersion = updated.version;
        claimToken = updated.claimToken;
        currentRetries = updated.retries;
      } catch (error) {
        log('WARN', 'Failed to transition task to PROCESSING', {
          taskId, error: String(error), workerId: WORKER_ID,
        });
        return { status: 'SKIPPED', taskId, reason: 'transition_failed' };
      }
    } else if (currentTask.status === 'PROCESSING') {
      try {
        const reclaimed = await taskRepo.reclaimStalledTask(taskId, taskVersion, WORKER_ID);
        taskVersion = reclaimed.version;
        claimToken = reclaimed.claimToken;
        currentRetries = reclaimed.retries;
        log('INFO', 'Reclaimed stalled task', { taskId, workerId: WORKER_ID });
      } catch (error) {
        log('WARN', 'Failed to reclaim stalled task', {
          taskId, error: String(error), workerId: WORKER_ID,
        });
        return { status: 'SKIPPED', taskId, reason: 'reclaim_failed' };
      }
    }

    try {
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const updatedTask = await taskRepo.getTask(taskId);
      if (!updatedTask || updatedTask.status === 'CANCELLED' || updatedTask.status === 'FAILED') {
        log('INFO', 'Task no longer processable, skipping completion', {
          taskId, status: updatedTask?.status, workerId: WORKER_ID,
        });
        return { status: 'SKIPPED', taskId, reason: updatedTask?.status?.toLowerCase() || 'not_found' };
      }

      await taskRepo.transitionStatus(taskId, taskVersion, 'COMPLETED', {
        completedAt: new Date(),
        result: { processedBy: WORKER_ID },
        claimToken,
      });

      workerMetrics.jobsCompleted++;
      log('INFO', 'Task completed', { taskId, workerId: WORKER_ID });
      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      workerMetrics.jobsFailed++;

      const errorMessage = error instanceof Error ? error.message : String(error);
      const maxAttempts = job.opts.attempts ?? 1;
      const isFinalAttempt = job.attemptsMade + 1 >= maxAttempts;

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
      } catch (transitionError) {
        log('ERROR', 'Failed to transition task after failure', {
          taskId, targetStatus: isFinalAttempt ? 'FAILED' : 'QUEUED',
          error: String(transitionError), workerId: WORKER_ID,
        });
      }

      log('ERROR', 'Task failed', {
        taskId,
        error: errorMessage,
        attempt: job.attemptsMade + 1,
        maxAttempts,
        isFinalAttempt,
        workerId: WORKER_ID,
      });

      throw error;
    }
  },
  {
    connection: redisClient,
    concurrency: parseInt(process.env.QUEUE_CONCURRENCY || '5'),
    maxStalledCount: 2,
    stalledInterval: 5000,
  },
);

worker.on('active', (job) => {
  log('INFO', 'Job active', { jobId: job.id, workerId: WORKER_ID });
});

worker.on('completed', (job) => {
  log('INFO', 'Job completed', { jobId: job.id, workerId: WORKER_ID });
});

worker.on('failed', (job, err) => {
  log('WARN', 'Job failed', {
    jobId: job?.id,
    error: err.message,
    attempt: job?.attemptsMade,
    workerId: WORKER_ID,
  });
});

worker.on('stalled', (jobId) => {
  log('WARN', 'Job stalled', { jobId, workerId: WORKER_ID });
});

worker.on('error', (err) => {
  log('ERROR', 'Worker error', { error: err.message, workerId: WORKER_ID });
});

const heartbeatInterval = setInterval(() => {
  updateWorkerStatus();
}, 10000);

let isShuttingDown = false;

async function gracefulShutdown(signal: string) {
  if (isShuttingDown) {
    log('WARN', 'Shutdown already in progress', { signal, workerId: WORKER_ID });
    process.exit(1);
  }

  isShuttingDown = true;
  log('INFO', 'Graceful shutdown initiated', { signal, workerId: WORKER_ID });

  clearInterval(heartbeatInterval);

  try {
    await redisClient.del(`worker:${WORKER_ID}`);
  } catch (error) {
    log('WARN', 'Failed to remove worker status', { error: String(error), workerId: WORKER_ID });
  }

  const shutdownTimeout = setTimeout(() => {
    log('WARN', 'Shutdown timeout exceeded, forcing exit', { workerId: WORKER_ID });
    process.exit(1);
  }, 30000);

  try {
    await worker.close();
    log('INFO', 'Worker closed', { workerId: WORKER_ID });
  } catch (error) {
    log('WARN', 'Error closing worker', { error: String(error), workerId: WORKER_ID });
  }

  try {
    redisClient.disconnect();
    log('INFO', 'Redis disconnected', { workerId: WORKER_ID });
  } catch (error) {
    log('WARN', 'Error disconnecting Redis', { error: String(error), workerId: WORKER_ID });
  }

  try {
    await pgPool.end();
    log('INFO', 'PostgreSQL pool closed', { workerId: WORKER_ID });
  } catch (error) {
    log('WARN', 'Error closing PostgreSQL pool', { error: String(error), workerId: WORKER_ID });
  }

  clearTimeout(shutdownTimeout);
  log('INFO', 'Graceful shutdown complete', { workerId: WORKER_ID });
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

(async () => {
  await runMigrations(pgPool);
  log('INFO', 'Database migrations complete', { workerId: WORKER_ID });

  await updateWorkerStatus();

  log('INFO', 'Worker initialized and listening for tasks', {
    workerId: WORKER_ID,
    concurrency: process.env.QUEUE_CONCURRENCY || '5',
    redisHost: process.env.REDIS_HOST || 'localhost',
    pgHost: process.env.POSTGRES_HOST || 'localhost',
  });
})();
