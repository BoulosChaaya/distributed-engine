import { Worker, Queue } from 'bullmq';
import { log, retryWithBackoff, Task } from '@repo/shared';
import { createClient } from 'redis';
import { randomUUID } from 'crypto';

// Redis connection
const redisClient = createClient({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

redisClient.on('error', (err) => log('ERROR', 'Redis error', err));
redisClient.on('connect', () => log('INFO', 'Redis connected'));

// Initialize queue
const taskQueue = new Queue('tasks', {
  connection: redisClient,
  defaultJobOptions: {
    removeOnComplete: true,
    removeOnFail: false,
  },
});

// Local task state cache (mirrors API's task store)
const taskStore = new Map<string, Task>();

// Worker identity and metrics
const WORKER_ID = randomUUID().substring(0, 8);
const workerMetrics = {
  id: WORKER_ID,
  startTime: Date.now(),
  jobsProcessed: 0,
  jobsFailed: 0,
  jobsCompleted: 0,
};

// Helper: Update worker status in Redis for API visibility
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

    // Store worker status in Redis with 30s TTL (heartbeat)
    await redisClient.setEx(
      `worker:${workerMetrics.id}`,
      30,
      JSON.stringify(status)
    );
  } catch (error) {
    log('WARN', 'Failed to update worker status', { error: String(error) });
  }
}

// Job processor - handles task execution and status updates
const worker = new Worker(
  'tasks',
  async (job) => {
    const { taskId, task } = job.data;
    log('INFO', 'Processing task', { taskId, name: task.name });
    workerMetrics.jobsProcessed++;

    // Update task status to PROCESSING
    task.status = 'PROCESSING';
    task.updatedAt = new Date();
    taskStore.set(taskId, task);

    try {
      // Simulate async work with retry logic
      await retryWithBackoff(async () => {
        // Simulate job execution (replace with real work)
        await new Promise((resolve) => setTimeout(resolve, 2000));

        // Simulate occasional failures for testing
        if (Math.random() < 0.1) {
          throw new Error('Simulated processing failure');
        }

        return task;
      });

      // Update task status to COMPLETED
      task.status = 'COMPLETED';
      task.completedAt = new Date();
      task.updatedAt = new Date();
      taskStore.set(taskId, task);
      workerMetrics.jobsCompleted++;
      log('INFO', 'Task completed', { taskId });

      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      // Update task status to FAILED
      task.status = 'FAILED';
      task.error = error instanceof Error ? error.message : String(error);
      task.retries++;
      task.updatedAt = new Date();
      taskStore.set(taskId, task);
      workerMetrics.jobsFailed++;

      log('ERROR', 'Task failed', {
        taskId,
        error: String(error),
        attempt: job.attemptsMade,
      });
      throw error;
    }
  },
  {
    connection: redisClient,
    concurrency: 5, // Process 5 jobs in parallel
    maxStalledCount: 2, // Restart job if stalled twice
    stalledInterval: 5000, // Check for stalled jobs every 5s
  }
);

// Event handlers for job lifecycle
worker.on('active', (job) => {
  log('INFO', 'Job active', { jobId: job.id, workerId: WORKER_ID });
});

worker.on('completed', (job) => {
  log('INFO', 'Job completed event', { jobId: job.id, workerId: WORKER_ID });
});

worker.on('failed', (job, err) => {
  log('WARN', 'Job failed event', {
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

// Periodic heartbeat - update worker status in Redis every 10s
const heartbeatInterval = setInterval(() => {
  updateWorkerStatus();
}, 10000);

// Graceful shutdown
process.on('SIGINT', async () => {
  log('INFO', 'Shutting down worker gracefully', { workerId: WORKER_ID });
  clearInterval(heartbeatInterval);
  await redisClient.del(`worker:${WORKER_ID}`); // Remove worker status
  await worker.close();
  await redisClient.quit();
  process.exit(0);
});

// Startup message
(async () => {
  // Wait for Redis connection
  await new Promise((resolve) => {
    const checkConnection = setInterval(() => {
      if (redisClient.isOpen) {
        clearInterval(checkConnection);
        resolve(undefined);
      }
    }, 100);
  });

  // Publish initial heartbeat
  await updateWorkerStatus();

  log('INFO', 'Worker initialized and listening for tasks', {
    workerId: WORKER_ID,
    concurrency: 5,
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || '6379',
  });
})();
