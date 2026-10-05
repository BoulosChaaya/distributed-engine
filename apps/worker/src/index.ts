import { Worker, Queue } from 'bullmq';
import { log, retryWithBackoff, Task } from '@repo/shared';
import { createClient } from 'redis';

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

// Job processor - handles task execution and status updates
const worker = new Worker(
  'tasks',
  async (job) => {
    const { taskId, task } = job.data;
    log('INFO', 'Processing task', { taskId, name: task.name });

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
      log('INFO', 'Task completed', { taskId });

      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      // Update task status to FAILED
      task.status = 'FAILED';
      task.error = error instanceof Error ? error.message : String(error);
      task.retries++;
      task.updatedAt = new Date();
      taskStore.set(taskId, task);

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
  log('INFO', 'Job active', { jobId: job.id });
});

worker.on('completed', (job) => {
  log('INFO', 'Job completed event', { jobId: job.id });
});

worker.on('failed', (job, err) => {
  log('WARN', 'Job failed event', {
    jobId: job?.id,
    error: err.message,
    attempt: job?.attemptsMade,
  });
});

worker.on('stalled', (jobId) => {
  log('WARN', 'Job stalled', { jobId });
});

worker.on('error', (err) => {
  log('ERROR', 'Worker error', { error: err.message });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  log('INFO', 'Shutting down worker gracefully');
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

  log('INFO', 'Worker initialized and listening for tasks', {
    concurrency: 5,
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || '6379',
  });
})();
