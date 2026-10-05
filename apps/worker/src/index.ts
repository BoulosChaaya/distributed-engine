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

// Job processor - this handles actual task execution
const worker = new Worker(
  'tasks',
  async (job) => {
    const { taskId, task } = job.data;
    log('INFO', 'Processing task', { taskId, name: task.name });

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

      log('INFO', 'Task completed', { taskId });
      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      log('ERROR', 'Task failed', { taskId, error: String(error) });
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
worker.on('completed', (job) => {
  log('INFO', 'Job completed event', { jobId: job.id, data: job.data });
});

worker.on('failed', (job, err) => {
  log('WARN', 'Job failed event', {
    jobId: job?.id,
    error: err.message,
    attempt: job?.attemptsMade,
  });
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

log('INFO', 'Worker initialized and listening for tasks');
