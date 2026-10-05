import express, { Request, Response, NextFunction } from 'express';
import { Queue } from 'bullmq';
import { createClient } from 'redis';
import { generateId, log, AppError } from '@repo/shared';
import { Task, TaskStatus, ApiResponse } from '@repo/shared';

const app = express();
const PORT = process.env.PORT || 3000;

// Redis connection for both job queue and state storage
const redisClient = createClient({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

redisClient.on('error', (err) => log('ERROR', 'Redis error', err));
redisClient.on('connect', () => log('INFO', 'Redis connected'));

// Initialize task queue
const taskQueue = new Queue('tasks', {
  connection: redisClient,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
    removeOnComplete: true,
  },
});

// Task state store (Redis-backed, but can be replaced with PostgreSQL)
const taskStore = new Map<string, Task>();

// Middleware
app.use(express.json());

// Request logging middleware
app.use((req: Request, res: Response, next: NextFunction) => {
  log('INFO', `${req.method} ${req.path}`);
  next();
});

// Error handling middleware
app.use((err: unknown, req: Request, res: Response, next: NextFunction) => {
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      error: err.message,
      timestamp: new Date(),
    });
  } else {
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      timestamp: new Date(),
    });
  }
});

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    success: true,
    data: { status: 'healthy', timestamp: new Date() },
    timestamp: new Date(),
  } as ApiResponse<{ status: string; timestamp: Date }>);
});

// Submit a new task - ENQUEUE to BullMQ
app.post('/tasks', async (req: Request, res: Response) => {
  try {
    const { name, payload, priority = 'NORMAL', maxRetries = 3 } = req.body;

    if (!name) {
      throw new AppError(400, 'Task name is required');
    }

    const taskId = generateId();
    const task: Task = {
      id: taskId,
      name,
      status: 'QUEUED' as TaskStatus,
      priority: priority as any,
      payload: payload || {},
      retries: 0,
      maxRetries,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    // Store task metadata locally
    taskStore.set(taskId, task);

    // Enqueue job to BullMQ (worker will process it)
    const job = await taskQueue.add(name, { taskId, task }, {
      jobId: taskId,
      priority: priorityToNumber(priority),
    });

    log('INFO', 'Task enqueued', { taskId, name, jobId: job.id });

    res.status(201).json({
      success: true,
      data: task,
      timestamp: new Date(),
    } as ApiResponse<Task>);
  } catch (error) {
    next(error);
  }
});

// Get task by ID
app.get('/tasks/:id', (req: Request, res: Response) => {
  const task = taskStore.get(req.params.id);

  if (!task) {
    throw new AppError(404, `Task ${req.params.id} not found`);
  }

  res.json({
    success: true,
    data: task,
    timestamp: new Date(),
  } as ApiResponse<Task>);
});

// List all tasks with pagination
app.get('/tasks', (req: Request, res: Response) => {
  const page = parseInt(req.query.page as string) || 1;
  const pageSize = parseInt(req.query.pageSize as string) || 10;
  const allTasks = Array.from(taskStore.values());
  const total = allTasks.length;
  const start = (page - 1) * pageSize;
  const items = allTasks.slice(start, start + pageSize);

  res.json({
    success: true,
    data: {
      items,
      total,
      page,
      pageSize,
      hasMore: start + pageSize < total,
    },
    timestamp: new Date(),
  });
});

// Cancel a task
app.put('/tasks/:id/cancel', async (req: Request, res: Response) => {
  try {
    const task = taskStore.get(req.params.id);

    if (!task) {
      throw new AppError(404, `Task ${req.params.id} not found`);
    }

    if (task.status === 'COMPLETED' || task.status === 'FAILED') {
      throw new AppError(400, `Cannot cancel task in ${task.status} state`);
    }

    // Try to cancel the job in BullMQ
    const job = await taskQueue.getJob(req.params.id);
    if (job) {
      await job.remove();
    }

    task.status = 'CANCELLED' as TaskStatus;
    task.updatedAt = new Date();
    log('INFO', 'Task cancelled', { taskId: task.id });

    res.json({
      success: true,
      data: task,
      timestamp: new Date(),
    } as ApiResponse<Task>);
  } catch (error) {
    if (!(error instanceof AppError)) {
      throw new AppError(500, 'Failed to cancel task');
    }
    throw error;
  }
});

// Helper: Convert priority string to BullMQ priority number
function priorityToNumber(priority: string): number {
  const priorities: Record<string, number> = {
    LOW: 10,
    NORMAL: 5,
    HIGH: 1,
    CRITICAL: 0,
  };
  return priorities[priority] || 5;
}

// Start server
app.listen(PORT, () => {
  log('INFO', `API server running on port ${PORT}`);
});
