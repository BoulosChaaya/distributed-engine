import express, { Request, Response, NextFunction } from 'express';
import { Queue } from 'bullmq';
import { createClient } from 'redis';
import { generateId, log, AppError } from '@repo/shared';
import { Task, TaskStatus, ApiResponse } from '@repo/shared';
import { SubmitTaskSchema, PaginationSchema, ValidationError } from './validation';
import { MetricsCollector } from './metrics';

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

// Metrics collector for observability
const metrics = new MetricsCollector(taskStore, taskQueue);

// Middleware
app.use(express.json({ limit: '10mb' }));

// Request logging and metrics middleware
app.use((req: Request, res: Response, next: NextFunction) => {
  metrics.recordRequest();
  log('INFO', `${req.method} ${req.path}`, { ip: req.ip });
  next();
});

// Enhanced error handling middleware with validation support
app.use((err: unknown, req: Request, res: Response, next: NextFunction) => {
  metrics.recordError();

  if (err instanceof ValidationError) {
    return res.status(422).json({
      success: false,
      error: 'Validation error',
      details: err.toJSON(),
      timestamp: new Date(),
    });
  }

  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      error: err.message,
      timestamp: new Date(),
    });
  }

  log('ERROR', 'Unhandled error', { error: err });
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    timestamp: new Date(),
  });
});

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  res.json({
    success: true,
    data: { status: 'healthy', timestamp: new Date() },
    timestamp: new Date(),
  } as ApiResponse<{ status: string; timestamp: Date }>);
});

// Metrics endpoint - System observability
app.get('/metrics', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const metricsData = await metrics.getMetrics();
    res.json({
      success: true,
      data: metricsData,
      timestamp: new Date(),
    });
  } catch (error) {
    next(error);
  }
});

// Workers endpoint - Get all active workers and their status
app.get('/workers', async (req: Request, res: Response, next: NextFunction) => {
  try {
    // Scan Redis for all worker status keys (pattern: worker:*)
    const keys = await redisClient.keys('worker:*');
    const workers = [];

    for (const key of keys) {
      const workerData = await redisClient.get(key);
      if (workerData) {
        workers.push(JSON.parse(workerData));
      }
    }

    res.json({
      success: true,
      data: {
        total: workers.length,
        workers: workers.sort((a, b) => a.id.localeCompare(b.id)),
      },
      timestamp: new Date(),
    });
  } catch (error) {
    next(error);
  }
});

// Submit a new task - ENQUEUE to BullMQ
app.post('/tasks', async (req: Request, res: Response, next: NextFunction) => {
  try {
    // Validate input using Zod schema
    const validatedData = SubmitTaskSchema.parse(req.body);

    const taskId = generateId();
    const task: Task = {
      id: taskId,
      name: validatedData.name,
      status: 'QUEUED' as TaskStatus,
      priority: validatedData.priority,
      payload: validatedData.payload,
      retries: 0,
      maxRetries: validatedData.maxRetries,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    // Store task metadata locally
    taskStore.set(taskId, task);

    // Enqueue job to BullMQ (worker will process it)
    const job = await taskQueue.add(validatedData.name, { taskId, task }, {
      jobId: taskId,
      priority: priorityToNumber(validatedData.priority),
    });

    log('INFO', 'Task enqueued', { taskId, name: validatedData.name, jobId: job.id });

    res.status(201).json({
      success: true,
      data: task,
      timestamp: new Date(),
    } as ApiResponse<Task>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else {
      next(error);
    }
  }
});

// Get task by ID
app.get('/tasks/:id', (req: Request, res: Response, next: NextFunction) => {
  try {
    const task = taskStore.get(req.params.id);

    if (!task) {
      throw new AppError(404, `Task ${req.params.id} not found`);
    }

    res.json({
      success: true,
      data: task,
      timestamp: new Date(),
    } as ApiResponse<Task>);
  } catch (error) {
    next(error);
  }
});

// List all tasks with pagination
app.get('/tasks', (req: Request, res: Response, next: NextFunction) => {
  try {
    // Validate pagination params
    const { page, pageSize } = PaginationSchema.parse(req.query);

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
  } catch (error) {
    next(error);
  }
});

// Cancel a task
app.put('/tasks/:id/cancel', async (req: Request, res: Response, next: NextFunction) => {
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
    next(error);
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
