import express, { Request, Response, NextFunction } from 'express';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import {
  log,
  AppError,
  TaskRepository,
  OutboxPublisher,
  runMigrations,
  InvalidTransitionError,
  StaleVersionError,
} from '@repo/shared';
import { Task, ApiResponse } from '@repo/shared';
import { SubmitTaskSchema, PaginationSchema, ValidationError } from './validation';
import { MetricsCollector } from './metrics';
import { CircuitBreaker } from './circuitbreaker';
import { ShutdownManager } from './shutdown';
import { config } from './config';

const app = express();

const pgPool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  database: config.postgres.database,
  user: config.postgres.user,
  password: config.postgres.password,
  max: config.postgres.maxConnections,
});

pgPool.on('error', (err) => log('ERROR', 'PostgreSQL pool error', { error: err.message }));

const redisClient = new IORedis({
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
  maxRetriesPerRequest: null,
  lazyConnect: true,
});

redisClient.on('error', (err) => log('ERROR', 'Redis error', { error: err.message }));
redisClient.on('connect', () => log('INFO', 'Redis connected'));

const taskQueue = new Queue('tasks', {
  connection: redisClient,
  defaultJobOptions: {
    attempts: config.queue.maxAttempts,
    backoff: {
      type: 'exponential',
      delay: config.queue.backoffDelayMs,
    },
    removeOnComplete: 100,
    removeOnFail: false,
  },
});

const taskRepo = new TaskRepository(pgPool);

const outboxPublisher = new OutboxPublisher(
  pgPool,
  taskQueue,
  config.outbox.pollIntervalMs,
  config.outbox.batchSize,
  config.outbox.maxAttempts,
);

const metrics = new MetricsCollector(taskRepo, taskQueue, outboxPublisher);

const queueCircuitBreaker = new CircuitBreaker(
  config.circuitBreaker.failureThreshold,
  config.circuitBreaker.successThreshold,
  config.circuitBreaker.resetTimeoutMs,
);

const shutdownManager = new ShutdownManager(config.gracefulShutdown.timeoutMs);

app.use(express.json({ limit: '10mb' }));

app.use((req: Request, res: Response, next: NextFunction) => {
  try {
    shutdownManager.incrementRequests();
  } catch {
    return res.status(503).json({
      success: false,
      error: 'Server is shutting down',
      timestamp: new Date(),
    });
  }

  metrics.recordRequest();
  log('INFO', `${req.method} ${req.path}`, { ip: req.ip });

  res.on('finish', () => {
    shutdownManager.decrementRequests();
  });

  next();
});

app.get('/health', async (req: Request, res: Response) => {
  const circuitState = queueCircuitBreaker.getState();

  let pgHealthy = false;
  try {
    await pgPool.query('SELECT 1');
    pgHealthy = true;
  } catch {
    // PG unavailable
  }

  let redisHealthy = false;
  try {
    await redisClient.ping();
    redisHealthy = true;
  } catch {
    // Redis unavailable
  }

  const isReady = pgHealthy && redisHealthy && circuitState !== 'OPEN';
  const isLive = pgHealthy;

  const data = {
    status: isReady ? 'healthy' : (isLive ? 'degraded' : 'unhealthy'),
    timestamp: new Date(),
    circuitBreaker: circuitState,
    dependencies: {
      postgres: pgHealthy ? 'up' : 'down',
      redis: redisHealthy ? 'up' : 'unknown',
    },
  };

  res.status(isReady ? 200 : 503).json({ success: isReady, data, timestamp: new Date() });
});

app.get('/ready', async (req: Request, res: Response) => {
  let pgHealthy = false;
  try {
    await pgPool.query('SELECT 1');
    pgHealthy = true;
  } catch {
    // PG unavailable
  }

  let redisHealthy = false;
  try {
    await redisClient.ping();
    redisHealthy = true;
  } catch {
    // Redis unavailable
  }

  const ready = pgHealthy && redisHealthy;
  res.status(ready ? 200 : 503).json({
    success: ready,
    data: { postgres: pgHealthy ? 'up' : 'down', redis: redisHealthy ? 'up' : 'unknown' },
  });
});

app.get('/live', (_req: Request, res: Response) => {
  res.json({ success: true, data: { status: 'alive' } });
});

app.get('/metrics', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const metricsData = await metrics.getMetrics();
    res.json({ success: true, data: metricsData, timestamp: new Date() });
  } catch (error) {
    next(error);
  }
});

app.get('/workers', async (req: Request, res: Response, next: NextFunction) => {
  try {
    let workers: unknown[] = [];
    try {
      const keys = await redisClient.keys('worker:*');
      for (const key of keys) {
        const workerData = await redisClient.get(key);
        if (workerData) {
          workers.push(JSON.parse(workerData));
        }
      }
    } catch {
      log('WARN', 'Unable to retrieve worker status from Redis');
    }

    res.json({
      success: true,
      data: {
        total: workers.length,
        workers: (workers as Array<{ id: string }>).sort((a, b) => a.id.localeCompare(b.id)),
      },
      timestamp: new Date(),
    });
  } catch (error) {
    next(error);
  }
});

app.post('/tasks', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validatedData = SubmitTaskSchema.parse(req.body);

    const { task } = await taskRepo.createTaskWithOutbox({
      name: validatedData.name,
      priority: validatedData.priority,
      payload: validatedData.payload,
      maxRetries: validatedData.maxRetries,
    });

    log('INFO', 'Task created', { taskId: task.id, name: task.name });

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

app.get('/tasks/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const task = await taskRepo.getTask(req.params.id);
    if (!task) {
      throw new AppError(404, `Task ${req.params.id} not found`);
    }
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    next(error);
  }
});

app.get('/tasks', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { page, pageSize } = PaginationSchema.parse(req.query);
    const { items, total } = await taskRepo.listTasks(page, pageSize);

    res.json({
      success: true,
      data: {
        items,
        total,
        page,
        pageSize,
        hasMore: (page - 1) * pageSize + items.length < total,
      },
      timestamp: new Date(),
    });
  } catch (error) {
    next(error);
  }
});

app.put('/tasks/:id/cancel', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const task = await taskRepo.cancelTask(req.params.id);

    try {
      const job = await taskQueue.getJob(req.params.id);
      if (job) {
        await job.remove();
      }
    } catch (queueError) {
      log('WARN', 'Failed to remove cancelled job from queue', { taskId: req.params.id });
    }

    log('INFO', 'Task cancelled', { taskId: task.id });
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    if (error instanceof InvalidTransitionError) {
      next(new AppError(400, error.message));
    } else if (error instanceof Error && error.message.includes('not found')) {
      next(new AppError(404, error.message));
    } else {
      next(error);
    }
  }
});

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

async function start() {
  try {
    await redisClient.connect();
    log('INFO', 'Redis connection established');
  } catch (error) {
    log('WARN', 'Redis not available at startup, outbox will retry', { error: String(error) });
  }

  await runMigrations(pgPool);
  log('INFO', 'Database migrations complete');

  outboxPublisher.start();
  log('INFO', 'Outbox publisher started');

  const server = app.listen(config.port, () => {
    log('INFO', `API server running on port ${config.port}`);
  });

  shutdownManager.registerHandlers(server, {
    taskQueue,
    redisClient,
    pgPool,
    outboxPublisher,
  });
}

start().catch((error) => {
  log('ERROR', 'Failed to start API server', { error: String(error) });
  process.exit(1);
});

export { app, pgPool, taskRepo, outboxPublisher, queueCircuitBreaker, shutdownManager };
