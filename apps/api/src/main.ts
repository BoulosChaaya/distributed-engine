import express, { Request, Response, NextFunction } from 'express';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { Pool } from 'pg';
import {
  AppError,
  TaskRepository,
  OutboxPublisher,
  ScheduleRepository,
  ScheduleStaleVersionError,
  TenantRepository,
  IdempotencyConflictError,
  QuotaExceededError,
  computeBillingPeriodStart,
  SchedulerService,
  RedisRateLimiter,
  runMigrations,
  InvalidTransitionError,
  initTelemetry,
  tracing,
  createLogger,
  getNextOccurrence,
  WeightedFairScheduler,
} from '@repo/shared';
import { Task, RecurringSchedule, ApiResponse } from '@repo/shared';
import {
  SubmitTaskSchema,
  PaginationSchema,
  CreateScheduleSchema,
  UpdateScheduleSchema,
  SetScheduleStatusSchema,
  TenantSubmitTaskSchema,
  TenantCreateScheduleSchema,
  ValidationError,
} from './validation';
import { createTenantAuth, createRateLimitMiddleware } from './tenant-middleware';
import { MetricsCollector } from './metrics';
import { ShutdownManager } from './shutdown';
import { config } from './config';

initTelemetry({
  serviceName: 'distributed-engine-api',
});

const logger = createLogger({
  service: 'api',
  environment: config.nodeEnv,
  level: config.logging.level,
});

const app = express();

const pgPool = new Pool({
  host: config.postgres.host,
  port: config.postgres.port,
  database: config.postgres.database,
  user: config.postgres.user,
  password: config.postgres.password,
  max: config.postgres.maxConnections,
});

pgPool.on('error', (err) => logger.error('PostgreSQL pool error', { errorType: err.name, reason: err.message }));

const redisClient = new IORedis({
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
  maxRetriesPerRequest: null,
  lazyConnect: true,
});

redisClient.on('error', (err) => logger.error('Redis connection error', { errorType: err.name, reason: err.message }));
redisClient.on('connect', () => logger.info('Redis connected'));

const taskQueue = new Queue('tasks', {
  connection: redisClient,
  defaultJobOptions: {
    removeOnComplete: 100,
    removeOnFail: false,
  },
});

const taskRepo = new TaskRepository(pgPool);
const scheduleRepo = new ScheduleRepository(pgPool);
const tenantRepo = new TenantRepository(pgPool);
const rateLimiter = new RedisRateLimiter(redisClient);

const tenantAuth = createTenantAuth(tenantRepo);
const rateLimitMiddleware = createRateLimitMiddleware(rateLimiter, tenantRepo);

const outboxPublisher = new OutboxPublisher(
  pgPool,
  taskQueue,
  config.outbox.pollIntervalMs,
  config.outbox.batchSize,
  config.outbox.maxAttempts,
  { fairScheduler: new WeightedFairScheduler() },
);

const schedulerService = new SchedulerService(pgPool, logger.child({ component: 'scheduler' }), {
  pollIntervalMs: config.scheduler?.pollIntervalMs ?? 5000,
  scheduledTaskBatchSize: config.scheduler?.batchSize ?? 50,
  recurringBatchSize: config.scheduler?.batchSize ?? 20,
  catchUpBatchSize: config.scheduler?.catchUpBatchSize ?? 10,
  executionLeaseDurationMs: config.scheduler?.executionLeaseDurationMs ?? 300000,
});

const metrics = new MetricsCollector(taskRepo, taskQueue, outboxPublisher);

const shutdownManager = new ShutdownManager(config.gracefulShutdown.timeoutMs);

app.use(express.json({ limit: '10mb' }));

app.use((_req: Request, res: Response, next: NextFunction) => {
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

  res.on('finish', () => {
    shutdownManager.decrementRequests();
  });

  next();
});

app.get('/health', async (_req: Request, res: Response) => {
  const outboxCircuitState = outboxPublisher.getCircuitState();

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

  const isReady = pgHealthy;
  const isDegraded = !redisHealthy || outboxCircuitState !== 'CLOSED';

  const data = {
    status: isReady ? (isDegraded ? 'degraded' : 'healthy') : 'unhealthy',
    timestamp: new Date(),
    outboxCircuitBreaker: outboxCircuitState,
    dependencies: {
      postgres: pgHealthy ? 'up' : 'down',
      redis: redisHealthy ? 'up' : 'down',
    },
  };

  res.status(isReady ? 200 : 503).json({ success: isReady, data, timestamp: new Date() });
});

app.get('/ready', async (_req: Request, res: Response) => {
  let pgHealthy = false;
  try {
    await pgPool.query('SELECT 1');
    pgHealthy = true;
  } catch {
    // PG unavailable
  }

  res.status(pgHealthy ? 200 : 503).json({
    success: pgHealthy,
    data: { postgres: pgHealthy ? 'up' : 'down' },
  });
});

app.get('/live', (_req: Request, res: Response) => {
  res.json({ success: true, data: { status: 'alive' } });
});

app.get('/metrics', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const metricsData = await metrics.getMetrics();
    res.json({ success: true, data: metricsData, timestamp: new Date() });
  } catch (error) {
    next(error);
  }
});

app.get('/workers', async (_req: Request, res: Response, next: NextFunction) => {
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
      logger.warn('Unable to retrieve worker status from Redis');
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

    if (validatedData.scheduledFor) {
      const scheduledFor = new Date(validatedData.scheduledFor);
      logger.info('Scheduled task creation requested', {
        taskName: validatedData.name,
        priority: validatedData.priority,
        scheduledFor: scheduledFor.toISOString(),
      });

      const { task } = await scheduleRepo.createScheduledTask({
        name: validatedData.name,
        priority: validatedData.priority,
        payload: validatedData.payload,
        maxRetries: validatedData.maxRetries,
        scheduledFor,
      });

      logger.info('Scheduled task created', {
        taskId: task.id,
        taskName: task.name,
        scheduledFor: scheduledFor.toISOString(),
      });

      res.status(201).json({
        success: true,
        data: task,
        timestamp: new Date(),
      } as ApiResponse<Task>);
      return;
    }

    logger.info('Task creation requested', { taskName: validatedData.name, priority: validatedData.priority });

    const span = tracing.startTaskCreation('pending', validatedData.name, validatedData.priority);

    let task: Task;
    try {
      const result = await tracing.withActiveSpan(span, () =>
        taskRepo.createTaskWithOutbox({
          name: validatedData.name,
          priority: validatedData.priority,
          payload: validatedData.payload,
          maxRetries: validatedData.maxRetries,
        }),
      );
      task = result.task;
      span.setAttribute('task.id', task.id);
      tracing.setSpanOk(span);
    } catch (error) {
      tracing.recordError(span, error);
      throw error;
    } finally {
      tracing.endSpan(span);
    }

    logger.info('Task created', { taskId: task.id, taskName: task.name, priority: task.priority });

    res.status(201).json({
      success: true,
      data: task,
      timestamp: new Date(),
    } as ApiResponse<Task>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      logger.warn('Task creation rejected: validation error', { errorType: 'ValidationError' });
      next(new ValidationError(error as any));
    } else {
      logger.error('Task creation failed', {
        errorType: error instanceof Error ? error.name : 'UnknownError',
        reason: error instanceof Error ? error.message : String(error),
      });
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
      logger.warn('Failed to remove cancelled job from queue', { taskId: req.params.id });
    }

    logger.info('Task cancelled', { taskId: task.id });
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

// --- Recurring Schedules ---

app.post('/schedules', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validatedData = CreateScheduleSchema.parse(req.body);

    const nextRunAt = getNextOccurrence(
      validatedData.cronExpression,
      validatedData.timezone,
      new Date(),
    );

    const schedule = await scheduleRepo.createSchedule({
      name: validatedData.name,
      taskName: validatedData.taskName,
      taskPriority: validatedData.taskPriority,
      taskPayload: validatedData.taskPayload,
      taskMaxRetries: validatedData.taskMaxRetries,
      cronExpression: validatedData.cronExpression,
      timezone: validatedData.timezone,
      nextRunAt,
      misfirePolicy: validatedData.misfirePolicy,
      overlapPolicy: validatedData.overlapPolicy,
    });

    logger.info('Schedule created', {
      scheduleId: schedule.id,
      scheduleName: schedule.name,
      cronExpression: schedule.cronExpression,
      timezone: schedule.timezone,
      nextRunAt: schedule.nextRunAt.toISOString(),
    });

    res.status(201).json({
      success: true,
      data: schedule,
      timestamp: new Date(),
    } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else {
      logger.error('Schedule creation failed', {
        errorType: error instanceof Error ? error.name : 'UnknownError',
        reason: error instanceof Error ? error.message : String(error),
      });
      next(error);
    }
  }
});

app.get('/schedules', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { page, pageSize } = PaginationSchema.parse(req.query);
    const { items, total } = await scheduleRepo.listSchedules(page, pageSize);

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

app.get('/schedules/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const schedule = await scheduleRepo.getSchedule(req.params.id);
    if (!schedule) {
      throw new AppError(404, `Schedule ${req.params.id} not found`);
    }
    res.json({ success: true, data: schedule, timestamp: new Date() } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    next(error);
  }
});

app.put('/schedules/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validatedData = UpdateScheduleSchema.parse(req.body);
    const { version, ...updateFields } = validatedData;

    let nextRunAt: Date | undefined;
    if (updateFields.cronExpression || updateFields.timezone) {
      const schedule = await scheduleRepo.getSchedule(req.params.id);
      if (!schedule) {
        throw new AppError(404, `Schedule ${req.params.id} not found`);
      }
      nextRunAt = getNextOccurrence(
        updateFields.cronExpression ?? schedule.cronExpression,
        updateFields.timezone ?? schedule.timezone,
        new Date(),
      );
    }

    const schedule = await scheduleRepo.updateSchedule(req.params.id, version, {
      ...updateFields,
      nextRunAt,
    });

    logger.info('Schedule updated', { scheduleId: schedule.id, scheduleName: schedule.name });

    res.json({ success: true, data: schedule, timestamp: new Date() } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else if (error instanceof ScheduleStaleVersionError) {
      next(new AppError(409, error.message));
    } else if (error instanceof Error && error.message.includes('not found')) {
      next(new AppError(404, error.message));
    } else {
      next(error);
    }
  }
});

app.put('/schedules/:id/status', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validatedData = SetScheduleStatusSchema.parse(req.body);

    const schedule = await scheduleRepo.setScheduleStatus(
      req.params.id,
      validatedData.version,
      validatedData.status,
    );

    logger.info('Schedule status changed', {
      scheduleId: schedule.id,
      scheduleName: schedule.name,
      newStatus: schedule.status,
    });

    res.json({ success: true, data: schedule, timestamp: new Date() } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else if (error instanceof ScheduleStaleVersionError) {
      next(new AppError(409, error.message));
    } else if (error instanceof Error && error.message.includes('not found')) {
      next(new AppError(404, error.message));
    } else {
      next(error);
    }
  }
});

// --- Tenant-scoped API ---

app.post('/tenant/tasks', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const validatedData = TenantSubmitTaskSchema.parse(req.body);
    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

    const result = await taskRepo.acceptTask({
      tenantId: tenant.id,
      name: validatedData.name,
      priority: validatedData.priority,
      payload: validatedData.payload,
      maxRetries: validatedData.maxRetries,
      idempotencyKey: validatedData.idempotencyKey,
      billingPeriodStart,
      maxJobsPerPeriod: limits.maxJobsPerPeriod,
      scheduledFor: validatedData.scheduledFor,
    });

    const status = result.idempotent ? 200 : 201;
    logger.info(result.idempotent ? 'Idempotent task returned' : 'Tenant task created', {
      taskId: result.task.id,
      tenantId: tenant.id,
    });

    res.status(status).json({
      success: true,
      data: result.task,
      idempotent: result.idempotent,
      timestamp: new Date(),
    });
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else if (error instanceof IdempotencyConflictError) {
      next(new AppError(409, error.message));
    } else if (error instanceof QuotaExceededError) {
      next(new AppError(429, error.message));
    } else {
      next(error);
    }
  }
});

app.get('/tenant/tasks', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const { page, pageSize } = PaginationSchema.parse(req.query);
    const { items, total } = await taskRepo.listTasksForTenant(tenant.id, page, pageSize);

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

app.get('/tenant/tasks/:id', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const task = await taskRepo.getTaskForTenant(req.params.id, tenant.id);
    if (!task) {
      throw new AppError(404, `Task ${req.params.id} not found`);
    }
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    next(error);
  }
});

app.put('/tenant/tasks/:id/cancel', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const task = await taskRepo.cancelTaskForTenant(req.params.id, tenant.id);
    logger.info('Tenant task cancelled', { taskId: task.id, tenantId: tenant.id });
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

app.post('/tenant/schedules', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const validatedData = TenantCreateScheduleSchema.parse(req.body);

    const nextRunAt = getNextOccurrence(
      validatedData.cronExpression,
      validatedData.timezone,
      new Date(),
    );

    const schedule = await scheduleRepo.createSchedule({
      tenantId: tenant.id,
      name: validatedData.name,
      taskName: validatedData.taskName,
      taskPriority: validatedData.taskPriority,
      taskPayload: validatedData.taskPayload,
      taskMaxRetries: validatedData.taskMaxRetries,
      cronExpression: validatedData.cronExpression,
      timezone: validatedData.timezone,
      nextRunAt,
      misfirePolicy: validatedData.misfirePolicy,
      overlapPolicy: validatedData.overlapPolicy,
    });

    logger.info('Tenant schedule created', {
      scheduleId: schedule.id,
      tenantId: tenant.id,
    });

    res.status(201).json({
      success: true,
      data: schedule,
      timestamp: new Date(),
    } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    if (error instanceof Error && error.name === 'ZodError') {
      next(new ValidationError(error as any));
    } else {
      next(error);
    }
  }
});

app.get('/tenant/schedules', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const { page, pageSize } = PaginationSchema.parse(req.query);
    const { items, total } = await scheduleRepo.listSchedulesForTenant(tenant.id, page, pageSize);

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

app.get('/tenant/schedules/:id', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const schedule = await scheduleRepo.getScheduleForTenant(req.params.id, tenant.id);
    if (!schedule) {
      throw new AppError(404, `Schedule ${req.params.id} not found`);
    }
    res.json({ success: true, data: schedule, timestamp: new Date() } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    next(error);
  }
});

app.get('/tenant/usage', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());
    const usage = await tenantRepo.getUsage(tenant.id, billingPeriodStart);
    const activeConcurrency = await tenantRepo.getActiveConcurrencyCount(tenant.id);

    res.json({
      success: true,
      data: {
        limits,
        usage: usage ? {
          acceptedJobs: usage.acceptedJobs,
          computeUnits: usage.computeUnits,
          storageMb: usage.storageMb,
          billingPeriodStart: usage.billingPeriodStart,
        } : { acceptedJobs: 0, computeUnits: 0, storageMb: 0, billingPeriodStart },
        activeConcurrency,
      },
      timestamp: new Date(),
    });
  } catch (error) {
    next(error);
  }
});

app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
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

  logger.error('Unhandled error', {
    errorType: err instanceof Error ? err.name : 'UnknownError',
    reason: err instanceof Error ? err.message : String(err),
  });
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    timestamp: new Date(),
  });
});

async function start() {
  try {
    await redisClient.connect();
    logger.info('Redis connection established');
  } catch (error) {
    logger.warn('Redis not available at startup, outbox will retry', {
      reason: error instanceof Error ? error.message : String(error),
    });
  }

  await runMigrations(pgPool);
  logger.info('Database migrations complete');

  outboxPublisher.start();
  logger.info('Outbox publisher started');

  schedulerService.start();
  logger.info('Scheduler service started');

  const server = app.listen(config.port, () => {
    logger.info('API server started', { port: config.port });
  });

  shutdownManager.registerHandlers(server, {
    taskQueue,
    redisClient,
    pgPool,
    outboxPublisher,
    schedulerService,
  });
}

start().catch((error) => {
  logger.error('Failed to start API server', {
    errorType: error instanceof Error ? error.name : 'UnknownError',
    reason: error instanceof Error ? error.message : String(error),
  });
  process.exit(1);
});

export { app, pgPool, taskRepo, scheduleRepo, tenantRepo, schedulerService, outboxPublisher, shutdownManager, logger };
