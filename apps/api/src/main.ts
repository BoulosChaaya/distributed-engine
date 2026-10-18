import express, { Request, Response, NextFunction } from 'express';
import { Queue } from 'bullmq';
import { Redis as IORedis } from 'ioredis';
import { Pool, Client as PgClient } from 'pg';
import {
  AppError,
  TaskRepository,
  OutboxPublisher,
  TaskReconciler,
  WorkflowRepository,
  ScheduleRepository,
  ScheduleStaleVersionError,
  TenantRepository,
  IdempotencyConflictError,
  QuotaExceededError,
  computeBillingPeriodStart,
  SchedulerService,
  RedisRateLimiter,
  runMigrations,
  waitForPostgres,
  InvalidTransitionError,
  TaskNotRetryableError,
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
  isZodError,
} from './validation.js';
import { createTenantAuth, createRateLimitMiddleware } from './tenant-middleware.js';
import { checkHealth, settleWithin } from './dependency-check.js';
import { createOperatorAuth } from './operator-auth.js';
import { routeId } from './route-params.js';
import { buildDefaultJobOptions } from './queue-options.js';
import { createTenantAdminRouter } from './tenant-admin.js';
import { createWorkflowRouter } from './workflow-routes.js';
import { buildRedisOptions } from './redis-options.js';
import { MetricsCollector } from './metrics.js';
import { ShutdownManager } from './shutdown.js';
import { notFoundHandler, createErrorHandler } from './http-errors.js';
import { config } from './config.js';

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

const redisOptions = buildRedisOptions(config.redis);

// Connects in the background and keeps reconnecting forever (redisRetryStrategy:
// 250 ms steps, capped at 2 s). It used to be lazyConnect + an explicit connect() at
// startup: if Redis was not reachable at that moment, the BullMQ Queue sharing
// this client failed its own initialisation and stayed broken
// ("Connection is closed") even after Redis came up.
const redisClient = new IORedis(redisOptions);

redisClient.on('error', (err) => logger.error('Redis connection error', { errorType: err.name, reason: err.message }));
redisClient.on('connect', () => logger.info('Redis connected'));

// The queue gets connection options, not the shared client, so BullMQ owns
// its connection lifecycle and recovers on its own when Redis returns.
const taskQueue = new Queue('tasks', {
  connection: redisOptions,
  defaultJobOptions: buildDefaultJobOptions(config.queue),
});

const taskRepo = new TaskRepository(pgPool);
const scheduleRepo = new ScheduleRepository(pgPool);
const tenantRepo = new TenantRepository(pgPool);
const workflowRepo = new WorkflowRepository(pgPool);
const rateLimiter = new RedisRateLimiter(redisClient);

const tenantAuth = createTenantAuth(tenantRepo);
const rateLimitMiddleware = createRateLimitMiddleware(rateLimiter, tenantRepo);

const outboxPublisher = new OutboxPublisher(
  pgPool,
  taskQueue,
  config.outbox.pollIntervalMs,
  config.outbox.batchSize,
  config.outbox.maxAttempts,
  {
    fairScheduler: new WeightedFairScheduler(),
    publishTimeoutMs: config.outbox.publishTimeoutMs,
    tenantAdmission: config.outbox.tenantAdmission,
  },
);

if (config.outbox.listen) {
  // Not from pgPool: held for the life of the process. Counted in the
  // connection budget (scripts/check-db-connection-budget.mjs).
  outboxPublisher.listenForWakeups(() => new PgClient({
    host: config.postgres.host,
    port: config.postgres.port,
    database: config.postgres.database,
    user: config.postgres.user,
    password: config.postgres.password,
  }));
}

const taskReconciler = new TaskReconciler(pgPool, taskQueue, {
  intervalMs: config.reconciler.intervalMs,
  queuedGraceMs: config.reconciler.queuedGraceMs,
  batchSize: config.reconciler.batchSize,
  deliveredOutboxRetentionMs: config.outbox.deliveredRetentionHours * 3_600_000,
  logger: logger.child({ component: 'reconciler' }),
});

const schedulerService = new SchedulerService(pgPool, logger.child({ component: 'scheduler' }), {
  pollIntervalMs: config.scheduler?.pollIntervalMs ?? 5000,
  scheduledTaskBatchSize: config.scheduler?.batchSize ?? 50,
  recurringBatchSize: config.scheduler?.batchSize ?? 20,
  catchUpBatchSize: config.scheduler?.catchUpBatchSize ?? 10,
  executionLeaseDurationMs: config.scheduler?.executionLeaseDurationMs ?? 300000,
  misfireThresholdMs: config.scheduler?.misfireThresholdMs ?? 60000,
});

const metrics = new MetricsCollector(taskRepo, taskQueue, outboxPublisher, config.health.dependencyTimeoutMs);

const shutdownManager = new ShutdownManager(config.gracefulShutdown.timeoutMs);

app.disable('x-powered-by');
app.use(express.json({ limit: '1mb' }));

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
  const { httpStatus, data } = await checkHealth({
    pingPostgres: () => pgPool.query('SELECT 1'),
    pingRedis: () => redisClient.ping(),
    circuitState: () => outboxPublisher.getCircuitState(),
    timeoutMs: config.health.dependencyTimeoutMs,
  });
  res.status(httpStatus).json({ success: httpStatus === 200, data, timestamp: new Date() });
});

app.get('/ready', async (_req: Request, res: Response) => {
  const pg = await settleWithin(pgPool.query('SELECT 1'), config.health.dependencyTimeoutMs);
  res.status(pg.ok ? 200 : 503).json({
    success: pg.ok,
    data: { postgres: pg.ok ? 'up' : 'down' },
  });
});

app.get('/live', (_req: Request, res: Response) => {
  res.json({ success: true, data: { status: 'alive' } });
});

// Everything below that is not /tenant/* or a health probe is operator-only.
// app.use with a path prefix matches '/tasks' and '/tasks/...', never '/tenant/tasks'.
const operatorAuth = createOperatorAuth(config.operator.apiKey);
app.use(['/metrics', '/workers', '/tasks', '/schedules', '/tenants', '/plans', '/workflows'], operatorAuth);
app.use(createTenantAdminRouter(tenantRepo));

const workflowRouter = createWorkflowRouter(workflowRepo, tenantRepo, taskQueue, (taskId) =>
  logger.warn('Failed to remove cancelled workflow job from queue', { taskId }));
app.use('/workflows', workflowRouter);
app.use('/tenant/workflows', tenantAuth, rateLimitMiddleware, workflowRouter);

app.get('/metrics', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const metricsData = await metrics.getMetrics();
    res.json({ success: true, data: metricsData, timestamp: new Date() });
  } catch (error) {
    next(error);
  }
});

// Same numbers for Prometheus. Behind the operator key like /metrics; the
// scrape config sends it (Prometheus `authorization.credentials_file`).
// /metrics stays JSON because the dashboard reads it.
app.get('/metrics/prometheus', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const { contentType, body } = await metrics.prometheus();
    res.type(contentType).send(body);
  } catch (error) {
    next(error);
  }
});

app.get('/workers', async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const workers: unknown[] = [];
    try {
      // SCAN, not KEYS: KEYS blocks Redis for the whole keyspace walk.
      const keys: string[] = [];
      let cursor = '0';
      do {
        const [next, batch] = await redisClient.scan(cursor, 'MATCH', 'worker:*', 'COUNT', 200);
        cursor = next;
        keys.push(...batch);
      } while (cursor !== '0');
      const values = keys.length > 0 ? await redisClient.mget(...keys) : [];
      for (const workerData of values) {
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
    if (isZodError(error)) {
      logger.warn('Task creation rejected: validation error', { errorType: 'ValidationError' });
      next(new ValidationError(error));
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
    const task = await taskRepo.getTask(routeId(req));
    if (!task) {
      throw new AppError(404, `Task ${routeId(req)} not found`);
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
    const task = await taskRepo.cancelTask(routeId(req));

    try {
      const job = await taskQueue.getJob(routeId(req));
      if (job) {
        await job.remove();
      }
    } catch (queueError) {
      logger.warn('Failed to remove cancelled job from queue', { taskId: routeId(req) });
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

// Operator redrive of a FAILED task: back to QUEUED with a fresh retry budget.
app.post('/tasks/:id/retry', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const task = await taskRepo.retryFailedTask(routeId(req));
    logger.info('Failed task sent back to the queue', { taskId: task.id });
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    if (error instanceof TaskNotRetryableError) {
      next(new AppError(409, error.message));
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
    if (isZodError(error)) {
      next(new ValidationError(error));
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
    const schedule = await scheduleRepo.getSchedule(routeId(req));
    if (!schedule) {
      throw new AppError(404, `Schedule ${routeId(req)} not found`);
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
      const schedule = await scheduleRepo.getSchedule(routeId(req));
      if (!schedule) {
        throw new AppError(404, `Schedule ${routeId(req)} not found`);
      }
      nextRunAt = getNextOccurrence(
        updateFields.cronExpression ?? schedule.cronExpression,
        updateFields.timezone ?? schedule.timezone,
        new Date(),
      );
    }

    const schedule = await scheduleRepo.updateSchedule(routeId(req), version, {
      ...updateFields,
      nextRunAt,
    });

    logger.info('Schedule updated', { scheduleId: schedule.id, scheduleName: schedule.name });

    res.json({ success: true, data: schedule, timestamp: new Date() } as ApiResponse<RecurringSchedule>);
  } catch (error) {
    if (isZodError(error)) {
      next(new ValidationError(error));
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
      routeId(req),
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
    if (isZodError(error)) {
      next(new ValidationError(error));
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
    if (isZodError(error)) {
      next(new ValidationError(error));
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
    const task = await taskRepo.getTaskForTenant(routeId(req), tenant.id);
    if (!task) {
      throw new AppError(404, `Task ${routeId(req)} not found`);
    }
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    next(error);
  }
});

app.put('/tenant/tasks/:id/cancel', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const task = await taskRepo.cancelTaskForTenant(routeId(req), tenant.id);
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

// A tenant re-runs its own FAILED task. Same rules as the operator route,
// plus the run counts as one job against the tenant's quota.
app.post('/tenant/tasks/:id/retry', tenantAuth, rateLimitMiddleware, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const tenant = req.tenant!;
    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const task = await taskRepo.retryFailedTask(routeId(req), tenant.id, {
      billingPeriodStart: computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date()),
      maxJobsPerPeriod: limits.maxJobsPerPeriod,
    });
    logger.info('Tenant task sent back to the queue', { taskId: task.id, tenantId: tenant.id });
    res.json({ success: true, data: task, timestamp: new Date() } as ApiResponse<Task>);
  } catch (error) {
    if (error instanceof TaskNotRetryableError) {
      next(new AppError(409, error.message));
    } else if (error instanceof QuotaExceededError) {
      next(new AppError(429, error.message));
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
    if (isZodError(error)) {
      next(new ValidationError(error));
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
    const schedule = await scheduleRepo.getScheduleForTenant(routeId(req), tenant.id);
    if (!schedule) {
      throw new AppError(404, `Schedule ${routeId(req)} not found`);
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

app.use(notFoundHandler);
app.use(createErrorHandler({ logger, recordError: () => metrics.recordError() }));

async function start() {
  // Redis is not awaited: the API serves health and reads from PostgreSQL
  // without it, and the outbox delivers everything once Redis is reachable.
  await waitForPostgres(pgPool, {
    onRetry: (attempt, error) => logger.warn('PostgreSQL not ready, retrying', { attempt, reason: String(error) }),
  });
  await runMigrations(pgPool);
  logger.info('Database migrations complete');

  if (!config.operator.apiKey) {
    logger.warn('OPERATOR_API_KEY is not set: /tasks, /schedules, /workers and /metrics will answer 503');
  }

  outboxPublisher.start();
  logger.info('Outbox publisher started');

  schedulerService.start();
  logger.info('Scheduler service started');

  taskReconciler.start();

  const server = app.listen(config.port, () => {
    logger.info('API server started', { port: config.port });
  });

  shutdownManager.registerHandlers(server, {
    taskQueue,
    redisClient,
    pgPool,
    outboxPublisher,
    schedulerService,
    taskReconciler,
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
