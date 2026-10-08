import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import {
  context,
  trace,
  SpanKind,
  SpanStatusCode,
  ROOT_CONTEXT,
} from '@opentelemetry/api';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
  NodeTracerProvider,
} from '@opentelemetry/sdk-trace-node';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks';
import { TaskRepository } from '../db/task-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { runMigrations } from '../db/migrations';
import {
  injectTraceContext,
  extractTraceContext,
  TRACE_CONTEXT_KEY,
} from '../telemetry/propagation';
import { tracing } from '../telemetry/spans';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const TEST_REDIS_HOST = process.env.TEST_REDIS_HOST || 'localhost';
const TEST_REDIS_PORT = parseInt(process.env.TEST_REDIS_PORT || '6379');

let pool: Pool;
let repo: TaskRepository;
let redis: IORedis;
let queue: Queue;
let exporter: InMemorySpanExporter;
let provider: NodeTracerProvider;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    throw new Error(`PostgreSQL not available. Error: ${err}`);
  }

  redis = new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, maxRetriesPerRequest: null });
  try {
    await redis.ping();
  } catch (err) {
    throw new Error(`Redis not available. Error: ${err}`);
  }

  await runMigrations(pool);
  repo = new TaskRepository(pool);

  queue = new Queue('tasks-test-telemetry', {
    connection: redis,
    defaultJobOptions: { removeOnComplete: true, removeOnFail: false },
  });

  exporter = new InMemorySpanExporter();
  const contextManager = new AsyncHooksContextManager();
  contextManager.enable();
  context.setGlobalContextManager(contextManager);

  provider = new NodeTracerProvider({
    resource: resourceFromAttributes({ 'service.name': 'test' }),
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  provider.register();
});

afterAll(async () => {
  await provider.shutdown();
  context.disable();
  if (queue) {
    try { await queue.obliterate({ force: true }); } catch {}
    await queue.close();
  }
  if (redis) redis.disconnect();
  if (pool) {
    await pool.query('TRUNCATE outbox_events, tasks CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  try { await queue.drain(); } catch {}
  exporter.reset();
});

describe('Trace context propagation', () => {
  it('should inject and extract W3C trace context', () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('test-span');
    const ctx = trace.setSpan(context.active(), span);

    const carrier = injectTraceContext(ctx);
    expect(carrier).toHaveProperty('traceparent');
    expect(carrier.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);

    const extractedCtx = extractTraceContext(carrier);
    const extractedSpanCtx = trace.getSpanContext(extractedCtx);
    const originalSpanCtx = span.spanContext();
    expect(extractedSpanCtx?.traceId).toBe(originalSpanCtx.traceId);

    span.end();
  });

  it('should return ROOT_CONTEXT for null/undefined/invalid carriers', () => {
    expect(extractTraceContext(null)).toBe(ROOT_CONTEXT);
    expect(extractTraceContext(undefined)).toBe(ROOT_CONTEXT);
    expect(extractTraceContext({} as any)).toBe(ROOT_CONTEXT);
    expect(extractTraceContext({ traceparent: 'invalid' })).not.toBeNull();
  });

  it('should inject trace context into outbox event during task creation', async () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('api-request');
    const ctx = trace.setSpan(context.active(), span);

    let outboxEvent: any;
    await context.with(ctx, async () => {
      const result = await repo.createTaskWithOutbox({
        name: 'trace-context-test',
        priority: 'NORMAL',
        payload: { test: true },
        maxRetries: 3,
      });
      outboxEvent = result.outboxEvent;
    });

    expect(outboxEvent.traceContext).toBeDefined();
    expect(outboxEvent.traceContext).toHaveProperty('traceparent');

    const extractedCtx = extractTraceContext(outboxEvent.traceContext);
    const extractedSpanCtx = trace.getSpanContext(extractedCtx);
    expect(extractedSpanCtx?.traceId).toBe(span.spanContext().traceId);

    span.end();
  });

  it('should store trace context in PostgreSQL and read it back', async () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('persist-test');
    const ctx = trace.setSpan(context.active(), span);

    let taskId: string;
    await context.with(ctx, async () => {
      const result = await repo.createTaskWithOutbox({
        name: 'persist-trace-test',
        priority: 'HIGH',
        payload: {},
        maxRetries: 1,
      });
      taskId = result.task.id;
    });

    const row = await pool.query(
      'SELECT trace_context FROM outbox_events WHERE task_id = $1',
      [taskId!],
    );
    expect(row.rows[0].trace_context).toBeDefined();
    expect(row.rows[0].trace_context).toHaveProperty('traceparent');

    span.end();
  });

  it('should propagate trace context through outbox publisher into BullMQ job', async () => {
    const tracer = trace.getTracer('test');
    const span = tracer.startSpan('e2e-propagation');
    const ctx = trace.setSpan(context.active(), span);

    let taskId: string;
    await context.with(ctx, async () => {
      const result = await repo.createTaskWithOutbox({
        name: 'propagation-test',
        priority: 'NORMAL',
        payload: { data: 'test' },
        maxRetries: 2,
      });
      taskId = result.task.id;
    });

    const publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
    const published = await publisher.processOutbox();
    expect(published).toBe(1);

    const job = await queue.getJob(taskId!);
    expect(job).toBeDefined();
    expect(job!.data[TRACE_CONTEXT_KEY]).toBeDefined();
    expect(job!.data[TRACE_CONTEXT_KEY]).toHaveProperty('traceparent');

    const jobTraceCtx = extractTraceContext(job!.data[TRACE_CONTEXT_KEY]);
    const jobSpanCtx = trace.getSpanContext(jobTraceCtx);
    expect(jobSpanCtx?.traceId).toBe(span.spanContext().traceId);

    expect(job!.data.publishedAt).toBeDefined();

    span.end();
  });
});

describe('Backward compatibility', () => {
  it('should handle outbox events without trace_context', async () => {
    const taskId = require('crypto').randomBytes(16).toString('hex');
    const outboxId = require('crypto').randomBytes(16).toString('hex');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO tasks (id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
         VALUES ($1, 'no-trace-task', 'QUEUED', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW())`,
        [taskId],
      );
      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at)
         VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW())`,
        [outboxId, taskId, JSON.stringify({ taskId, taskName: 'no-trace-task', priority: 'NORMAL', payload: {}, maxRetries: 3 })],
      );
      await client.query('COMMIT');
    } finally {
      client.release();
    }

    const publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
    const published = await publisher.processOutbox();
    expect(published).toBe(1);

    const job = await queue.getJob(taskId);
    expect(job).toBeDefined();
    expect(job!.data.taskId).toBe(taskId);
  });

  it('should handle malformed trace_context without breaking processing', async () => {
    const taskId = require('crypto').randomBytes(16).toString('hex');
    const outboxId = require('crypto').randomBytes(16).toString('hex');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO tasks (id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
         VALUES ($1, 'bad-trace-task', 'QUEUED', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW())`,
        [taskId],
      );
      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW(), $4)`,
        [
          outboxId,
          taskId,
          JSON.stringify({ taskId, taskName: 'bad-trace-task', priority: 'NORMAL', payload: {}, maxRetries: 3 }),
          JSON.stringify({ traceparent: 'not-a-valid-traceparent' }),
        ],
      );
      await client.query('COMMIT');
    } finally {
      client.release();
    }

    const publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
    const published = await publisher.processOutbox();
    expect(published).toBe(1);

    const job = await queue.getJob(taskId);
    expect(job).toBeDefined();
  });
});

describe('Failure isolation', () => {
  it('should not fail task creation when tracing is unavailable', async () => {
    const result = await repo.createTaskWithOutbox({
      name: 'no-tracing-task',
      priority: 'NORMAL',
      payload: { important: true },
      maxRetries: 3,
    });
    expect(result.task.id).toBeDefined();
    expect(result.task.status).toBe('QUEUED');
    expect(result.outboxEvent.id).toBeDefined();
  });

  it('should not change task status when span operations fail', async () => {
    const result = await repo.createTaskWithOutbox({
      name: 'span-fail-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(result.task.status).toBe('QUEUED');
    const fetched = await repo.getTask(result.task.id);
    expect(fetched?.status).toBe('QUEUED');
  });
});

describe('Manual spans', () => {
  it('should create task.create span with correct attributes', () => {
    const span = tracing.startTaskCreation('task-123', 'test-task', 'HIGH');
    expect(span).toBeDefined();
    tracing.setSpanOk(span);
    tracing.endSpan(span);

    const spans = exporter.getFinishedSpans();
    const createSpan = spans.find(s => s.name === 'task.create');
    expect(createSpan).toBeDefined();
    expect(createSpan!.kind).toBe(SpanKind.PRODUCER);
    expect(createSpan!.attributes['task.id']).toBe('task-123');
    expect(createSpan!.attributes['task.name']).toBe('test-task');
    expect(createSpan!.attributes['task.priority']).toBe('HIGH');
  });

  it('should create outbox.publish span with messaging attributes', () => {
    const span = tracing.startOutboxPublish('event-1', 'task-1');
    tracing.setSpanOk(span);
    tracing.endSpan(span);

    const spans = exporter.getFinishedSpans();
    const publishSpan = spans.find(s => s.name === 'outbox.publish');
    expect(publishSpan).toBeDefined();
    expect(publishSpan!.kind).toBe(SpanKind.PRODUCER);
    expect(publishSpan!.attributes['messaging.system']).toBe('bullmq');
    expect(publishSpan!.attributes['messaging.operation']).toBe('publish');
  });

  it('should create task.process span as CONSUMER linked to parent trace via traceId', () => {
    const tracer = trace.getTracer('test');
    const parentSpan = tracer.startSpan('parent');
    const parentCtx = trace.setSpan(ROOT_CONTEXT, parentSpan);

    const carrier = injectTraceContext(parentCtx);
    const extractedCtx = extractTraceContext(carrier);

    const processSpan = tracing.startTaskProcess(
      'task-456', 'process-test', 'worker-1', 1, extractedCtx,
    );
    tracing.setSpanOk(processSpan);
    tracing.endSpan(processSpan);
    parentSpan.end();

    const spans = exporter.getFinishedSpans();
    const pSpan = spans.find(s => s.name === 'task.process');
    expect(pSpan).toBeDefined();
    expect(pSpan!.kind).toBe(SpanKind.CONSUMER);
    expect(pSpan!.attributes['task.id']).toBe('task-456');
    expect(pSpan!.attributes['worker.id']).toBe('worker-1');
    expect(pSpan!.attributes['task.attempt']).toBe(1);
    expect(pSpan!.spanContext().traceId).toBe(parentSpan.spanContext().traceId);
  });

  it('should link child span to parent via parentSpanContext', () => {
    const tracer = trace.getTracer('test');
    const parentSpan = tracer.startSpan('parent');
    const parentCtx = trace.setSpan(ROOT_CONTEXT, parentSpan);

    const processSpan = tracing.startTaskProcess(
      'task-789', 'local-parent-test', 'worker-2', 1, parentCtx,
    );
    tracing.endSpan(processSpan);
    parentSpan.end();

    const spans = exporter.getFinishedSpans();
    const pSpan = spans.find(s => s.name === 'task.process') as any;
    expect(pSpan).toBeDefined();
    expect(pSpan!.spanContext().traceId).toBe(parentSpan.spanContext().traceId);
    expect(pSpan.parentSpanContext?.spanId).toBe(parentSpan.spanContext().spanId);
  });

  it('should record errors on spans without throwing', () => {
    const span = tracing.startTaskCreation('err-task', 'err', 'LOW');
    const error = new Error('test failure');
    tracing.recordError(span, error);
    tracing.endSpan(span);

    const spans = exporter.getFinishedSpans();
    const errSpan = spans.find(s => s.name === 'task.create');
    expect(errSpan).toBeDefined();
    expect(errSpan!.status.code).toBe(SpanStatusCode.ERROR);
    expect(errSpan!.events.length).toBeGreaterThan(0);
    expect(errSpan!.events[0].name).toBe('exception');
  });

  it('should handle non-Error objects in recordError', () => {
    const span = tracing.startTaskCreation('str-err', 'test', 'NORMAL');
    expect(() => tracing.recordError(span, 'string error')).not.toThrow();
    expect(() => tracing.recordError(span, 42)).not.toThrow();
    expect(() => tracing.recordError(span, null)).not.toThrow();
    tracing.endSpan(span);
  });
});

describe('Distributed trace correlation', () => {
  it('should maintain trace ID across API → outbox → BullMQ path', async () => {
    const tracer = trace.getTracer('test');
    const apiSpan = tracer.startSpan('POST /tasks');
    const apiCtx = trace.setSpan(context.active(), apiSpan);
    const traceId = apiSpan.spanContext().traceId;

    let taskId: string;
    await context.with(apiCtx, async () => {
      const createSpan = tracing.startTaskCreation('pending', 'correlation-test', 'NORMAL');
      const result = await tracing.withActiveSpan(createSpan, () =>
        repo.createTaskWithOutbox({
          name: 'correlation-test',
          priority: 'NORMAL',
          payload: {},
          maxRetries: 3,
        }),
      );
      taskId = result.task.id;
      createSpan.setAttribute('task.id', taskId);
      tracing.endSpan(createSpan);
    });

    const publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
    await publisher.processOutbox();

    const job = await queue.getJob(taskId!);
    const jobCtx = extractTraceContext(job!.data[TRACE_CONTEXT_KEY]);
    const jobSpanCtx = trace.getSpanContext(jobCtx);
    expect(jobSpanCtx?.traceId).toBe(traceId);

    const processSpan = tracing.startTaskProcess(
      taskId!, 'correlation-test', 'test-worker', 1, jobCtx,
    );
    expect(processSpan.spanContext().traceId).toBe(traceId);
    tracing.endSpan(processSpan);

    apiSpan.end();
  });

  it('should form correct parent chain: API → publisher → worker via parentSpanContext', async () => {
    exporter.reset();

    const tracer = trace.getTracer('test');
    const apiSpan = tracer.startSpan('POST /tasks');
    const apiCtx = trace.setSpan(context.active(), apiSpan);
    const traceId = apiSpan.spanContext().traceId;

    let taskId: string;
    let createSpanId: string;
    await context.with(apiCtx, async () => {
      const createSpan = tracing.startTaskCreation('pending', 'parent-chain-test', 'HIGH');
      createSpanId = createSpan.spanContext().spanId;
      const result = await tracing.withActiveSpan(createSpan, () =>
        repo.createTaskWithOutbox({
          name: 'parent-chain-test',
          priority: 'HIGH',
          payload: {},
          maxRetries: 2,
        }),
      );
      taskId = result.task.id;
      createSpan.setAttribute('task.id', taskId);
      tracing.endSpan(createSpan);
    });

    const publisher = new OutboxPublisher(pool, queue, 60000, 10, 3);
    await publisher.processOutbox();

    const job = await queue.getJob(taskId!);
    expect(job).toBeDefined();

    const jobCtx = extractTraceContext(job!.data[TRACE_CONTEXT_KEY]);
    const processSpan = tracing.startTaskProcess(
      taskId!, 'parent-chain-test', 'test-worker', 1, jobCtx,
    );
    tracing.endSpan(processSpan);
    apiSpan.end();

    const spans = exporter.getFinishedSpans();

    const taskCreateSpan = spans.find(s => s.name === 'task.create');
    const outboxPublishSpan = spans.find(s => s.name === 'outbox.publish');
    const taskProcessSpan = spans.find(s => s.name === 'task.process');

    expect(taskCreateSpan).toBeDefined();
    expect(outboxPublishSpan).toBeDefined();
    expect(taskProcessSpan).toBeDefined();

    expect(taskCreateSpan!.spanContext().traceId).toBe(traceId);
    expect(outboxPublishSpan!.spanContext().traceId).toBe(traceId);
    expect(taskProcessSpan!.spanContext().traceId).toBe(traceId);

    expect((outboxPublishSpan as any).parentSpanContext?.spanId).toBe(createSpanId!);
    expect((outboxPublishSpan as any).parentSpanContext?.traceId).toBe(traceId);

    expect((taskProcessSpan as any).parentSpanContext?.spanId).toBe(
      outboxPublishSpan!.spanContext().spanId,
    );
    expect((taskProcessSpan as any).parentSpanContext?.traceId).toBe(traceId);
  });
});
