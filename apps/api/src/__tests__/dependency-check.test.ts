import { describe, it, expect } from '@jest/globals';
import { checkHealth, settleWithin } from '../dependency-check';
import { MetricsCollector } from '../metrics';

const never = () => new Promise<never>(() => {});
const ok = () => Promise.resolve('PONG');
const fail = () => Promise.reject(new Error('ECONNREFUSED'));

describe('settleWithin', () => {
  it('returns the value when the promise settles in time', async () => {
    expect(await settleWithin(Promise.resolve(42), 100)).toEqual({ ok: true, value: 42 });
  });
  it('reports an error without throwing', async () => {
    expect(await settleWithin(fail(), 100)).toEqual({ ok: false, reason: 'error' });
  });
  it('gives up after the budget when the promise never settles', async () => {
    const started = Date.now();
    expect(await settleWithin(never(), 50)).toEqual({ ok: false, reason: 'timeout' });
    expect(Date.now() - started).toBeLessThan(500);
  });
});

describe('checkHealth', () => {
  const base = { pingPostgres: ok, pingRedis: ok, circuitState: () => 'CLOSED', timeoutMs: 100 };

  it('is healthy when both dependencies answer and the breaker is closed', async () => {
    const r = await checkHealth(base);
    expect(r.httpStatus).toBe(200);
    expect(r.data.status).toBe('healthy');
  });

  it('answers promptly as degraded when Redis hangs (the recorded outage)', async () => {
    const started = Date.now();
    const r = await checkHealth({ ...base, pingRedis: never });
    expect(Date.now() - started).toBeLessThan(1000);
    expect(r.httpStatus).toBe(200);
    expect(r.data.status).toBe('degraded');
    expect(r.data.dependencies).toEqual({ postgres: 'up', redis: 'down' });
  });

  it('is degraded when the outbox breaker is not closed', async () => {
    const r = await checkHealth({ ...base, circuitState: () => 'OPEN' });
    expect(r.data.status).toBe('degraded');
  });

  it('is unhealthy (503) when PostgreSQL does not answer in time', async () => {
    const r = await checkHealth({ ...base, pingPostgres: never });
    expect(r.httpStatus).toBe(503);
    expect(r.data.status).toBe('unhealthy');
  });

  it('is unhealthy when PostgreSQL errors', async () => {
    const r = await checkHealth({ ...base, pingPostgres: fail });
    expect(r.httpStatus).toBe(503);
  });
});

describe('MetricsCollector with Redis unreachable', () => {
  it('returns promptly with queue.available false and null counts instead of hanging', async () => {
    const taskRepo = { getTaskStatusCounts: async () => ({ QUEUED: 3 }), getTaskCount: async () => 3 };
    const queue = { count: never, getActiveCount: never, getWaitingCount: never };
    const outbox = { getPendingCount: async () => 3, getFailedCount: async () => 0 };
    const metrics = new MetricsCollector(taskRepo as never, queue as never, outbox as never, 50);

    const started = Date.now();
    const m = await metrics.getMetrics();

    expect(Date.now() - started).toBeLessThan(1000);
    expect(m.queue).toEqual({ available: false, total: null, active: null, waiting: null });
    expect(m.tasks.total).toBe(3);
    expect(m.outbox.pending).toBe(3);
  });

  it('reports real counts when Redis answers', async () => {
    const taskRepo = { getTaskStatusCounts: async () => ({}), getTaskCount: async () => 0 };
    const queue = { count: async () => 5, getActiveCount: async () => 2, getWaitingCount: async () => 3 };
    const outbox = { getPendingCount: async () => 0, getFailedCount: async () => 0 };
    const m = await new MetricsCollector(taskRepo as never, queue as never, outbox as never, 50).getMetrics();
    expect(m.queue).toEqual({ available: true, total: 5, active: 2, waiting: 3 });
  });
});
