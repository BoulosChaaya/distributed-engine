import { describe, it, expect } from '@jest/globals';
import { MetricsCollector } from '../metrics';

const never = () => new Promise<never>(() => {});
const statusCounts = async () => ({ SCHEDULED: 0, BLOCKED: 0, QUEUED: 2, PROCESSING: 1, COMPLETED: 7, FAILED: 1, CANCELLED: 0 });
const taskRepo = { getTaskStatusCounts: statusCounts, getTaskCount: async () => 11 };
const outbox = (over: Record<string, unknown> = {}) => ({
  getStats: async () => ({ pending: 4, failed: 1, oldestPendingAgeSeconds: 12.5 }),
  getCircuitState: () => 'CLOSED',
  ...over,
});
const queue = (over: Record<string, unknown> = {}) => ({
  getJobCounts: async () => ({ waiting: 3, prioritized: 2, delayed: 1, active: 1, failed: 0 }),
  ...over,
});

const sample = (body: string, series: string) => {
  const line = body.split('\n').find((l) => l.startsWith(series + ' '));
  return line === undefined ? undefined : Number(line.slice(series.length + 1));
};

describe('MetricsCollector.prometheus', () => {
  it('exposes task, queue and outbox gauges in Prometheus text format', async () => {
    const m = new MetricsCollector(taskRepo as never, queue() as never, outbox() as never, 50);
    m.recordRequest();
    m.recordRequest();
    m.recordError();
    const { contentType, body } = await m.prometheus();

    expect(contentType).toMatch(/^text\/plain; version=0\.0\.4/);
    expect(sample(body, 'engine_tasks{status="COMPLETED"}')).toBe(7);
    expect(sample(body, 'engine_tasks{status="QUEUED"}')).toBe(2);
    expect(sample(body, 'engine_queue_up')).toBe(1);
    expect(sample(body, 'engine_queue_jobs{state="prioritized"}')).toBe(2);
    expect(sample(body, 'engine_queue_jobs{state="waiting"}')).toBe(3);
    expect(sample(body, 'engine_outbox_up')).toBe(1);
    expect(sample(body, 'engine_outbox_events{status="pending"}')).toBe(4);
    expect(sample(body, 'engine_outbox_oldest_event_age_seconds{status="pending"}')).toBe(12.5);
    expect(sample(body, 'engine_outbox_circuit_state{state="CLOSED"}')).toBe(1);
    expect(sample(body, 'engine_outbox_circuit_state{state="OPEN"}')).toBe(0);
    expect(sample(body, 'engine_http_requests_total')).toBe(2);
    expect(sample(body, 'engine_http_errors_total')).toBe(1);
    expect(body).toContain('engine_api_process_resident_memory_bytes');
  });

  it('reports Redis down as engine_queue_up 0 with no job series, promptly', async () => {
    const m = new MetricsCollector(taskRepo as never, queue({ getJobCounts: never }) as never, outbox() as never, 50);
    const started = Date.now();
    const { body } = await m.prometheus();
    expect(Date.now() - started).toBeLessThan(1000);
    expect(sample(body, 'engine_queue_up')).toBe(0);
    expect(body).not.toMatch(/^engine_queue_jobs\{/m);
    expect(sample(body, 'engine_tasks{status="COMPLETED"}')).toBe(7);
  });

  it('drops queue series that were present on an earlier scrape once Redis stops answering', async () => {
    let redisUp = true;
    const q = { getJobCounts: () => (redisUp ? queue().getJobCounts() : never()) };
    const m = new MetricsCollector(taskRepo as never, q as never, outbox() as never, 50);
    expect(sample((await m.prometheus()).body, 'engine_queue_jobs{state="waiting"}')).toBe(3);
    redisUp = false;
    expect((await m.prometheus()).body).not.toMatch(/^engine_queue_jobs\{/m);
  });

  it('reports an unreadable outbox as engine_outbox_up 0, not as zero events', async () => {
    const broken = outbox({ getStats: async () => { throw new Error('boom'); } });
    const { body } = await new MetricsCollector(taskRepo as never, queue() as never, broken as never, 50).prometheus();
    expect(sample(body, 'engine_outbox_up')).toBe(0);
    expect(body).not.toMatch(/^engine_outbox_events\{/m);
    expect(body).not.toMatch(/^engine_outbox_oldest_event_age_seconds\{/m);
  });

  it('fails the scrape when PostgreSQL fails', async () => {
    const pgDown = { getTaskStatusCounts: async () => { throw new Error('ECONNREFUSED'); }, getTaskCount: async () => 0 };
    await expect(new MetricsCollector(pgDown as never, queue() as never, outbox() as never, 50).prometheus()).rejects.toThrow('ECONNREFUSED');
  });
});
