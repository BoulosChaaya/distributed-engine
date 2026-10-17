import { describe, it, expect, beforeAll, afterAll, beforeEach, jest } from '@jest/globals';
import { Pool } from 'pg';
import { runMigrations } from '../db/migrations';
import { ScheduleRepository } from '../db/schedule-repository';
import { SchedulerService } from '../scheduler/scheduler-service';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const silent = { info() {}, warn() {}, error() {}, debug() {}, child() { return silent; } };

let pool: Pool;
let repo: ScheduleRepository;
beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  repo = new ScheduleRepository(pool);
});
afterAll(async () => {
  await pool.query('TRUNCATE outbox_events, tasks, recurring_schedules CASCADE').catch(() => {});
  await pool.end();
});
beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks, recurring_schedules CASCADE');
});

describe('msUntilNextDue (requires PostgreSQL)', () => {
  it('is null when nothing is waiting', async () => {
    expect(await repo.msUntilNextDue()).toBeNull();
  });

  it('measures the earliest SCHEDULED task on the database clock', async () => {
    await repo.createScheduledTask({ name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 3, scheduledFor: new Date(Date.now() + 3000) });
    await repo.createScheduledTask({ name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 3, scheduledFor: new Date(Date.now() + 9000) });
    const ms = (await repo.msUntilNextDue())!;
    expect(ms).toBeGreaterThan(2000);
    expect(ms).toBeLessThanOrEqual(3000);
  });

  it('includes ACTIVE recurring schedules and ignores paused ones', async () => {
    const s = await repo.createSchedule({
      name: 'r', taskName: 'echo', taskPriority: 'NORMAL', taskPayload: {}, taskMaxRetries: 3,
      cronExpression: '0 * * * *', timezone: 'UTC', nextRunAt: new Date(Date.now() + 1500),
      misfirePolicy: 'SKIP_MISSED', overlapPolicy: 'ALLOW_OVERLAP',
    });
    expect((await repo.msUntilNextDue())!).toBeLessThanOrEqual(1500);
    await repo.setScheduleStatus(s.id, s.version, 'PAUSED');
    expect(await repo.msUntilNextDue()).toBeNull();
  });
});

describe('scheduler wake-up timing', () => {
  function serviceWith(msUntilNextDue: number | null) {
    const svc = new SchedulerService(pool, silent as never, { pollIntervalMs: 5000 });
    (svc as any).scheduleRepo.msUntilNextDue = jest.fn(async () => msUntilNextDue);
    return svc;
  }
  const delay = (svc: SchedulerService) => (svc as any).nextPollDelay() as Promise<number>;

  it('sleeps only until the next due time when it is sooner than the interval', async () => {
    expect(await delay(serviceWith(1200))).toBe(1200);
  });
  it('never sleeps longer than the poll interval', async () => {
    expect(await delay(serviceWith(60_000))).toBe(5000);
    expect(await delay(serviceWith(null))).toBe(5000);
  });
  it('does not spin when something is still overdue after a poll', async () => {
    expect(await delay(serviceWith(-3000))).toBe(1000);
  });
  it('keeps a small floor for due times a few ms away', async () => {
    expect(await delay(serviceWith(3))).toBe(50);
  });
  it('falls back to the interval if the lookup fails', async () => {
    const svc = new SchedulerService(pool, silent as never, { pollIntervalMs: 5000 });
    (svc as any).scheduleRepo.msUntilNextDue = jest.fn(async () => { throw new Error('db down'); });
    expect(await delay(svc)).toBe(5000);
  });
});
