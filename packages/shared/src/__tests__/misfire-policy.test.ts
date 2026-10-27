import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { runMigrations } from '../db/migrations';
import { ScheduleRepository } from '../db/schedule-repository';
import { SchedulerService } from '../scheduler/scheduler-service';
import type { MisfirePolicy } from '../types';

// Misfire policies against real PostgreSQL: what each one does when a schedule
// comes back from downtime (or a pause) with occurrences in the past.
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

// Hourly at a minute half an hour away from now. With '0 * * * *' a run in the
// first minute of an hour saw the latest occurrence as on time (less than the
// 60 s misfire threshold), so SKIP_MISSED correctly ran it and the test
// failed: it failed in CI on a dependency PR that ran at 13:00.
const minute = (new Date().getUTCMinutes() + 30) % 60;
const HOURLY = `${minute} * * * *`;

async function scheduleDue(policy: MisfirePolicy, nextRunAt: Date, cron = HOURLY) {
  return repo.createSchedule({
    name: `misfire-${policy}`, taskName: 'echo', taskPriority: 'NORMAL', taskPayload: {}, taskMaxRetries: 3,
    cronExpression: cron, timezone: 'UTC', nextRunAt, misfirePolicy: policy, overlapPolicy: 'ALLOW_OVERLAP',
  });
}
const occurrences = async (id: string) =>
  (await pool.query('SELECT scheduled_for FROM tasks WHERE schedule_id = $1 ORDER BY scheduled_for', [id])).rows;

describe('Misfire policies after 3 hours of downtime (requires PostgreSQL)', () => {
  const threeHoursAgo = () => { const d = new Date(Date.now() - 3 * 3600e3); d.setUTCMinutes(minute, 0, 0); return d; };

  it('SKIP_MISSED runs nothing and moves to the next future hour', async () => {
    const s = await scheduleDue('SKIP_MISSED', threeHoursAgo());
    await new SchedulerService(pool, silent as never).processRecurringSchedules();
    expect(await occurrences(s.id)).toHaveLength(0);
    expect((await repo.getSchedule(s.id))!.nextRunAt.getTime()).toBeGreaterThan(Date.now());
  });

  it('RUN_ONCE runs exactly one catch-up occurrence and moves to the next future hour', async () => {
    const s = await scheduleDue('RUN_ONCE', threeHoursAgo());
    await new SchedulerService(pool, silent as never).processRecurringSchedules();
    expect(await occurrences(s.id)).toHaveLength(1);
    expect((await repo.getSchedule(s.id))!.nextRunAt.getTime()).toBeGreaterThan(Date.now());
  });

  it('CATCH_UP_ALL runs every missed occurrence (bounded by the batch size)', async () => {
    const s = await scheduleDue('CATCH_UP_ALL', threeHoursAgo());
    await new SchedulerService(pool, silent as never, { catchUpBatchSize: 10 }).processRecurringSchedules();
    expect((await occurrences(s.id)).length).toBeGreaterThanOrEqual(3);
  });
});

describe('On-time occurrences are unaffected (requires PostgreSQL)', () => {
  it.each(['SKIP_MISSED', 'RUN_ONCE'] as const)('%s runs an occurrence that is only polling-late', async (policy) => {
    const due = new Date(Math.floor((Date.now() - 3000) / 10_000) * 10_000);
    const s = await scheduleDue(policy, due, '*/10 * * * * *');
    await new SchedulerService(pool, silent as never).processRecurringSchedules();
    const rows = await occurrences(s.id);
    expect(rows).toHaveLength(1);
  });

  it('the threshold is configurable', async () => {
    const twoMinutesAgo = new Date(Math.floor((Date.now() - 120_000) / 60_000) * 60_000);
    const s = await scheduleDue('SKIP_MISSED', twoMinutesAgo, '*/5 * * * *');
    await new SchedulerService(pool, silent as never, { misfireThresholdMs: 3600_000 }).processRecurringSchedules();
    expect(await occurrences(s.id)).toHaveLength(1);
  });
});
