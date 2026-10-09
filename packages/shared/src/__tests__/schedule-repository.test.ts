import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { ScheduleRepository, ScheduleStaleVersionError, DuplicateOccurrenceError } from '../db/schedule-repository';
import { TaskRepository } from '../db/task-repository';
import { runMigrations } from '../db/migrations';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let scheduleRepo: ScheduleRepository;
let taskRepo: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    throw new Error(
      `PostgreSQL not available at ${TEST_PG_URL}. Integration tests require PostgreSQL. Error: ${err}`
    );
  }
  await runMigrations(pool);
  scheduleRepo = new ScheduleRepository(pool);
  taskRepo = new TaskRepository(pool);
});

afterAll(async () => {
  if (pool) {
    await pool.query('TRUNCATE outbox_events, tasks, recurring_schedules CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks, recurring_schedules CASCADE');
});

// --- One-time scheduled tasks ---

describe('One-time scheduled tasks (requires PostgreSQL)', () => {
  it('should create a SCHEDULED task with a future scheduledFor', async () => {
    const futureDate = new Date(Date.now() + 3600000);
    const result = await scheduleRepo.createScheduledTask({
      name: 'future-task',
      priority: 'HIGH',
      payload: { key: 'value' },
      maxRetries: 3,
      scheduledFor: futureDate,
    });

    expect(result.task.status).toBe('SCHEDULED');
    expect(result.task.scheduledFor).toBeDefined();
    expect(result.task.scheduledFor!.getTime()).toBe(futureDate.getTime());
    expect(result.task.name).toBe('future-task');
    expect(result.task.priority).toBe('HIGH');
  });

  it('should not release a future scheduled task', async () => {
    const futureDate = new Date(Date.now() + 3600000);
    await scheduleRepo.createScheduledTask({
      name: 'not-yet-due',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
      scheduledFor: futureDate,
    });

    const released = await scheduleRepo.releaseDueScheduledTasks(10);
    expect(released.length).toBe(0);
  });

  it('should release a due scheduled task: SCHEDULED -> QUEUED with outbox', async () => {
    const pastDate = new Date(Date.now() - 1000);
    const { task } = await scheduleRepo.createScheduledTask({
      name: 'due-task',
      priority: 'NORMAL',
      payload: { foo: 'bar' },
      maxRetries: 3,
      scheduledFor: pastDate,
    });

    const released = await scheduleRepo.releaseDueScheduledTasks(10);
    expect(released.length).toBe(1);
    expect(released[0].task.status).toBe('QUEUED');
    expect(released[0].task.id).toBe(task.id);
    expect(released[0].outboxEvent.taskId).toBe(task.id);
    expect(released[0].outboxEvent.eventType).toBe('SCHEDULED_TASK_RELEASED');
    expect(released[0].outboxEvent.status).toBe('PENDING');
  });

  it('should atomically transition task and create outbox event', async () => {
    const pastDate = new Date(Date.now() - 1000);
    const { task } = await scheduleRepo.createScheduledTask({
      name: 'atomic-scheduled',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
      scheduledFor: pastDate,
    });

    await scheduleRepo.releaseDueScheduledTasks(10);

    const dbTask = await pool.query('SELECT * FROM tasks WHERE id = $1', [task.id]);
    expect(dbTask.rows[0].status).toBe('QUEUED');

    const dbOutbox = await pool.query('SELECT * FROM outbox_events WHERE task_id = $1', [task.id]);
    expect(dbOutbox.rows.length).toBe(1);
    expect(dbOutbox.rows[0].event_type).toBe('SCHEDULED_TASK_RELEASED');
    expect(dbOutbox.rows[0].status).toBe('PENDING');
  });

  it('should not release same scheduled task twice (SKIP LOCKED)', async () => {
    const pastDate = new Date(Date.now() - 1000);
    for (let i = 0; i < 3; i++) {
      await scheduleRepo.createScheduledTask({
        name: `task-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
        scheduledFor: pastDate,
      });
    }

    const [batch1, batch2] = await Promise.all([
      scheduleRepo.releaseDueScheduledTasks(10),
      scheduleRepo.releaseDueScheduledTasks(10),
    ]);

    const totalReleased = batch1.length + batch2.length;
    expect(totalReleased).toBe(3);

    const allTaskIds = [...batch1, ...batch2].map(r => r.task.id);
    const uniqueIds = new Set(allTaskIds);
    expect(uniqueIds.size).toBe(3);
  });

  it('should allow cancellation of a SCHEDULED task', async () => {
    const futureDate = new Date(Date.now() + 3600000);
    const { task } = await scheduleRepo.createScheduledTask({
      name: 'cancel-scheduled',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
      scheduledFor: futureDate,
    });

    const cancelled = await taskRepo.cancelTask(task.id);
    expect(cancelled.status).toBe('CANCELLED');

    const released = await scheduleRepo.releaseDueScheduledTasks(10);
    expect(released.length).toBe(0);
  });
});

// --- Recurring schedule CRUD ---

describe('Recurring schedule CRUD (requires PostgreSQL)', () => {
  it('should create a recurring schedule', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'daily-report',
      taskName: 'generate-report',
      taskPriority: 'NORMAL',
      taskPayload: { type: 'daily' },
      taskMaxRetries: 3,
      cronExpression: '0 20 * * *',
      timezone: 'Asia/Beirut',
      nextRunAt: new Date('2025-01-01T18:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    expect(schedule.id).toBeDefined();
    expect(schedule.name).toBe('daily-report');
    expect(schedule.taskName).toBe('generate-report');
    expect(schedule.cronExpression).toBe('0 20 * * *');
    expect(schedule.timezone).toBe('Asia/Beirut');
    expect(schedule.status).toBe('ACTIVE');
    expect(schedule.version).toBe(1);
  });

  it('should retrieve a schedule by ID', async () => {
    const created = await scheduleRepo.createSchedule({
      name: 'test-schedule',
      taskName: 'test-task',
      taskPriority: 'HIGH',
      taskPayload: {},
      taskMaxRetries: 5,
      cronExpression: '*/5 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'RUN_ONCE',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const retrieved = await scheduleRepo.getSchedule(created.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.id).toBe(created.id);
    expect(retrieved!.misfirePolicy).toBe('RUN_ONCE');
    expect(retrieved!.overlapPolicy).toBe('FORBID_OVERLAP');
  });

  it('should list schedules with pagination', async () => {
    for (let i = 0; i < 5; i++) {
      await scheduleRepo.createSchedule({
        name: `schedule-${i}`,
        taskName: 'task',
        taskPriority: 'NORMAL',
        taskPayload: {},
        taskMaxRetries: 3,
        cronExpression: '0 0 * * *',
        timezone: 'UTC',
        nextRunAt: new Date(),
        misfirePolicy: 'SKIP_MISSED',
        overlapPolicy: 'ALLOW_OVERLAP',
      });
    }

    const page1 = await scheduleRepo.listSchedules(1, 3);
    expect(page1.items.length).toBe(3);
    expect(page1.total).toBe(5);
  });

  it('should update a schedule with optimistic concurrency', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'updatable',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    const updated = await scheduleRepo.updateSchedule(schedule.id, 1, {
      name: 'updated-name',
      cronExpression: '*/10 * * * *',
      misfirePolicy: 'CATCH_UP_ALL',
    });

    expect(updated.name).toBe('updated-name');
    expect(updated.cronExpression).toBe('*/10 * * * *');
    expect(updated.misfirePolicy).toBe('CATCH_UP_ALL');
    expect(updated.version).toBe(2);
  });

  it('should reject stale version updates', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'stale-test',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.updateSchedule(schedule.id, 1, { name: 'first-update' });

    await expect(
      scheduleRepo.updateSchedule(schedule.id, 1, { name: 'stale-update' }),
    ).rejects.toThrow(ScheduleStaleVersionError);
  });

  it('should change schedule status (disable/pause/enable)', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'status-test',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    const disabled = await scheduleRepo.setScheduleStatus(schedule.id, 1, 'DISABLED');
    expect(disabled.status).toBe('DISABLED');
    expect(disabled.version).toBe(2);

    const enabled = await scheduleRepo.setScheduleStatus(schedule.id, 2, 'ACTIVE');
    expect(enabled.status).toBe('ACTIVE');
    expect(enabled.version).toBe(3);
  });

  it('should reject status change with stale version', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'stale-status',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 0 * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.setScheduleStatus(schedule.id, 1, 'PAUSED');

    await expect(
      scheduleRepo.setScheduleStatus(schedule.id, 1, 'DISABLED'),
    ).rejects.toThrow(ScheduleStaleVersionError);
  });
});

// --- Recurring occurrence generation ---

describe('Recurring occurrence generation (requires PostgreSQL)', () => {
  it('should generate an occurrence atomically', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'occurrence-test',
      taskName: 'report',
      taskPriority: 'HIGH',
      taskPayload: { type: 'test' },
      taskMaxRetries: 2,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    const result = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );

    expect(result.task.name).toBe('report');
    expect(result.task.status).toBe('QUEUED');
    expect(result.task.priority).toBe('HIGH');
    expect(result.task.scheduleId).toBe(schedule.id);
    expect(result.task.scheduledFor!.toISOString()).toBe('2025-01-01T00:00:00.000Z');
    expect(result.outboxEvent.eventType).toBe('SCHEDULED_TASK_RELEASED');
    expect(result.outboxEvent.status).toBe('PENDING');

    const updated = await scheduleRepo.getSchedule(schedule.id);
    expect(updated!.nextRunAt.toISOString()).toBe('2025-01-01T01:00:00.000Z');
    expect(updated!.version).toBe(2);
  });

  it('should prevent duplicate occurrences via UNIQUE constraint', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'dup-test',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );

    await expect(
      scheduleRepo.generateOccurrence(
        schedule.id,
        new Date('2025-01-01T00:00:00Z'),
        new Date('2025-01-01T02:00:00Z'),
      ),
    ).rejects.toThrow(DuplicateOccurrenceError);
  });

  it('should not generate occurrence for non-ACTIVE schedule', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'disabled-gen',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.setScheduleStatus(schedule.id, 1, 'DISABLED');

    await expect(
      scheduleRepo.generateOccurrence(
        schedule.id,
        new Date('2025-01-01T00:00:00Z'),
        new Date('2025-01-01T01:00:00Z'),
      ),
    ).rejects.toThrow('not ACTIVE');
  });

  it('should allow multiple schedulers: SKIP LOCKED prevents double processing', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'concurrent-schedule',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(Date.now() - 60000),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    const results = await Promise.allSettled([
      scheduleRepo.processScheduleWithLock(schedule.id, async (s, c) => {
        await scheduleRepo.generateOccurrence(
          s.id,
          s.nextRunAt,
          new Date(s.nextRunAt.getTime() + 3600000),
          c,
        );
      }),
      scheduleRepo.processScheduleWithLock(schedule.id, async (s, c) => {
        await scheduleRepo.generateOccurrence(
          s.id,
          s.nextRunAt,
          new Date(s.nextRunAt.getTime() + 3600000),
          c,
        );
      }),
    ]);

    const fulfilled = results.filter(r => r.status === 'fulfilled');
    expect(fulfilled.length).toBeGreaterThanOrEqual(1);

    const tasks = await pool.query(
      `SELECT * FROM tasks WHERE schedule_id = $1`,
      [schedule.id],
    );
    expect(tasks.rows.length).toBe(1);
  });
});

// --- Edit/disable ordering ---

describe('Schedule edit/disable ordering (requires PostgreSQL)', () => {
  it('disable does not retroactively erase already-created occurrence', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'disable-order',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );

    await scheduleRepo.setScheduleStatus(schedule.id, 2, 'DISABLED');

    const tasks = await pool.query(
      `SELECT * FROM tasks WHERE schedule_id = $1`,
      [schedule.id],
    );
    expect(tasks.rows.length).toBe(1);
    expect(tasks.rows[0].status).toBe('QUEUED');
  });

  it('edit-before-scheduler: scheduler sees new definition', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'edit-before',
      taskName: 'old-task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(Date.now() - 60000),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.updateSchedule(schedule.id, 1, { taskName: 'new-task' });

    const updated = await scheduleRepo.getSchedule(schedule.id);
    expect(updated!.taskName).toBe('new-task');

    const result = await scheduleRepo.generateOccurrence(
      schedule.id,
      updated!.nextRunAt,
      new Date(updated!.nextRunAt.getTime() + 3600000),
    );

    expect(result.task.name).toBe('new-task');
  });

  it('disable-before-generation prevents new occurrences', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'disable-before',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(Date.now() - 60000),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    await scheduleRepo.setScheduleStatus(schedule.id, 1, 'DISABLED');

    await expect(
      scheduleRepo.generateOccurrence(
        schedule.id,
        schedule.nextRunAt,
        new Date(schedule.nextRunAt.getTime() + 3600000),
      ),
    ).rejects.toThrow('not ACTIVE');
  });
});

// --- Execution overlap ---

describe('Execution overlap policy (requires PostgreSQL)', () => {
  it('should detect active occurrences for overlap check', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'overlap-check',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const noActive = await scheduleRepo.hasActiveOccurrence(schedule.id);
    expect(noActive).toBe(false);

    await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );

    const hasActive = await scheduleRepo.hasActiveOccurrence(schedule.id);
    expect(hasActive).toBe(true);
  });

  it('should acquire and release execution lease', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'lease-test',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const lease1 = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(lease1.acquired).toBe(true);
    expect(lease1.leaseToken).toBeDefined();

    const lease2 = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(lease2.acquired).toBe(false);

    await scheduleRepo.releaseExecutionLease(schedule.id, lease1.leaseToken!);

    const lease3 = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(lease3.acquired).toBe(true);
  });

  it('should allow lease acquisition after expiry', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'lease-expiry',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const lease1 = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(lease1.acquired).toBe(true);

    await pool.query(
      `UPDATE recurring_schedules SET execution_lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [schedule.id],
    );

    const lease2 = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(lease2.acquired).toBe(true);
    expect(lease2.leaseToken).not.toBe(lease1.leaseToken);
  });

  it('should reject lease release with wrong token', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'wrong-release',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    await scheduleRepo.acquireExecutionLease(schedule.id, 60000);

    await expect(
      scheduleRepo.releaseExecutionLease(schedule.id, 'wrong-token'),
    ).rejects.toThrow('token mismatch');
  });
});

// --- Execution lease primitives (PostgreSQL-backed) ---

describe('Execution lease primitives (requires PostgreSQL)', () => {
  it('1. ALLOW_OVERLAP: two occurrences may execute concurrently without lease', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'allow-overlap',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'ALLOW_OVERLAP',
    });

    const occ1 = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );
    const occ2 = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T01:00:00Z'),
      new Date('2025-01-01T02:00:00Z'),
    );

    const t1 = await taskRepo.transitionStatus(occ1.task.id, 1, 'PROCESSING', { startedAt: new Date() });
    const t2 = await taskRepo.transitionStatus(occ2.task.id, 1, 'PROCESSING', { startedAt: new Date() });
    expect(t1.status).toBe('PROCESSING');
    expect(t2.status).toBe('PROCESSING');
  });

  it('2. FORBID_OVERLAP: occurrence A holds lease, B cannot acquire while valid', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'forbid-overlap-block',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);
    expect(leaseA.leaseToken).toBeDefined();

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(false);
    expect(leaseB.expiresAt).toBeDefined();
    expect(leaseB.expiresAt!.getTime()).toBeGreaterThan(Date.now());
  });

  it('3. Deferral: failed acquisition returns expiresAt for worker deferral timing', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'deferral-timing',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    await scheduleRepo.acquireExecutionLease(schedule.id, 60000);

    const deferResult = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(deferResult.acquired).toBe(false);
    expect(deferResult.expiresAt).toBeInstanceOf(Date);
    const margin = 2000;
    const deferUntil = deferResult.expiresAt!.getTime() + margin;
    expect(deferUntil).toBeGreaterThan(Date.now());
  });

  it('4. Release: after A completes and releases, B can acquire', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'release-then-acquire',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);

    await scheduleRepo.releaseExecutionLease(schedule.id, leaseA.leaseToken!);

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(true);
    expect(leaseB.leaseToken).toBeDefined();
    expect(leaseB.leaseToken).not.toBe(leaseA.leaseToken);
  });

  it('5. Worker failure/lease expiry: A crashes, lease expires, B can acquire', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'crash-recovery',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);

    await pool.query(
      `UPDATE recurring_schedules SET execution_lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [schedule.id],
    );

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(true);
    expect(leaseB.leaseToken).not.toBe(leaseA.leaseToken);
  });

  it('6. Renewal: active owner can renew, wrong token cannot', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'renewal-test',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const lease = await scheduleRepo.acquireExecutionLease(schedule.id, 5000);
    expect(lease.acquired).toBe(true);

    await expect(
      scheduleRepo.renewExecutionLease(schedule.id, lease.leaseToken!, 60000),
    ).resolves.toBeUndefined();

    const afterRenew = await scheduleRepo.getSchedule(schedule.id);
    expect(afterRenew!.executionLeaseExpiresAt!.getTime()).toBeGreaterThan(Date.now() + 50000);

    await expect(
      scheduleRepo.renewExecutionLease(schedule.id, 'wrong-token', 60000),
    ).rejects.toThrow('token mismatch');
  });

  it('7. Stale release: old token cannot release lease held by new owner', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'stale-release',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);

    await pool.query(
      `UPDATE recurring_schedules SET execution_lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [schedule.id],
    );

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(true);

    await expect(
      scheduleRepo.releaseExecutionLease(schedule.id, leaseA.leaseToken!),
    ).rejects.toThrow('token mismatch');

    const current = await scheduleRepo.getSchedule(schedule.id);
    expect(current!.executionLeaseToken).toBe(leaseB.leaseToken);
  });

  it('8. Ownership loss: renewal failure signals loss to worker', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'ownership-loss',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date(),
      misfirePolicy: 'SKIP_MISSED',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);

    await pool.query(
      `UPDATE recurring_schedules SET execution_lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [schedule.id],
    );

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(true);

    let ownershipLost = false;
    try {
      await scheduleRepo.renewExecutionLease(schedule.id, leaseA.leaseToken!, 60000);
    } catch {
      ownershipLost = true;
    }
    expect(ownershipLost).toBe(true);
  });

  it('9. CATCH_UP_ALL + FORBID_OVERLAP: all occurrences durable, execution serialized', async () => {
    const schedule = await scheduleRepo.createSchedule({
      name: 'catchup-forbid',
      taskName: 'task',
      taskPriority: 'NORMAL',
      taskPayload: {},
      taskMaxRetries: 3,
      cronExpression: '0 * * * *',
      timezone: 'UTC',
      nextRunAt: new Date('2025-01-01T00:00:00Z'),
      misfirePolicy: 'CATCH_UP_ALL',
      overlapPolicy: 'FORBID_OVERLAP',
    });

    const occ1 = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T01:00:00Z'),
    );
    const occ2 = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T01:00:00Z'),
      new Date('2025-01-01T02:00:00Z'),
    );
    const occ3 = await scheduleRepo.generateOccurrence(
      schedule.id,
      new Date('2025-01-01T02:00:00Z'),
      new Date('2025-01-01T03:00:00Z'),
    );

    const tasks = await pool.query(
      `SELECT * FROM tasks WHERE schedule_id = $1 ORDER BY scheduled_for ASC`,
      [schedule.id],
    );
    expect(tasks.rows.length).toBe(3);
    expect(tasks.rows[0].status).toBe('QUEUED');
    expect(tasks.rows[1].status).toBe('QUEUED');
    expect(tasks.rows[2].status).toBe('QUEUED');

    const leaseA = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseA.acquired).toBe(true);

    const leaseB = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseB.acquired).toBe(false);

    await scheduleRepo.releaseExecutionLease(schedule.id, leaseA.leaseToken!);

    const leaseC = await scheduleRepo.acquireExecutionLease(schedule.id, 60000);
    expect(leaseC.acquired).toBe(true);
  });

  it('10. Existing worker/task lease behavior unaffected by schedule lease', async () => {
    const { task } = await taskRepo.createTaskWithOutbox({
      name: 'immediate-task',
      priority: 'NORMAL',
      payload: { test: true },
      maxRetries: 3,
    });

    expect(task.status).toBe('QUEUED');
    expect(task.scheduleId).toBeUndefined();

    const processing = await taskRepo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-1',
    });
    expect(processing.status).toBe('PROCESSING');
    expect(processing.claimToken).toBeDefined();

    await taskRepo.renewClaim(task.id, processing.claimToken!);

    const completed = await taskRepo.transitionStatus(task.id, processing.version, 'COMPLETED', {
      completedAt: new Date(),
      claimToken: processing.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });
});

// --- Immediate tasks still work ---

describe('Existing immediate task behavior preserved (requires PostgreSQL)', () => {
  it('should create immediate tasks as QUEUED with outbox', async () => {
    const { task, outboxEvent } = await taskRepo.createTaskWithOutbox({
      name: 'immediate',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(task.status).toBe('QUEUED');
    expect(task.scheduledFor).toBeUndefined();
    expect(task.scheduleId).toBeUndefined();
    expect(outboxEvent.status).toBe('PENDING');
  });

  it('should still allow all existing task transitions', async () => {
    const { task } = await taskRepo.createTaskWithOutbox({
      name: 'transitions',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await taskRepo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
    });
    expect(processing.status).toBe('PROCESSING');

    const completed = await taskRepo.transitionStatus(task.id, 2, 'COMPLETED', {
      completedAt: new Date(),
      claimToken: processing.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });

  it('should still allow cancellation', async () => {
    const { task } = await taskRepo.createTaskWithOutbox({
      name: 'cancel-immediate',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const cancelled = await taskRepo.cancelTask(task.id);
    expect(cancelled.status).toBe('CANCELLED');
  });
});
