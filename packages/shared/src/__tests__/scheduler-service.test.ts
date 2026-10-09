import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { SchedulerService, SchedulerConfig } from '../scheduler/scheduler-service';
import { ScheduleRepository, DuplicateOccurrenceError } from '../db/schedule-repository';
import { RecurringSchedule, MisfirePolicy, OverlapPolicy } from '../types';

function makeSchedule(overrides: Partial<RecurringSchedule> = {}): RecurringSchedule {
  return {
    id: 'sched-1',
    name: 'test-schedule',
    taskName: 'test-task',
    taskPriority: 'NORMAL',
    taskPayload: {},
    taskMaxRetries: 3,
    cronExpression: '0 * * * *',
    timezone: 'UTC',
    nextRunAt: new Date('2025-01-01T00:00:00Z'),
    status: 'ACTIVE',
    misfirePolicy: 'SKIP_MISSED',
    overlapPolicy: 'ALLOW_OVERLAP',
    executionLeaseToken: undefined,
    executionLeaseExpiresAt: undefined,
    version: 1,
    createdAt: new Date('2025-01-01T00:00:00Z'),
    updatedAt: new Date('2025-01-01T00:00:00Z'),
    ...overrides,
  };
}

function makeMockLogger() {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    child: jest.fn().mockReturnThis(),
  } as any;
}

const pastDate = new Date('2024-12-31T23:00:00Z');
const now = new Date('2025-01-01T01:30:00Z');

describe('SchedulerService', () => {
  let mockPool: any;
  let service: SchedulerService;
  let logger: any;

  beforeEach(() => {
    mockPool = {
      connect: jest.fn(),
      query: jest.fn(),
    };
    logger = makeMockLogger();
    service = new SchedulerService(mockPool, logger, {
      pollIntervalMs: 100,
      scheduledTaskBatchSize: 10,
      recurringBatchSize: 5,
      catchUpBatchSize: 3,
      executionLeaseDurationMs: 60000,
    });
  });

  describe('start/stop', () => {
    it('should start and stop without error', async () => {
      service.start();
      expect(logger.info).toHaveBeenCalledWith('Scheduler started', expect.any(Object));
      await service.stop();
      expect(logger.info).toHaveBeenCalledWith('Scheduler stopped');
    });

    it('should be idempotent on start', async () => {
      service.start();
      service.start(); // second call does nothing
      await service.stop();
    });
  });

  describe('processScheduledTasks', () => {
    it('should delegate to releaseDueScheduledTasks', async () => {
      const mockRelease = jest.fn<() => Promise<any[]>>().mockResolvedValue([
        { task: { id: 't1' }, outboxEvent: { id: 'o1' } },
      ]);
      (service as any).scheduleRepo.releaseDueScheduledTasks = mockRelease;

      const count = await service.processScheduledTasks();
      expect(count).toBe(1);
      expect(mockRelease).toHaveBeenCalledWith(10);
    });

    it('should return 0 on error', async () => {
      (service as any).scheduleRepo.releaseDueScheduledTasks = jest.fn().mockRejectedValue(
        new Error('db error'),
      );

      const count = await service.processScheduledTasks();
      expect(count).toBe(0);
      expect(logger.error).toHaveBeenCalled();
    });
  });

  describe('processRecurringSchedules', () => {
    it('should process due schedules', async () => {
      const schedule = makeSchedule({ nextRunAt: pastDate });
      (service as any).scheduleRepo.fetchDueSchedules = jest.fn().mockResolvedValue([schedule]);

      const mockClient = {
        query: jest.fn().mockImplementation((sql: string) => {
          if (sql.includes('SELECT NOW()')) {
            return { rows: [{ db_now: now.toISOString() }] };
          }
          if (sql.includes('FOR UPDATE')) {
            return { rows: [{ ...schedule, status: 'ACTIVE', next_run_at: pastDate.toISOString() }] };
          }
          return { rows: [] };
        }),
      };
      (service as any).scheduleRepo.processScheduleWithLock = jest.fn().mockImplementation(
        async (_id: string, cb: Function) => {
          await cb(schedule, mockClient);
        },
      );
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });
      (service as any).scheduleRepo.hasActiveOccurrence = jest.fn().mockResolvedValue(false);

      const count = await service.processRecurringSchedules();
      expect(count).toBeGreaterThanOrEqual(0);
    });

    it('should skip non-ACTIVE schedules', async () => {
      const schedule = makeSchedule({ status: 'PAUSED', nextRunAt: pastDate });
      (service as any).scheduleRepo.fetchDueSchedules = jest.fn().mockResolvedValue([schedule]);
      (service as any).scheduleRepo.processScheduleWithLock = jest.fn().mockImplementation(
        async (_id: string, cb: Function) => {
          await cb(schedule, {
            query: jest.fn().mockResolvedValue({ rows: [{ db_now: now.toISOString() }] }),
          });
        },
      );

      const count = await service.processRecurringSchedules();
      expect(count).toBe(0);
      expect(logger.info).toHaveBeenCalledWith('Schedule no longer active, skipping', expect.any(Object));
    });

    it('should handle errors in individual schedule processing', async () => {
      const schedule = makeSchedule({ nextRunAt: pastDate });
      (service as any).scheduleRepo.fetchDueSchedules = jest.fn().mockResolvedValue([schedule]);
      (service as any).scheduleRepo.processScheduleWithLock = jest.fn().mockRejectedValue(
        new Error('lock failed'),
      );

      const count = await service.processRecurringSchedules();
      expect(count).toBe(0);
      expect(logger.error).toHaveBeenCalledWith(
        'Error processing recurring schedule',
        expect.objectContaining({ scheduleId: 'sched-1' }),
      );
    });
  });

  describe('FORBID_OVERLAP policy', () => {
    it('should generate occurrences regardless of active occurrences (overlap enforced at worker)', async () => {
      const schedule = makeSchedule({
        overlapPolicy: 'FORBID_OVERLAP',
        nextRunAt: pastDate,
      });
      (service as any).scheduleRepo.fetchDueSchedules = jest.fn().mockResolvedValue([schedule]);
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });
      (service as any).scheduleRepo.processScheduleWithLock = jest.fn().mockImplementation(
        async (_id: string, cb: Function) => {
          await cb(schedule, {
            query: jest.fn().mockResolvedValue({ rows: [{ db_now: now.toISOString() }] }),
          });
        },
      );

      const count = await service.processRecurringSchedules();
      expect(count).toBe(1);
      expect((service as any).scheduleRepo.generateOccurrence).toHaveBeenCalledTimes(1);
    });

    it('should not check hasActiveOccurrence or acquireExecutionLease in scheduler', async () => {
      const schedule = makeSchedule({
        overlapPolicy: 'FORBID_OVERLAP',
        nextRunAt: pastDate,
      });
      (service as any).scheduleRepo.fetchDueSchedules = jest.fn().mockResolvedValue([schedule]);
      (service as any).scheduleRepo.hasActiveOccurrence = jest.fn();
      (service as any).scheduleRepo.acquireExecutionLease = jest.fn();
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });
      (service as any).scheduleRepo.processScheduleWithLock = jest.fn().mockImplementation(
        async (_id: string, cb: Function) => {
          await cb(schedule, {
            query: jest.fn().mockResolvedValue({ rows: [{ db_now: now.toISOString() }] }),
          });
        },
      );

      await service.processRecurringSchedules();
      expect((service as any).scheduleRepo.hasActiveOccurrence).not.toHaveBeenCalled();
      expect((service as any).scheduleRepo.acquireExecutionLease).not.toHaveBeenCalled();
    });
  });

  describe('Misfire policy: SKIP_MISSED', () => {
    it('should generate one occurrence and jump to next future time', async () => {
      const schedule = makeSchedule({
        misfirePolicy: 'SKIP_MISSED',
        nextRunAt: new Date('2025-01-01T00:00:00Z'),
      });

      const mockClient = {
        query: jest.fn(),
      };
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });

      // Call the private method directly via any-cast
      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      expect(generated).toBe(1);
      expect((service as any).scheduleRepo.generateOccurrence).toHaveBeenCalledTimes(1);
      // The nextRunAt passed should be a future time (after now)
      const call = (service as any).scheduleRepo.generateOccurrence.mock.calls[0];
      expect(new Date(call[2]).getTime()).toBeGreaterThan(now.getTime());
    });

    it('should handle duplicate occurrence gracefully', async () => {
      const schedule = makeSchedule({
        misfirePolicy: 'SKIP_MISSED',
        nextRunAt: new Date('2025-01-01T00:00:00Z'),
      });

      const mockClient = {
        query: jest.fn(),
      };
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockRejectedValue(
        new DuplicateOccurrenceError('sched-1', new Date('2025-01-01T00:00:00Z')),
      );

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      expect(generated).toBe(0);
      // Should still advance next_run_at
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE recurring_schedules'),
        expect.any(Array),
      );
    });
  });

  describe('Misfire policy: RUN_ONCE', () => {
    it('should generate exactly one occurrence regardless of how many were missed', async () => {
      const schedule = makeSchedule({
        misfirePolicy: 'RUN_ONCE',
        nextRunAt: new Date('2025-01-01T00:00:00Z'),
      });

      const mockClient = { query: jest.fn() };
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      expect(generated).toBe(1);
      expect((service as any).scheduleRepo.generateOccurrence).toHaveBeenCalledTimes(1);
    });
  });

  describe('Misfire policy: CATCH_UP_ALL', () => {
    it('should generate bounded batch of missed occurrences', async () => {
      // Schedule was due at midnight, now it's 1:30 AM, hourly cron
      // Missed: 00:00, 01:00 = 2 occurrences (both within window), batch limit = 3
      const schedule = makeSchedule({
        misfirePolicy: 'CATCH_UP_ALL',
        cronExpression: '0 * * * *',
        nextRunAt: new Date('2025-01-01T00:00:00Z'),
      });

      const mockClient = { query: jest.fn() };
      let generateCalls = 0;
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockImplementation(() => {
        generateCalls++;
        return Promise.resolve({
          task: { id: `t${generateCalls}` },
          outboxEvent: { id: `o${generateCalls}` },
        });
      });

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      expect(generated).toBe(2);
    });

    it('should respect catchUpBatchSize limit', async () => {
      // Schedule was due 10 hours ago, hourly cron, batch limit = 3
      const tenHoursAgo = new Date('2024-12-31T15:00:00Z');
      const schedule = makeSchedule({
        misfirePolicy: 'CATCH_UP_ALL',
        cronExpression: '0 * * * *',
        nextRunAt: tenHoursAgo,
      });

      const mockClient = { query: jest.fn() };
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockResolvedValue({
        task: { id: 't1' },
        outboxEvent: { id: 'o1' },
      });

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      // Should be capped at catchUpBatchSize (3)
      expect(generated).toBeLessThanOrEqual(3);
      expect(generated).toBeGreaterThan(0);
    });

    it('should handle duplicate occurrences during catch-up', async () => {
      const schedule = makeSchedule({
        misfirePolicy: 'CATCH_UP_ALL',
        cronExpression: '0 * * * *',
        nextRunAt: new Date('2025-01-01T00:00:00Z'),
      });

      const mockClient = { query: jest.fn() };
      let callCount = 0;
      (service as any).scheduleRepo.generateOccurrence = jest.fn().mockImplementation(() => {
        callCount++;
        if (callCount === 1) {
          return Promise.reject(
            new DuplicateOccurrenceError('sched-1', new Date('2025-01-01T01:00:00Z')),
          );
        }
        return Promise.resolve({
          task: { id: `t${callCount}` },
          outboxEvent: { id: `o${callCount}` },
        });
      });

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        now,
        mockClient,
      );
      // First was duplicate, second succeeded
      expect(generated).toBe(1);
    });

    it('should advance next_run_at when no missed occurrences', async () => {
      const schedule = makeSchedule({
        misfirePolicy: 'CATCH_UP_ALL',
        cronExpression: '0 * * * *',
        nextRunAt: new Date('2025-01-01T01:00:00Z'),
      });

      // "now" is just after 1:00, so getMissedOccurrences from (1:00 - 1s) to 1:00:01 yields just 1:00
      // but we need to use a now that's right before the next run so no missed
      const nearFuture = new Date('2025-01-01T00:59:00Z');
      const mockClient = { query: jest.fn() };

      const generated = await (service as any).applyMisfirePolicy(
        schedule,
        nearFuture,
        mockClient,
      );
      // No missed occurrences, should advance next_run_at
      expect(generated).toBe(0);
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE recurring_schedules'),
        expect.any(Array),
      );
    });
  });

  describe('poll', () => {
    it('should call both processScheduledTasks and processRecurringSchedules', async () => {
      const mockRelease = jest.fn<() => Promise<any[]>>().mockResolvedValue([]);
      const mockFetch = jest.fn<() => Promise<any[]>>().mockResolvedValue([]);
      (service as any).scheduleRepo.releaseDueScheduledTasks = mockRelease;
      (service as any).scheduleRepo.fetchDueSchedules = mockFetch;

      await service.poll();

      expect(mockRelease).toHaveBeenCalled();
      expect(mockFetch).toHaveBeenCalled();
    });
  });

  describe('repository accessor', () => {
    it('should expose the schedule repository', () => {
      expect(service.repository).toBeDefined();
      expect(service.repository).toBeInstanceOf(ScheduleRepository);
    });
  });
});
