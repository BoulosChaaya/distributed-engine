import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals';
import {
  createProcessTask,
  computeRenewalInterval,
  validateScheduleLeaseDuration,
  ProcessTaskDeps,
} from '../process-task';

class MockDelayedError extends Error {
  constructor() {
    super('DelayedError');
    this.name = 'DelayedError';
  }
}

class MockClaimNotExpiredError extends Error {
  expiresAt: Date;
  constructor(expiresAt: Date) {
    super('Claim not expired');
    this.name = 'ClaimNotExpiredError';
    this.expiresAt = expiresAt;
  }
}

function makeMockSpan(): any {
  return {
    setAttribute: jest.fn(),
    setStatus: jest.fn(),
    recordException: jest.fn(),
    end: jest.fn(),
  };
}

function makeMockTracing(): ProcessTaskDeps['tracing'] {
  return {
    startTaskClaim: jest.fn().mockReturnValue(makeMockSpan()),
    startTaskComplete: jest.fn().mockReturnValue(makeMockSpan()),
    startTaskFail: jest.fn().mockReturnValue(makeMockSpan()),
    setSpanOk: jest.fn(),
    recordError: jest.fn(),
    endSpan: jest.fn(),
  };
}

function makeMockLogger(): ProcessTaskDeps['logger'] {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  };
}

function makeMockTaskRepo(overrides: Partial<ProcessTaskDeps['taskRepo']> = {}): ProcessTaskDeps['taskRepo'] {
  return {
    claimTtl: 30000,
    getTask: jest.fn<any>().mockResolvedValue(null),
    transitionStatus: jest.fn<any>().mockResolvedValue({ version: 2, claimToken: 'ct-1', retries: 0 }),
    reclaimStalledTask: jest.fn<any>().mockResolvedValue({ version: 2, claimToken: 'ct-1', retries: 0 }),
    renewClaim: jest.fn<any>().mockResolvedValue(undefined),
    ...overrides,
  };
}

function makeMockScheduleRepo(overrides: Partial<ProcessTaskDeps['scheduleRepo']> = {}): ProcessTaskDeps['scheduleRepo'] {
  return {
    getSchedule: jest.fn<any>().mockResolvedValue(null),
    acquireExecutionLease: jest.fn<any>().mockResolvedValue({ acquired: true, leaseToken: 'lt-1' }),
    renewExecutionLease: jest.fn<any>().mockResolvedValue(undefined),
    releaseExecutionLease: jest.fn<any>().mockResolvedValue(undefined),
    ...overrides,
  };
}

function makeMockJob(overrides: any = {}): any {
  return {
    id: 'job-1',
    token: 'job-token-1',
    attemptsMade: 0,
    opts: { attempts: 3 },
    data: { taskId: 'task-1', taskName: 'test-task' },
    moveToDelayed: jest.fn<any>().mockResolvedValue(undefined),
    ...overrides,
  };
}

function makeTask(overrides: any = {}) {
  return {
    id: 'task-1',
    name: 'test-task',
    status: 'QUEUED',
    version: 1,
    retries: 0,
    scheduleId: undefined,
    ...overrides,
  };
}

async function flushMicrotasks() {
  for (let i = 0; i < 20; i++) {
    await Promise.resolve();
  }
}

describe('Worker orchestration', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('computeRenewalInterval', () => {
    it('1. renewal cadence cannot be longer than safe fraction of schedule lease TTL', () => {
      const taskClaimTtl = 30000;
      const shortScheduleLease = 9000;
      const interval = computeRenewalInterval(taskClaimTtl, shortScheduleLease);
      expect(interval).toBe(Math.floor(shortScheduleLease / 3));
      expect(interval).toBeLessThanOrEqual(Math.floor(shortScheduleLease / 3));

      const longScheduleLease = 300000;
      const interval2 = computeRenewalInterval(taskClaimTtl, longScheduleLease);
      expect(interval2).toBe(Math.floor(taskClaimTtl / 3));
      expect(interval2).toBeLessThanOrEqual(Math.floor(taskClaimTtl / 3));
    });
  });

  describe('validateScheduleLeaseDuration', () => {
    it('rejects non-positive durations', () => {
      expect(() => validateScheduleLeaseDuration(0)).toThrow('must be a positive number');
      expect(() => validateScheduleLeaseDuration(-1)).toThrow('must be a positive number');
      expect(() => validateScheduleLeaseDuration(NaN)).toThrow('must be a positive number');
      expect(() => validateScheduleLeaseDuration(Infinity)).toThrow('must be a positive number');
    });

    it('rejects durations below 3000ms', () => {
      expect(() => validateScheduleLeaseDuration(1000)).toThrow('must be at least 3000ms');
      expect(() => validateScheduleLeaseDuration(2999)).toThrow('must be at least 3000ms');
    });

    it('accepts valid durations', () => {
      expect(() => validateScheduleLeaseDuration(3000)).not.toThrow();
      expect(() => validateScheduleLeaseDuration(300000)).not.toThrow();
    });
  });

  it('2. short configured schedule lease is renewed before expiry', async () => {
    const scheduleLeaseDurationMs = 3000;
    const taskClaimTtl = 30000;
    const renewalIntervalMs = computeRenewalInterval(taskClaimTtl, scheduleLeaseDurationMs);
    expect(renewalIntervalMs).toBe(1000);

    const renewClaim = jest.fn<any>().mockResolvedValue(undefined);
    const renewExecutionLease = jest.fn<any>().mockResolvedValue(undefined);

    let getTaskCallCount = 0;
    const taskRepo = makeMockTaskRepo({
      claimTtl: taskClaimTtl,
      renewClaim,
      getTask: jest.fn<any>().mockImplementation(() => {
        getTaskCallCount++;
        if (getTaskCallCount === 1) {
          return Promise.resolve(makeTask({ scheduleId: 'sched-1' }));
        }
        return Promise.resolve(makeTask({ scheduleId: 'sched-1', status: 'PROCESSING' }));
      }),
    });

    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'FORBID_OVERLAP',
      }),
      renewExecutionLease,
    });

    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs,
      workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    const span = makeMockSpan();

    const resultPromise = processTask(job, 'task-1', span);

    await flushMicrotasks();

    jest.advanceTimersByTime(renewalIntervalMs);
    await flushMicrotasks();

    jest.advanceTimersByTime(2000 - renewalIntervalMs);
    await flushMicrotasks();

    const result = await resultPromise;
    expect(result.status).toBe('COMPLETED');
    expect(renewClaim).toHaveBeenCalled();
    expect(renewExecutionLease).toHaveBeenCalledWith('sched-1', 'lt-1', scheduleLeaseDurationMs);
  });

  it('3. schedule lease renewal failure sets ownershipLost and prevents durable completion', async () => {
    const scheduleLeaseDurationMs = 3000;
    const renewalIntervalMs = 1000;

    const renewClaim = jest.fn<any>().mockResolvedValue(undefined);
    const renewExecutionLease = jest.fn<any>().mockRejectedValue(new Error('lease expired'));
    const transitionStatus = jest.fn<any>().mockResolvedValue({ version: 2, claimToken: 'ct-1', retries: 0 });

    let getTaskCallCount = 0;
    const taskRepo = makeMockTaskRepo({
      claimTtl: 30000,
      renewClaim,
      transitionStatus,
      getTask: jest.fn<any>().mockImplementation(() => {
        getTaskCallCount++;
        if (getTaskCallCount === 1) {
          return Promise.resolve(makeTask({ scheduleId: 'sched-1' }));
        }
        return Promise.resolve(makeTask({ scheduleId: 'sched-1', status: 'PROCESSING' }));
      }),
    });

    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'FORBID_OVERLAP',
      }),
      renewExecutionLease,
    });

    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs,
      workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    const span = makeMockSpan();

    const resultPromise = processTask(job, 'task-1', span);

    await flushMicrotasks();

    jest.advanceTimersByTime(renewalIntervalMs);
    await flushMicrotasks();

    jest.advanceTimersByTime(2000 - renewalIntervalMs);
    await flushMicrotasks();

    const result = await resultPromise;

    expect(result.status).toBe('SKIPPED');
    expect(result.reason).toBe('ownership_lost');

    const completionCalls = transitionStatus.mock.calls.filter(
      (call: any[]) => call[2] === 'COMPLETED',
    );
    expect(completionCalls).toHaveLength(0);
  });

  it('4. FORBID_OVERLAP contention defers through moveToDelayed + DelayedError', async () => {
    const leaseExpiresAt = new Date(Date.now() + 60000);
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask({ scheduleId: 'sched-1' })),
    });
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'FORBID_OVERLAP',
      }),
      acquireExecutionLease: jest.fn<any>().mockResolvedValue({
        acquired: false,
        expiresAt: leaseExpiresAt,
      }),
    });

    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    const span = makeMockSpan();

    await expect(processTask(job, 'task-1', span)).rejects.toThrow('DelayedError');

    expect(job.moveToDelayed).toHaveBeenCalledWith(
      leaseExpiresAt.getTime() + 2000,
      job.token,
    );
  });

  it('5. overlap deferral occurs before QUEUED to PROCESSING transition', async () => {
    const transitionStatus = jest.fn<any>();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask({ scheduleId: 'sched-1', status: 'QUEUED' })),
      transitionStatus,
    });
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'FORBID_OVERLAP',
      }),
      acquireExecutionLease: jest.fn<any>().mockResolvedValue({
        acquired: false,
        expiresAt: new Date(Date.now() + 60000),
      }),
    });

    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    await expect(processTask(job, 'task-1', makeMockSpan())).rejects.toThrow('DelayedError');

    expect(transitionStatus).not.toHaveBeenCalled();
  });

  it('6. overlap deferral does not increment or persist task retry count', async () => {
    const transitionStatus = jest.fn<any>();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask({
        scheduleId: 'sched-1',
        status: 'QUEUED',
        retries: 0,
      })),
      transitionStatus,
    });
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'FORBID_OVERLAP',
      }),
      acquireExecutionLease: jest.fn<any>().mockResolvedValue({
        acquired: false,
        expiresAt: new Date(Date.now() + 60000),
      }),
    });

    const metrics = { jobsCompleted: 0, jobsFailed: 0 };
    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics: metrics,
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    await expect(processTask(job, 'task-1', makeMockSpan())).rejects.toThrow('DelayedError');

    expect(transitionStatus).not.toHaveBeenCalled();
    expect(metrics.jobsFailed).toBe(0);
  });

  it('7. ALLOW_OVERLAP does not acquire schedule execution lease', async () => {
    const acquireExecutionLease = jest.fn<any>();

    let getTaskCallCount = 0;
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockImplementation(() => {
        getTaskCallCount++;
        if (getTaskCallCount === 1) {
          return Promise.resolve(makeTask({ scheduleId: 'sched-1' }));
        }
        return Promise.resolve(makeTask({ scheduleId: 'sched-1', status: 'PROCESSING' }));
      }),
    });
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({
        id: 'sched-1',
        overlapPolicy: 'ALLOW_OVERLAP',
      }),
      acquireExecutionLease,
    });

    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    const resultPromise = processTask(job, 'task-1', makeMockSpan());

    await flushMicrotasks();
    jest.advanceTimersByTime(2000);
    await flushMicrotasks();

    const result = await resultPromise;
    expect(result.status).toBe('COMPLETED');
    expect(acquireExecutionLease).not.toHaveBeenCalled();
  });

  it('8. ordinary non-scheduled tasks remain unaffected by overlap logic', async () => {
    const getSchedule = jest.fn<any>();
    const acquireExecutionLease = jest.fn<any>();
    const releaseExecutionLease = jest.fn<any>();

    let getTaskCallCount = 0;
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockImplementation(() => {
        getTaskCallCount++;
        if (getTaskCallCount === 1) {
          return Promise.resolve(makeTask({ scheduleId: undefined }));
        }
        return Promise.resolve(makeTask({ scheduleId: undefined, status: 'PROCESSING' }));
      }),
    });
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule,
      acquireExecutionLease,
      releaseExecutionLease,
    });

    const metrics = { jobsCompleted: 0, jobsFailed: 0 };
    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo,
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics: metrics,
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
    });

    const job = makeMockJob();
    const resultPromise = processTask(job, 'task-1', makeMockSpan());

    await flushMicrotasks();
    jest.advanceTimersByTime(2000);
    await flushMicrotasks();

    const result = await resultPromise;

    expect(result.status).toBe('COMPLETED');
    expect(getSchedule).not.toHaveBeenCalled();
    expect(acquireExecutionLease).not.toHaveBeenCalled();
    expect(releaseExecutionLease).not.toHaveBeenCalled();
    expect(metrics.jobsCompleted).toBe(1);
  });
});
