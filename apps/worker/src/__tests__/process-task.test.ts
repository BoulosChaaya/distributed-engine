import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals';
import {
  createProcessTask,
  computeRenewalInterval,
  validateScheduleLeaseDuration,
  ProcessTaskDeps,
} from '../process-task';

// Stand-in handler with the timing the orchestration tests were written against.
const sleepExecute = () => new Promise<unknown>((resolve) => setTimeout(() => resolve({ slept: 2000 }), 2000));

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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
      execute: sleepExecute,
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
  describe('retry budget is owned by PostgreSQL', () => {
    class MockUnrecoverableError extends Error {
      constructor(message: string) {
        super(message);
        this.name = 'UnrecoverableError';
      }
    }

    function setup(retriesInDb: number, maxRetries: number) {
      const transitions: Array<{ status: string; opts: any }> = [];
      const taskRepo = makeMockTaskRepo({
        getTask: jest.fn<any>().mockImplementation(() =>
          Promise.resolve(makeTask({ status: 'QUEUED', retries: retriesInDb, maxRetries })),
        ),
        transitionStatus: jest.fn<any>().mockImplementation((_id: string, _v: number, status: string, opts: any) => {
          transitions.push({ status, opts });
          if (status === 'PROCESSING') return Promise.resolve({ version: 2, claimToken: 'ct-1', retries: retriesInDb });
          if (status === 'COMPLETED') return Promise.reject(new Error('handler blew up'));
          return Promise.resolve({ version: 3 });
        }),
      });
      const processTask = createProcessTask({
        taskRepo,
        scheduleRepo: makeMockScheduleRepo(),
        tracing: makeMockTracing(),
        logger: makeMockLogger(),
        workerId: 'w-1',
        scheduleLeaseDurationMs: 300000,
        leaseDeferralMarginMs: 2000,
        renewalIntervalMs: 10000,
        workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
        ClaimNotExpiredError: MockClaimNotExpiredError,
        DelayedError: MockDelayedError,
      execute: sleepExecute,
        UnrecoverableError: MockUnrecoverableError,
      });
      return { processTask, transitions };
    }

    async function run(processTask: ReturnType<typeof createProcessTask>, job: any) {
      const promise = processTask(job, 'task-1', makeMockSpan());
      promise.catch(() => {});
      await flushMicrotasks();
      jest.advanceTimersByTime(2000);
      await flushMicrotasks();
      return promise;
    }

    it('fails the task and stops BullMQ when the database budget is spent, even if the job has attempts left', async () => {
      // A job re-published by the reconciler starts at attemptsMade = 0.
      const { processTask, transitions } = setup(3, 3);
      const job = makeMockJob({ attemptsMade: 0, opts: { attempts: 4 } });

      await expect(run(processTask, job)).rejects.toBeInstanceOf(MockUnrecoverableError);
      const last = transitions[transitions.length - 1];
      expect(last.status).toBe('FAILED');
      expect(last.opts.retries).toBe(4);
    });

    it('requeues with a normal retryable error while budget remains', async () => {
      const { processTask, transitions } = setup(1, 3);
      const job = makeMockJob({ attemptsMade: 1, opts: { attempts: 4 } });

      const err = await run(processTask, job).catch((e) => e);
      expect(err).not.toBeInstanceOf(MockUnrecoverableError);
      expect((err as Error).message).toBe('handler blew up');
      const last = transitions[transitions.length - 1];
      expect(last.status).toBe('QUEUED');
      expect(last.opts.retries).toBe(2);
    });

    it('still honours the BullMQ attempt limit on the final attempt', async () => {
      const { processTask, transitions } = setup(1, 3);
      const job = makeMockJob({ attemptsMade: 3, opts: { attempts: 4 } });

      const err = await run(processTask, job).catch((e) => e);
      expect(err).not.toBeInstanceOf(MockUnrecoverableError);
      expect(transitions[transitions.length - 1].status).toBe('FAILED');
    });
  });
  describe('task handlers', () => {
    class MockUnrecoverableError extends Error {
      constructor(message: string) {
        super(message);
        this.name = 'UnrecoverableError';
      }
    }

    function build(execute: ProcessTaskDeps['execute'], overrides: Partial<ProcessTaskDeps['taskRepo']> = {}) {
      const transitions: Array<{ status: string; opts: any }> = [];
      const taskRepo = makeMockTaskRepo({
        getTask: jest.fn<any>().mockImplementation(() =>
          Promise.resolve(makeTask({ name: 'echo', payload: { hello: 'world' }, retries: 0, maxRetries: 3 })),
        ),
        transitionStatus: jest.fn<any>().mockImplementation((_id: string, _v: number, status: string, opts: any) => {
          transitions.push({ status, opts });
          return Promise.resolve({ version: 2, claimToken: 'ct-1', retries: 0 });
        }),
        ...overrides,
      });
      const processTask = createProcessTask({
        taskRepo,
        scheduleRepo: makeMockScheduleRepo(),
        tracing: makeMockTracing(),
        logger: makeMockLogger(),
        workerId: 'w-1',
        scheduleLeaseDurationMs: 300000,
        leaseDeferralMarginMs: 2000,
        renewalIntervalMs: 1000,
        workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
        ClaimNotExpiredError: MockClaimNotExpiredError,
        DelayedError: MockDelayedError,
        UnrecoverableError: MockUnrecoverableError,
        execute,
      });
      return { processTask, transitions, taskRepo };
    }

    it('passes name, payload and attempt to the handler and stores its output', async () => {
      const execute = jest.fn<any>().mockResolvedValue({ echo: { hello: 'world' } });
      const { processTask, transitions } = build(execute);

      const result = await processTask(makeMockJob(), 'task-1', makeMockSpan());

      expect(result.status).toBe('COMPLETED');
      const ctx = execute.mock.calls[0][0] as any;
      expect(ctx).toMatchObject({ taskId: 'task-1', name: 'echo', payload: { hello: 'world' }, attempt: 1 });
      expect(ctx.signal).toBeInstanceOf(AbortSignal);
      const done = transitions.find((t) => t.status === 'COMPLETED')!;
      expect(done.opts.result).toEqual({ processedBy: 'w-1', output: { echo: { hello: 'world' } } });
      expect(done.opts.error).toBeNull();
    });

    it('fails a permanent error on the first attempt and stops BullMQ retries', async () => {
      const permanent = new Error('No handler registered for task type "nope"');
      permanent.name = 'NonRetryableTaskError';
      const { processTask, transitions } = build(jest.fn<any>().mockRejectedValue(permanent));

      await expect(processTask(makeMockJob({ attemptsMade: 0, opts: { attempts: 4 } }), 'task-1', makeMockSpan()))
        .rejects.toBeInstanceOf(MockUnrecoverableError);
      const last = transitions[transitions.length - 1];
      expect(last.status).toBe('FAILED');
      expect(last.opts.error).toMatch(/No handler registered/);
    });

    it('requeues a transient error while retries remain', async () => {
      const { processTask, transitions } = build(jest.fn<any>().mockRejectedValue(new Error('upstream responded 503')));

      const err = await processTask(makeMockJob({ attemptsMade: 0, opts: { attempts: 4 } }), 'task-1', makeMockSpan()).catch((e) => e);
      expect(err).not.toBeInstanceOf(MockUnrecoverableError);
      expect(transitions[transitions.length - 1].status).toBe('QUEUED');
    });

    it('never runs a BLOCKED or SCHEDULED task, even if a job for it shows up', async () => {
      for (const status of ['BLOCKED', 'SCHEDULED']) {
        const execute = jest.fn<any>();
        const transitionStatus = jest.fn<any>();
        const { processTask } = build(execute, {
          getTask: jest.fn<any>().mockResolvedValue(makeTask({ status })),
          transitionStatus,
        });
        const result = await processTask(makeMockJob(), 'task-1', makeMockSpan());
        expect(result).toMatchObject({ status: 'SKIPPED', reason: 'not_runnable' });
        expect(execute).not.toHaveBeenCalled();
        expect(transitionStatus).not.toHaveBeenCalled();
      }
    });

    it('gives a workflow step its parents\' outputs', async () => {
      const execute = jest.fn<any>().mockResolvedValue({ ok: true });
      const getUpstreamOutputs = jest.fn<any>().mockResolvedValue({ left: 1, right: 2 });
      const { processTask } = build(execute, {
        getTask: jest.fn<any>().mockResolvedValue(makeTask({ name: 'echo', workflowId: 'wf-1', stepKey: 'join', maxRetries: 3 })),
        getUpstreamOutputs,
      });
      await processTask(makeMockJob(), 'task-1', makeMockSpan());
      expect(getUpstreamOutputs).toHaveBeenCalledWith('task-1');
      expect((execute.mock.calls[0][0] as any).upstream).toEqual({ left: 1, right: 2 });
    });

    it('aborts the handler and writes nothing when the lease is lost mid-run', async () => {
      let seenSignal: AbortSignal | undefined;
      const execute = jest.fn<any>().mockImplementation((ctx: any) => {
        seenSignal = ctx.signal;
        return new Promise((_resolve, reject) => {
          ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason));
        });
      });
      const { processTask, transitions } = build(execute, {
        renewClaim: jest.fn<any>().mockRejectedValue(new Error('claim token mismatch')),
      });

      const promise = processTask(makeMockJob(), 'task-1', makeMockSpan());
      await flushMicrotasks();
      jest.advanceTimersByTime(1000);
      await flushMicrotasks();
      const result = await promise;

      expect(seenSignal!.aborted).toBe(true);
      expect(result).toMatchObject({ status: 'SKIPPED', reason: 'ownership_lost' });
      expect(transitions.map((t) => t.status)).toEqual(['PROCESSING']);
    });
  });
});

describe('Worker shutdown (found by tracing: a deploy charged a retry)', () => {
  const waitForAbort = ({ signal }: { signal: AbortSignal }) =>
    new Promise<unknown>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));

  function setup(execute: ProcessTaskDeps['execute']) {
    const shutdown = new AbortController();
    const transitionStatus = jest.fn<any>()
      .mockResolvedValueOnce({ version: 2, claimToken: 'ct-1', retries: 0 })
      .mockResolvedValue({ version: 3, claimToken: undefined, retries: 0 });
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask({ maxRetries: 0 })),
      transitionStatus,
    });
    const workerMetrics = { jobsCompleted: 0, jobsFailed: 0 };
    const processTask = createProcessTask({
      taskRepo,
      scheduleRepo: makeMockScheduleRepo(),
      tracing: makeMockTracing(),
      logger: makeMockLogger(),
      workerId: 'w-1',
      scheduleLeaseDurationMs: 300000,
      leaseDeferralMarginMs: 2000,
      renewalIntervalMs: 10000,
      workerMetrics,
      ClaimNotExpiredError: MockClaimNotExpiredError,
      DelayedError: MockDelayedError,
      shutdownSignal: shutdown.signal,
      execute,
    });
    return { shutdown, transitionStatus, workerMetrics, processTask };
  }

  it('hands a still-running task back as QUEUED without charging a retry', async () => {
    const { shutdown, transitionStatus, workerMetrics, processTask } = setup(waitForAbort);
    const job = makeMockJob();

    const run = processTask(job, 'task-1', makeMockSpan());
    await flushMicrotasks();
    shutdown.abort();

    await expect(run).rejects.toThrow('DelayedError');
    const release = transitionStatus.mock.calls[1] as [string, number, string, Record<string, unknown>];
    expect(release[2]).toBe('QUEUED');
    expect(release[3].claimToken).toBe('ct-1');
    expect(release[3]).not.toHaveProperty('retries');
    expect(job.moveToDelayed).toHaveBeenCalledTimes(1);
    expect(workerMetrics.jobsFailed).toBe(0);
  });

  it('a task that finishes despite the shutdown is still recorded COMPLETED', async () => {
    const { shutdown, transitionStatus, processTask } = setup(async () => ({ done: true }));
    shutdown.abort();

    const result = await processTask(makeMockJob(), 'task-1', makeMockSpan());

    expect(result.status).toBe('COMPLETED');
    expect((transitionStatus.mock.calls[1] as unknown[])[2]).toBe('COMPLETED');
  });

  it('if the release write fails, the job is still put back (the lease covers the task)', async () => {
    const { shutdown, transitionStatus, processTask } = setup(waitForAbort);
    transitionStatus.mockReset();
    transitionStatus
      .mockResolvedValueOnce({ version: 2, claimToken: 'ct-1', retries: 0 })
      .mockRejectedValue(new Error('PostgreSQL down'));
    const job = makeMockJob();

    const run = processTask(job, 'task-1', makeMockSpan());
    await flushMicrotasks();
    shutdown.abort();

    await expect(run).rejects.toThrow('DelayedError');
    expect(job.moveToDelayed).toHaveBeenCalledTimes(1);
  });
});
