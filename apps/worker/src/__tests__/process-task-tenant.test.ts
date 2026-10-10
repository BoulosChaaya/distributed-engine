import { describe, it, expect, beforeEach, jest, afterEach } from '@jest/globals';
import {
  createProcessTask,
  ProcessTaskDeps,
  TenantRepoLike,
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

function makeMockTenantRepo(overrides: Partial<TenantRepoLike> = {}): TenantRepoLike {
  return {
    getTenant: jest.fn<any>().mockResolvedValue({ id: 'tenant-1', status: 'ACTIVE' }),
    getEffectiveLimits: jest.fn<any>().mockResolvedValue({ maxConcurrentExecutions: 5 }),
    acquireConcurrencyLease: jest.fn<any>().mockResolvedValue({ acquired: true, lease: { leaseToken: 'cl-1' } }),
    renewConcurrencyLease: jest.fn<any>().mockResolvedValue(undefined),
    releaseConcurrencyLease: jest.fn<any>().mockResolvedValue(undefined),
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
    tenantId: 'tenant-1',
    scheduleId: undefined,
    ...overrides,
  };
}

async function flushMicrotasks() {
  for (let i = 0; i < 20; i++) {
    await Promise.resolve();
  }
}

function makeDeps(overrides: Partial<ProcessTaskDeps> = {}): ProcessTaskDeps {
  return {
    taskRepo: makeMockTaskRepo(),
    scheduleRepo: makeMockScheduleRepo(),
    tenantRepo: makeMockTenantRepo(),
    tracing: makeMockTracing(),
    logger: makeMockLogger(),
    workerId: 'w-test',
    scheduleLeaseDurationMs: 300000,
    leaseDeferralMarginMs: 2000,
    renewalIntervalMs: 10000,
    workerMetrics: { jobsCompleted: 0, jobsFailed: 0 },
    ClaimNotExpiredError: MockClaimNotExpiredError,
    DelayedError: MockDelayedError,
    ...overrides,
  };
}

describe('processTask — tenant integration', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should skip task when tenant is SUSPENDED', async () => {
    const tenantRepo = makeMockTenantRepo({
      getTenant: jest.fn<any>().mockResolvedValue({ id: 'tenant-1', status: 'SUSPENDED' }),
    });
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask()),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const result = await processTask(makeMockJob(), 'task-1', makeMockSpan());
    expect(result.status).toBe('SKIPPED');
    expect(result.reason).toBe('tenant_suspended');
  });

  it('should acquire concurrency lease for tenant tasks', async () => {
    const tenantRepo = makeMockTenantRepo();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask())
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING' })),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();
    jest.advanceTimersByTime(2100);
    await flushMicrotasks();
    const result = await resultPromise;

    expect(result.status).toBe('COMPLETED');
    expect(tenantRepo.acquireConcurrencyLease).toHaveBeenCalledWith(
      'tenant-1', 'task-1', 'w-test', 5, expect.any(Number),
    );
  });

  it('should defer when concurrency limit reached (without consuming retry)', async () => {
    const tenantRepo = makeMockTenantRepo({
      acquireConcurrencyLease: jest.fn<any>().mockResolvedValue({ acquired: false }),
    });
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask()),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);
    const job = makeMockJob();

    await expect(processTask(job, 'task-1', makeMockSpan()))
      .rejects.toThrow(MockDelayedError);

    expect(job.moveToDelayed).toHaveBeenCalled();
    // Verify no transition to FAILED or QUEUED (retry not consumed)
    expect(taskRepo.transitionStatus).not.toHaveBeenCalled();
  });

  it('should release schedule lease when concurrency deferral happens', async () => {
    const scheduleRepo = makeMockScheduleRepo({
      getSchedule: jest.fn<any>().mockResolvedValue({ id: 'sched-1', overlapPolicy: 'FORBID_OVERLAP' }),
    });
    const tenantRepo = makeMockTenantRepo({
      acquireConcurrencyLease: jest.fn<any>().mockResolvedValue({ acquired: false }),
    });
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask({ scheduleId: 'sched-1' })),
    });
    const deps = makeDeps({ taskRepo, scheduleRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    await expect(processTask(makeMockJob(), 'task-1', makeMockSpan()))
      .rejects.toThrow(MockDelayedError);

    expect(scheduleRepo.releaseExecutionLease).toHaveBeenCalledWith('sched-1', 'lt-1');
  });

  it('should release concurrency lease on successful completion', async () => {
    const tenantRepo = makeMockTenantRepo();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask())
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING' })),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();
    jest.advanceTimersByTime(2100);
    await flushMicrotasks();
    await resultPromise;

    expect(tenantRepo.releaseConcurrencyLease).toHaveBeenCalledWith('cl-1');
  });

  it('should release concurrency lease on failure (finally block)', async () => {
    const tenantRepo = makeMockTenantRepo();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask())
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING' })),
      transitionStatus: jest.fn<any>()
        .mockResolvedValueOnce({ version: 2, claimToken: 'ct-1', retries: 0 })
        .mockRejectedValueOnce(new Error('completion failed')),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();
    jest.advanceTimersByTime(2100);
    await flushMicrotasks();
    await expect(resultPromise).rejects.toThrow('completion failed');

    expect(tenantRepo.releaseConcurrencyLease).toHaveBeenCalled();
  });

  it('should release concurrency lease on claim transition failure', async () => {
    const tenantRepo = makeMockTenantRepo();
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>().mockResolvedValue(makeTask()),
      transitionStatus: jest.fn<any>().mockRejectedValue(new Error('transition error')),
    });
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const result = await processTask(makeMockJob(), 'task-1', makeMockSpan());
    expect(result.status).toBe('SKIPPED');
    expect(result.reason).toBe('transition_failed');
    expect(tenantRepo.releaseConcurrencyLease).toHaveBeenCalled();
  });

  it('should renew concurrency lease in renewal timer', async () => {
    const tenantRepo = makeMockTenantRepo();
    const renewalIntervalMs = 1000;
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask())
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING' })),
    });
    const deps = makeDeps({
      taskRepo,
      tenantRepo,
      renewalIntervalMs,
    });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();

    jest.advanceTimersByTime(renewalIntervalMs);
    await flushMicrotasks();

    jest.advanceTimersByTime(2000 - renewalIntervalMs);
    await flushMicrotasks();

    const result = await resultPromise;
    expect(result.status).toBe('COMPLETED');
    expect(tenantRepo.renewConcurrencyLease).toHaveBeenCalled();
  });

  it('should work without tenantRepo (backward compatible)', async () => {
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask({ tenantId: undefined }))
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING', tenantId: undefined })),
    });
    const deps = makeDeps({ taskRepo, tenantRepo: undefined });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();
    jest.advanceTimersByTime(2100);
    await flushMicrotasks();
    const result = await resultPromise;

    expect(result.status).toBe('COMPLETED');
  });

  it('should work without tenantId on task (backward compatible)', async () => {
    const taskRepo = makeMockTaskRepo({
      getTask: jest.fn<any>()
        .mockResolvedValueOnce(makeTask({ tenantId: undefined }))
        .mockResolvedValueOnce(makeTask({ status: 'PROCESSING', tenantId: undefined })),
    });
    const tenantRepo = makeMockTenantRepo();
    const deps = makeDeps({ taskRepo, tenantRepo });
    const processTask = createProcessTask(deps);

    const resultPromise = processTask(makeMockJob(), 'task-1', makeMockSpan());
    await flushMicrotasks();
    jest.advanceTimersByTime(2100);
    await flushMicrotasks();
    const result = await resultPromise;

    expect(result.status).toBe('COMPLETED');
    expect(tenantRepo.getTenant).not.toHaveBeenCalled();
    expect(tenantRepo.acquireConcurrencyLease).not.toHaveBeenCalled();
  });
});
