import { describe, it, expect, jest } from '@jest/globals';
import { createProcessTask, ProcessTaskDeps } from '../process-task';

// Cancelling a running task: the handler's signal must fire within one check
// interval, and the job must end as SKIPPED (no FAILED write, no retry).

class MockDelayedError extends Error {}
class MockClaimNotExpiredError extends Error { expiresAt = new Date(); }

const span = () => ({ setAttribute: jest.fn(), setStatus: jest.fn(), recordException: jest.fn(), end: jest.fn() });
const task = (status: string) => ({ id: 'task-1', name: 'sleep', status, version: 1, retries: 0, maxRetries: 3, payload: {} });

function deps(statuses: string[], execute: ProcessTaskDeps['execute'], cancelCheckIntervalMs: number) {
  // getTask returns each status in turn, then repeats the last one.
  let i = 0;
  const taskRepo = {
    claimTtl: 30000,
    getTask: jest.fn(async () => task(statuses[Math.min(i++, statuses.length - 1)])),
    transitionStatus: jest.fn(async () => ({ version: 2, claimToken: 'ct-1', retries: 0 })),
    reclaimStalledTask: jest.fn(),
    renewClaim: jest.fn(async () => undefined),
  };
  const d: ProcessTaskDeps = {
    taskRepo: taskRepo as never,
    scheduleRepo: { getSchedule: jest.fn(async () => null) } as never,
    tracing: { startTaskClaim: span, startTaskComplete: span, startTaskFail: span, setSpanOk: jest.fn(), recordError: jest.fn(), endSpan: jest.fn() } as never,
    logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
    workerId: 'w-test',
    scheduleLeaseDurationMs: 300000,
    leaseDeferralMarginMs: 2000,
    renewalIntervalMs: 10000,
    cancelCheckIntervalMs,
    workerMetrics: { jobsCompleted: 0, jobsFailed: 0 } as never,
    ClaimNotExpiredError: MockClaimNotExpiredError,
    DelayedError: MockDelayedError,
    execute,
  };
  return { d, taskRepo };
}

const job = () => ({ id: 'task-1', token: 't', attemptsMade: 0, opts: { attempts: 4 }, data: {}, moveToDelayed: jest.fn() }) as never;

// A handler that runs until its signal fires (like the built-in sleep handler).
const untilAborted: ProcessTaskDeps['execute'] = ({ signal }) =>
  new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));

describe('cancelling a running task', () => {
  it('stops the handler within one check interval and ends SKIPPED, without writing FAILED', async () => {
    // 1st read: before claiming (QUEUED). Then PROCESSING, then the operator cancels.
    const { d, taskRepo } = deps(['QUEUED', 'PROCESSING', 'CANCELLED'], untilAborted, 50);
    const started = Date.now();

    const result = await createProcessTask(d)(job(), 'task-1', span() as never);

    expect(result).toMatchObject({ status: 'SKIPPED', reason: 'cancelled' });
    expect(Date.now() - started).toBeLessThan(1000);
    // Only the claim was written; no QUEUED (retry) or FAILED transition.
    expect(taskRepo.transitionStatus).toHaveBeenCalledTimes(1);
    expect((taskRepo.transitionStatus.mock.calls[0] as unknown[])[2]).toBe('PROCESSING');
  });

  it('discards the result of a handler that ignores the signal', async () => {
    const ignoring: ProcessTaskDeps['execute'] = () => new Promise((r) => setTimeout(() => r({ done: true }), 300));
    const { d, taskRepo } = deps(['QUEUED', 'PROCESSING', 'CANCELLED'], ignoring, 50);

    const result = await createProcessTask(d)(job(), 'task-1', span() as never);

    expect(result).toMatchObject({ status: 'SKIPPED', reason: 'cancelled' });
    expect(taskRepo.transitionStatus).toHaveBeenCalledTimes(1);
  });

  it('does not check when disabled (0)', async () => {
    const quick: ProcessTaskDeps['execute'] = () => new Promise((r) => setTimeout(() => r({ ok: 1 }), 150));
    const { d, taskRepo } = deps(['QUEUED', 'PROCESSING'], quick, 0);

    const result = await createProcessTask(d)(job(), 'task-1', span() as never);

    expect(result).toMatchObject({ status: 'COMPLETED' });
    // getTask: once before claiming, once before completing; never polled.
    expect(taskRepo.getTask).toHaveBeenCalledTimes(2);
  });
});
