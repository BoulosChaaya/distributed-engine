import type { Job } from 'bullmq';
import type { Span } from '@opentelemetry/api';

export interface TaskRepoLike {
  claimTtl: number;
  getTask(taskId: string): Promise<any>;
  transitionStatus(taskId: string, version: number, status: string, opts: any): Promise<any>;
  reclaimStalledTask(taskId: string, version: number, workerId: string): Promise<any>;
  renewClaim(taskId: string, claimToken: string): Promise<any>;
  /** Workflow steps: outputs of direct dependencies keyed by step key. */
  getUpstreamOutputs?(taskId: string): Promise<Record<string, unknown>>;
}

export interface ScheduleRepoLike {
  getSchedule(scheduleId: string): Promise<any>;
  acquireExecutionLease(scheduleId: string, durationMs: number): Promise<{ acquired: boolean; leaseToken?: string; expiresAt?: Date }>;
  renewExecutionLease(scheduleId: string, leaseToken: string, durationMs: number): Promise<void>;
  releaseExecutionLease(scheduleId: string, leaseToken: string): Promise<void>;
}

export interface TracingLike {
  startTaskClaim(taskId: string, workerId: string, kind: string): Span;
  startTaskComplete(taskId: string, workerId: string): Span;
  startTaskFail(taskId: string, workerId: string, isFinal: boolean): Span;
  setSpanOk(span: Span): void;
  recordError(span: Span, error: unknown): void;
  endSpan(span: Span): void;
}

export interface LoggerLike {
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
  debug(msg: string, meta?: Record<string, unknown>): void;
}

export interface WorkerMetrics {
  jobsCompleted: number;
  jobsFailed: number;
}

export interface TenantRepoLike {
  getTenant(tenantId: string): Promise<any>;
  getEffectiveLimits(tenantId: string): Promise<any>;
  acquireConcurrencyLease(tenantId: string, taskId: string, workerId: string, maxConcurrent: number, leaseDurationMs: number): Promise<{ acquired: boolean; lease?: any }>;
  renewConcurrencyLease(leaseToken: string, leaseDurationMs: number): Promise<void>;
  releaseConcurrencyLease(leaseToken: string): Promise<void>;
}

export interface ProcessTaskDeps {
  taskRepo: TaskRepoLike;
  scheduleRepo: ScheduleRepoLike;
  tenantRepo?: TenantRepoLike;
  tracing: TracingLike;
  logger: LoggerLike;
  workerId: string;
  scheduleLeaseDurationMs: number;
  leaseDeferralMarginMs: number;
  /**
   * How long a job of a suspended tenant waits before the worker looks again.
   * The job is delayed, not completed, so it stays alive in BullMQ and the
   * reconciler leaves it alone. Default 60 s.
   */
  suspendedTenantRecheckMs?: number;
  renewalIntervalMs: number;
  workerMetrics: WorkerMetrics;
  ClaimNotExpiredError: new (...args: any[]) => any;
  DelayedError: new (...args: any[]) => any;
  /**
   * BullMQ's UnrecoverableError. Thrown when PostgreSQL says the retry budget
   * is spent but the BullMQ job still has attempts left (a job re-published by
   * the reconciler starts with a fresh attempt counter), so BullMQ stops
   * retrying a task the database already marked FAILED.
   */
  UnrecoverableError?: new (message: string) => Error;
  /**
   * Runs the task's handler. The signal fires when this worker loses its
   * lease, so the handler can stop work another worker may now own.
   */
  execute: (ctx: ExecuteContext) => Promise<unknown>;
}

export interface ExecuteContext {
  taskId: string;
  name: string;
  payload: Record<string, unknown>;
  attempt: number;
  signal: AbortSignal;
  upstream?: Record<string, unknown>;
}

/** Matches NonRetryableTaskError from @repo/shared without importing it here. */
function isNonRetryable(error: unknown): boolean {
  return error instanceof Error && error.name === 'NonRetryableTaskError';
}

export function computeRenewalInterval(taskClaimTtlMs: number, scheduleLeaseDurationMs: number): number {
  return Math.floor(Math.min(taskClaimTtlMs, scheduleLeaseDurationMs) / 3);
}

export function validateScheduleLeaseDuration(durationMs: number): void {
  if (!Number.isFinite(durationMs) || durationMs <= 0) {
    throw new Error(
      `SCHEDULE_LEASE_DURATION_MS must be a positive number, got: ${durationMs}`,
    );
  }
  if (durationMs < 3000) {
    throw new Error(
      `SCHEDULE_LEASE_DURATION_MS must be at least 3000ms to allow safe renewal cadence, got: ${durationMs}`,
    );
  }
}

export function createProcessTask(deps: ProcessTaskDeps) {
  const {
    taskRepo,
    scheduleRepo,
    tenantRepo,
    tracing,
    logger,
    workerId,
    scheduleLeaseDurationMs,
    leaseDeferralMarginMs,
    suspendedTenantRecheckMs = 60_000,
    renewalIntervalMs,
    workerMetrics,
    ClaimNotExpiredError,
    DelayedError,
    UnrecoverableError,
    execute,
  } = deps;

  return async function processTask(
    job: Job,
    taskId: string,
    parentSpan: Span,
  ): Promise<Record<string, unknown>> {
    const currentTask = await taskRepo.getTask(taskId);
    if (!currentTask) {
      logger.warn('Task not found in database, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'not_found');
      return { status: 'SKIPPED', taskId, reason: 'not_found' };
    }

    if (currentTask.status === 'CANCELLED') {
      logger.info('Task was cancelled, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'cancelled');
      return { status: 'SKIPPED', taskId, reason: 'cancelled' };
    }

    if (currentTask.status === 'COMPLETED') {
      logger.info('Task already completed, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'already_completed');
      return { status: 'SKIPPED', taskId, reason: 'already_completed' };
    }

    if (currentTask.status === 'FAILED') {
      logger.info('Task already failed, skipping', { taskId });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'already_failed');
      return { status: 'SKIPPED', taskId, reason: 'already_failed' };
    }

    // Only QUEUED (claim) and PROCESSING (reclaim) can run. A job for a
    // SCHEDULED or BLOCKED task would otherwise fall through to execution
    // without a claim. That cannot happen through the outbox, but a stray or
    // replayed job must never execute a step whose dependencies are not done.
    if (currentTask.status !== 'QUEUED' && currentTask.status !== 'PROCESSING') {
      logger.warn('Task is not runnable, skipping', { taskId, status: currentTask.status });
      parentSpan.setAttribute('task.skipped', true);
      parentSpan.setAttribute('task.skip_reason', 'not_runnable');
      return { status: 'SKIPPED', taskId, reason: 'not_runnable' };
    }

    if (tenantRepo && currentTask.tenantId) {
      const tenant = await tenantRepo.getTenant(currentTask.tenantId);
      if (tenant?.status === 'SUSPENDED') {
        // Park the job instead of finishing it. A "skipped" job is completed in
        // BullMQ while the row stays QUEUED; the reconciler then sees a dead job,
        // requeues the task, and the cycle repeats for as long as the tenant is
        // suspended (one extra outbox event per cycle). A delayed job is live:
        // the reconciler ignores it, and the task runs once the tenant is active.
        const recheckAt = Date.now() + suspendedTenantRecheckMs;
        logger.info('Tenant suspended, deferring task', {
          taskId,
          tenantId: currentTask.tenantId,
          recheckAt: new Date(recheckAt).toISOString(),
        });
        parentSpan.setAttribute('task.deferred', true);
        parentSpan.setAttribute('task.defer_reason', 'tenant_suspended');
        await job.moveToDelayed(recheckAt, job.token);
        throw new DelayedError();
      }
    }

    let taskVersion = currentTask.version;
    let claimToken: string | undefined;
    let currentRetries = currentTask.retries;
    let renewalTimer: ReturnType<typeof setInterval> | undefined;
    let ownershipLost = false;
    const abort = new AbortController();
    let scheduleLeaseToken: string | undefined;
    let concurrencyLeaseToken: string | undefined;

    if (currentTask.scheduleId) {
      const schedule = await scheduleRepo.getSchedule(currentTask.scheduleId);
      if (schedule?.overlapPolicy === 'FORBID_OVERLAP') {
        const leaseResult = await scheduleRepo.acquireExecutionLease(
          schedule.id,
          scheduleLeaseDurationMs,
        );
        if (!leaseResult.acquired) {
          const deferUntil = (leaseResult.expiresAt?.getTime() ?? Date.now()) + leaseDeferralMarginMs;
          logger.info('Schedule execution lease held, deferring', {
            taskId,
            scheduleId: schedule.id,
            deferUntil: new Date(deferUntil).toISOString(),
          });
          await job.moveToDelayed(deferUntil, job.token);
          throw new DelayedError();
        }
        scheduleLeaseToken = leaseResult.leaseToken;
        logger.info('Schedule execution lease acquired', {
          taskId,
          scheduleId: schedule.id,
        });
      }
    }

    if (tenantRepo && currentTask.tenantId) {
      const limits = await tenantRepo.getEffectiveLimits(currentTask.tenantId);
      const concurrencyLeaseDurationMs = Math.min(taskRepo.claimTtl, scheduleLeaseDurationMs);
      const leaseResult = await tenantRepo.acquireConcurrencyLease(
        currentTask.tenantId,
        taskId,
        workerId,
        limits.maxConcurrentExecutions,
        concurrencyLeaseDurationMs,
      );
      if (!leaseResult.acquired) {
        const deferUntil = Date.now() + leaseDeferralMarginMs;
        logger.info('Tenant concurrency limit reached, deferring', {
          taskId,
          tenantId: currentTask.tenantId,
          maxConcurrent: limits.maxConcurrentExecutions,
        });
        if (scheduleLeaseToken && currentTask.scheduleId) {
          await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken).catch(() => {});
        }
        await job.moveToDelayed(deferUntil, job.token);
        throw new DelayedError();
      }
      concurrencyLeaseToken = leaseResult.lease?.leaseToken;
      logger.info('Tenant concurrency lease acquired', { taskId, tenantId: currentTask.tenantId });
    }

    if (currentTask.status === 'QUEUED') {
      const claimSpan = tracing.startTaskClaim(taskId, workerId, 'initial');
      try {
        const updated = await taskRepo.transitionStatus(taskId, taskVersion, 'PROCESSING', {
          startedAt: new Date(),
          claimedBy: workerId,
        });
        taskVersion = updated.version;
        claimToken = updated.claimToken;
        currentRetries = updated.retries;
        logger.info('Task claimed', { taskId });
        tracing.setSpanOk(claimSpan);
      } catch (error) {
        tracing.recordError(claimSpan, error);
        logger.warn('Failed to transition task to PROCESSING', {
          taskId,
          reason: String(error),
        });
        if (scheduleLeaseToken && currentTask.scheduleId) {
          await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken).catch(() => {});
        }
        if (concurrencyLeaseToken && tenantRepo) {
          await tenantRepo.releaseConcurrencyLease(concurrencyLeaseToken).catch(() => {});
        }
        return { status: 'SKIPPED', taskId, reason: 'transition_failed' };
      } finally {
        tracing.endSpan(claimSpan);
      }
    } else if (currentTask.status === 'PROCESSING') {
      const reclaimSpan = tracing.startTaskClaim(taskId, workerId, 'reclaim');
      try {
        const reclaimed = await taskRepo.reclaimStalledTask(taskId, taskVersion, workerId);
        taskVersion = reclaimed.version;
        claimToken = reclaimed.claimToken;
        currentRetries = reclaimed.retries;
        logger.info('Reclaimed stalled task', { taskId });
        tracing.setSpanOk(reclaimSpan);
      } catch (error) {
        if (error instanceof ClaimNotExpiredError) {
          reclaimSpan.setAttribute('task.claim.deferred', true);
          tracing.endSpan(reclaimSpan);
          if (scheduleLeaseToken && currentTask.scheduleId) {
            await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken).catch(() => {});
            scheduleLeaseToken = undefined;
          }
          if (concurrencyLeaseToken && tenantRepo) {
            await tenantRepo.releaseConcurrencyLease(concurrencyLeaseToken).catch(() => {});
            concurrencyLeaseToken = undefined;
          }
          const deferUntil = (error as any).expiresAt.getTime() + leaseDeferralMarginMs;
          logger.info('Lease not expired, deferring job', {
            taskId,
            expiresAt: (error as any).expiresAt.toISOString(),
            deferUntil: new Date(deferUntil).toISOString(),
          });
          await job.moveToDelayed(deferUntil, job.token);
          throw new DelayedError();
        }
        tracing.recordError(reclaimSpan, error);
        logger.warn('Failed to reclaim stalled task', {
          taskId,
          reason: String(error),
        });
        if (scheduleLeaseToken && currentTask.scheduleId) {
          await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken).catch(() => {});
        }
        if (concurrencyLeaseToken && tenantRepo) {
          await tenantRepo.releaseConcurrencyLease(concurrencyLeaseToken).catch(() => {});
        }
        return { status: 'SKIPPED', taskId, reason: 'reclaim_failed' };
      } finally {
        tracing.endSpan(reclaimSpan);
      }
    }

    try {
      renewalTimer = setInterval(async () => {
        if (!claimToken) return;
        try {
          await taskRepo.renewClaim(taskId, claimToken);
          if (scheduleLeaseToken && currentTask.scheduleId) {
            await scheduleRepo.renewExecutionLease(currentTask.scheduleId, scheduleLeaseToken, scheduleLeaseDurationMs);
          }
          if (concurrencyLeaseToken && tenantRepo) {
            const concurrencyLeaseDurationMs = Math.min(taskRepo.claimTtl, scheduleLeaseDurationMs);
            await tenantRepo.renewConcurrencyLease(concurrencyLeaseToken, concurrencyLeaseDurationMs);
          }
          logger.debug('Lease renewed', { taskId });
        } catch (renewError) {
          logger.warn('Lease renewal failed, ownership lost', { taskId, reason: String(renewError) });
          ownershipLost = true;
          abort.abort(new Error('task lease lost'));
          if (renewalTimer) {
            clearInterval(renewalTimer);
            renewalTimer = undefined;
          }
        }
      }, renewalIntervalMs);

      logger.info('Task execution started', { taskId });

      const upstream = currentTask.workflowId && taskRepo.getUpstreamOutputs
        ? await taskRepo.getUpstreamOutputs(taskId)
        : undefined;
      const output = await execute({
        taskId,
        name: currentTask.name,
        payload: (currentTask.payload ?? {}) as Record<string, unknown>,
        attempt: currentRetries + 1,
        signal: abort.signal,
        upstream,
      });

      if (ownershipLost) {
        logger.warn('Ownership lost during execution, aborting', { taskId });
        parentSpan.setAttribute('task.ownership_lost', true);
        return { status: 'SKIPPED', taskId, reason: 'ownership_lost' };
      }

      const updatedTask = await taskRepo.getTask(taskId);
      if (!updatedTask || updatedTask.status === 'CANCELLED' || updatedTask.status === 'FAILED') {
        logger.info('Task no longer processable, skipping completion', {
          taskId, status: updatedTask?.status,
        });
        return { status: 'SKIPPED', taskId, reason: updatedTask?.status?.toLowerCase() || 'not_found' };
      }

      if (ownershipLost) {
        logger.warn('Ownership lost before completion, aborting', { taskId });
        parentSpan.setAttribute('task.ownership_lost', true);
        return { status: 'SKIPPED', taskId, reason: 'ownership_lost' };
      }

      const completeSpan = tracing.startTaskComplete(taskId, workerId);
      try {
        await taskRepo.transitionStatus(taskId, taskVersion, 'COMPLETED', {
          completedAt: new Date(),
          result: { processedBy: workerId, output },
          // Earlier attempts' errors stay visible through `retries`; a completed task has no current error.
          error: null,
          claimToken,
        });
        tracing.setSpanOk(completeSpan);
      } catch (error) {
        tracing.recordError(completeSpan, error);
        throw error;
      } finally {
        tracing.endSpan(completeSpan);
      }

      if (scheduleLeaseToken && currentTask.scheduleId) {
        await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken);
        scheduleLeaseToken = undefined;
      }
      if (concurrencyLeaseToken && tenantRepo) {
        await tenantRepo.releaseConcurrencyLease(concurrencyLeaseToken);
        concurrencyLeaseToken = undefined;
      }
      workerMetrics.jobsCompleted++;
      logger.info('Task durably completed', { taskId });
      return { status: 'COMPLETED', taskId, completedAt: new Date() };
    } catch (error) {
      if (ownershipLost) {
        // Another worker may own the task now; any write from here would be
        // rejected by the claim token anyway, so stand down cleanly.
        logger.warn('Ownership lost during execution, aborting', { taskId, reason: String(error) });
        parentSpan.setAttribute('task.ownership_lost', true);
        return { status: 'SKIPPED', taskId, reason: 'ownership_lost' };
      }

      workerMetrics.jobsFailed++;

      const errorMessage = error instanceof Error ? error.message : String(error);
      const permanent = isNonRetryable(error);
      const maxAttempts = job.opts.attempts ?? 1;
      const queueBudgetSpent = job.attemptsMade + 1 >= maxAttempts;
      // PostgreSQL owns the retry budget. BullMQ's attempt counter is per job,
      // and a job re-published after Redis lost it starts again from zero.
      const dbBudgetSpent = typeof currentTask.maxRetries === 'number'
        && currentRetries + 1 > currentTask.maxRetries;
      const isFinalAttempt = permanent || queueBudgetSpent || dbBudgetSpent;

      const failSpan = tracing.startTaskFail(taskId, workerId, isFinalAttempt);
      try {
        if (isFinalAttempt) {
          await taskRepo.transitionStatus(taskId, taskVersion, 'FAILED', {
            error: errorMessage,
            retries: currentRetries + 1,
            claimToken,
          });
        } else {
          await taskRepo.transitionStatus(taskId, taskVersion, 'QUEUED', {
            error: errorMessage,
            retries: currentRetries + 1,
            claimToken,
          });
        }
        failSpan.setAttribute('task.status', isFinalAttempt ? 'FAILED' : 'QUEUED');
      } catch (transitionError) {
        tracing.recordError(failSpan, transitionError);
        logger.error('Task failure persistence failed', {
          taskId,
          targetStatus: isFinalAttempt ? 'FAILED' : 'QUEUED',
          reason: String(transitionError),
        });
      } finally {
        tracing.endSpan(failSpan);
      }

      logger.error('Task execution failed', {
        taskId,
        reason: errorMessage,
        attempt: job.attemptsMade + 1,
        maxAttempts,
        retryable: !isFinalAttempt,
        errorType: error instanceof Error ? error.name : 'UnknownError',
      });

      if (isFinalAttempt && !queueBudgetSpent && UnrecoverableError) {
        throw new UnrecoverableError(errorMessage);
      }
      throw error;
    } finally {
      if (renewalTimer) {
        clearInterval(renewalTimer);
        renewalTimer = undefined;
      }
      if (scheduleLeaseToken && currentTask.scheduleId) {
        await scheduleRepo.releaseExecutionLease(currentTask.scheduleId, scheduleLeaseToken).catch(() => {});
      }
      if (concurrencyLeaseToken && tenantRepo) {
        await tenantRepo.releaseConcurrencyLease(concurrencyLeaseToken).catch(() => {});
      }
    }
  };
}
