import { Pool, PoolClient } from 'pg';
import { Task, TaskStatus, TaskPriority, OutboxEvent } from '../types.js';
import { assertValidTransition } from '../state-machine.js';
import { generateId } from '../utils.js';
import { injectTraceContext } from '../telemetry/propagation.js';
import { computeIdempotencyHash, IdempotencyConflictError, QuotaExceededError } from './tenant-repository.js';
import { TERMINAL_STATUSES, lockWorkflowForTask, onTaskTerminal } from './workflow-engine.js';

export interface CreateTaskInput {
  name: string;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  maxRetries: number;
}

export interface AcceptTaskInput {
  tenantId: string;
  name: string;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  maxRetries: number;
  idempotencyKey?: string;
  billingPeriodStart: Date;
  maxJobsPerPeriod: number;
  scheduledFor?: string;
  scheduleId?: string;
}

export interface AcceptTaskResult {
  task: Task;
  idempotent: boolean;
  outboxEventId?: string;
}

export interface TaskWithOutbox {
  task: Task;
  outboxEvent: OutboxEvent;
}

export function rowToTask(row: Record<string, unknown>): Task {
  return {
    id: row.id as string,
    tenantId: (row.tenant_id as string) || '',
    name: row.name as string,
    status: row.status as TaskStatus,
    priority: row.priority as TaskPriority,
    payload: (row.payload as Record<string, unknown>) ?? {},
    result: row.result as Record<string, unknown> | undefined,
    error: row.error as string | undefined,
    retries: row.retries as number,
    maxRetries: row.max_retries as number,
    version: row.version as number,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
    startedAt: row.started_at ? new Date(row.started_at as string) : undefined,
    completedAt: row.completed_at ? new Date(row.completed_at as string) : undefined,
    scheduledFor: row.scheduled_for ? new Date(row.scheduled_for as string) : undefined,
    scheduleId: (row.schedule_id as string) || undefined,
    claimedBy: (row.claimed_by as string) || undefined,
    claimToken: (row.claim_token as string) || undefined,
    claimExpiresAt: row.claim_expires_at ? new Date(row.claim_expires_at as string) : undefined,
    idempotencyKey: (row.idempotency_key as string) || undefined,
    idempotencyHash: (row.idempotency_hash as string) || undefined,
    workflowId: (row.workflow_id as string) || undefined,
    stepKey: (row.step_key as string) || undefined,
    pendingDependencies: row.pending_dependencies === undefined ? undefined : (row.pending_dependencies as number),
  };
}

export class TaskRepository {
  constructor(private pool: Pool, private claimTtlMs: number = 30000) {}

  async createTaskWithOutbox(input: CreateTaskInput): Promise<TaskWithOutbox> {
    const taskId = generateId();
    const outboxId = generateId();
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const taskResult = await client.query(
        `INSERT INTO tasks (id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
         VALUES ($1, $2, 'QUEUED', $3, $4, $5, 0, 1, NOW(), NOW())
         RETURNING *`,
        [taskId, input.name, input.priority, JSON.stringify(input.payload), input.maxRetries],
      );

      const outboxPayload = {
        taskId,
        taskName: input.name,
        priority: input.priority,
        payload: input.payload,
        maxRetries: input.maxRetries,
      };

      const traceCtx = injectTraceContext();
      const hasTraceContext = Object.keys(traceCtx).length > 0;

      const outboxResult = await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW(), $4)
         RETURNING *`,
        [outboxId, taskId, JSON.stringify(outboxPayload), hasTraceContext ? JSON.stringify(traceCtx) : null],
      );

      await client.query('COMMIT');

      const task = rowToTask(taskResult.rows[0]);
      const outboxEvent: OutboxEvent = {
        id: outboxResult.rows[0].id,
        taskId: outboxResult.rows[0].task_id,
        eventType: outboxResult.rows[0].event_type,
        payload: outboxResult.rows[0].payload,
        status: outboxResult.rows[0].status,
        attempts: outboxResult.rows[0].attempts,
        createdAt: new Date(outboxResult.rows[0].created_at),
        processedAt: outboxResult.rows[0].processed_at ? new Date(outboxResult.rows[0].processed_at) : undefined,
        traceContext: outboxResult.rows[0].trace_context ?? undefined,
      };

      return { task, outboxEvent };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getTask(taskId: string): Promise<Task | null> {
    const result = await this.pool.query('SELECT * FROM tasks WHERE id = $1', [taskId]);
    if (result.rows.length === 0) return null;
    return rowToTask(result.rows[0]);
  }

  async listTasks(page: number, pageSize: number): Promise<{ items: Task[]; total: number }> {
    const offset = (page - 1) * pageSize;
    const [itemsResult, countResult] = await Promise.all([
      this.pool.query('SELECT * FROM tasks ORDER BY created_at DESC LIMIT $1 OFFSET $2', [pageSize, offset]),
      this.pool.query('SELECT COUNT(*) FROM tasks'),
    ]);
    return {
      items: itemsResult.rows.map(rowToTask),
      total: parseInt(countResult.rows[0].count, 10),
    };
  }

  async transitionStatus(
    taskId: string,
    expectedVersion: number,
    toStatus: TaskStatus,
    extra?: Partial<Pick<Task, 'error' | 'result' | 'retries' | 'startedAt' | 'completedAt' | 'claimedBy' | 'claimToken'>>,
  ): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      // Workflow row before task row (lock order, see workflow-engine.ts).
      const workflowId = TERMINAL_STATUSES.has(toStatus) ? await lockWorkflowForTask(client, taskId) : null;

      const lockResult = await client.query(
        'SELECT * FROM tasks WHERE id = $1 FOR UPDATE',
        [taskId],
      );

      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found`);
      }

      const current = rowToTask(lockResult.rows[0]);

      if (current.version !== expectedVersion) {
        await client.query('ROLLBACK');
        throw new StaleVersionError(taskId, expectedVersion, current.version);
      }

      assertValidTransition(current.status, toStatus);

      if (current.status === 'PROCESSING' && current.claimToken) {
        if (!extra?.claimToken || extra.claimToken !== current.claimToken) {
          await client.query('ROLLBACK');
          throw new ClaimTokenMismatchError(taskId, extra?.claimToken, current.claimToken);
        }
      }

      const setClauses = [
        'status = $2',
        'version = version + 1',
        'updated_at = NOW()',
      ];
      const values: unknown[] = [taskId, toStatus];
      let paramIndex = 3;

      if (extra?.error !== undefined) {
        setClauses.push(`error = $${paramIndex}`);
        values.push(extra.error);
        paramIndex++;
      }
      if (extra?.result !== undefined) {
        setClauses.push(`result = $${paramIndex}`);
        values.push(JSON.stringify(extra.result));
        paramIndex++;
      }
      if (extra?.retries !== undefined) {
        setClauses.push(`retries = $${paramIndex}`);
        values.push(extra.retries);
        paramIndex++;
      }
      if (extra?.startedAt !== undefined) {
        setClauses.push(`started_at = $${paramIndex}`);
        values.push(extra.startedAt);
        paramIndex++;
      }
      if (extra?.completedAt !== undefined) {
        setClauses.push(`completed_at = $${paramIndex}`);
        values.push(extra.completedAt);
        paramIndex++;
      }

      if (toStatus === 'PROCESSING') {
        const claimToken = generateId();
        setClauses.push(`claim_token = $${paramIndex}`);
        values.push(claimToken);
        paramIndex++;
        setClauses.push(`claim_expires_at = NOW() + $${paramIndex} * INTERVAL '1 millisecond'`);
        values.push(this.claimTtlMs);
        paramIndex++;
      } else if (current.status === 'PROCESSING') {
        setClauses.push('claim_token = NULL');
        setClauses.push('claim_expires_at = NULL');
      }

      if (extra?.claimedBy !== undefined) {
        setClauses.push(`claimed_by = $${paramIndex}`);
        values.push(extra.claimedBy);
        paramIndex++;
      } else if (toStatus !== 'PROCESSING') {
        setClauses.push('claimed_by = NULL');
      }

      const updateResult = await client.query(
        `UPDATE tasks SET ${setClauses.join(', ')} WHERE id = $1 RETURNING *`,
        values,
      );

      if (workflowId) {
        await onTaskTerminal(client, workflowId, taskId, toStatus);
      }

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Take over a PROCESSING task whose lease expired (its worker died or hung).
   *
   * The lost run counts as an attempt and is charged to the retry budget, as
   * the reconciler already does for the same situation. Without that, a task
   * whose execution kills its worker (a "poison" task) was reclaimed for free
   * until BullMQ's own stall limit gave up, then requeued by the reconciler,
   * which charged only then: three worker crashes per retry. When the budget
   * is already spent the task is failed here and RetryBudgetExhaustedError
   * is thrown, so the caller does not run it again.
   */
  async reclaimStalledTask(taskId: string, expectedVersion: number, workerId?: string): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // Lock order workflow row -> task row, as for every terminal transition:
      // this reclaim may fail a workflow step.
      const workflowId = await lockWorkflowForTask(client, taskId);

      const lockResult = await client.query(
        'SELECT *, NOW() as db_now FROM tasks WHERE id = $1 FOR UPDATE',
        [taskId],
      );

      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found`);
      }

      const current = rowToTask(lockResult.rows[0]);

      if (current.status !== 'PROCESSING') {
        await client.query('ROLLBACK');
        throw new Error(`Cannot reclaim task ${taskId}: status is ${current.status}, expected PROCESSING`);
      }

      if (current.version !== expectedVersion) {
        await client.query('ROLLBACK');
        throw new StaleVersionError(taskId, expectedVersion, current.version);
      }

      if (current.claimedBy && current.claimedBy === workerId) {
        await client.query('ROLLBACK');
        throw new Error(`Cannot reclaim task ${taskId}: already claimed by this worker (${workerId})`);
      }

      const dbNow = new Date(lockResult.rows[0].db_now as string);
      if (current.claimExpiresAt && current.claimExpiresAt > dbNow) {
        await client.query('ROLLBACK');
        throw new ClaimNotExpiredError(taskId, current.claimExpiresAt);
      }

      const retries = current.retries + 1;
      if (retries > current.maxRetries) {
        await client.query(
          `UPDATE tasks SET status = 'FAILED', retries = $2, error = $3, version = version + 1, updated_at = NOW(),
             claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL
           WHERE id = $1`,
          [taskId, retries, 'Worker lost the task lease and the retry budget is exhausted'],
        );
        if (workflowId) await onTaskTerminal(client, workflowId, taskId, 'FAILED');
        await client.query('COMMIT');
        throw new RetryBudgetExhaustedError(taskId, retries);
      }

      const claimToken = generateId();

      const updateResult = await client.query(
        `UPDATE tasks SET version = version + 1, updated_at = NOW(), retries = $5,
           error = $6,
           claimed_by = $2, claim_token = $3, claim_expires_at = NOW() + $4 * INTERVAL '1 millisecond'
         WHERE id = $1 RETURNING *`,
        [taskId, workerId || null, claimToken, this.claimTtlMs, retries,
          `Worker lost the task lease; attempt ${retries} charged and the task reclaimed`],
      );

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  get claimTtl(): number {
    return this.claimTtlMs;
  }

  async renewClaim(taskId: string, expectedClaimToken: string): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      const lockResult = await client.query(
        'SELECT * FROM tasks WHERE id = $1 FOR UPDATE',
        [taskId],
      );

      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found`);
      }

      const current = rowToTask(lockResult.rows[0]);

      if (current.status !== 'PROCESSING') {
        await client.query('ROLLBACK');
        throw new Error(`Cannot renew claim on task ${taskId}: status is ${current.status}, expected PROCESSING`);
      }

      if (!current.claimToken || current.claimToken !== expectedClaimToken) {
        await client.query('ROLLBACK');
        throw new ClaimTokenMismatchError(taskId, expectedClaimToken, current.claimToken || 'none');
      }

      const updateResult = await client.query(
        `UPDATE tasks SET claim_expires_at = NOW() + $2 * INTERVAL '1 millisecond', updated_at = NOW()
         WHERE id = $1 RETURNING *`,
        [taskId, this.claimTtlMs],
      );

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async cancelTask(taskId: string): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const workflowId = await lockWorkflowForTask(client, taskId);

      const lockResult = await client.query(
        'SELECT * FROM tasks WHERE id = $1 FOR UPDATE',
        [taskId],
      );

      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found`);
      }

      const current = rowToTask(lockResult.rows[0]);
      assertValidTransition(current.status, 'CANCELLED');

      const updateResult = await client.query(
        `UPDATE tasks SET status = 'CANCELLED', version = version + 1, updated_at = NOW(), claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL
         WHERE id = $1 RETURNING *`,
        [taskId],
      );
      if (workflowId) await onTaskTerminal(client, workflowId, taskId, 'CANCELLED');

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Operator redrive of a FAILED task: FAILED → QUEUED with a fresh retry
   * budget, plus a TASK_REQUEUED outbox event, in one transaction. This is the
   * one caller of the FAILED → QUEUED transition the state machine allows.
   *
   * Workflow steps are refused: when a step failed, its descendants were
   * cancelled and the workflow became FAILED in the same transaction, so
   * re-running one step alone would leave a "successful" step inside a failed
   * workflow with its children still cancelled. Resubmit the workflow instead.
   *
   * The last error is kept until the new run ends (completion clears it), so
   * the operator can still see why it failed while it waits.
   */
  /**
   * Send a FAILED task back to QUEUED with a fresh retry budget. With
   * `tenantId` only that tenant's task is found. With `quota` the run is
   * charged as one job, in the same transaction, exactly like a new
   * submission: otherwise a tenant could fail a task on purpose and retry it
   * forever without touching its quota.
   */
  async retryFailedTask(
    taskId: string,
    tenantId?: string,
    quota?: { billingPeriodStart: Date; maxJobsPerPeriod: number },
  ): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const lockResult = await client.query(
        'SELECT * FROM tasks WHERE id = $1 AND ($2::text IS NULL OR tenant_id = $2) FOR UPDATE',
        [taskId, tenantId ?? null],
      );
      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found`);
      }
      const current = rowToTask(lockResult.rows[0]);
      if (current.status !== 'FAILED') {
        await client.query('ROLLBACK');
        throw new TaskNotRetryableError(taskId, `status is ${current.status}; only FAILED tasks can be retried`);
      }
      if (lockResult.rows[0].workflow_id) {
        await client.query('ROLLBACK');
        throw new TaskNotRetryableError(taskId, 'it is a workflow step; resubmit the workflow instead');
      }
      assertValidTransition(current.status, 'QUEUED');
      if (quota && tenantId) {
        await chargeTenantJob(client, tenantId, quota.billingPeriodStart, quota.maxJobsPerPeriod);
      }

      const updated = await client.query(
        `UPDATE tasks SET status = 'QUEUED', retries = 0, version = version + 1, updated_at = NOW(),
           claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL, started_at = NULL, completed_at = NULL
         WHERE id = $1 RETURNING *`,
        [taskId],
      );
      const row = updated.rows[0];
      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'TASK_REQUEUED', $3, 'PENDING', 0, NOW(), $4)`,
        [
          generateId(),
          taskId,
          JSON.stringify({ taskId, taskName: row.name, priority: row.priority, payload: row.payload, maxRetries: row.max_retries }),
          JSON.stringify(injectTraceContext()),
        ],
      );
      await client.query('COMMIT');
      return rowToTask(row);
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  /** Outputs of a workflow step's direct parents, keyed by step key. */
  async getUpstreamOutputs(taskId: string): Promise<Record<string, unknown>> {
    const result = await this.pool.query(
      `SELECT p.step_key, p.result -> 'output' AS output
       FROM task_dependencies d JOIN tasks p ON p.id = d.depends_on
       WHERE d.task_id = $1`,
      [taskId],
    );
    return Object.fromEntries(result.rows.map((r) => [r.step_key as string, r.output ?? null]));
  }

  async getTaskStatusCounts(): Promise<Record<TaskStatus, number>> {
    const result = await this.pool.query(
      `SELECT status, COUNT(*)::int as count FROM tasks GROUP BY status`,
    );
    const counts: Record<string, number> = {
      SCHEDULED: 0, BLOCKED: 0, QUEUED: 0, PROCESSING: 0, COMPLETED: 0, FAILED: 0, CANCELLED: 0,
    };
    for (const row of result.rows) {
      counts[row.status] = row.count;
    }
    return counts as Record<TaskStatus, number>;
  }

  async getTaskCount(): Promise<number> {
    const result = await this.pool.query('SELECT COUNT(*)::int as count FROM tasks');
    return result.rows[0].count;
  }

  async acceptTask(input: AcceptTaskInput): Promise<AcceptTaskResult> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      if (input.idempotencyKey) {
        const existing = await client.query(
          `SELECT * FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2`,
          [input.tenantId, input.idempotencyKey],
        );
        if (existing.rows.length > 0) {
          const hash = computeIdempotencyHash(input.name, input.priority, input.maxRetries, input.payload, input.scheduledFor, input.scheduleId);
          if (existing.rows[0].idempotency_hash !== hash) {
            await client.query('ROLLBACK');
            throw new IdempotencyConflictError(input.tenantId, input.idempotencyKey);
          }
          await client.query('COMMIT');
          return { task: rowToTask(existing.rows[0]), idempotent: true };
        }
      }

      await chargeTenantJob(client, input.tenantId, input.billingPeriodStart, input.maxJobsPerPeriod);

      const taskId = generateId();
      const outboxId = generateId();
      const traceCtx = injectTraceContext();
      const hasTraceContext = Object.keys(traceCtx).length > 0;
      const idempotencyHash = input.idempotencyKey
        ? computeIdempotencyHash(input.name, input.priority, input.maxRetries, input.payload, input.scheduledFor, input.scheduleId)
        : null;

      // A future scheduledFor is stored SCHEDULED with no outbox event, exactly
      // like the operator route: the scheduler releases it when it is due, and
      // until then there is no job, so no worker can run it early. "Future" is
      // decided by the database clock, the same clock the scheduler uses.
      const taskResult = await client.query(
        `INSERT INTO tasks (id, tenant_id, name, status, priority, payload, max_retries, retries, version,
           idempotency_key, idempotency_hash, scheduled_for, created_at, updated_at)
         VALUES ($1, $2, $3,
                 CASE WHEN $9::timestamptz > NOW() THEN 'SCHEDULED' ELSE 'QUEUED' END,
                 $4, $5, $6, 0, 1, $7, $8, $9::timestamptz, NOW(), NOW())
         RETURNING *`,
        [taskId, input.tenantId, input.name, input.priority,
         JSON.stringify(input.payload), input.maxRetries,
         input.idempotencyKey ?? null, idempotencyHash, input.scheduledFor ?? null],
      );

      if (taskResult.rows[0].status === 'SCHEDULED') {
        await client.query('COMMIT');
        return { task: rowToTask(taskResult.rows[0]), idempotent: false };
      }

      const outboxPayload = {
        taskId,
        taskName: input.name,
        priority: input.priority,
        payload: input.payload,
        maxRetries: input.maxRetries,
      };

      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW(), $4)`,
        [outboxId, taskId, JSON.stringify(outboxPayload), hasTraceContext ? JSON.stringify(traceCtx) : null],
      );

      await client.query('COMMIT');
      return { task: rowToTask(taskResult.rows[0]), idempotent: false, outboxEventId: outboxId };
    } catch (error: unknown) {
      await client.query('ROLLBACK').catch(() => {});

      const pgError = error as { code?: string };
      if (pgError.code === '23505' && input.idempotencyKey) {
        const winner = await client.query(
          'SELECT * FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2',
          [input.tenantId, input.idempotencyKey],
        );
        if (winner.rows.length > 0) {
          const hash = computeIdempotencyHash(input.name, input.priority, input.maxRetries, input.payload, input.scheduledFor, input.scheduleId);
          if (winner.rows[0].idempotency_hash === hash) {
            return { task: rowToTask(winner.rows[0]), idempotent: true };
          }
          throw new IdempotencyConflictError(input.tenantId, input.idempotencyKey);
        }
      }

      throw error;
    } finally {
      client.release();
    }
  }

  async getTaskForTenant(taskId: string, tenantId: string): Promise<Task | null> {
    const result = await this.pool.query(
      'SELECT * FROM tasks WHERE id = $1 AND tenant_id = $2',
      [taskId, tenantId],
    );
    if (result.rows.length === 0) return null;
    return rowToTask(result.rows[0]);
  }

  async listTasksForTenant(tenantId: string, page: number, pageSize: number): Promise<{ items: Task[]; total: number }> {
    const offset = (page - 1) * pageSize;
    const [itemsResult, countResult] = await Promise.all([
      this.pool.query(
        'SELECT * FROM tasks WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3',
        [tenantId, pageSize, offset],
      ),
      this.pool.query('SELECT COUNT(*)::int as count FROM tasks WHERE tenant_id = $1', [tenantId]),
    ]);
    return {
      items: itemsResult.rows.map(rowToTask),
      total: countResult.rows[0].count,
    };
  }

  async cancelTaskForTenant(taskId: string, tenantId: string): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const workflowId = await lockWorkflowForTask(client, taskId);

      const lockResult = await client.query(
        'SELECT * FROM tasks WHERE id = $1 AND tenant_id = $2 FOR UPDATE',
        [taskId, tenantId],
      );

      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Task ${taskId} not found for tenant ${tenantId}`);
      }

      const current = rowToTask(lockResult.rows[0]);
      assertValidTransition(current.status, 'CANCELLED');

      const updateResult = await client.query(
        `UPDATE tasks SET status = 'CANCELLED', version = version + 1, updated_at = NOW(),
           claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL
         WHERE id = $1 RETURNING *`,
        [taskId],
      );
      if (workflowId) await onTaskTerminal(client, workflowId, taskId, 'CANCELLED');

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

export class StaleVersionError extends Error {
  constructor(
    public readonly taskId: string,
    public readonly expected: number,
    public readonly actual: number,
  ) {
    super(`Stale version for task ${taskId}: expected ${expected}, got ${actual}`);
    this.name = 'StaleVersionError';
  }
}

export class ClaimTokenMismatchError extends Error {
  constructor(
    public readonly taskId: string,
    public readonly provided: string | undefined,
    public readonly expected: string,
  ) {
    super(`Claim token mismatch for task ${taskId}: provided ${provided || 'none'}, expected ${expected}`);
    this.name = 'ClaimTokenMismatchError';
  }
}

/** Reclaim found the retry budget spent; the task is now FAILED. */
export class RetryBudgetExhaustedError extends Error {
  constructor(public readonly taskId: string, public readonly retries: number) {
    super(`Task ${taskId} failed: worker lost the lease and the retry budget is exhausted (${retries} attempts)`);
    this.name = 'RetryBudgetExhaustedError';
  }
}

export class ClaimNotExpiredError extends Error {
  constructor(
    public readonly taskId: string,
    public readonly expiresAt: Date,
  ) {
    super(`Cannot reclaim task ${taskId}: claim has not expired (expires at ${expiresAt.toISOString()})`);
    this.name = 'ClaimNotExpiredError';
  }
}

export class TaskNotRetryableError extends Error {
  constructor(
    public readonly taskId: string,
    public readonly reason: string,
  ) {
    super(`Task ${taskId} cannot be retried: ${reason}`);
    this.name = 'TaskNotRetryableError';
  }
}

/**
 * Count one job against a tenant's quota for the billing period, inside the
 * caller's transaction. Throws QuotaExceededError when the period is full;
 * the caller's catch rolls the transaction back.
 */
async function chargeTenantJob(
  client: PoolClient,
  tenantId: string,
  billingPeriodStart: Date,
  maxJobsPerPeriod: number,
): Promise<void> {
  const usageResult = await client.query(
    `INSERT INTO tenant_usage (id, tenant_id, billing_period_start, accepted_jobs, compute_units, storage_mb, created_at, updated_at)
     VALUES ($1, $2, $3, 0, 0, 0, NOW(), NOW())
     ON CONFLICT (tenant_id, billing_period_start) DO UPDATE SET updated_at = NOW()
     RETURNING accepted_jobs`,
    [generateId(), tenantId, billingPeriodStart],
  );
  const currentJobs = usageResult.rows[0].accepted_jobs as number;
  if (currentJobs >= maxJobsPerPeriod) {
    throw new QuotaExceededError(tenantId, maxJobsPerPeriod, currentJobs);
  }
  await client.query(
    `UPDATE tenant_usage SET accepted_jobs = accepted_jobs + 1, updated_at = NOW()
     WHERE tenant_id = $1 AND billing_period_start = $2`,
    [tenantId, billingPeriodStart],
  );
}
