import { Pool } from 'pg';
import { Task, TaskStatus, TaskPriority, OutboxEvent } from '../types';
import { assertValidTransition } from '../state-machine';
import { generateId } from '../utils';
import { injectTraceContext } from '../telemetry/propagation';

export interface CreateTaskInput {
  name: string;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  maxRetries: number;
}

export interface TaskWithOutbox {
  task: Task;
  outboxEvent: OutboxEvent;
}

function rowToTask(row: Record<string, unknown>): Task {
  return {
    id: row.id as string,
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
    claimedBy: (row.claimed_by as string) || undefined,
    claimToken: (row.claim_token as string) || undefined,
    claimExpiresAt: row.claim_expires_at ? new Date(row.claim_expires_at as string) : undefined,
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

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async reclaimStalledTask(taskId: string, expectedVersion: number, workerId?: string): Promise<Task> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

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

      const claimToken = generateId();

      const updateResult = await client.query(
        `UPDATE tasks SET version = version + 1, updated_at = NOW(), claimed_by = $2, claim_token = $3, claim_expires_at = NOW() + $4 * INTERVAL '1 millisecond'
         WHERE id = $1 RETURNING *`,
        [taskId, workerId || null, claimToken, this.claimTtlMs],
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

      await client.query('COMMIT');
      return rowToTask(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getTaskStatusCounts(): Promise<Record<TaskStatus, number>> {
    const result = await this.pool.query(
      `SELECT status, COUNT(*)::int as count FROM tasks GROUP BY status`,
    );
    const counts: Record<string, number> = {
      QUEUED: 0, PROCESSING: 0, COMPLETED: 0, FAILED: 0, CANCELLED: 0,
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

export class ClaimNotExpiredError extends Error {
  constructor(
    public readonly taskId: string,
    public readonly expiresAt: Date,
  ) {
    super(`Cannot reclaim task ${taskId}: claim has not expired (expires at ${expiresAt.toISOString()})`);
    this.name = 'ClaimNotExpiredError';
  }
}
