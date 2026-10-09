import { Pool, PoolClient } from 'pg';
import {
  RecurringSchedule,
  ScheduleStatus,
  MisfirePolicy,
  OverlapPolicy,
  TaskPriority,
  Task,
  TaskStatus,
  OutboxEvent,
} from '../types';
import { generateId } from '../utils';
import { injectTraceContext } from '../telemetry/propagation';

export interface CreateScheduleInput {
  name: string;
  taskName: string;
  taskPriority: TaskPriority;
  taskPayload: Record<string, unknown>;
  taskMaxRetries: number;
  cronExpression: string;
  timezone: string;
  nextRunAt: Date;
  misfirePolicy: MisfirePolicy;
  overlapPolicy: OverlapPolicy;
}

export interface UpdateScheduleInput {
  name?: string;
  taskName?: string;
  taskPriority?: TaskPriority;
  taskPayload?: Record<string, unknown>;
  taskMaxRetries?: number;
  cronExpression?: string;
  timezone?: string;
  nextRunAt?: Date;
  misfirePolicy?: MisfirePolicy;
  overlapPolicy?: OverlapPolicy;
}

export interface CreateScheduledTaskInput {
  name: string;
  priority: TaskPriority;
  payload: Record<string, unknown>;
  maxRetries: number;
  scheduledFor: Date;
}

export interface ScheduledTaskWithOutbox {
  task: Task;
  outboxEvent: OutboxEvent;
}

export interface OccurrenceResult {
  task: Task;
  outboxEvent: OutboxEvent;
}

function rowToSchedule(row: Record<string, unknown>): RecurringSchedule {
  return {
    id: row.id as string,
    name: row.name as string,
    taskName: row.task_name as string,
    taskPriority: row.task_priority as TaskPriority,
    taskPayload: (row.task_payload as Record<string, unknown>) ?? {},
    taskMaxRetries: row.task_max_retries as number,
    cronExpression: row.cron_expression as string,
    timezone: row.timezone as string,
    nextRunAt: new Date(row.next_run_at as string),
    status: row.status as ScheduleStatus,
    misfirePolicy: row.misfire_policy as MisfirePolicy,
    overlapPolicy: row.overlap_policy as OverlapPolicy,
    executionLeaseToken: (row.execution_lease_token as string) || undefined,
    executionLeaseExpiresAt: row.execution_lease_expires_at
      ? new Date(row.execution_lease_expires_at as string)
      : undefined,
    version: row.version as number,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
  };
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
    scheduledFor: row.scheduled_for ? new Date(row.scheduled_for as string) : undefined,
    scheduleId: (row.schedule_id as string) || undefined,
    claimedBy: (row.claimed_by as string) || undefined,
    claimToken: (row.claim_token as string) || undefined,
    claimExpiresAt: row.claim_expires_at ? new Date(row.claim_expires_at as string) : undefined,
  };
}

export class ScheduleStaleVersionError extends Error {
  constructor(
    public readonly scheduleId: string,
    public readonly expected: number,
    public readonly actual: number,
  ) {
    super(`Stale version for schedule ${scheduleId}: expected ${expected}, got ${actual}`);
    this.name = 'ScheduleStaleVersionError';
  }
}

export class DuplicateOccurrenceError extends Error {
  constructor(
    public readonly scheduleId: string,
    public readonly scheduledFor: Date,
  ) {
    super(`Duplicate occurrence for schedule ${scheduleId} at ${scheduledFor.toISOString()}`);
    this.name = 'DuplicateOccurrenceError';
  }
}

export class ScheduleRepository {
  constructor(private pool: Pool) {}

  async createSchedule(input: CreateScheduleInput): Promise<RecurringSchedule> {
    const id = generateId();
    const result = await this.pool.query(
      `INSERT INTO recurring_schedules (id, name, task_name, task_priority, task_payload, task_max_retries,
         cron_expression, timezone, next_run_at, status, misfire_policy, overlap_policy, version, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ACTIVE', $10, $11, 1, NOW(), NOW())
       RETURNING *`,
      [
        id, input.name, input.taskName, input.taskPriority,
        JSON.stringify(input.taskPayload), input.taskMaxRetries,
        input.cronExpression, input.timezone, input.nextRunAt,
        input.misfirePolicy, input.overlapPolicy,
      ],
    );
    return rowToSchedule(result.rows[0]);
  }

  async getSchedule(scheduleId: string): Promise<RecurringSchedule | null> {
    const result = await this.pool.query('SELECT * FROM recurring_schedules WHERE id = $1', [scheduleId]);
    if (result.rows.length === 0) return null;
    return rowToSchedule(result.rows[0]);
  }

  async listSchedules(page: number, pageSize: number): Promise<{ items: RecurringSchedule[]; total: number }> {
    const offset = (page - 1) * pageSize;
    const [itemsResult, countResult] = await Promise.all([
      this.pool.query('SELECT * FROM recurring_schedules ORDER BY created_at DESC LIMIT $1 OFFSET $2', [pageSize, offset]),
      this.pool.query('SELECT COUNT(*) FROM recurring_schedules'),
    ]);
    return {
      items: itemsResult.rows.map(rowToSchedule),
      total: parseInt(countResult.rows[0].count, 10),
    };
  }

  async updateSchedule(
    scheduleId: string,
    expectedVersion: number,
    input: UpdateScheduleInput,
  ): Promise<RecurringSchedule> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const lockResult = await client.query(
        'SELECT * FROM recurring_schedules WHERE id = $1 FOR UPDATE',
        [scheduleId],
      );
      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Schedule ${scheduleId} not found`);
      }
      const current = rowToSchedule(lockResult.rows[0]);
      if (current.version !== expectedVersion) {
        await client.query('ROLLBACK');
        throw new ScheduleStaleVersionError(scheduleId, expectedVersion, current.version);
      }

      const setClauses: string[] = ['version = version + 1', 'updated_at = NOW()'];
      const values: unknown[] = [scheduleId];
      let paramIndex = 2;

      if (input.name !== undefined) {
        setClauses.push(`name = $${paramIndex}`); values.push(input.name); paramIndex++;
      }
      if (input.taskName !== undefined) {
        setClauses.push(`task_name = $${paramIndex}`); values.push(input.taskName); paramIndex++;
      }
      if (input.taskPriority !== undefined) {
        setClauses.push(`task_priority = $${paramIndex}`); values.push(input.taskPriority); paramIndex++;
      }
      if (input.taskPayload !== undefined) {
        setClauses.push(`task_payload = $${paramIndex}`); values.push(JSON.stringify(input.taskPayload)); paramIndex++;
      }
      if (input.taskMaxRetries !== undefined) {
        setClauses.push(`task_max_retries = $${paramIndex}`); values.push(input.taskMaxRetries); paramIndex++;
      }
      if (input.cronExpression !== undefined) {
        setClauses.push(`cron_expression = $${paramIndex}`); values.push(input.cronExpression); paramIndex++;
      }
      if (input.timezone !== undefined) {
        setClauses.push(`timezone = $${paramIndex}`); values.push(input.timezone); paramIndex++;
      }
      if (input.nextRunAt !== undefined) {
        setClauses.push(`next_run_at = $${paramIndex}`); values.push(input.nextRunAt); paramIndex++;
      }
      if (input.misfirePolicy !== undefined) {
        setClauses.push(`misfire_policy = $${paramIndex}`); values.push(input.misfirePolicy); paramIndex++;
      }
      if (input.overlapPolicy !== undefined) {
        setClauses.push(`overlap_policy = $${paramIndex}`); values.push(input.overlapPolicy); paramIndex++;
      }

      const updateResult = await client.query(
        `UPDATE recurring_schedules SET ${setClauses.join(', ')} WHERE id = $1 RETURNING *`,
        values,
      );
      await client.query('COMMIT');
      return rowToSchedule(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async setScheduleStatus(
    scheduleId: string,
    expectedVersion: number,
    status: ScheduleStatus,
  ): Promise<RecurringSchedule> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const lockResult = await client.query(
        'SELECT * FROM recurring_schedules WHERE id = $1 FOR UPDATE',
        [scheduleId],
      );
      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Schedule ${scheduleId} not found`);
      }
      const current = rowToSchedule(lockResult.rows[0]);
      if (current.version !== expectedVersion) {
        await client.query('ROLLBACK');
        throw new ScheduleStaleVersionError(scheduleId, expectedVersion, current.version);
      }

      const updateResult = await client.query(
        `UPDATE recurring_schedules SET status = $2, version = version + 1, updated_at = NOW()
         WHERE id = $1 RETURNING *`,
        [scheduleId, status],
      );
      await client.query('COMMIT');
      return rowToSchedule(updateResult.rows[0]);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async createScheduledTask(input: CreateScheduledTaskInput): Promise<ScheduledTaskWithOutbox> {
    const taskId = generateId();
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const taskResult = await client.query(
        `INSERT INTO tasks (id, name, status, priority, payload, max_retries, retries, version, scheduled_for, created_at, updated_at)
         VALUES ($1, $2, 'SCHEDULED', $3, $4, $5, 0, 1, $6, NOW(), NOW())
         RETURNING *`,
        [taskId, input.name, input.priority, JSON.stringify(input.payload), input.maxRetries, input.scheduledFor],
      );
      await client.query('COMMIT');
      // Scheduled task has no outbox event yet — it gets one when released
      return {
        task: rowToTask(taskResult.rows[0]),
        outboxEvent: undefined as unknown as OutboxEvent,
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async releaseDueScheduledTasks(batchSize: number): Promise<ScheduledTaskWithOutbox[]> {
    const client = await this.pool.connect();
    const results: ScheduledTaskWithOutbox[] = [];
    try {
      await client.query('BEGIN');
      const dueRows = await client.query(
        `SELECT * FROM tasks
         WHERE status = 'SCHEDULED' AND scheduled_for <= NOW() AND schedule_id IS NULL
         ORDER BY scheduled_for ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED`,
        [batchSize],
      );

      for (const row of dueRows.rows) {
        const taskId = row.id as string;
        const outboxId = generateId();
        const traceCtx = injectTraceContext();
        const hasTraceContext = Object.keys(traceCtx).length > 0;

        await client.query(
          `UPDATE tasks SET status = 'QUEUED', version = version + 1, updated_at = NOW()
           WHERE id = $1`,
          [taskId],
        );

        const outboxPayload = {
          taskId,
          taskName: row.name,
          priority: row.priority,
          payload: row.payload,
          maxRetries: row.max_retries,
        };

        const outboxResult = await client.query(
          `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
           VALUES ($1, $2, 'SCHEDULED_TASK_RELEASED', $3, 'PENDING', 0, NOW(), $4)
           RETURNING *`,
          [outboxId, taskId, JSON.stringify(outboxPayload), hasTraceContext ? JSON.stringify(traceCtx) : null],
        );

        const updatedTask = await client.query('SELECT * FROM tasks WHERE id = $1', [taskId]);

        results.push({
          task: rowToTask(updatedTask.rows[0]),
          outboxEvent: {
            id: outboxResult.rows[0].id,
            taskId: outboxResult.rows[0].task_id,
            eventType: outboxResult.rows[0].event_type,
            payload: outboxResult.rows[0].payload,
            status: outboxResult.rows[0].status,
            attempts: outboxResult.rows[0].attempts,
            createdAt: new Date(outboxResult.rows[0].created_at),
            traceContext: outboxResult.rows[0].trace_context ?? undefined,
          },
        });
      }

      await client.query('COMMIT');
      return results;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async fetchDueSchedules(batchSize: number): Promise<RecurringSchedule[]> {
    const result = await this.pool.query(
      `SELECT * FROM recurring_schedules
       WHERE status = 'ACTIVE' AND next_run_at <= NOW()
       ORDER BY next_run_at ASC
       LIMIT $1`,
      [batchSize],
    );
    return result.rows.map(rowToSchedule);
  }

  async generateOccurrence(
    scheduleId: string,
    scheduledFor: Date,
    newNextRunAt: Date,
    client?: PoolClient,
  ): Promise<OccurrenceResult> {
    const ownClient = !client;
    const c = client ?? await this.pool.connect();
    try {
      if (ownClient) await c.query('BEGIN');

      const lockResult = await c.query(
        'SELECT * FROM recurring_schedules WHERE id = $1 FOR UPDATE',
        [scheduleId],
      );
      if (lockResult.rows.length === 0) {
        if (ownClient) await c.query('ROLLBACK');
        throw new Error(`Schedule ${scheduleId} not found`);
      }
      const schedule = rowToSchedule(lockResult.rows[0]);

      if (schedule.status !== 'ACTIVE') {
        if (ownClient) await c.query('ROLLBACK');
        throw new Error(`Schedule ${scheduleId} is not ACTIVE (status: ${schedule.status})`);
      }

      const taskId = generateId();
      const outboxId = generateId();
      const traceCtx = injectTraceContext();
      const hasTraceContext = Object.keys(traceCtx).length > 0;

      let taskResult;
      try {
        taskResult = await c.query(
          `INSERT INTO tasks (id, name, status, priority, payload, max_retries, retries, version,
             scheduled_for, schedule_id, created_at, updated_at)
           VALUES ($1, $2, 'QUEUED', $3, $4, $5, 0, 1, $6, $7, NOW(), NOW())
           RETURNING *`,
          [
            taskId, schedule.taskName, schedule.taskPriority,
            JSON.stringify(schedule.taskPayload), schedule.taskMaxRetries,
            scheduledFor, scheduleId,
          ],
        );
      } catch (error: unknown) {
        if (error instanceof Error && error.message.includes('idx_unique_occurrence')) {
          if (ownClient) await c.query('ROLLBACK');
          throw new DuplicateOccurrenceError(scheduleId, scheduledFor);
        }
        throw error;
      }

      const outboxPayload = {
        taskId,
        taskName: schedule.taskName,
        priority: schedule.taskPriority,
        payload: schedule.taskPayload,
        maxRetries: schedule.taskMaxRetries,
      };

      const outboxResult = await c.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'SCHEDULED_TASK_RELEASED', $3, 'PENDING', 0, NOW(), $4)
         RETURNING *`,
        [outboxId, taskId, JSON.stringify(outboxPayload), hasTraceContext ? JSON.stringify(traceCtx) : null],
      );

      await c.query(
        `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
         WHERE id = $1`,
        [scheduleId, newNextRunAt],
      );

      if (ownClient) await c.query('COMMIT');

      return {
        task: rowToTask(taskResult.rows[0]),
        outboxEvent: {
          id: outboxResult.rows[0].id,
          taskId: outboxResult.rows[0].task_id,
          eventType: outboxResult.rows[0].event_type,
          payload: outboxResult.rows[0].payload,
          status: outboxResult.rows[0].status,
          attempts: outboxResult.rows[0].attempts,
          createdAt: new Date(outboxResult.rows[0].created_at),
          traceContext: outboxResult.rows[0].trace_context ?? undefined,
        },
      };
    } catch (error) {
      if (ownClient) await c.query('ROLLBACK');
      throw error;
    } finally {
      if (ownClient) c.release();
    }
  }

  async processScheduleWithLock(
    scheduleId: string,
    callback: (schedule: RecurringSchedule, client: PoolClient) => Promise<void>,
  ): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const lockResult = await client.query(
        'SELECT * FROM recurring_schedules WHERE id = $1 FOR UPDATE SKIP LOCKED',
        [scheduleId],
      );
      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return;
      }
      const schedule = rowToSchedule(lockResult.rows[0]);
      await callback(schedule, client);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async acquireExecutionLease(
    scheduleId: string,
    leaseDurationMs: number = 300000,
  ): Promise<{ acquired: boolean; leaseToken?: string }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const lockResult = await client.query(
        'SELECT *, NOW() as db_now FROM recurring_schedules WHERE id = $1 FOR UPDATE',
        [scheduleId],
      );
      if (lockResult.rows.length === 0) {
        await client.query('ROLLBACK');
        return { acquired: false };
      }

      const schedule = rowToSchedule(lockResult.rows[0]);
      const dbNow = new Date(lockResult.rows[0].db_now as string);

      if (
        schedule.executionLeaseToken &&
        schedule.executionLeaseExpiresAt &&
        schedule.executionLeaseExpiresAt > dbNow
      ) {
        await client.query('ROLLBACK');
        return { acquired: false };
      }

      const leaseToken = generateId();
      await client.query(
        `UPDATE recurring_schedules SET execution_lease_token = $2, execution_lease_expires_at = NOW() + $3 * INTERVAL '1 millisecond', updated_at = NOW()
         WHERE id = $1`,
        [scheduleId, leaseToken, leaseDurationMs],
      );
      await client.query('COMMIT');
      return { acquired: true, leaseToken };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async releaseExecutionLease(scheduleId: string, leaseToken: string): Promise<void> {
    const result = await this.pool.query(
      `UPDATE recurring_schedules SET execution_lease_token = NULL, execution_lease_expires_at = NULL, updated_at = NOW()
       WHERE id = $1 AND execution_lease_token = $2`,
      [scheduleId, leaseToken],
    );
    if (result.rowCount === 0) {
      throw new Error(`Cannot release execution lease: token mismatch or schedule ${scheduleId} not found`);
    }
  }

  async hasActiveOccurrence(scheduleId: string): Promise<boolean> {
    const result = await this.pool.query(
      `SELECT 1 FROM tasks
       WHERE schedule_id = $1 AND status IN ('QUEUED', 'PROCESSING')
       LIMIT 1`,
      [scheduleId],
    );
    return result.rows.length > 0;
  }
}
