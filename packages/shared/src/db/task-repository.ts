import { Pool } from 'pg';
import { Task, TaskStatus, TaskPriority, OutboxEvent } from '../types';
import { assertValidTransition } from '../state-machine';
import { generateId } from '../utils';

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
  };
}

export class TaskRepository {
  constructor(private pool: Pool) {}

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

      const outboxResult = await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at)
         VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW())
         RETURNING *`,
        [outboxId, taskId, JSON.stringify(outboxPayload)],
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
    extra?: Partial<Pick<Task, 'error' | 'result' | 'retries' | 'startedAt' | 'completedAt' | 'claimedBy'>>,
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

      const updateResult = await client.query(
        `UPDATE tasks SET version = version + 1, updated_at = NOW(), claimed_by = $2
         WHERE id = $1 RETURNING *`,
        [taskId, workerId || null],
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
        `UPDATE tasks SET status = 'CANCELLED', version = version + 1, updated_at = NOW(), claimed_by = NULL
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
