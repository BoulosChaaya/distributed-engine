import type { Pool } from 'pg';
import type { TaskRepository, CreateTaskInput } from '../../db/task-repository';
import type { OutboxEvent, Task } from '../../types';

/**
 * Creates a task the way the API does: POST /tasks calls acceptTask with no
 * tenant (since 1.0.1). Tests start from that path, not a separate insert
 * that production never uses. Returns the task and its outbox event.
 */
export async function submitTask(
  repo: TaskRepository,
  input: CreateTaskInput & { idempotencyKey?: string },
): Promise<{ task: Task; outboxEvent: OutboxEvent }> {
  const result = await repo.acceptTask({ ...input, tenantId: null });
  const pool = (repo as unknown as { pool: Pool }).pool;
  const row = (await pool.query('SELECT * FROM outbox_events WHERE id = $1', [result.outboxEventId])).rows[0];
  const outboxEvent: OutboxEvent = {
    id: row.id,
    taskId: row.task_id,
    eventType: row.event_type,
    payload: row.payload,
    status: row.status,
    attempts: row.attempts,
    createdAt: new Date(row.created_at),
    processedAt: row.processed_at ? new Date(row.processed_at) : undefined,
    traceContext: row.trace_context ?? undefined,
  };
  return { task: result.task, outboxEvent };
}
