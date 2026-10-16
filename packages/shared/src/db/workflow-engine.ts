import type { PoolClient } from 'pg';
import { generateId } from '../utils.js';
import type { TaskStatus, WorkflowStepInput } from '../types.js';

/**
 * Workflow (DAG) mechanics that must run inside the same transaction as a
 * task's state change.
 *
 * A workflow is a set of tasks plus `task_dependencies` edges. A step with
 * dependencies is created BLOCKED, with `pending_dependencies` set to its
 * number of parents. It has no outbox event and no queue job, so no worker can
 * see it. Every terminal transition of a workflow task calls onTaskTerminal()
 * in the transaction that records it:
 *
 *   COMPLETED          -> decrement each BLOCKED child's counter; a child that
 *                         reaches 0 becomes QUEUED and gets an outbox event in
 *                         this same transaction (fan-in = wait for all parents)
 *   FAILED / CANCELLED -> every BLOCKED descendant becomes CANCELLED
 *                         ("upstream failed"); independent branches keep running
 *   then               -> if no step is left unfinished, the workflow becomes
 *                         COMPLETED, FAILED or CANCELLED
 *
 * Doing this in the parent's transaction is what makes the DAG crash-safe.
 * There is no moment where a parent is COMPLETED but its children were not
 * released: both commit together or neither does. A worker dying right after
 * the commit loses nothing, because the release is already an outbox row.
 *
 * Lock order: workflow row first, then task rows. Every path that changes a
 * workflow task to a terminal state calls lockWorkflowForTask() before
 * locking the task. Otherwise a parent completing (task -> children) could
 * deadlock against a child being cancelled (child -> workflow). Holding the
 * workflow row also serialises the "is everything finished?" check, so two
 * last steps finishing together cannot both miss the final state.
 */

export const TERMINAL_STATUSES: ReadonlySet<TaskStatus> = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);

export const MAX_WORKFLOW_STEPS = 200;
const STEP_KEY = /^[A-Za-z0-9_-]{1,64}$/;

export class WorkflowValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkflowValidationError';
  }
}

/**
 * Checks a workflow definition and returns its steps in topological order
 * (Kahn's algorithm). Rejects duplicate or malformed keys, unknown or
 * repeated dependencies, self-dependencies and cycles (reporting the cycle).
 */
export function validateWorkflowDefinition(steps: WorkflowStepInput[]): WorkflowStepInput[] {
  if (!Array.isArray(steps) || steps.length === 0) {
    throw new WorkflowValidationError('A workflow needs at least one step');
  }
  if (steps.length > MAX_WORKFLOW_STEPS) {
    throw new WorkflowValidationError(`A workflow may have at most ${MAX_WORKFLOW_STEPS} steps`);
  }

  const byKey = new Map<string, WorkflowStepInput>();
  for (const step of steps) {
    if (typeof step.key !== 'string' || !STEP_KEY.test(step.key)) {
      throw new WorkflowValidationError(`Step key "${String(step.key)}" must match ${STEP_KEY}`);
    }
    if (byKey.has(step.key)) throw new WorkflowValidationError(`Duplicate step key "${step.key}"`);
    byKey.set(step.key, step);
  }

  const indegree = new Map<string, number>();
  const children = new Map<string, string[]>();
  for (const step of steps) {
    const deps = step.dependsOn ?? [];
    if (new Set(deps).size !== deps.length) {
      throw new WorkflowValidationError(`Step "${step.key}" lists the same dependency twice`);
    }
    for (const dep of deps) {
      if (dep === step.key) throw new WorkflowValidationError(`Step "${step.key}" depends on itself`);
      if (!byKey.has(dep)) throw new WorkflowValidationError(`Step "${step.key}" depends on unknown step "${dep}"`);
      children.set(dep, [...(children.get(dep) ?? []), step.key]);
    }
    indegree.set(step.key, deps.length);
  }

  const ready = steps.filter((s) => (indegree.get(s.key) ?? 0) === 0).map((s) => s.key);
  const order: WorkflowStepInput[] = [];
  while (ready.length > 0) {
    const key = ready.shift()!;
    order.push(byKey.get(key)!);
    for (const child of children.get(key) ?? []) {
      const left = (indegree.get(child) ?? 0) - 1;
      indegree.set(child, left);
      if (left === 0) ready.push(child);
    }
  }

  if (order.length !== steps.length) {
    throw new WorkflowValidationError(`Dependency cycle: ${findCycle(steps).join(' -> ')}`);
  }
  return order;
}

function findCycle(steps: WorkflowStepInput[]): string[] {
  const deps = new Map(steps.map((s) => [s.key, s.dependsOn ?? []]));
  const state = new Map<string, 'visiting' | 'done'>();
  const stack: string[] = [];
  const visit = (key: string): string[] | null => {
    if (state.get(key) === 'done') return null;
    if (state.get(key) === 'visiting') return [...stack.slice(stack.indexOf(key)), key];
    state.set(key, 'visiting');
    stack.push(key);
    for (const dep of deps.get(key) ?? []) {
      const cycle = visit(dep);
      if (cycle) return cycle;
    }
    stack.pop();
    state.set(key, 'done');
    return null;
  };
  for (const s of steps) {
    const cycle = visit(s.key);
    if (cycle) return cycle.reverse();
  }
  return [];
}

/**
 * Locks the workflow a task belongs to, if any, and returns its id.
 * Call before locking the task row (see lock order above).
 */
export async function lockWorkflowForTask(client: PoolClient, taskId: string): Promise<string | null> {
  const r = await client.query('SELECT workflow_id FROM tasks WHERE id = $1', [taskId]);
  const workflowId = (r.rows[0]?.workflow_id as string | null) ?? null;
  if (workflowId) {
    await client.query('SELECT id FROM workflows WHERE id = $1 FOR UPDATE', [workflowId]);
  }
  return workflowId;
}

/**
 * Applies the DAG consequences of a workflow task reaching a terminal state.
 * Must run in the transaction that wrote that state, with the workflow row
 * already locked.
 */
export async function onTaskTerminal(
  client: PoolClient,
  workflowId: string,
  taskId: string,
  status: TaskStatus,
): Promise<void> {
  if (status === 'COMPLETED') {
    const decremented = await client.query(
      `UPDATE tasks t
       SET pending_dependencies = t.pending_dependencies - 1, updated_at = NOW()
       FROM task_dependencies d
       WHERE d.depends_on = $1 AND t.id = d.task_id AND t.status = 'BLOCKED'
       RETURNING t.id, t.name, t.priority, t.payload, t.max_retries, t.pending_dependencies`,
      [taskId],
    );
    for (const child of decremented.rows) {
      if (child.pending_dependencies !== 0) continue;
      await client.query(
        `UPDATE tasks SET status = 'QUEUED', version = version + 1, updated_at = NOW() WHERE id = $1`,
        [child.id],
      );
      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'WORKFLOW_STEP_READY', $3, 'PENDING', 0, NOW(),
                 (SELECT trace_context FROM outbox_events WHERE task_id = $4 ORDER BY created_at DESC LIMIT 1))`,
        [
          generateId(),
          child.id,
          JSON.stringify({
            taskId: child.id,
            taskName: child.name,
            priority: child.priority,
            payload: child.payload,
            maxRetries: child.max_retries,
          }),
          taskId,
        ],
      );
    }
  } else if (status === 'FAILED' || status === 'CANCELLED') {
    const step = await client.query('SELECT step_key FROM tasks WHERE id = $1', [taskId]);
    const why = status === 'FAILED' ? 'failed' : 'was cancelled';
    await client.query(
      `WITH RECURSIVE downstream(id) AS (
         SELECT d.task_id FROM task_dependencies d WHERE d.depends_on = $1
         UNION
         SELECT d.task_id FROM task_dependencies d JOIN downstream ON d.depends_on = downstream.id
       )
       UPDATE tasks SET status = 'CANCELLED', version = version + 1, updated_at = NOW(),
         error = $2
       WHERE id IN (SELECT id FROM downstream) AND status = 'BLOCKED'`,
      [taskId, `Upstream step "${step.rows[0]?.step_key ?? taskId}" ${why}`],
    );
  }

  await finalizeWorkflowIfDone(client, workflowId);
}

/** Sets the workflow's final state once none of its steps can still change. */
export async function finalizeWorkflowIfDone(client: PoolClient, workflowId: string): Promise<void> {
  const counts = await client.query(
    `SELECT
       COUNT(*) FILTER (WHERE status NOT IN ('COMPLETED','FAILED','CANCELLED'))::int AS open,
       COUNT(*) FILTER (WHERE status IN ('FAILED','CANCELLED'))::int AS bad
     FROM tasks WHERE workflow_id = $1`,
    [workflowId],
  );
  const { open, bad } = counts.rows[0];
  if (open > 0) return;
  await client.query(
    `UPDATE workflows
     SET status = CASE WHEN status = 'CANCELLED' THEN 'CANCELLED'
                       WHEN $2::int > 0 THEN 'FAILED'
                       ELSE 'COMPLETED' END,
         completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
     WHERE id = $1`,
    [workflowId, bad],
  );
}
