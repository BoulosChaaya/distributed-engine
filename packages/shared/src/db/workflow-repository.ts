import { Pool } from 'pg';
import { createHash } from 'crypto';
import type { TaskPriority, TaskStatus, Workflow, WorkflowStatus, WorkflowStepInput } from '../types.js';
import { generateId } from '../utils.js';
import { injectTraceContext } from '../telemetry/propagation.js';
import { QuotaExceededError, IdempotencyConflictError } from './tenant-repository.js';
import { validateWorkflowDefinition, finalizeWorkflowIfDone } from './workflow-engine.js';

export interface CreateWorkflowInput {
  name: string;
  steps: WorkflowStepInput[];
  tenantId?: string;
  idempotencyKey?: string;
  /** Required with tenantId: the whole workflow is one quota check for N steps. */
  billingPeriodStart?: Date;
  maxJobsPerPeriod?: number;
}

export interface WorkflowStepView {
  key: string;
  taskId: string;
  name: string;
  status: TaskStatus;
  dependsOn: string[];
  /** 0 for roots; 1 + the deepest dependency otherwise. Used for drawing. */
  level: number;
  retries: number;
  maxRetries: number;
  error?: string;
  output?: unknown;
  updatedAt: Date;
}

export interface WorkflowDetail {
  workflow: Workflow;
  steps: WorkflowStepView[];
}

export interface WorkflowSummary extends Workflow {
  totalSteps: number;
  completedSteps: number;
  failedSteps: number;
}

export class WorkflowStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkflowStateError';
  }
}

function rowToWorkflow(row: Record<string, unknown>): Workflow {
  return {
    id: row.id as string,
    tenantId: (row.tenant_id as string) || undefined,
    name: row.name as string,
    status: row.status as WorkflowStatus,
    idempotencyKey: (row.idempotency_key as string) || undefined,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
    completedAt: row.completed_at ? new Date(row.completed_at as string) : undefined,
  };
}

function definitionHash(name: string, steps: WorkflowStepInput[]): string {
  const canonical = steps
    .map((s) => ({
      key: s.key,
      name: s.name,
      payload: s.payload ?? {},
      priority: s.priority ?? 'NORMAL',
      maxRetries: s.maxRetries ?? 3,
      dependsOn: [...(s.dependsOn ?? [])].sort(),
    }))
    .sort((a, b) => a.key.localeCompare(b.key));
  return createHash('sha256').update(JSON.stringify({ name, steps: canonical })).digest('hex');
}

/**
 * Workflows (DAGs of tasks). Creation writes the workflow, every step, every
 * dependency edge and the root steps' outbox events in one transaction. Either
 * the whole graph exists or none of it does. Progress after that is driven by
 * onTaskTerminal() in workflow-engine.ts.
 */
export class WorkflowRepository {
  constructor(private pool: Pool) {}

  async createWorkflow(input: CreateWorkflowInput): Promise<{ detail: WorkflowDetail; idempotent: boolean }> {
    const ordered = validateWorkflowDefinition(input.steps);
    const hash = input.idempotencyKey ? definitionHash(input.name, input.steps) : null;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      if (input.idempotencyKey) {
        const existing = await client.query(
          `SELECT id, idempotency_hash FROM workflows
           WHERE COALESCE(tenant_id, '') = COALESCE($1, '') AND idempotency_key = $2`,
          [input.tenantId ?? null, input.idempotencyKey],
        );
        if (existing.rows.length > 0) {
          await client.query('ROLLBACK');
          if (existing.rows[0].idempotency_hash !== hash) {
            throw new IdempotencyConflictError(input.tenantId ?? 'operator', input.idempotencyKey);
          }
          return { detail: (await this.getWorkflow(existing.rows[0].id))!, idempotent: true };
        }
      }

      if (input.tenantId) {
        if (!input.billingPeriodStart || input.maxJobsPerPeriod === undefined) {
          throw new Error('Tenant workflows need billingPeriodStart and maxJobsPerPeriod');
        }
        // Same row-locked usage counter as single tasks; a workflow of N steps uses N jobs.
        const usage = await client.query(
          `INSERT INTO tenant_usage (id, tenant_id, billing_period_start, accepted_jobs, compute_units, storage_mb, created_at, updated_at)
           VALUES ($1, $2, $3, 0, 0, 0, NOW(), NOW())
           ON CONFLICT (tenant_id, billing_period_start) DO UPDATE SET updated_at = NOW()
           RETURNING accepted_jobs`,
          [generateId(), input.tenantId, input.billingPeriodStart],
        );
        const used = usage.rows[0].accepted_jobs as number;
        if (used + ordered.length > input.maxJobsPerPeriod) {
          throw new QuotaExceededError(input.tenantId, input.maxJobsPerPeriod, used);
        }
        await client.query(
          `UPDATE tenant_usage SET accepted_jobs = accepted_jobs + $3, updated_at = NOW()
           WHERE tenant_id = $1 AND billing_period_start = $2`,
          [input.tenantId, input.billingPeriodStart, ordered.length],
        );
      }

      const workflowId = generateId();
      await client.query(
        `INSERT INTO workflows (id, tenant_id, name, status, idempotency_key, idempotency_hash, created_at, updated_at)
         VALUES ($1, $2, $3, 'RUNNING', $4, $5, NOW(), NOW())`,
        [workflowId, input.tenantId ?? null, input.name, input.idempotencyKey ?? null, hash],
      );

      const traceCtx = injectTraceContext();
      const trace = Object.keys(traceCtx).length > 0 ? JSON.stringify(traceCtx) : null;
      const ids = new Map<string, string>();

      // Topological order, so every parent row exists before its edges.
      for (const step of ordered) {
        const taskId = generateId();
        ids.set(step.key, taskId);
        const deps = step.dependsOn ?? [];
        const priority: TaskPriority = step.priority ?? 'NORMAL';
        const maxRetries = step.maxRetries ?? 3;
        const payload = step.payload ?? {};
        await client.query(
          `INSERT INTO tasks (id, tenant_id, name, status, priority, payload, max_retries, retries, version,
             workflow_id, step_key, pending_dependencies, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 1, $8, $9, $10, NOW(), NOW())`,
          [taskId, input.tenantId ?? null, step.name, deps.length === 0 ? 'QUEUED' : 'BLOCKED', priority,
           JSON.stringify(payload), maxRetries, workflowId, step.key, deps.length],
        );
        for (const dep of deps) {
          await client.query(
            'INSERT INTO task_dependencies (task_id, depends_on) VALUES ($1, $2)',
            [taskId, ids.get(dep)],
          );
        }
        if (deps.length === 0) {
          await client.query(
            `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
             VALUES ($1, $2, 'TASK_CREATED', $3, 'PENDING', 0, NOW(), $4)`,
            [generateId(), taskId,
             JSON.stringify({ taskId, taskName: step.name, priority, payload, maxRetries }), trace],
          );
        }
      }

      await client.query('COMMIT');
      return { detail: (await this.getWorkflow(workflowId))!, idempotent: false };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      const pgError = error as { code?: string };
      if (pgError.code === '23505' && input.idempotencyKey) {
        // Lost a race with an identical request: return the winner.
        const winner = await this.pool.query(
          `SELECT id, idempotency_hash FROM workflows
           WHERE COALESCE(tenant_id, '') = COALESCE($1, '') AND idempotency_key = $2`,
          [input.tenantId ?? null, input.idempotencyKey],
        );
        if (winner.rows.length > 0) {
          if (winner.rows[0].idempotency_hash !== hash) {
            throw new IdempotencyConflictError(input.tenantId ?? 'operator', input.idempotencyKey);
          }
          return { detail: (await this.getWorkflow(winner.rows[0].id))!, idempotent: true };
        }
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async getWorkflow(workflowId: string, tenantId?: string): Promise<WorkflowDetail | null> {
    const wf = await this.pool.query(
      `SELECT * FROM workflows WHERE id = $1 AND ($2::text IS NULL OR tenant_id = $2)`,
      [workflowId, tenantId ?? null],
    );
    if (wf.rows.length === 0) return null;

    const rows = await this.pool.query(
      `SELECT t.id, t.step_key, t.name, t.status, t.retries, t.max_retries, t.error,
              t.result -> 'output' AS output, t.updated_at,
              COALESCE(ARRAY(
                SELECT p.step_key FROM task_dependencies d JOIN tasks p ON p.id = d.depends_on
                WHERE d.task_id = t.id ORDER BY p.step_key
              ), '{}') AS depends_on
       FROM tasks t WHERE t.workflow_id = $1`,
      [workflowId],
    );

    const deps = new Map<string, string[]>(rows.rows.map((r) => [r.step_key as string, r.depends_on as string[]]));
    const levels = new Map<string, number>();
    const levelOf = (key: string): number => {
      const known = levels.get(key);
      if (known !== undefined) return known;
      const parents = deps.get(key) ?? [];
      const level = parents.length === 0 ? 0 : 1 + Math.max(...parents.map(levelOf));
      levels.set(key, level);
      return level;
    };

    const steps: WorkflowStepView[] = rows.rows
      .map((r) => ({
        key: r.step_key as string,
        taskId: r.id as string,
        name: r.name as string,
        status: r.status as TaskStatus,
        dependsOn: r.depends_on as string[],
        level: levelOf(r.step_key as string),
        retries: r.retries as number,
        maxRetries: r.max_retries as number,
        error: (r.error as string) || undefined,
        output: r.output ?? undefined,
        updatedAt: new Date(r.updated_at as string),
      }))
      .sort((a, b) => a.level - b.level || a.key.localeCompare(b.key));

    return { workflow: rowToWorkflow(wf.rows[0]), steps };
  }

  async listWorkflows(page: number, pageSize: number, tenantId?: string): Promise<{ items: WorkflowSummary[]; total: number }> {
    const where = '($1::text IS NULL OR w.tenant_id = $1)';
    const total = await this.pool.query(`SELECT COUNT(*)::int AS c FROM workflows w WHERE ${where}`, [tenantId ?? null]);
    const rows = await this.pool.query(
      `SELECT w.*,
         (SELECT COUNT(*)::int FROM tasks t WHERE t.workflow_id = w.id) AS total_steps,
         (SELECT COUNT(*)::int FROM tasks t WHERE t.workflow_id = w.id AND t.status = 'COMPLETED') AS completed_steps,
         (SELECT COUNT(*)::int FROM tasks t WHERE t.workflow_id = w.id AND t.status IN ('FAILED','CANCELLED')) AS failed_steps
       FROM workflows w WHERE ${where}
       ORDER BY w.created_at DESC, w.id
       LIMIT $2 OFFSET $3`,
      [tenantId ?? null, pageSize, (page - 1) * pageSize],
    );
    return {
      total: total.rows[0].c,
      items: rows.rows.map((r) => ({
        ...rowToWorkflow(r),
        totalSteps: r.total_steps,
        completedSteps: r.completed_steps,
        failedSteps: r.failed_steps,
      })),
    };
  }

  /**
   * Cancels a running workflow and every step that has not finished, in one
   * transaction (workflow row locked first, per the lock order). Returns the
   * cancelled task ids so the caller can drop their queue jobs best-effort.
   * Workers holding a cancelled step lose on their next write (version and
   * claim token changed).
   */
  async cancelWorkflow(workflowId: string, tenantId?: string): Promise<{ workflow: Workflow; cancelledTaskIds: string[] }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const wf = await client.query(
        `SELECT * FROM workflows WHERE id = $1 AND ($2::text IS NULL OR tenant_id = $2) FOR UPDATE`,
        [workflowId, tenantId ?? null],
      );
      if (wf.rows.length === 0) throw new Error(`Workflow ${workflowId} not found`);
      if (wf.rows[0].status !== 'RUNNING') {
        throw new WorkflowStateError(`Workflow ${workflowId} is already ${wf.rows[0].status}`);
      }
      await client.query(`UPDATE workflows SET status = 'CANCELLED', updated_at = NOW() WHERE id = $1`, [workflowId]);
      const cancelled = await client.query(
        `UPDATE tasks SET status = 'CANCELLED', version = version + 1, updated_at = NOW(),
           claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL, error = 'Workflow cancelled'
         WHERE workflow_id = $1 AND status IN ('SCHEDULED','BLOCKED','QUEUED','PROCESSING')
         RETURNING id`,
        [workflowId],
      );
      await finalizeWorkflowIfDone(client, workflowId);
      const final = await client.query('SELECT * FROM workflows WHERE id = $1', [workflowId]);
      await client.query('COMMIT');
      return { workflow: rowToWorkflow(final.rows[0]), cancelledTaskIds: cancelled.rows.map((r) => r.id as string) };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
