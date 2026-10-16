import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue, Worker, Job } from 'bullmq';
import IORedis from 'ioredis';
import { runMigrations } from '../db/migrations';
import { TaskRepository } from '../db/task-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { TaskReconciler } from '../db/task-reconciler';
import { WorkflowRepository, WorkflowStateError } from '../db/workflow-repository';
import { validateWorkflowDefinition, WorkflowValidationError, MAX_WORKFLOW_STEPS } from '../db/workflow-engine';
import { QuotaExceededError, IdempotencyConflictError } from '../db/tenant-repository';
import { createBuiltinHandlers, runTaskHandler } from '../task-handlers';
import type { WorkflowStepInput } from '../types';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const TEST_REDIS_HOST = process.env.TEST_REDIS_HOST || 'localhost';
const TEST_REDIS_PORT = parseInt(process.env.TEST_REDIS_PORT || '6379');
const TEST_REDIS_PASSWORD = process.env.REDIS_PASSWORD;
const QUEUE_NAME = 'tasks-test-workflows';
const silent: any = { info() {}, warn() {}, error() {}, debug() {}, child() { return silent; } };

let pool: Pool;
let tasks: TaskRepository;
let workflows: WorkflowRepository;
const connections: IORedis[] = [];
function redisConn(): IORedis {
  const c = new IORedis({ host: TEST_REDIS_HOST, port: TEST_REDIS_PORT, password: TEST_REDIS_PASSWORD, maxRetriesPerRequest: null });
  connections.push(c);
  return c;
}

// fetch -> (left, right) -> join : the classic diamond (fan-out, fan-in)
const diamond: WorkflowStepInput[] = [
  { key: 'fetch', name: 'echo', payload: { n: 1 } },
  { key: 'left', name: 'echo', payload: { side: 'L' }, dependsOn: ['fetch'] },
  { key: 'right', name: 'echo', payload: { side: 'R' }, dependsOn: ['fetch'] },
  { key: 'join', name: 'echo', payload: { done: true }, dependsOn: ['left', 'right'] },
];

async function stepRow(workflowId: string, key: string) {
  const r = await pool.query('SELECT * FROM tasks WHERE workflow_id = $1 AND step_key = $2', [workflowId, key]);
  return r.rows[0];
}

/** Drives one step through claim -> COMPLETED/FAILED the way the worker does. */
async function finish(workflowId: string, key: string, outcome: 'COMPLETED' | 'FAILED' = 'COMPLETED', output: unknown = { ok: key }) {
  const row = await stepRow(workflowId, key);
  const claimed = await tasks.transitionStatus(row.id, row.version, 'PROCESSING', { claimedBy: 'w' });
  return tasks.transitionStatus(row.id, claimed.version, outcome, {
    claimToken: claimed.claimToken,
    ...(outcome === 'COMPLETED' ? { result: { processedBy: 'w', output } } : { error: 'boom', retries: claimed.retries + 1 }),
  });
}

async function statuses(workflowId: string): Promise<Record<string, string>> {
  const r = await pool.query('SELECT step_key, status FROM tasks WHERE workflow_id = $1', [workflowId]);
  return Object.fromEntries(r.rows.map((x) => [x.step_key, x.status]));
}

async function workflowStatus(id: string) {
  return (await pool.query('SELECT status, completed_at FROM workflows WHERE id = $1', [id])).rows[0];
}

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  tasks = new TaskRepository(pool, 2000);
  workflows = new WorkflowRepository(pool);
});

afterAll(async () => {
  await pool.query('TRUNCATE outbox_events, tasks, workflows, tenant_usage, tenants CASCADE').catch(() => {});
  for (const c of connections) c.disconnect();
  await pool.end();
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks, workflows, tenant_usage, tenant_concurrency_leases, tenants CASCADE');
});

describe('workflow definition validation', () => {
  it('orders steps topologically', () => {
    const order = validateWorkflowDefinition([diamond[3], diamond[1], diamond[0], diamond[2]]).map((s) => s.key);
    expect(order.indexOf('fetch')).toBe(0);
    expect(order.indexOf('join')).toBe(3);
  });

  it.each([
    [[], /at least one step/],
    [[{ key: 'a', name: 'echo' }, { key: 'a', name: 'echo' }], /Duplicate step key "a"/],
    [[{ key: 'a b', name: 'echo' }], /must match/],
    [[{ key: 'a', name: 'echo', dependsOn: ['ghost'] }], /unknown step "ghost"/],
    [[{ key: 'a', name: 'echo', dependsOn: ['a'] }], /depends on itself/],
    [[{ key: 'a', name: 'echo' }, { key: 'b', name: 'echo', dependsOn: ['a', 'a'] }], /same dependency twice/],
  ])('rejects %j', (steps, message) => {
    expect(() => validateWorkflowDefinition(steps as WorkflowStepInput[])).toThrow(message);
  });

  it('names the cycle it finds', () => {
    const steps = [
      { key: 'a', name: 'echo', dependsOn: ['c'] },
      { key: 'b', name: 'echo', dependsOn: ['a'] },
      { key: 'c', name: 'echo', dependsOn: ['b'] },
      { key: 'd', name: 'echo' },
    ];
    expect(() => validateWorkflowDefinition(steps)).toThrow(WorkflowValidationError);
    expect(() => validateWorkflowDefinition(steps)).toThrow(/Dependency cycle: (a -> b -> c -> a|b -> c -> a -> b|c -> a -> b -> c)/);
  });

  it('caps the size of a workflow', () => {
    const steps = Array.from({ length: MAX_WORKFLOW_STEPS + 1 }, (_, i) => ({ key: `s${i}`, name: 'echo' }));
    expect(() => validateWorkflowDefinition(steps)).toThrow(/at most/);
  });
});

describe('workflow execution in PostgreSQL', () => {
  it('creates the whole graph atomically: roots QUEUED with an outbox event, the rest BLOCKED', async () => {
    const { detail } = await workflows.createWorkflow({ name: 'diamond', steps: diamond });
    const id = detail.workflow.id;
    expect(detail.workflow.status).toBe('RUNNING');
    expect(await statuses(id)).toEqual({ fetch: 'QUEUED', left: 'BLOCKED', right: 'BLOCKED', join: 'BLOCKED' });
    expect((await stepRow(id, 'join')).pending_dependencies).toBe(2);
    const events = await pool.query(
      `SELECT t.step_key FROM outbox_events o JOIN tasks t ON t.id = o.task_id WHERE t.workflow_id = $1`, [id]);
    expect(events.rows.map((r) => r.step_key)).toEqual(['fetch']);
    expect(detail.steps.map((s) => [s.key, s.level])).toEqual([['fetch', 0], ['left', 1], ['right', 1], ['join', 2]]);
    expect(detail.steps.find((s) => s.key === 'join')!.dependsOn).toEqual(['left', 'right']);
  });

  it('writes nothing when the definition is invalid', async () => {
    await expect(workflows.createWorkflow({ name: 'bad', steps: [{ key: 'a', name: 'echo', dependsOn: ['a'] }] }))
      .rejects.toThrow(WorkflowValidationError);
    expect((await pool.query('SELECT COUNT(*)::int AS c FROM workflows')).rows[0].c).toBe(0);
    expect((await pool.query('SELECT COUNT(*)::int AS c FROM tasks')).rows[0].c).toBe(0);
  });

  it('releases children when a parent completes, and waits for every parent before a join', async () => {
    const id = (await workflows.createWorkflow({ name: 'diamond', steps: diamond })).detail.workflow.id;

    await finish(id, 'fetch');
    expect(await statuses(id)).toMatchObject({ fetch: 'COMPLETED', left: 'QUEUED', right: 'QUEUED', join: 'BLOCKED' });
    const ready = await pool.query(
      `SELECT t.step_key FROM outbox_events o JOIN tasks t ON t.id = o.task_id
       WHERE t.workflow_id = $1 AND o.event_type = 'WORKFLOW_STEP_READY' ORDER BY t.step_key`, [id]);
    expect(ready.rows.map((r) => r.step_key)).toEqual(['left', 'right']);

    await finish(id, 'left');
    expect((await statuses(id)).join).toBe('BLOCKED');
    expect((await stepRow(id, 'join')).pending_dependencies).toBe(1);

    await finish(id, 'right');
    expect((await statuses(id)).join).toBe('QUEUED');
    expect((await workflowStatus(id)).status).toBe('RUNNING');

    await finish(id, 'join');
    const wf = await workflowStatus(id);
    expect(wf.status).toBe('COMPLETED');
    expect(wf.completed_at).not.toBeNull();
  });

  it('hands a step the outputs of its direct parents', async () => {
    const id = (await workflows.createWorkflow({ name: 'diamond', steps: diamond })).detail.workflow.id;
    await finish(id, 'fetch', 'COMPLETED', { rows: 3 });
    await finish(id, 'left', 'COMPLETED', 'L-out');
    await finish(id, 'right', 'COMPLETED', { r: true });
    const join = await stepRow(id, 'join');
    expect(await tasks.getUpstreamOutputs(join.id)).toEqual({ left: 'L-out', right: { r: true } });
  });

  it('a failed step cancels only its downstream; independent branches finish; the workflow ends FAILED', async () => {
    const steps: WorkflowStepInput[] = [
      ...diamond,
      { key: 'audit', name: 'echo' }, // independent root
    ];
    const id = (await workflows.createWorkflow({ name: 'partial', steps })).detail.workflow.id;
    await finish(id, 'fetch');
    await finish(id, 'left', 'FAILED');

    const s = await statuses(id);
    expect(s).toMatchObject({ left: 'FAILED', right: 'QUEUED', join: 'CANCELLED', audit: 'QUEUED' });
    expect((await stepRow(id, 'join')).error).toBe('Upstream step "left" failed');
    expect((await workflowStatus(id)).status).toBe('RUNNING');

    await finish(id, 'right');
    await finish(id, 'audit');
    expect((await workflowStatus(id)).status).toBe('FAILED');
  });

  it('cancelling one step cancels its descendants', async () => {
    const id = (await workflows.createWorkflow({ name: 'diamond', steps: diamond })).detail.workflow.id;
    await tasks.cancelTask((await stepRow(id, 'fetch')).id);
    expect(await statuses(id)).toEqual({ fetch: 'CANCELLED', left: 'CANCELLED', right: 'CANCELLED', join: 'CANCELLED' });
    expect((await stepRow(id, 'left')).error).toBe('Upstream step "fetch" was cancelled');
    expect((await workflowStatus(id)).status).toBe('FAILED');
  });

  it('cancels a whole workflow, including a step a worker holds, and refuses to cancel twice', async () => {
    const id = (await workflows.createWorkflow({ name: 'diamond', steps: diamond })).detail.workflow.id;
    await finish(id, 'fetch');
    const left = await stepRow(id, 'left');
    const claimed = await tasks.transitionStatus(left.id, left.version, 'PROCESSING', { claimedBy: 'w' });

    const result = await workflows.cancelWorkflow(id);
    expect(result.workflow.status).toBe('CANCELLED');
    expect(result.cancelledTaskIds).toHaveLength(3);
    expect(await statuses(id)).toEqual({ fetch: 'COMPLETED', left: 'CANCELLED', right: 'CANCELLED', join: 'CANCELLED' });

    // The worker that held "left" cannot record an outcome any more.
    await expect(tasks.transitionStatus(left.id, claimed.version, 'COMPLETED', { claimToken: claimed.claimToken }))
      .rejects.toThrow();
    await expect(workflows.cancelWorkflow(id)).rejects.toThrow(WorkflowStateError);
  });

  it('two parents completing at the same moment release the join exactly once', async () => {
    for (let round = 0; round < 10; round++) {
      const id = (await workflows.createWorkflow({ name: `race-${round}`, steps: diamond })).detail.workflow.id;
      await finish(id, 'fetch');
      await Promise.all([finish(id, 'left'), finish(id, 'right')]);
      const join = await stepRow(id, 'join');
      expect(join.status).toBe('QUEUED');
      expect(join.pending_dependencies).toBe(0);
      const ev = await pool.query(`SELECT COUNT(*)::int AS c FROM outbox_events WHERE task_id = $1`, [join.id]);
      expect(ev.rows[0].c).toBe(1);
    }
  });

  it('is idempotent on a key: same definition returns the same workflow, a different one conflicts', async () => {
    const a = await workflows.createWorkflow({ name: 'once', steps: diamond, idempotencyKey: 'k1' });
    const b = await workflows.createWorkflow({ name: 'once', steps: [...diamond].reverse(), idempotencyKey: 'k1' });
    expect(b.idempotent).toBe(true);
    expect(b.detail.workflow.id).toBe(a.detail.workflow.id);
    await expect(workflows.createWorkflow({ name: 'once', steps: diamond.slice(0, 1), idempotencyKey: 'k1' }))
      .rejects.toThrow(IdempotencyConflictError);
    expect((await pool.query('SELECT COUNT(*)::int AS c FROM workflows')).rows[0].c).toBe(1);
  });

  it('charges a tenant one job per step and refuses a workflow that would exceed the quota', async () => {
    await pool.query(`INSERT INTO tenants (id, name, status, plan_id, api_key_hash, created_at, updated_at)
                      VALUES ('t1', 'T', 'ACTIVE', 'plan_starter', 'h', NOW(), NOW())`);
    const period = new Date('2026-10-01T00:00:00Z');
    await workflows.createWorkflow({ name: 'ok', steps: diamond, tenantId: 't1', billingPeriodStart: period, maxJobsPerPeriod: 6 });
    const used = await pool.query(`SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = 't1'`);
    expect(used.rows[0].accepted_jobs).toBe(4);

    await expect(workflows.createWorkflow({ name: 'too-big', steps: diamond, tenantId: 't1', billingPeriodStart: period, maxJobsPerPeriod: 6 }))
      .rejects.toThrow(QuotaExceededError);
    expect((await pool.query(`SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = 't1'`)).rows[0].accepted_jobs).toBe(4);
    expect((await pool.query('SELECT COUNT(*)::int AS c FROM workflows')).rows[0].c).toBe(1);

    // Tenant scoping: another tenant cannot see it.
    const mine = await workflows.listWorkflows(1, 10, 't1');
    expect(mine.total).toBe(1);
    expect((await workflows.listWorkflows(1, 10, 'other')).total).toBe(0);
  });
});

describe('workflow end to end through the queue (requires PostgreSQL + Redis)', () => {
  it('runs a fan-out/fan-in DAG with data flowing to the join, surviving a Redis wipe mid-run', async () => {
    const queue = new Queue(QUEUE_NAME, { connection: redisConn() });
    const stale = await redisConn().keys(`bull:${QUEUE_NAME}:*`);
    if (stale.length) await redisConn().del(...stale);
    const publisher = new OutboxPublisher(pool, queue, 100, 50, 5, { logger: silent });
    const reconciler = new TaskReconciler(pool, queue as any, { intervalMs: 300, queuedGraceMs: 800, logger: silent });
    const handlers = createBuiltinHandlers();
    const executions = new Map<string, number>();

    // Same contract as the real worker: claim, run the registered handler
    // with upstream outputs, record the outcome with the claim token.
    const worker = new Worker(QUEUE_NAME, async (job: Job) => {
      const task = await tasks.getTask(job.data.taskId);
      if (!task || task.status !== 'QUEUED') return;
      const claimed = await tasks.transitionStatus(task.id, task.version, 'PROCESSING', { claimedBy: 'w' });
      executions.set(task.stepKey!, (executions.get(task.stepKey!) ?? 0) + 1);
      const upstream = await tasks.getUpstreamOutputs(task.id);
      const output = await runTaskHandler(handlers, {
        taskId: task.id, name: task.name, payload: task.payload, attempt: 1, upstream,
        signal: new AbortController().signal,
      }, 10_000);
      await tasks.transitionStatus(task.id, claimed.version, 'COMPLETED', { claimToken: claimed.claimToken, result: { output } });
    }, { connection: redisConn(), concurrency: 4 });
    worker.on('error', () => {});

    const steps: WorkflowStepInput[] = [
      { key: 'extract', name: 'echo', payload: { source: 'db' } },
      ...Array.from({ length: 6 }, (_, i) => ({ key: `shard${i}`, name: 'sleep', payload: { ms: 300 }, dependsOn: ['extract'] })),
      { key: 'merge', name: 'echo', payload: { merged: true }, dependsOn: Array.from({ length: 6 }, (_, i) => `shard${i}`) },
      { key: 'report', name: 'echo', payload: {}, dependsOn: ['merge'] },
    ];
    const id = (await workflows.createWorkflow({ name: 'etl', steps })).detail.workflow.id;

    publisher.start();
    reconciler.start();
    try {
      // Let the shards start, then lose everything Redis holds.
      for (let i = 0; i < 100 && (await statuses(id)).extract !== 'COMPLETED'; i++) await new Promise((r) => setTimeout(r, 50));
      await new Promise((r) => setTimeout(r, 150));
      const keys = await redisConn().keys(`bull:${QUEUE_NAME}:*`);
      expect(keys.length).toBeGreaterThan(0);
      await redisConn().del(...keys);

      for (let i = 0; i < 300 && (await workflowStatus(id)).status === 'RUNNING'; i++) await new Promise((r) => setTimeout(r, 100));
    } finally {
      await reconciler.stop();
      await publisher.stop();
      await worker.close();
      await queue.close();
    }

    expect((await workflowStatus(id)).status).toBe('COMPLETED');
    expect(Object.values(await statuses(id)).every((s) => s === 'COMPLETED')).toBe(true);
    for (const step of steps) expect(executions.get(step.key)).toBe(1);

    const merge = await stepRow(id, 'merge');
    expect(Object.keys(merge.result.output.upstream).sort()).toEqual(['shard0', 'shard1', 'shard2', 'shard3', 'shard4', 'shard5']);
    expect(merge.result.output.upstream.shard0).toEqual({ sleptMs: 300 });
    const report = await stepRow(id, 'report');
    expect(report.result.output.upstream.merge.echo).toEqual({ merged: true });
  }, 60_000);
});
