import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import express, { Request, Response, NextFunction } from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { Pool } from 'pg';
import { AppError, TenantRepository, WorkflowRepository, runMigrations } from '@repo/shared';
import { createOperatorAuth } from '../operator-auth';
import { createTenantAdminRouter } from '../tenant-admin';
import { createTenantAuth } from '../tenant-middleware';
import { createWorkflowRouter } from '../workflow-routes';
import { ValidationError } from '../validation';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const OPERATOR_KEY = 'w'.repeat(40);
const op = { Authorization: `Bearer ${OPERATOR_KEY}`, 'Content-Type': 'application/json' };

let pool: Pool;
let server: Server;
let base: string;
const removed: string[] = [];

const diamond = {
  name: 'diamond',
  steps: [
    { key: 'a', name: 'echo' },
    { key: 'b', name: 'echo', dependsOn: ['a'] },
    { key: 'c', name: 'echo', dependsOn: ['a'] },
    { key: 'd', name: 'echo', dependsOn: ['b', 'c'] },
  ],
};

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  const tenants = new TenantRepository(pool);
  const router = createWorkflowRouter(new WorkflowRepository(pool), tenants, {
    getJob: async (id: string) => ({ remove: async () => { removed.push(id); } }) as any,
  });

  // Mirrors main.ts wiring for these routes.
  const app = express();
  app.use(express.json());
  app.use(['/workflows', '/tenants'], createOperatorAuth(OPERATOR_KEY));
  app.use(createTenantAdminRouter(tenants));
  app.use('/workflows', router);
  app.use('/tenant/workflows', createTenantAuth(tenants), router);
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ValidationError) return res.status(422).json({ success: false, error: 'Validation error' });
    if (err instanceof AppError) return res.status(err.statusCode).json({ success: false, error: err.message });
    res.status(500).json({ success: false, error: String(err) });
  });
  await new Promise<void>((r) => { server = app.listen(0, () => r()); });
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  await new Promise((r) => server.close(r));
  await pool.query('TRUNCATE outbox_events, tasks, workflows, tenant_usage, tenants CASCADE').catch(() => {});
  await pool.end();
});

beforeEach(async () => {
  removed.length = 0;
  await pool.query('TRUNCATE outbox_events, tasks, workflows, tenant_usage, tenant_concurrency_leases, tenants CASCADE');
});

async function post(path: string, body: unknown, headers: Record<string, string> = op) {
  const res = await fetch(base + path, { method: 'POST', headers, body: JSON.stringify(body) });
  return { status: res.status, body: (await res.json()) as any };
}

async function newTenant(planId = 'plan_starter') {
  const { body } = await post('/tenants', { name: 'T', planId });
  return { id: body.data.tenant.id as string, headers: { Authorization: `Bearer ${body.data.apiKey}`, 'Content-Type': 'application/json' } };
}

describe('workflow routes', () => {
  it('require the operator key on /workflows', async () => {
    expect((await fetch(`${base}/workflows`)).status).toBe(401);
  });

  it('create a DAG and return it with levels and dependencies', async () => {
    const { status, body } = await post('/workflows', diamond);
    expect(status).toBe(201);
    expect(body.data.workflow.status).toBe('RUNNING');
    expect(body.data.steps.map((s: any) => `${s.key}:${s.level}:${s.status}`))
      .toEqual(['a:0:QUEUED', 'b:1:BLOCKED', 'c:1:BLOCKED', 'd:2:BLOCKED']);

    const one = await (await fetch(`${base}/workflows/${body.data.workflow.id}`, { headers: op })).json() as any;
    expect(one.data.steps.find((s: any) => s.key === 'd').dependsOn).toEqual(['b', 'c']);
    const list = await (await fetch(`${base}/workflows`, { headers: op })).json() as any;
    expect(list.data.items[0]).toMatchObject({ name: 'diamond', totalSteps: 4, completedSteps: 0 });
  });

  it.each([
    [{ name: 'x', steps: [] }, 422],
    [{ name: 'x', steps: [{ key: 'a', name: 'echo', dependsOn: ['b'] }, { key: 'b', name: 'echo', dependsOn: ['a'] }] }, 422],
    [{ name: 'x', steps: [{ key: 'bad key', name: 'echo' }] }, 422],
  ])('reject invalid definitions %j', async (body, code) => {
    const r = await post('/workflows', body);
    expect(r.status).toBe(code);
  });

  it('explain the cycle in the error', async () => {
    const r = await post('/workflows', { name: 'x', steps: [{ key: 'a', name: 'echo', dependsOn: ['b'] }, { key: 'b', name: 'echo', dependsOn: ['a'] }] });
    expect(r.body.error).toMatch(/Dependency cycle/);
  });

  it('cancel a workflow, drop its queued jobs, and 409 on a second cancel', async () => {
    const { body } = await post('/workflows', diamond);
    const id = body.data.workflow.id;
    const c = await fetch(`${base}/workflows/${id}/cancel`, { method: 'PUT', headers: op });
    const cancelled = (await c.json()) as any;
    expect(cancelled.data.workflow.status).toBe('CANCELLED');
    expect(removed).toHaveLength(4);
    expect((await fetch(`${base}/workflows/${id}/cancel`, { method: 'PUT', headers: op })).status).toBe(409);
  });

  it('idempotency key returns the original (200) instead of creating twice', async () => {
    const first = await post('/workflows', { ...diamond, idempotencyKey: 'nightly-2026-10-17' });
    const again = await post('/workflows', { ...diamond, idempotencyKey: 'nightly-2026-10-17' });
    expect(first.status).toBe(201);
    expect(again.status).toBe(200);
    expect(again.body.data.workflow.id).toBe(first.body.data.workflow.id);
  });

  it('tenants get their own scoped view and are charged per step', async () => {
    const t1 = await newTenant();
    const t2 = await newTenant();
    const created = await post('/tenant/workflows', diamond, t1.headers);
    expect(created.status).toBe(201);
    const id = created.body.data.workflow.id;

    expect((await fetch(`${base}/tenant/workflows/${id}`, { headers: t1.headers })).status).toBe(200);
    expect((await fetch(`${base}/tenant/workflows/${id}`, { headers: t2.headers })).status).toBe(404);
    expect((await fetch(`${base}/tenant/workflows/${id}/cancel`, { method: 'PUT', headers: t2.headers })).status).toBe(404);
    const t2list = await (await fetch(`${base}/tenant/workflows`, { headers: t2.headers })).json() as any;
    expect(t2list.data.total).toBe(0);

    const usage = await pool.query('SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = $1', [t1.id]);
    expect(usage.rows[0].accepted_jobs).toBe(4);
    const tasks = await pool.query('SELECT DISTINCT tenant_id FROM tasks WHERE workflow_id = $1', [id]);
    expect(tasks.rows).toEqual([{ tenant_id: t1.id }]);

    // The operator sees every tenant's workflows.
    const all = await (await fetch(`${base}/workflows`, { headers: op })).json() as any;
    expect(all.data.total).toBe(1);
  });

  it('refuse a tenant workflow that would exceed the quota (429)', async () => {
    const t = await newTenant();
    await pool.query(`INSERT INTO tenant_overrides (id, tenant_id, max_jobs_per_period, created_at, updated_at)
                      VALUES ('o1', $1, 5, NOW(), NOW())`, [t.id]);
    expect((await post('/tenant/workflows', diamond, t.headers)).status).toBe(201);
    expect((await post('/tenant/workflows', diamond, t.headers)).status).toBe(429);
  });
});
