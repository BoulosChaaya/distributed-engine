import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { Pool } from 'pg';
import { TaskRepository } from '../db/task-repository';
import { IdempotencyConflictError, TenantRepository, computeBillingPeriodStart } from '../db/tenant-repository';
import { runMigrations } from '../db/migrations';

// Operator submissions (no tenant) with an idempotency key. Found by tracing
// the system map: a 201 lost on its way back left the client with only one
// option, resending, and every resend created another task. Real PostgreSQL.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let tasks: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL, max: 10 });
  await runMigrations(pool);
  tasks = new TaskRepository(pool);
});
afterAll(async () => { await pool.end(); });

const submit = (key: string, payload: Record<string, unknown> = { hello: 'laptop' }) =>
  tasks.acceptTask({ tenantId: null, name: 'echo', priority: 'NORMAL', payload, maxRetries: 3, idempotencyKey: key });

const rowsFor = async (key: string) =>
  (await pool.query('SELECT count(*)::int AS n FROM tasks WHERE tenant_id IS NULL AND idempotency_key = $1', [key])).rows[0].n;

describe('operator idempotency key (requires PostgreSQL)', () => {
  it('a resend with the same key returns the first task and creates nothing', async () => {
    const key = `op-${Math.random()}`;
    const first = await submit(key);
    const again = await submit(key);

    expect(first.idempotent).toBe(false);
    expect(again.idempotent).toBe(true);
    expect(again.task.id).toBe(first.task.id);
    expect(await rowsFor(key)).toBe(1);
    const events = await pool.query('SELECT count(*)::int AS n FROM outbox_events WHERE task_id = $1', [first.task.id]);
    expect(events.rows[0].n).toBe(1);
  });

  it('identical requests at the same moment still make one task', async () => {
    const key = `op-race-${Math.random()}`;
    const results = await Promise.all([submit(key), submit(key), submit(key), submit(key)]);

    expect(new Set(results.map((r) => r.task.id)).size).toBe(1);
    expect(results.filter((r) => !r.idempotent)).toHaveLength(1);
    expect(await rowsFor(key)).toBe(1);
  });

  it('the same key with a different body is refused', async () => {
    const key = `op-conflict-${Math.random()}`;
    await submit(key, { a: 1 });
    await expect(submit(key, { a: 2 })).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(await rowsFor(key)).toBe(1);
  });

  it("an operator key and a tenant key with the same text are separate", async () => {
    const key = `shared-${Math.random()}`;
    const tenants = new TenantRepository(pool);
    const plan = (await tenants.listPlans())[0];
    const tenant = await tenants.createTenant('key-tenant', plan.id, `hash-${Date.now()}-${Math.random()}`);
    const limits = await tenants.getEffectiveLimits(tenant.id);
    const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

    const op = await submit(key);
    const ten = await tasks.acceptTask({
      tenantId: tenant.id, name: 'echo', priority: 'NORMAL', payload: { hello: 'laptop' }, maxRetries: 3,
      idempotencyKey: key, billingPeriodStart, maxJobsPerPeriod: limits.maxJobsPerPeriod,
    });

    expect(ten.idempotent).toBe(false);
    expect(ten.task.id).not.toBe(op.task.id);
  });

  it('without a key every submission is a new task, as before', async () => {
    const a = await tasks.acceptTask({ tenantId: null, name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 3 });
    const b = await tasks.acceptTask({ tenantId: null, name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 3 });
    expect(a.task.id).not.toBe(b.task.id);
  });

  it('a future scheduledFor is stored SCHEDULED with no outbox event', async () => {
    const when = new Date(Date.now() + 60_000).toISOString();
    const r = await tasks.acceptTask({ tenantId: null, name: 'echo', priority: 'NORMAL', payload: {}, maxRetries: 3, scheduledFor: when });
    expect(r.task.status).toBe('SCHEDULED');
    const events = await pool.query('SELECT count(*)::int AS n FROM outbox_events WHERE task_id = $1', [r.task.id]);
    expect(events.rows[0].n).toBe(0);
  });
});
