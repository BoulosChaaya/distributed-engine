import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { Pool } from 'pg';
import { TenantRepository, QuotaExceededError, computeBillingPeriodStart } from '../db/tenant-repository';
import { TaskRepository } from '../db/task-repository';
import { runMigrations } from '../db/migrations';

// A tenant retrying its own FAILED task: scoped to the tenant and charged
// against its quota like a new submission. Real PostgreSQL.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let tenants: TenantRepository;
let tasks: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  tenants = new TenantRepository(pool);
  tasks = new TaskRepository(pool);
});
afterAll(async () => { await pool.end(); });

async function tenantWithFailedTask() {
  const plan = (await tenants.listPlans())[0];
  const tenant = await tenants.createTenant('retry-tenant', plan.id, `hash-${Date.now()}-${Math.random()}`);
  const limits = await tenants.getEffectiveLimits(tenant.id);
  const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());
  const { task } = await tasks.acceptTask({
    tenantId: tenant.id, name: 'http', priority: 'NORMAL', payload: {}, maxRetries: 0,
    billingPeriodStart, maxJobsPerPeriod: limits.maxJobsPerPeriod,
  });
  await pool.query(`UPDATE tasks SET status = 'FAILED', retries = 1, error = 'upstream responded 503' WHERE id = $1`, [task.id]);
  return { tenant, task, billingPeriodStart, max: limits.maxJobsPerPeriod };
}

async function used(tenantId: string, billingPeriodStart: Date): Promise<number> {
  const r = await pool.query(
    'SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = $1 AND billing_period_start = $2',
    [tenantId, billingPeriodStart],
  );
  return r.rows[0].accepted_jobs;
}

describe('tenant retry of a FAILED task (requires PostgreSQL)', () => {
  it('requeues the task and charges one job', async () => {
    const { tenant, task, billingPeriodStart, max } = await tenantWithFailedTask();
    expect(await used(tenant.id, billingPeriodStart)).toBe(1);

    const retried = await tasks.retryFailedTask(task.id, tenant.id, { billingPeriodStart, maxJobsPerPeriod: max });

    expect(retried.status).toBe('QUEUED');
    expect(retried.retries).toBe(0);
    expect(await used(tenant.id, billingPeriodStart)).toBe(2);
  });

  it('refuses when the quota is used up, and changes nothing', async () => {
    const { tenant, task, billingPeriodStart } = await tenantWithFailedTask();

    const err = await tasks.retryFailedTask(task.id, tenant.id, { billingPeriodStart, maxJobsPerPeriod: 1 }).catch((e) => e);

    expect(err).toBeInstanceOf(QuotaExceededError);
    const row = (await pool.query('SELECT status, retries FROM tasks WHERE id = $1', [task.id])).rows[0];
    expect(row).toEqual({ status: 'FAILED', retries: 1 });
    expect(await used(tenant.id, billingPeriodStart)).toBe(1);
    const events = await pool.query(`SELECT 1 FROM outbox_events WHERE task_id = $1 AND event_type = 'TASK_REQUEUED'`, [task.id]);
    expect(events.rows).toHaveLength(0);
  });

  it("cannot reach another tenant's task", async () => {
    const a = await tenantWithFailedTask();
    const b = await tenantWithFailedTask();
    await expect(
      tasks.retryFailedTask(a.task.id, b.tenant.id, { billingPeriodStart: b.billingPeriodStart, maxJobsPerPeriod: b.max }),
    ).rejects.toThrow(/not found/);
    expect(await used(b.tenant.id, b.billingPeriodStart)).toBe(1);
  });
});
