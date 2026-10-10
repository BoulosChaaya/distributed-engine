import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { TenantRepository, QuotaExceededError, IdempotencyConflictError, computeBillingPeriodStart, computeIdempotencyHash } from '../db/tenant-repository';
import { TaskRepository } from '../db/task-repository';
import { runMigrations } from '../db/migrations';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let tenantRepo: TenantRepository;
let taskRepo: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  try {
    await pool.query('SELECT 1');
  } catch (err) {
    throw new Error(
      `PostgreSQL not available at ${TEST_PG_URL}. Integration tests require PostgreSQL. Error: ${err}`
    );
  }
  await runMigrations(pool);
  tenantRepo = new TenantRepository(pool);
  taskRepo = new TaskRepository(pool);
});

afterAll(async () => {
  if (pool) {
    await pool.query('TRUNCATE tenant_concurrency_leases, tenant_usage, tenant_overrides, outbox_events, tasks, tenants CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  await pool.query('TRUNCATE tenant_concurrency_leases, tenant_usage, tenant_overrides, outbox_events, tasks CASCADE');
  await pool.query('DELETE FROM tenants');
});

describe('TenantRepository (requires PostgreSQL)', () => {
  async function createTestTenant(name = 'test-tenant', planName = 'starter') {
    const plans = await tenantRepo.listPlans();
    const plan = plans.find(p => p.name === planName) || plans[0];
    const apiKeyHash = `hash-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    return tenantRepo.createTenant(name, plan.id, apiKeyHash);
  }

  it('should list seeded plans', async () => {
    const plans = await tenantRepo.listPlans();
    expect(plans.length).toBeGreaterThanOrEqual(3);
    const names = plans.map(p => p.name);
    expect(names).toContain('starter');
    expect(names).toContain('professional');
    expect(names).toContain('enterprise');
  });

  it('should create and retrieve a tenant', async () => {
    const plans = await tenantRepo.listPlans();
    const apiKeyHash = 'test-api-key-hash-unique';
    const tenant = await tenantRepo.createTenant('my-tenant', plans[0].id, apiKeyHash);

    expect(tenant.id).toBeDefined();
    expect(tenant.name).toBe('my-tenant');
    expect(tenant.status).toBe('ACTIVE');

    const retrieved = await tenantRepo.getTenant(tenant.id);
    expect(retrieved).toBeDefined();
    expect(retrieved!.id).toBe(tenant.id);

    const byKey = await tenantRepo.getTenantByApiKeyHash(apiKeyHash);
    expect(byKey).toBeDefined();
    expect(byKey!.id).toBe(tenant.id);
  });

  it('should suspend and reactivate a tenant', async () => {
    const tenant = await createTestTenant();
    const suspended = await tenantRepo.setTenantStatus(tenant.id, 'SUSPENDED');
    expect(suspended.status).toBe('SUSPENDED');

    const reactivated = await tenantRepo.setTenantStatus(tenant.id, 'ACTIVE');
    expect(reactivated.status).toBe('ACTIVE');
  });

  it('should compute effective limits with plan defaults', async () => {
    const tenant = await createTestTenant('limits-test', 'starter');
    const limits = await tenantRepo.getEffectiveLimits(tenant.id);

    expect(limits.rateLimit).toBeGreaterThan(0);
    expect(limits.maxConcurrentExecutions).toBeGreaterThan(0);
    expect(limits.maxJobsPerPeriod).toBeGreaterThan(0);
    expect(limits.weight).toBeGreaterThan(0);
  });

  it('should override plan limits with tenant overrides', async () => {
    const tenant = await createTestTenant('override-test');
    const baseLimits = await tenantRepo.getEffectiveLimits(tenant.id);

    await tenantRepo.setOverrides(tenant.id, { rateLimit: 9999 });
    const overriddenLimits = await tenantRepo.getEffectiveLimits(tenant.id);

    expect(overriddenLimits.rateLimit).toBe(9999);
    expect(overriddenLimits.maxConcurrentExecutions).toBe(baseLimits.maxConcurrentExecutions);
  });

  describe('concurrency leases', () => {
    async function createTaskForTenant(tenantId: string, name: string): Promise<string> {
      const result = await pool.query(
        `INSERT INTO tasks (id, tenant_id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
         VALUES ($1, $2, $3, 'QUEUED', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW()) RETURNING id`,
        [`task-${Date.now()}-${Math.random().toString(36).slice(2)}`, tenantId, name],
      );
      return result.rows[0].id;
    }

    it('should acquire and release concurrency lease', async () => {
      const tenant = await createTestTenant();
      const taskId = await createTaskForTenant(tenant.id, 'lease-test-1');
      const result = await tenantRepo.acquireConcurrencyLease(tenant.id, taskId, 'worker-1', 5, 30000);

      expect(result.acquired).toBe(true);
      expect(result.lease).toBeDefined();
      expect(result.lease!.leaseToken).toBeDefined();

      const count = await tenantRepo.getActiveConcurrencyCount(tenant.id);
      expect(count).toBe(1);

      await tenantRepo.releaseConcurrencyLease(result.lease!.leaseToken);
      const countAfter = await tenantRepo.getActiveConcurrencyCount(tenant.id);
      expect(countAfter).toBe(0);
    });

    it('should enforce concurrency limit', async () => {
      const tenant = await createTestTenant();
      const t1 = await createTaskForTenant(tenant.id, 'limit-1');
      const t2 = await createTaskForTenant(tenant.id, 'limit-2');
      const t3 = await createTaskForTenant(tenant.id, 'limit-3');

      const r1 = await tenantRepo.acquireConcurrencyLease(tenant.id, t1, 'w-1', 2, 30000);
      expect(r1.acquired).toBe(true);

      const r2 = await tenantRepo.acquireConcurrencyLease(tenant.id, t2, 'w-2', 2, 30000);
      expect(r2.acquired).toBe(true);

      const r3 = await tenantRepo.acquireConcurrencyLease(tenant.id, t3, 'w-3', 2, 30000);
      expect(r3.acquired).toBe(false);
    });

    it('should renew concurrency lease', async () => {
      const tenant = await createTestTenant();
      const taskId = await createTaskForTenant(tenant.id, 'renew-test');
      const result = await tenantRepo.acquireConcurrencyLease(tenant.id, taskId, 'w-1', 5, 30000);
      expect(result.acquired).toBe(true);

      await expect(
        tenantRepo.renewConcurrencyLease(result.lease!.leaseToken, 60000)
      ).resolves.toBeUndefined();
    });
  });

  describe('acceptTask with idempotency', () => {
    it('should accept a task and charge quota', async () => {
      const tenant = await createTestTenant();
      const limits = await tenantRepo.getEffectiveLimits(tenant.id);
      const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

      const result = await taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'my-task',
        priority: 'NORMAL',
        payload: { x: 1 },
        maxRetries: 3,
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      expect(result.task.tenantId).toBe(tenant.id);
      expect(result.idempotent).toBe(false);
      expect(result.task.status).toBe('QUEUED');
    });

    it('should return existing task for same idempotency key and material', async () => {
      const tenant = await createTestTenant();
      const limits = await tenantRepo.getEffectiveLimits(tenant.id);
      const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

      const r1 = await taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'idempotent-task',
        priority: 'HIGH',
        payload: {},
        maxRetries: 2,
        idempotencyKey: 'idem-1',
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      const r2 = await taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'idempotent-task',
        priority: 'HIGH',
        payload: {},
        maxRetries: 2,
        idempotencyKey: 'idem-1',
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      expect(r2.idempotent).toBe(true);
      expect(r2.task.id).toBe(r1.task.id);
    });

    it('should reject idempotency key with different material', async () => {
      const tenant = await createTestTenant();
      const limits = await tenantRepo.getEffectiveLimits(tenant.id);
      const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

      await taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'task-a',
        priority: 'HIGH',
        payload: {},
        maxRetries: 2,
        idempotencyKey: 'conflict-key',
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      await expect(taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'task-different-name',
        priority: 'LOW',
        payload: {},
        maxRetries: 1,
        idempotencyKey: 'conflict-key',
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      })).rejects.toThrow(IdempotencyConflictError);
    });

    it('should enforce quota', async () => {
      const tenant = await createTestTenant();
      const limits = await tenantRepo.getEffectiveLimits(tenant.id);
      const billingPeriodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

      for (let i = 0; i < limits.maxJobsPerPeriod; i++) {
        await taskRepo.acceptTask({
          tenantId: tenant.id,
          name: `quota-task-${i}`,
          priority: 'NORMAL',
          payload: {},
          maxRetries: 1,
          billingPeriodStart,
          maxJobsPerPeriod: limits.maxJobsPerPeriod,
        });
      }

      await expect(taskRepo.acceptTask({
        tenantId: tenant.id,
        name: 'one-too-many',
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      })).rejects.toThrow(QuotaExceededError);
    });
  });

  describe('tenant-scoped queries', () => {
    it('should only return tasks for the requesting tenant', async () => {
      const tenant1 = await createTestTenant('tenant-1');
      const tenant2 = await createTestTenant('tenant-2');
      const limits = await tenantRepo.getEffectiveLimits(tenant1.id);
      const bp = computeBillingPeriodStart(tenant1.createdAt, limits.billingPeriodDays, new Date());

      await taskRepo.acceptTask({
        tenantId: tenant1.id, name: 't1-task', priority: 'NORMAL', payload: {},
        maxRetries: 1, billingPeriodStart: bp, maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      const limits2 = await tenantRepo.getEffectiveLimits(tenant2.id);
      const bp2 = computeBillingPeriodStart(tenant2.createdAt, limits2.billingPeriodDays, new Date());
      await taskRepo.acceptTask({
        tenantId: tenant2.id, name: 't2-task', priority: 'NORMAL', payload: {},
        maxRetries: 1, billingPeriodStart: bp2, maxJobsPerPeriod: limits2.maxJobsPerPeriod,
      });

      const t1Tasks = await taskRepo.listTasksForTenant(tenant1.id, 1, 100);
      expect(t1Tasks.items.length).toBe(1);
      expect(t1Tasks.items[0].name).toBe('t1-task');

      const t2Tasks = await taskRepo.listTasksForTenant(tenant2.id, 1, 100);
      expect(t2Tasks.items.length).toBe(1);
      expect(t2Tasks.items[0].name).toBe('t2-task');
    });

    it('should cancel only the tenant own task', async () => {
      const tenant = await createTestTenant();
      const limits = await tenantRepo.getEffectiveLimits(tenant.id);
      const bp = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

      const { task } = await taskRepo.acceptTask({
        tenantId: tenant.id, name: 'cancelable', priority: 'NORMAL', payload: {},
        maxRetries: 1, billingPeriodStart: bp, maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });

      const cancelled = await taskRepo.cancelTaskForTenant(task.id, tenant.id);
      expect(cancelled.status).toBe('CANCELLED');

      await expect(taskRepo.cancelTaskForTenant(task.id, 'wrong-tenant-id'))
        .rejects.toThrow(/not found/);
    });
  });
});

describe('computeBillingPeriodStart', () => {
  it('should compute period start relative to tenant creation', () => {
    const tenantCreated = new Date('2024-01-01T12:00:00Z');
    const now = new Date('2024-01-15T06:00:00Z');
    const start = computeBillingPeriodStart(tenantCreated, 30, now);
    expect(start.getTime()).toBeLessThanOrEqual(now.getTime());
    expect(start.getTime()).toBeGreaterThanOrEqual(tenantCreated.getTime() - 24 * 60 * 60 * 1000);
  });

  it('should roll over at period boundary', () => {
    const tenantCreated = new Date('2024-01-01T00:00:00Z');
    const now = new Date('2024-03-05T00:00:00Z');
    const start = computeBillingPeriodStart(tenantCreated, 30, now);
    const diffDays = (now.getTime() - start.getTime()) / (24 * 60 * 60 * 1000);
    expect(diffDays).toBeLessThan(30);
  });
});

describe('computeIdempotencyHash', () => {
  it('should produce same hash for same material', () => {
    const h1 = computeIdempotencyHash('name', 'HIGH', 3, { key: 'value' });
    const h2 = computeIdempotencyHash('name', 'HIGH', 3, { key: 'value' });
    expect(h1).toBe(h2);
  });

  it('should produce different hash for different material', () => {
    const h1 = computeIdempotencyHash('name', 'HIGH', 3, {});
    const h2 = computeIdempotencyHash('name', 'LOW', 3, {});
    expect(h1).not.toBe(h2);
  });

  it('should include scheduling info in hash', () => {
    const h1 = computeIdempotencyHash('name', 'HIGH', 3, {});
    const h2 = computeIdempotencyHash('name', 'HIGH', 3, {}, '2024-01-01T00:00:00Z');
    expect(h1).not.toBe(h2);
  });

  it('A: identical payload deduplicates', () => {
    const h1 = computeIdempotencyHash('task', 'NORMAL', 3, { a: 1, b: 'two' });
    const h2 = computeIdempotencyHash('task', 'NORMAL', 3, { a: 1, b: 'two' });
    expect(h1).toBe(h2);
  });

  it('B: key-order equivalence (canonical serialization)', () => {
    const h1 = computeIdempotencyHash('task', 'NORMAL', 3, { z: 1, a: 2 });
    const h2 = computeIdempotencyHash('task', 'NORMAL', 3, { a: 2, z: 1 });
    expect(h1).toBe(h2);
  });

  it('C: different payload produces different hash', () => {
    const h1 = computeIdempotencyHash('task', 'NORMAL', 3, { url: '/a' });
    const h2 = computeIdempotencyHash('task', 'NORMAL', 3, { url: '/b' });
    expect(h1).not.toBe(h2);
  });

  it('D: different name/priority/retries produces different hash', () => {
    const payload = { data: 'same' };
    const h1 = computeIdempotencyHash('taskA', 'NORMAL', 3, payload);
    const h2 = computeIdempotencyHash('taskB', 'NORMAL', 3, payload);
    expect(h1).not.toBe(h2);
  });

  it('E: cross-tenant independence (same material, different key scope)', () => {
    const h1 = computeIdempotencyHash('task', 'NORMAL', 3, { x: 1 });
    const h2 = computeIdempotencyHash('task', 'NORMAL', 3, { x: 1 });
    expect(h1).toBe(h2);
  });

  it('F: hash is a 64-char hex SHA-256', () => {
    const h = computeIdempotencyHash('task', 'NORMAL', 3, { key: 'val' });
    expect(h).toMatch(/^[a-f0-9]{64}$/);
  });

  it('should include nested objects in canonical order', () => {
    const h1 = computeIdempotencyHash('t', 'NORMAL', 1, { outer: { b: 2, a: 1 } });
    const h2 = computeIdempotencyHash('t', 'NORMAL', 1, { outer: { a: 1, b: 2 } });
    expect(h1).toBe(h2);
  });
});

describe('Concurrent concurrency lease race (requires PostgreSQL)', () => {
  it('should serialize concurrent acquisitions — exactly one wins the final slot', async () => {
    const plans = await tenantRepo.listPlans();
    const plan = plans[0];
    const tenant = await tenantRepo.createTenant('race-tenant', plan.id, `race-hash-${Date.now()}`);

    const t1 = await pool.query(
      `INSERT INTO tasks (id, tenant_id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
       VALUES ($1, $2, 'race-task-1', 'QUEUED', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW()) RETURNING id`,
      [`race-t1-${Date.now()}`, tenant.id],
    );
    const t2 = await pool.query(
      `INSERT INTO tasks (id, tenant_id, name, status, priority, payload, max_retries, retries, version, created_at, updated_at)
       VALUES ($1, $2, 'race-task-2', 'QUEUED', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW()) RETURNING id`,
      [`race-t2-${Date.now()}`, tenant.id],
    );

    const pool2 = new Pool({ connectionString: TEST_PG_URL });
    const tenantRepo2 = new TenantRepository(pool2);

    try {
      const [r1, r2] = await Promise.all([
        tenantRepo.acquireConcurrencyLease(tenant.id, t1.rows[0].id, 'w-1', 1, 30000),
        tenantRepo2.acquireConcurrencyLease(tenant.id, t2.rows[0].id, 'w-2', 1, 30000),
      ]);

      const acquired = [r1.acquired, r2.acquired];
      expect(acquired.filter(Boolean).length).toBe(1);
      expect(acquired.filter(v => !v).length).toBe(1);
    } finally {
      await pool.query(`DELETE FROM tenant_concurrency_leases WHERE tenant_id = $1`, [tenant.id]);
      await pool2.end();
    }
  });
});

describe('Concurrent idempotency (requires PostgreSQL)', () => {
  it('F: concurrent same-key same-fingerprint submissions both succeed with same task ID', async () => {
    const tenant = await (async () => {
      const plans = await tenantRepo.listPlans();
      const plan = plans[0];
      const hash = `idem-race-${Date.now()}`;
      return tenantRepo.createTenant('idem-race-tenant', plan.id, hash);
    })();

    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const bp = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

    const pool2 = new Pool({ connectionString: TEST_PG_URL });
    const taskRepo2 = new TaskRepository(pool2);
    const idemKey = `concurrent-idem-${Date.now()}`;

    try {
      const input = {
        tenantId: tenant.id,
        name: 'concurrent-idem-task',
        priority: 'NORMAL' as const,
        payload: { action: 'test' },
        maxRetries: 2,
        idempotencyKey: idemKey,
        billingPeriodStart: bp,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      };

      const results = await Promise.allSettled([
        taskRepo.acceptTask(input),
        taskRepo2.acceptTask(input),
      ]);

      const fulfilled = results.filter(r => r.status === 'fulfilled');
      expect(fulfilled.length).toBe(2);

      const ids = fulfilled.map(r => (r as PromiseFulfilledResult<any>).value.task.id);
      expect(ids[0]).toBe(ids[1]);

      const idempotentFlags = fulfilled.map(r => (r as PromiseFulfilledResult<any>).value.idempotent);
      expect(idempotentFlags.filter(Boolean).length).toBeGreaterThanOrEqual(1);

      const allTasks = await pool.query(
        `SELECT * FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2`,
        [tenant.id, idemKey],
      );
      expect(allTasks.rows.length).toBe(1);

      const outboxEvents = await pool.query(
        `SELECT * FROM outbox_events WHERE task_id = $1`,
        [ids[0]],
      );
      expect(outboxEvents.rows.length).toBe(1);

      const usage = await pool.query(
        `SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = $1 AND billing_period_start = $2`,
        [tenant.id, bp],
      );
      expect(usage.rows[0].accepted_jobs).toBe(1);
    } finally {
      await pool2.end();
    }
  });

  it('concurrent same-key different-fingerprint: one succeeds, one gets IdempotencyConflictError', async () => {
    const tenant = await (async () => {
      const plans = await tenantRepo.listPlans();
      const plan = plans[0];
      const hash = `idem-conflict-${Date.now()}`;
      return tenantRepo.createTenant('idem-conflict-tenant', plan.id, hash);
    })();

    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const bp = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

    const pool2 = new Pool({ connectionString: TEST_PG_URL });
    const taskRepo2 = new TaskRepository(pool2);
    const idemKey = `concurrent-conflict-${Date.now()}`;

    try {
      const base = {
        tenantId: tenant.id,
        priority: 'NORMAL' as const,
        maxRetries: 2,
        idempotencyKey: idemKey,
        billingPeriodStart: bp,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      };

      const results = await Promise.allSettled([
        taskRepo.acceptTask({ ...base, name: 'task-a', payload: { variant: 'a' } }),
        taskRepo2.acceptTask({ ...base, name: 'task-b', payload: { variant: 'b' } }),
      ]);

      const fulfilled = results.filter(r => r.status === 'fulfilled');
      const rejected = results.filter(r => r.status === 'rejected');

      expect(fulfilled.length).toBe(1);
      expect(rejected.length).toBe(1);
      expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(IdempotencyConflictError);

      const allTasks = await pool.query(
        `SELECT * FROM tasks WHERE tenant_id = $1 AND idempotency_key = $2`,
        [tenant.id, idemKey],
      );
      expect(allTasks.rows.length).toBe(1);

      const usage = await pool.query(
        `SELECT accepted_jobs FROM tenant_usage WHERE tenant_id = $1 AND billing_period_start = $2`,
        [tenant.id, bp],
      );
      expect(usage.rows[0].accepted_jobs).toBe(1);
    } finally {
      await pool2.end();
    }
  });
});

describe('Concurrent quota race (requires PostgreSQL)', () => {
  it('should allow exactly one submission when quota remaining is 1', async () => {
    const tenant = await (async () => {
      const plans = await tenantRepo.listPlans();
      const plan = plans[0];
      const hash = `quota-race-${Date.now()}`;
      return tenantRepo.createTenant('quota-race-tenant', plan.id, hash);
    })();

    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const bp = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());

    for (let i = 0; i < limits.maxJobsPerPeriod - 1; i++) {
      await taskRepo.acceptTask({
        tenantId: tenant.id,
        name: `quota-fill-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 1,
        billingPeriodStart: bp,
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      });
    }

    const pool2 = new Pool({ connectionString: TEST_PG_URL });
    const taskRepo2 = new TaskRepository(pool2);

    try {
      const results = await Promise.allSettled([
        taskRepo.acceptTask({
          tenantId: tenant.id,
          name: 'quota-race-a',
          priority: 'NORMAL',
          payload: {},
          maxRetries: 1,
          billingPeriodStart: bp,
          maxJobsPerPeriod: limits.maxJobsPerPeriod,
        }),
        taskRepo2.acceptTask({
          tenantId: tenant.id,
          name: 'quota-race-b',
          priority: 'NORMAL',
          payload: {},
          maxRetries: 1,
          billingPeriodStart: bp,
          maxJobsPerPeriod: limits.maxJobsPerPeriod,
        }),
      ]);

      const fulfilled = results.filter(r => r.status === 'fulfilled');
      const rejected = results.filter(r => r.status === 'rejected');

      expect(fulfilled.length).toBe(1);
      expect(rejected.length).toBe(1);
      expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(QuotaExceededError);
    } finally {
      await pool2.end();
    }
  });
});
