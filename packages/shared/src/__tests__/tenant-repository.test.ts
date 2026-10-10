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
    it('should acquire and release concurrency lease', async () => {
      const tenant = await createTestTenant();
      const result = await tenantRepo.acquireConcurrencyLease(tenant.id, 'task-1', 'worker-1', 5, 30000);

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
      const r1 = await tenantRepo.acquireConcurrencyLease(tenant.id, 'task-1', 'w-1', 2, 30000);
      expect(r1.acquired).toBe(true);

      const r2 = await tenantRepo.acquireConcurrencyLease(tenant.id, 'task-2', 'w-2', 2, 30000);
      expect(r2.acquired).toBe(true);

      const r3 = await tenantRepo.acquireConcurrencyLease(tenant.id, 'task-3', 'w-3', 2, 30000);
      expect(r3.acquired).toBe(false);
    });

    it('should renew concurrency lease', async () => {
      const tenant = await createTestTenant();
      const result = await tenantRepo.acquireConcurrencyLease(tenant.id, 'task-1', 'w-1', 5, 30000);
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
        payload: { different: true },
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
    const h1 = computeIdempotencyHash('name', 'HIGH', 3);
    const h2 = computeIdempotencyHash('name', 'HIGH', 3);
    expect(h1).toBe(h2);
  });

  it('should produce different hash for different material', () => {
    const h1 = computeIdempotencyHash('name', 'HIGH', 3);
    const h2 = computeIdempotencyHash('name', 'LOW', 3);
    expect(h1).not.toBe(h2);
  });

  it('should include scheduling info in hash', () => {
    const h1 = computeIdempotencyHash('name', 'HIGH', 3);
    const h2 = computeIdempotencyHash('name', 'HIGH', 3, '2024-01-01T00:00:00Z');
    expect(h1).not.toBe(h2);
  });
});
