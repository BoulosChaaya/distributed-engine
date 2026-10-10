import { Pool } from 'pg';
import { Tenant, TenantStatus, Plan, TenantOverride, TenantUsage, EffectiveLimits, TenantConcurrencyLease } from '../types';
import { generateId } from '../utils';

function rowToTenant(row: Record<string, unknown>): Tenant {
  return {
    id: row.id as string,
    name: row.name as string,
    status: row.status as TenantStatus,
    planId: row.plan_id as string,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
  };
}

function rowToPlan(row: Record<string, unknown>): Plan {
  return {
    id: row.id as string,
    name: row.name as string,
    rateLimit: row.rate_limit as number,
    maxConcurrentExecutions: row.max_concurrent_executions as number,
    maxJobsPerPeriod: row.max_jobs_per_period as number,
    billingPeriodDays: row.billing_period_days as number,
    maxComputeUnitsPerPeriod: row.max_compute_units_per_period as number,
    maxStorageMb: row.max_storage_mb as number,
    weight: row.weight as number,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
  };
}

function rowToOverride(row: Record<string, unknown>): TenantOverride {
  return {
    id: row.id as string,
    tenantId: row.tenant_id as string,
    rateLimit: row.rate_limit as number | undefined,
    maxConcurrentExecutions: row.max_concurrent_executions as number | undefined,
    maxJobsPerPeriod: row.max_jobs_per_period as number | undefined,
    billingPeriodDays: row.billing_period_days as number | undefined,
    maxComputeUnitsPerPeriod: row.max_compute_units_per_period as number | undefined,
    maxStorageMb: row.max_storage_mb as number | undefined,
    weight: row.weight as number | undefined,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
  };
}

function rowToUsage(row: Record<string, unknown>): TenantUsage {
  return {
    id: row.id as string,
    tenantId: row.tenant_id as string,
    billingPeriodStart: new Date(row.billing_period_start as string),
    acceptedJobs: row.accepted_jobs as number,
    computeUnits: row.compute_units as number,
    storageMb: row.storage_mb as number,
    createdAt: new Date(row.created_at as string),
    updatedAt: new Date(row.updated_at as string),
  };
}

function rowToLease(row: Record<string, unknown>): TenantConcurrencyLease {
  return {
    id: row.id as string,
    tenantId: row.tenant_id as string,
    taskId: row.task_id as string,
    workerId: row.worker_id as string,
    leaseToken: row.lease_token as string,
    expiresAt: new Date(row.expires_at as string),
    createdAt: new Date(row.created_at as string),
  };
}

export function computeBillingPeriodStart(tenantCreatedAt: Date, billingPeriodDays: number, now: Date): Date {
  const start = new Date(tenantCreatedAt);
  start.setUTCHours(0, 0, 0, 0);
  const periodMs = billingPeriodDays * 24 * 60 * 60 * 1000;
  const elapsed = now.getTime() - start.getTime();
  const periods = Math.floor(elapsed / periodMs);
  return new Date(start.getTime() + periods * periodMs);
}

export function computeIdempotencyHash(name: string, priority: string, maxRetries: number, scheduledFor?: string, scheduleId?: string): string {
  const parts = [name, priority, String(maxRetries)];
  if (scheduledFor) parts.push(scheduledFor);
  if (scheduleId) parts.push(scheduleId);
  return parts.join('|');
}

export class IdempotencyConflictError extends Error {
  constructor(
    public readonly tenantId: string,
    public readonly idempotencyKey: string,
  ) {
    super(`Idempotency conflict for tenant ${tenantId}, key ${idempotencyKey}: request material differs from existing task`);
    this.name = 'IdempotencyConflictError';
  }
}

export class QuotaExceededError extends Error {
  constructor(
    public readonly tenantId: string,
    public readonly limit: number,
    public readonly current: number,
  ) {
    super(`Quota exceeded for tenant ${tenantId}: ${current}/${limit} jobs used`);
    this.name = 'QuotaExceededError';
  }
}

export class TenantSuspendedError extends Error {
  constructor(public readonly tenantId: string) {
    super(`Tenant ${tenantId} is suspended`);
    this.name = 'TenantSuspendedError';
  }
}

export class ConcurrencyLimitError extends Error {
  constructor(
    public readonly tenantId: string,
    public readonly limit: number,
  ) {
    super(`Concurrency limit (${limit}) reached for tenant ${tenantId}`);
    this.name = 'ConcurrencyLimitError';
  }
}

export class TenantRepository {
  constructor(private pool: Pool) {}

  async getTenantByApiKeyHash(apiKeyHash: string): Promise<Tenant | null> {
    const result = await this.pool.query('SELECT * FROM tenants WHERE api_key_hash = $1', [apiKeyHash]);
    if (result.rows.length === 0) return null;
    return rowToTenant(result.rows[0]);
  }

  async getTenant(tenantId: string): Promise<Tenant | null> {
    const result = await this.pool.query('SELECT * FROM tenants WHERE id = $1', [tenantId]);
    if (result.rows.length === 0) return null;
    return rowToTenant(result.rows[0]);
  }

  async createTenant(name: string, planId: string, apiKeyHash: string): Promise<Tenant> {
    const id = generateId();
    const result = await this.pool.query(
      `INSERT INTO tenants (id, name, status, plan_id, api_key_hash, created_at, updated_at)
       VALUES ($1, $2, 'ACTIVE', $3, $4, NOW(), NOW()) RETURNING *`,
      [id, name, planId, apiKeyHash],
    );
    return rowToTenant(result.rows[0]);
  }

  async setTenantStatus(tenantId: string, status: TenantStatus): Promise<Tenant> {
    const result = await this.pool.query(
      `UPDATE tenants SET status = $2, updated_at = NOW() WHERE id = $1 RETURNING *`,
      [tenantId, status],
    );
    if (result.rows.length === 0) throw new Error(`Tenant ${tenantId} not found`);
    return rowToTenant(result.rows[0]);
  }

  async getPlan(planId: string): Promise<Plan | null> {
    const result = await this.pool.query('SELECT * FROM plans WHERE id = $1', [planId]);
    if (result.rows.length === 0) return null;
    return rowToPlan(result.rows[0]);
  }

  async listPlans(): Promise<Plan[]> {
    const result = await this.pool.query('SELECT * FROM plans ORDER BY weight ASC');
    return result.rows.map(rowToPlan);
  }

  async getOverrides(tenantId: string): Promise<TenantOverride | null> {
    const result = await this.pool.query('SELECT * FROM tenant_overrides WHERE tenant_id = $1', [tenantId]);
    if (result.rows.length === 0) return null;
    return rowToOverride(result.rows[0]);
  }

  async setOverrides(tenantId: string, overrides: Partial<Omit<TenantOverride, 'id' | 'tenantId' | 'createdAt' | 'updatedAt'>>): Promise<TenantOverride> {
    const id = generateId();
    const result = await this.pool.query(
      `INSERT INTO tenant_overrides (id, tenant_id, rate_limit, max_concurrent_executions, max_jobs_per_period, billing_period_days, max_compute_units_per_period, max_storage_mb, weight, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
       ON CONFLICT (tenant_id) DO UPDATE SET
         rate_limit = COALESCE(EXCLUDED.rate_limit, tenant_overrides.rate_limit),
         max_concurrent_executions = COALESCE(EXCLUDED.max_concurrent_executions, tenant_overrides.max_concurrent_executions),
         max_jobs_per_period = COALESCE(EXCLUDED.max_jobs_per_period, tenant_overrides.max_jobs_per_period),
         billing_period_days = COALESCE(EXCLUDED.billing_period_days, tenant_overrides.billing_period_days),
         max_compute_units_per_period = COALESCE(EXCLUDED.max_compute_units_per_period, tenant_overrides.max_compute_units_per_period),
         max_storage_mb = COALESCE(EXCLUDED.max_storage_mb, tenant_overrides.max_storage_mb),
         weight = COALESCE(EXCLUDED.weight, tenant_overrides.weight),
         updated_at = NOW()
       RETURNING *`,
      [id, tenantId, overrides.rateLimit ?? null, overrides.maxConcurrentExecutions ?? null,
       overrides.maxJobsPerPeriod ?? null, overrides.billingPeriodDays ?? null,
       overrides.maxComputeUnitsPerPeriod ?? null, overrides.maxStorageMb ?? null,
       overrides.weight ?? null],
    );
    return rowToOverride(result.rows[0]);
  }

  async getEffectiveLimits(tenantId: string): Promise<EffectiveLimits> {
    const result = await this.pool.query(
      `SELECT
         COALESCE(o.rate_limit, p.rate_limit) as rate_limit,
         COALESCE(o.max_concurrent_executions, p.max_concurrent_executions) as max_concurrent_executions,
         COALESCE(o.max_jobs_per_period, p.max_jobs_per_period) as max_jobs_per_period,
         COALESCE(o.billing_period_days, p.billing_period_days) as billing_period_days,
         COALESCE(o.max_compute_units_per_period, p.max_compute_units_per_period) as max_compute_units_per_period,
         COALESCE(o.max_storage_mb, p.max_storage_mb) as max_storage_mb,
         COALESCE(o.weight, p.weight) as weight
       FROM tenants t
       JOIN plans p ON t.plan_id = p.id
       LEFT JOIN tenant_overrides o ON o.tenant_id = t.id
       WHERE t.id = $1`,
      [tenantId],
    );
    if (result.rows.length === 0) throw new Error(`Tenant ${tenantId} not found`);
    const row = result.rows[0];
    return {
      rateLimit: row.rate_limit as number,
      maxConcurrentExecutions: row.max_concurrent_executions as number,
      maxJobsPerPeriod: row.max_jobs_per_period as number,
      billingPeriodDays: row.billing_period_days as number,
      maxComputeUnitsPerPeriod: row.max_compute_units_per_period as number,
      maxStorageMb: row.max_storage_mb as number,
      weight: row.weight as number,
    };
  }

  async getUsage(tenantId: string, billingPeriodStart: Date): Promise<TenantUsage | null> {
    const result = await this.pool.query(
      'SELECT * FROM tenant_usage WHERE tenant_id = $1 AND billing_period_start = $2',
      [tenantId, billingPeriodStart],
    );
    if (result.rows.length === 0) return null;
    return rowToUsage(result.rows[0]);
  }

  async acquireConcurrencyLease(
    tenantId: string,
    taskId: string,
    workerId: string,
    maxConcurrent: number,
    leaseDurationMs: number,
  ): Promise<{ acquired: boolean; lease?: TenantConcurrencyLease }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(
        `DELETE FROM tenant_concurrency_leases WHERE expires_at < NOW()`,
      );

      const countResult = await client.query(
        `SELECT COUNT(*)::int as count FROM tenant_concurrency_leases WHERE tenant_id = $1`,
        [tenantId],
      );
      const active = countResult.rows[0].count as number;

      if (active >= maxConcurrent) {
        await client.query('ROLLBACK');
        return { acquired: false };
      }

      const id = generateId();
      const leaseToken = generateId();
      const leaseResult = await client.query(
        `INSERT INTO tenant_concurrency_leases (id, tenant_id, task_id, worker_id, lease_token, expires_at, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW() + $6 * INTERVAL '1 millisecond', NOW())
         RETURNING *`,
        [id, tenantId, taskId, workerId, leaseToken, leaseDurationMs],
      );

      await client.query('COMMIT');
      return { acquired: true, lease: rowToLease(leaseResult.rows[0]) };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async renewConcurrencyLease(leaseToken: string, leaseDurationMs: number): Promise<void> {
    const result = await this.pool.query(
      `UPDATE tenant_concurrency_leases SET expires_at = NOW() + $2 * INTERVAL '1 millisecond'
       WHERE lease_token = $1`,
      [leaseToken, leaseDurationMs],
    );
    if (result.rowCount === 0) {
      throw new Error(`Concurrency lease not found for token ${leaseToken}`);
    }
  }

  async releaseConcurrencyLease(leaseToken: string): Promise<void> {
    await this.pool.query(
      'DELETE FROM tenant_concurrency_leases WHERE lease_token = $1',
      [leaseToken],
    );
  }

  async getActiveConcurrencyCount(tenantId: string): Promise<number> {
    const result = await this.pool.query(
      `SELECT COUNT(*)::int as count FROM tenant_concurrency_leases
       WHERE tenant_id = $1 AND expires_at > NOW()`,
      [tenantId],
    );
    return result.rows[0].count as number;
  }
}
