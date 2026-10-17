import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import express, { Request, Response, NextFunction } from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { Pool } from 'pg';
import { AppError, TenantRepository, runMigrations } from '@repo/shared';
import { createOperatorAuth } from '../operator-auth';
import { createTenantAdminRouter, generateTenantApiKey } from '../tenant-admin';
import { createTenantAuth } from '../tenant-middleware';
import { ValidationError } from '../validation';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const OPERATOR_KEY = 'o'.repeat(40);

let pool: Pool;
let server: Server;
let base: string;

const op = { Authorization: `Bearer ${OPERATOR_KEY}`, 'Content-Type': 'application/json' };

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  const repo = new TenantRepository(pool);

  // Same wiring as main.ts, minus everything unrelated to tenants.
  const app = express();
  app.use(express.json());
  app.use(['/tenants', '/plans'], createOperatorAuth(OPERATOR_KEY));
  app.use(createTenantAdminRouter(repo));
  app.get('/tenant/whoami', createTenantAuth(repo), (req: Request, res: Response) => {
    res.json({ success: true, data: req.tenant });
  });
  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof ValidationError) return res.status(422).json({ success: false, error: 'Validation error' });
    if (err instanceof AppError) return res.status(err.statusCode).json({ success: false, error: err.message });
    res.status(500).json({ success: false, error: String(err) });
  });

  await new Promise<void>((resolve) => {
    server = app.listen(0, () => resolve());
  });
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  await new Promise((r) => server.close(r));
  await pool.query('TRUNCATE tenant_concurrency_leases, tenant_usage, tenant_overrides, outbox_events, tasks, tenants CASCADE').catch(() => {});
  await pool.end();
});

beforeEach(async () => {
  await pool.query('TRUNCATE tenant_concurrency_leases, tenant_usage, tenant_overrides, outbox_events, tasks, tenants CASCADE');
});

async function createTenant(name = 'Acme', planId = 'plan_starter') {
  const res = await fetch(`${base}/tenants`, { method: 'POST', headers: op, body: JSON.stringify({ name, planId }) });
  return { status: res.status, body: (await res.json()) as any };
}

describe('operator tenant provisioning', () => {
  it('requires the operator key', async () => {
    expect((await fetch(`${base}/tenants`)).status).toBe(401);
    expect((await fetch(`${base}/plans`)).status).toBe(401);
  });

  it('lists the seeded plans', async () => {
    const body = (await (await fetch(`${base}/plans`, { headers: op })).json()) as any;
    expect(body.data.map((p: any) => p.id)).toEqual(['plan_starter', 'plan_professional', 'plan_enterprise']);
  });

  it('creates a tenant and returns a working key exactly once', async () => {
    const { status, body } = await createTenant();
    expect(status).toBe(201);
    expect(body.data.apiKey).toMatch(/^te_[A-Za-z0-9_-]{43}$/);
    expect(body.data.tenant).toMatchObject({ name: 'Acme', status: 'ACTIVE', planId: 'plan_starter' });

    const who = await fetch(`${base}/tenant/whoami`, { headers: { Authorization: `Bearer ${body.data.apiKey}` } });
    expect(who.status).toBe(200);
    expect(((await who.json()) as any).data.id).toBe(body.data.tenant.id);

    // Only the hash is stored; nothing returns the key again.
    const row = await pool.query('SELECT api_key_hash FROM tenants WHERE id = $1', [body.data.tenant.id]);
    expect(row.rows[0].api_key_hash).not.toContain(body.data.apiKey);
    const list = JSON.stringify(await (await fetch(`${base}/tenants`, { headers: op })).json());
    expect(list).not.toContain(body.data.apiKey);
    expect(list).not.toContain(row.rows[0].api_key_hash);
  });

  it('rejects unknown plans and bad input', async () => {
    expect((await createTenant('X', 'plan_gold')).status).toBe(422);
    expect((await createTenant('', 'plan_starter')).status).toBe(422);
  });

  it('shows limits and usage for one tenant', async () => {
    const { body } = await createTenant('Beta', 'plan_professional');
    const res = await fetch(`${base}/tenants/${body.data.tenant.id}`, { headers: op });
    const detail = ((await res.json()) as any).data;
    expect(detail.tenant.id).toBe(body.data.tenant.id);
    expect(detail.limits.maxConcurrentExecutions).toBe(20);
    expect(detail.usage.acceptedJobs).toBe(0);
    expect((await fetch(`${base}/tenants/nope`, { headers: op })).status).toBe(404);
  });

  it('suspends and reactivates a tenant; a suspended key is refused', async () => {
    const { body } = await createTenant();
    const id = body.data.tenant.id;
    const key = body.data.apiKey;

    const s = await fetch(`${base}/tenants/${id}/status`, { method: 'PUT', headers: op, body: JSON.stringify({ status: 'SUSPENDED' }) });
    expect(((await s.json()) as any).data.status).toBe('SUSPENDED');
    expect((await fetch(`${base}/tenant/whoami`, { headers: { Authorization: `Bearer ${key}` } })).status).toBe(403);

    await fetch(`${base}/tenants/${id}/status`, { method: 'PUT', headers: op, body: JSON.stringify({ status: 'ACTIVE' }) });
    expect((await fetch(`${base}/tenant/whoami`, { headers: { Authorization: `Bearer ${key}` } })).status).toBe(200);

    const bad = await fetch(`${base}/tenants/${id}/status`, { method: 'PUT', headers: op, body: JSON.stringify({ status: 'DELETED' }) });
    expect(bad.status).toBe(422);
  });

  it('rotating the key revokes the old one immediately', async () => {
    const { body } = await createTenant();
    const oldKey = body.data.apiKey;
    const r = await fetch(`${base}/tenants/${body.data.tenant.id}/api-key`, { method: 'POST', headers: op });
    const newKey = ((await r.json()) as any).data.apiKey;
    expect(newKey).not.toBe(oldKey);
    expect((await fetch(`${base}/tenant/whoami`, { headers: { Authorization: `Bearer ${oldKey}` } })).status).toBe(401);
    expect((await fetch(`${base}/tenant/whoami`, { headers: { Authorization: `Bearer ${newKey}` } })).status).toBe(200);
  });

  it('generates distinct high-entropy keys', () => {
    const keys = new Set(Array.from({ length: 200 }, generateTenantApiKey));
    expect(keys.size).toBe(200);
  });
});
