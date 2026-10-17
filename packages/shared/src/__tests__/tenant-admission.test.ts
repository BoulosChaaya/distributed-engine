import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { TaskRepository } from '../db/task-repository';
import { TenantRepository, computeBillingPeriodStart } from '../db/tenant-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { WeightedFairScheduler } from '../fairness';
import { runMigrations } from '../db/migrations';

// Publish-time admission: a tenant's tasks reach BullMQ only into free
// concurrency slots, in priority-then-age order. Real PostgreSQL + Redis.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';
const silent: any = { info() {}, warn() {}, error() {}, debug() {}, child() { return silent; } };

let pool: Pool;
let redis: IORedis;
let queue: Queue;
let tasks: TaskRepository;
let tenants: TenantRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  redis = new IORedis({
    host: process.env.TEST_REDIS_HOST || 'localhost',
    port: Number(process.env.TEST_REDIS_PORT || 6379),
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: null,
  });
  await runMigrations(pool);
  tasks = new TaskRepository(pool);
  tenants = new TenantRepository(pool);
  queue = new Queue('tasks-test-admission', { connection: redis });
  await queue.waitUntilReady();
});

afterAll(async () => {
  await queue.obliterate({ force: true }).catch(() => {});
  await queue.close();
  redis.disconnect();
  await pool.end();
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  await queue.obliterate({ force: true }).catch(() => {});
});

function publisher(tenantAdmission: boolean) {
  return new OutboxPublisher(pool, queue, 60000, 10, 5, {
    fairScheduler: new WeightedFairScheduler(), tenantAdmission, logger: silent,
  });
}

async function tenantWithLimit(maxConcurrentExecutions: number) {
  const plan = (await tenants.listPlans())[0];
  const tenant = await tenants.createTenant('adm', plan.id, `adm-${Date.now()}-${Math.random()}`);
  await tenants.setOverrides(tenant.id, { maxConcurrentExecutions });
  const limits = await tenants.getEffectiveLimits(tenant.id);
  const submit = async (name: string, priority: 'LOW' | 'NORMAL' | 'HIGH' | 'CRITICAL') => (await tasks.acceptTask({
    tenantId: tenant.id, name, priority, payload: {}, maxRetries: 0,
    billingPeriodStart: computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date()),
    maxJobsPerPeriod: limits.maxJobsPerPeriod,
  })).task.id;
  return { tenant, submit };
}

/** Names of tasks whose event was published, in publish order. */
async function published(): Promise<string[]> {
  const r = await pool.query(
    `SELECT t.name FROM outbox_events oe JOIN tasks t ON t.id = oe.task_id
     WHERE oe.status = 'DELIVERED' ORDER BY oe.processed_at, oe.id`,
  );
  return r.rows.map((x) => x.name);
}

async function finish(name: string) {
  await pool.query(`UPDATE tasks SET status = 'COMPLETED' WHERE name = $1`, [name]);
}

describe('publish-time tenant admission (requires PostgreSQL + Redis)', () => {
  it('without admission every task is published at once (the old behaviour)', async () => {
    const { submit } = await tenantWithLimit(1);
    for (const [n, p] of [['a', 'NORMAL'], ['b', 'LOW'], ['c', 'HIGH'], ['d', 'NORMAL']] as const) await submit(n, p);
    expect(await publisher(false).processOutbox()).toBe(4);
  });

  it('publishes one task per free slot, highest priority first, then oldest', async () => {
    const { submit } = await tenantWithLimit(1);
    for (const [n, p] of [['a', 'NORMAL'], ['b', 'LOW'], ['c', 'HIGH'], ['d', 'NORMAL']] as const) await submit(n, p);
    const pub = publisher(true);

    expect(await pub.processOutbox()).toBe(1);
    expect(await pub.processOutbox()).toBe(0); // c is in flight, the slot is taken
    for (const done of ['c', 'a', 'd']) {
      await finish(done);
      expect(await pub.processOutbox()).toBe(1);
    }
    expect(await published()).toEqual(['c', 'a', 'd', 'b']);
  });

  it('fills every free slot when the limit is higher', async () => {
    const { submit } = await tenantWithLimit(3);
    for (let i = 0; i < 5; i++) await submit(`t${i}`, 'NORMAL');
    expect(await publisher(true).processOutbox()).toBe(3);
  });

  it('a task in flight (PROCESSING, or QUEUED after leaving the outbox) holds its slot', async () => {
    const { submit } = await tenantWithLimit(2);
    await submit('x', 'NORMAL');
    await submit('y', 'NORMAL');
    await submit('z', 'NORMAL');
    const pub = publisher(true);
    expect(await pub.processOutbox()).toBe(2);
    await pool.query(`UPDATE tasks SET status = 'PROCESSING' WHERE name = 'x'`);
    expect(await pub.processOutbox()).toBe(0);
    await finish('y');
    expect(await pub.processOutbox()).toBe(1);
  });

  it('holds back a suspended tenant entirely, without blocking others', async () => {
    const a = await tenantWithLimit(5);
    const b = await tenantWithLimit(5);
    await a.submit('a1', 'NORMAL');
    await b.submit('b1', 'NORMAL');
    await tenants.setTenantStatus(a.tenant.id, 'SUSPENDED');
    expect(await publisher(true).processOutbox()).toBe(1);
    expect(await published()).toEqual(['b1']);
  });

  it('never blocks tasks without a tenant', async () => {
    const { submit } = await tenantWithLimit(1);
    await submit('t1', 'NORMAL');
    await submit('t2', 'NORMAL');
    await tasks.createTaskWithOutbox({ name: 'operator', priority: 'NORMAL', payload: {}, maxRetries: 0 });
    expect(await publisher(true).processOutbox()).toBe(2);
    expect((await published()).sort()).toEqual(['operator', 't1']);
  });
});
