import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from '@jest/globals';
import { Pool, Client } from 'pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { TaskRepository } from '../db/task-repository';
import { TenantRepository, computeBillingPeriodStart } from '../db/tenant-repository';
import { OutboxPublisher } from '../db/outbox-publisher';
import { WeightedFairScheduler } from '../fairness';
import { runMigrations } from '../db/migrations';
import { submitTask } from './helpers/submit-task';

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
    await submitTask(tasks, { name: 'operator', priority: 'NORMAL', payload: {}, maxRetries: 0 });
    expect(await publisher(true).processOutbox()).toBe(2);
    expect((await published()).sort()).toEqual(['operator', 't1']);
  });
});

describe('publisher wake-ups (requires PostgreSQL + Redis)', () => {
  // 60 s poll interval in every case: anything published within a second
  // was published because PostgreSQL said so, not because a poll came round.
  const running: OutboxPublisher[] = [];
  afterEach(async () => {
    for (const p of running.splice(0)) await p.stop();
  });

  async function startPublisher(listen: boolean): Promise<OutboxPublisher> {
    const pub = new OutboxPublisher(pool, queue, 60000, 10, 5, {
      fairScheduler: new WeightedFairScheduler(), tenantAdmission: true, logger: silent,
    });
    if (listen) pub.listenForWakeups(() => new Client({ connectionString: TEST_PG_URL }));
    running.push(pub);
    pub.start();
    if (listen) {
      for (let i = 0; i < 100 && !pub.isListening(); i++) await sleep(20);
      expect(pub.isListening()).toBe(true);
    }
    return pub;
  }

  async function waitForPublished(names: string[], ms: number): Promise<string[]> {
    const until = Date.now() + ms;
    let got = await published();
    while (Date.now() < until && !names.every((n) => got.includes(n))) {
      await sleep(25);
      got = await published();
    }
    return got;
  }

  it('publishes a new task as soon as it is committed', async () => {
    await startPublisher(true);
    const { submit } = await tenantWithLimit(5);
    const started = Date.now();
    await submit('fresh', 'NORMAL');
    expect(await waitForPublished(['fresh'], 1000)).toEqual(['fresh']);
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it('refills a freed concurrency slot as soon as a task finishes', async () => {
    const { submit } = await tenantWithLimit(1);
    await submit('first', 'NORMAL');
    await submit('second', 'NORMAL');
    await startPublisher(true);
    expect(await waitForPublished(['first'], 1000)).toEqual(['first']);

    // Through the repository, as a worker would: QUEUED -> PROCESSING -> COMPLETED.
    const first = (await pool.query(`SELECT id FROM tasks WHERE name = 'first'`)).rows[0].id;
    const claimed = await tasks.transitionStatus(first, 1, 'PROCESSING', { startedAt: new Date(), claimedBy: 'w' });
    await tasks.transitionStatus(first, claimed.version, 'COMPLETED', { completedAt: new Date(), claimToken: claimed.claimToken });

    expect(await waitForPublished(['first', 'second'], 1000)).toEqual(['first', 'second']);
  });

  it('without the wake-up the freed slot waits for the next poll (the old behaviour)', async () => {
    const { submit } = await tenantWithLimit(1);
    await submit('first', 'NORMAL');
    await submit('second', 'NORMAL');
    const pub = await startPublisher(false);
    await pub.processOutbox();
    await finish('first');
    expect(await waitForPublished(['first', 'second'], 1000)).toEqual(['first']);
  });

  it('notices a LISTEN connection that stopped answering and reconnects', async () => {
    // The first connection goes half-open after LISTEN: SELECT 1 never
    // returns and notifications never arrive, with no error and no 'end',
    // which is what a dead peer that never closed the socket looks like.
    class HalfOpen extends Client {
      query(...args: unknown[]): any {
        if (args[0] === 'SELECT 1') return new Promise(() => {});
        return (Client.prototype.query as (...a: unknown[]) => unknown).apply(this, args);
      }
      emit(event: string | symbol, ...args: unknown[]): boolean {
        return event === 'notification' ? false : super.emit(event, ...args);
      }
    }
    let made = 0;
    const pub = new OutboxPublisher(pool, queue, 60000, 10, 5, {
      fairScheduler: new WeightedFairScheduler(), tenantAdmission: true, logger: silent,
      listenerHeartbeatMs: 100, listenerTimeoutMs: 100,
    });
    pub.listenForWakeups(() => (made++ === 0 ? new HalfOpen({ connectionString: TEST_PG_URL }) : new Client({ connectionString: TEST_PG_URL })));
    running.push(pub);
    pub.start();

    // Heartbeat after 100 ms, no answer for 100 ms, reconnect after the 1 s backoff.
    for (let i = 0; i < 150 && made < 2; i++) await sleep(20);
    expect(made).toBe(2);
    for (let i = 0; i < 100 && !pub.isListening(); i++) await sleep(20);
    expect(pub.isListening()).toBe(true);

    const { submit } = await tenantWithLimit(5);
    await submit('after-reconnect', 'NORMAL');
    expect(await waitForPublished(['after-reconnect'], 1000)).toEqual(['after-reconnect']);
  });

  it('gives up connecting to an address that drops packets, and tries again', async () => {
    let made = 0;
    const pub = new OutboxPublisher(pool, queue, 60000, 10, 5, {
      fairScheduler: new WeightedFairScheduler(), tenantAdmission: true, logger: silent,
      listenerTimeoutMs: 200,
    });
    pub.listenForWakeups(() => { made++; return new Client({ host: '10.255.255.1', port: 5432, user: 'x', password: 'x', database: 'x' }); });
    running.push(pub);
    pub.start();
    await sleep(1600); // 200 ms timeout, 1 s backoff, second attempt
    expect(made).toBeGreaterThanOrEqual(2);
    expect(pub.isListening()).toBe(false);
  });

  it('keeps working by polling when the LISTEN connection cannot be opened', async () => {
    const pub = new OutboxPublisher(pool, queue, 200, 10, 5, {
      fairScheduler: new WeightedFairScheduler(), tenantAdmission: true, logger: silent,
    });
    pub.listenForWakeups(() => new Client({ connectionString: 'postgresql://nobody:wrong@127.0.0.1:1/none' }));
    running.push(pub);
    pub.start();
    const { submit } = await tenantWithLimit(5);
    await submit('polled', 'NORMAL');
    expect(await waitForPublished(['polled'], 2000)).toEqual(['polled']);
    expect(pub.isListening()).toBe(false);
  });
});

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
