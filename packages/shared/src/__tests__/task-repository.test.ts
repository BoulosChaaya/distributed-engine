import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { TaskRepository, StaleVersionError, ClaimTokenMismatchError, ClaimNotExpiredError } from '../db/task-repository';
import { InvalidTransitionError } from '../state-machine';
import { runMigrations } from '../db/migrations';

const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

function requirePg(): void {
  if (!pool || !repo) {
    throw new Error('PostgreSQL is required for this test. Set TEST_POSTGRES_URL or start a local PostgreSQL instance.');
  }
}

let pool: Pool;
let repo: TaskRepository;

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
  repo = new TaskRepository(pool);
});

afterAll(async () => {
  if (pool) {
    await pool.query('TRUNCATE outbox_events, tasks CASCADE').catch(() => {});
    await pool.end();
  }
});

beforeEach(async () => {
  requirePg();
  await pool.query('TRUNCATE outbox_events, tasks CASCADE');
});

describe('TaskRepository (requires PostgreSQL)', () => {
  it('should create a task with outbox event atomically', async () => {
    requirePg();

    const { task, outboxEvent } = await repo.createTaskWithOutbox({
      name: 'test-task',
      priority: 'HIGH',
      payload: { key: 'value' },
      maxRetries: 3,
    });

    expect(task.id).toBeDefined();
    expect(task.name).toBe('test-task');
    expect(task.status).toBe('QUEUED');
    expect(task.priority).toBe('HIGH');
    expect(task.version).toBe(1);

    expect(outboxEvent.id).toBeDefined();
    expect(outboxEvent.taskId).toBe(task.id);
    expect(outboxEvent.eventType).toBe('TASK_CREATED');
    expect(outboxEvent.status).toBe('PENDING');
  });

  it('should retrieve a task by ID', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'get-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const retrieved = await repo.getTask(task.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.id).toBe(task.id);
    expect(retrieved!.name).toBe('get-task');
  });

  it('should return null for non-existent task', async () => {
    requirePg();
    const result = await repo.getTask('nonexistent-id');
    expect(result).toBeNull();
  });

  it('should list tasks with pagination', async () => {
    requirePg();

    for (let i = 0; i < 5; i++) {
      await repo.createTaskWithOutbox({
        name: `task-${i}`,
        priority: 'NORMAL',
        payload: {},
        maxRetries: 3,
      });
    }

    const page1 = await repo.listTasks(1, 3);
    expect(page1.items.length).toBe(3);
    expect(page1.total).toBe(5);

    const page2 = await repo.listTasks(2, 3);
    expect(page2.items.length).toBe(2);
    expect(page2.total).toBe(5);
  });

  it('should transition status with version check', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'transition-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(task.status).toBe('QUEUED');

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
    });
    expect(processing.status).toBe('PROCESSING');
    expect(processing.version).toBe(2);
    expect(processing.startedAt).toBeDefined();
    expect(processing.claimToken).toBeDefined();

    const completed = await repo.transitionStatus(task.id, 2, 'COMPLETED', {
      completedAt: new Date(),
      result: { output: 'done' },
      claimToken: processing.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
    expect(completed.version).toBe(3);
  });

  it('should reject invalid state transitions', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'invalid-transition',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await expect(
      repo.transitionStatus(task.id, 1, 'COMPLETED'),
    ).rejects.toThrow(InvalidTransitionError);
  });

  it('should reject stale version updates', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'stale-version',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.transitionStatus(task.id, 1, 'PROCESSING');

    await expect(
      repo.transitionStatus(task.id, 1, 'CANCELLED'),
    ).rejects.toThrow(StaleVersionError);
  });

  it('should handle concurrent transitions safely', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'concurrent-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const results = await Promise.allSettled([
      repo.transitionStatus(task.id, 1, 'PROCESSING'),
      repo.transitionStatus(task.id, 1, 'CANCELLED'),
    ]);

    const fulfilled = results.filter(r => r.status === 'fulfilled');
    const rejected = results.filter(r => r.status === 'rejected');

    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);
  });

  it('should cancel a task atomically', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'cancel-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const cancelled = await repo.cancelTask(task.id);
    expect(cancelled.status).toBe('CANCELLED');
  });

  it('should reject cancellation of completed tasks', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'no-cancel-completed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING');
    await repo.transitionStatus(task.id, 2, 'COMPLETED', {
      completedAt: new Date(),
      claimToken: processing.claimToken,
    });

    await expect(repo.cancelTask(task.id)).rejects.toThrow(InvalidTransitionError);
  });

  it('should get task status counts', async () => {
    requirePg();

    const { task: t1 } = await repo.createTaskWithOutbox({
      name: 'count-1', priority: 'NORMAL', payload: {}, maxRetries: 3,
    });
    const { task: t2 } = await repo.createTaskWithOutbox({
      name: 'count-2', priority: 'NORMAL', payload: {}, maxRetries: 3,
    });
    await repo.createTaskWithOutbox({
      name: 'count-3', priority: 'NORMAL', payload: {}, maxRetries: 3,
    });

    await repo.transitionStatus(t1.id, 1, 'PROCESSING');
    await repo.transitionStatus(t2.id, 1, 'PROCESSING');

    const counts = await repo.getTaskStatusCounts();
    expect(counts.QUEUED).toBe(1);
    expect(counts.PROCESSING).toBe(2);
  });

  it('should handle FAILED -> QUEUED retry transition', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'retry-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING');
    await repo.transitionStatus(task.id, 2, 'FAILED', {
      error: 'temporary error',
      retries: 1,
      claimToken: processing.claimToken,
    });

    const retried = await repo.transitionStatus(task.id, 3, 'QUEUED');
    expect(retried.status).toBe('QUEUED');
    expect(retried.version).toBe(4);
  });

  it('should handle PROCESSING -> QUEUED retry transition', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'processing-retry-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING');

    const requeued = await repo.transitionStatus(task.id, 2, 'QUEUED', {
      error: 'intermediate failure',
      retries: 1,
      claimToken: processing.claimToken,
    });
    expect(requeued.status).toBe('QUEUED');
    expect(requeued.version).toBe(3);
    expect(requeued.retries).toBe(1);
  });

  it('should handle atomicity: task and outbox event created together', async () => {
    requirePg();

    const { task, outboxEvent } = await repo.createTaskWithOutbox({
      name: 'atomic-test',
      priority: 'NORMAL',
      payload: { foo: 'bar' },
      maxRetries: 3,
    });

    const dbTask = await pool.query('SELECT * FROM tasks WHERE id = $1', [task.id]);
    const dbOutbox = await pool.query('SELECT * FROM outbox_events WHERE id = $1', [outboxEvent.id]);

    expect(dbTask.rows.length).toBe(1);
    expect(dbOutbox.rows.length).toBe(1);
    expect(dbOutbox.rows[0].task_id).toBe(task.id);
  });

  it('should reclaim a stalled PROCESSING task with expired claim', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'stalled-task',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
    });

    // Expire the claim so reclaim is allowed
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version);
    expect(reclaimed.status).toBe('PROCESSING');
    expect(reclaimed.version).toBe(processing.version + 1);
    expect(reclaimed.claimToken).toBeDefined();
    expect(reclaimed.claimToken).not.toBe(processing.claimToken);
  });

  it('should reject reclaim on non-PROCESSING task', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'no-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await expect(
      repo.reclaimStalledTask(task.id, task.version),
    ).rejects.toThrow('Cannot reclaim');
  });

  it('should reject reclaim with stale version', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'stale-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.transitionStatus(task.id, 1, 'PROCESSING');
    await expect(
      repo.reclaimStalledTask(task.id, 1),
    ).rejects.toThrow(StaleVersionError);
  });
});

describe('Migration concurrency (requires PostgreSQL)', () => {
  it('should handle concurrent migration calls safely', async () => {
    requirePg();

    const pool1 = new Pool({ connectionString: TEST_PG_URL });
    const pool2 = new Pool({ connectionString: TEST_PG_URL });

    try {
      const results = await Promise.allSettled([
        runMigrations(pool1),
        runMigrations(pool2),
      ]);

      const fulfilled = results.filter(r => r.status === 'fulfilled');
      expect(fulfilled.length).toBe(2);
    } finally {
      await pool1.end();
      await pool2.end();
    }
  });
});

describe('Clean-database migration (requires PostgreSQL)', () => {
  it('should produce correct schema on a fresh database', async () => {
    requirePg();

    await pool.query('DROP TABLE IF EXISTS outbox_events, tasks, schema_migrations CASCADE');
    await runMigrations(pool);

    const colDefault = await pool.query(`
      SELECT column_default FROM information_schema.columns
      WHERE table_name = 'tasks' AND column_name = 'status'
    `);
    expect(colDefault.rows[0].column_default).toBe("'QUEUED'::text");

    const checkConstraint = await pool.query(`
      SELECT pg_get_constraintdef(c.oid) as def
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
      WHERE t.relname = 'tasks' AND c.conname = 'valid_status'
    `);
    expect(checkConstraint.rows.length).toBe(1);
    const constraintDef = checkConstraint.rows[0].def;
    expect(constraintDef).not.toContain('PENDING');
    expect(constraintDef).toContain('QUEUED');

    const claimedByCol = await pool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_name = 'tasks' AND column_name = 'claimed_by'
    `);
    expect(claimedByCol.rows.length).toBe(1);

    // Verify claim_token and claim_expires_at columns exist (migration 6)
    const claimTokenCol = await pool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_name = 'tasks' AND column_name = 'claim_token'
    `);
    expect(claimTokenCol.rows.length).toBe(1);

    const claimExpiresCol = await pool.query(`
      SELECT column_name, data_type FROM information_schema.columns
      WHERE table_name = 'tasks' AND column_name = 'claim_expires_at'
    `);
    expect(claimExpiresCol.rows.length).toBe(1);

    const insertResult = await pool.query(
      `INSERT INTO tasks (id, name, priority, payload, max_retries, retries, version, created_at, updated_at)
       VALUES ('clean-db-test', 'test', 'NORMAL', '{}', 3, 0, 1, NOW(), NOW())
       RETURNING status`
    );
    expect(insertResult.rows[0].status).toBe('QUEUED');

    await pool.query('TRUNCATE outbox_events, tasks CASCADE');
  });
});

describe('Task ownership semantics (requires PostgreSQL)', () => {
  it('should set claimed_by when transitioning to PROCESSING', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-processing',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    expect(processing.claimedBy).toBe('worker-A');
  });

  it('should generate claim token on QUEUED -> PROCESSING', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-claim-token',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    expect(task.claimToken).toBeUndefined();

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    expect(processing.claimToken).toBeDefined();
    expect(processing.claimToken!.length).toBeGreaterThan(0);
    expect(processing.claimExpiresAt).toBeDefined();
    expect(processing.claimExpiresAt!.getTime()).toBeGreaterThan(Date.now() - 1000);
  });

  it('should clear claimed_by when transitioning to COMPLETED', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-completed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const completed = await repo.transitionStatus(task.id, 2, 'COMPLETED', {
      completedAt: new Date(),
      claimToken: processing.claimToken,
    });

    expect(completed.claimedBy).toBeUndefined();
    expect(completed.claimToken).toBeUndefined();
    expect(completed.claimExpiresAt).toBeUndefined();
  });

  it('should clear claimed_by when transitioning to FAILED', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-failed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const failed = await repo.transitionStatus(task.id, 2, 'FAILED', {
      error: 'test error',
      claimToken: processing.claimToken,
    });

    expect(failed.claimedBy).toBeUndefined();
    expect(failed.claimToken).toBeUndefined();
  });

  it('should clear claimed_by when transitioning to CANCELLED', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-cancelled',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const cancelled = await repo.cancelTask(task.id);
    expect(cancelled.claimedBy).toBeUndefined();
    expect(cancelled.claimToken).toBeUndefined();
    expect(cancelled.claimExpiresAt).toBeUndefined();
  });

  it('should update claimed_by on reclaim with new worker ID', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Expire the claim
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version, 'worker-B');
    expect(reclaimed.claimedBy).toBe('worker-B');
    expect(reclaimed.version).toBe(processing.version + 1);
    expect(reclaimed.claimToken).toBeDefined();
    expect(reclaimed.claimToken).not.toBe(processing.claimToken);
  });

  it('should reject self-reclaim (same worker ID)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-self-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await expect(
      repo.reclaimStalledTask(task.id, processing.version, 'worker-A'),
    ).rejects.toThrow('already claimed by this worker');
  });

  it('should allow reclaim without worker ID when claim expired', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'ownership-no-id-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
    });

    // Expire the claim
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version);
    expect(reclaimed.version).toBe(processing.version + 1);
  });
});

describe('Worker completion race (requires PostgreSQL)', () => {
  it('should prevent COMPLETED transition on a CANCELLED task via version check', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'completion-race',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await repo.cancelTask(task.id);

    await expect(
      repo.transitionStatus(task.id, 2, 'COMPLETED', {
        completedAt: new Date(),
        claimToken: processing.claimToken,
      }),
    ).rejects.toThrow(StaleVersionError);

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('CANCELLED');
  });

  it('should prevent COMPLETED transition when another worker reclaimed the task', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'double-reclaim-race',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Expire claim and reclaim by worker B
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );
    await repo.reclaimStalledTask(task.id, 2, 'worker-B');

    // Worker A tries to complete with stale version 2
    await expect(
      repo.transitionStatus(task.id, 2, 'COMPLETED', {
        completedAt: new Date(),
        claimToken: processing.claimToken,
      }),
    ).rejects.toThrow(StaleVersionError);

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimedBy).toBe('worker-B');
  });

  it('should clear claimed_by when transitioning PROCESSING -> QUEUED on retry', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'retry-clear-owner',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const requeued = await repo.transitionStatus(task.id, 2, 'QUEUED', {
      error: 'intermediate failure',
      retries: 1,
      claimToken: processing.claimToken,
    });

    expect(requeued.claimedBy).toBeUndefined();
    expect(requeued.claimToken).toBeUndefined();
    expect(requeued.status).toBe('QUEUED');
  });
});

describe('Execution ownership protocol (requires PostgreSQL)', () => {
  it('should reject reclaim when claim has not expired (live owner theft)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'live-owner-theft',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Claim is still valid (not expired) — reclaim must be rejected
    await expect(
      repo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
    ).rejects.toThrow(ClaimNotExpiredError);

    // Task remains owned by worker-A
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.claimedBy).toBe('worker-A');
    expect(dbTask!.claimToken).toBe(processing.claimToken);
  });

  it('should allow reclaim only after claim expires', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'expired-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Claim not expired — reclaim rejected
    await expect(
      repo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
    ).rejects.toThrow(ClaimNotExpiredError);

    // Expire the claim
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    // Now reclaim succeeds
    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version, 'worker-B');
    expect(reclaimed.claimedBy).toBe('worker-B');
    expect(reclaimed.claimToken).toBeDefined();
    expect(reclaimed.claimToken).not.toBe(processing.claimToken);
    expect(reclaimed.claimExpiresAt).toBeDefined();
    expect(reclaimed.claimExpiresAt!.getTime()).toBeGreaterThan(Date.now() - 1000);
  });

  it('should reject stale completion after ownership transfer (claim token mismatch)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'stale-completion',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processingA = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Expire claim and reclaim by worker B
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );
    const processingB = await repo.reclaimStalledTask(task.id, processingA.version, 'worker-B');

    // Worker A tries to complete with its old claim token but version 3
    // (hypothetically if it had the right version somehow)
    await expect(
      repo.transitionStatus(task.id, processingB.version, 'COMPLETED', {
        completedAt: new Date(),
        claimToken: processingA.claimToken,
      }),
    ).rejects.toThrow(ClaimTokenMismatchError);

    // Task remains PROCESSING owned by worker-B
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimedBy).toBe('worker-B');
    expect(dbTask!.claimToken).toBe(processingB.claimToken);
  });

  it('should reject stale failure transition after ownership transfer', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'stale-failure',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processingA = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Expire claim and reclaim by worker B
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );
    const processingB = await repo.reclaimStalledTask(task.id, processingA.version, 'worker-B');

    // Worker A tries to fail the task with its old claim token
    await expect(
      repo.transitionStatus(task.id, processingB.version, 'FAILED', {
        error: 'worker-A failed',
        claimToken: processingA.claimToken,
      }),
    ).rejects.toThrow(ClaimTokenMismatchError);

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimedBy).toBe('worker-B');
  });

  it('should allow current owner to complete with correct claim token', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'owner-completes',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const completed = await repo.transitionStatus(task.id, processing.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { output: 'success' },
      claimToken: processing.claimToken,
    });

    expect(completed.status).toBe('COMPLETED');
    expect(completed.claimToken).toBeUndefined();
    expect(completed.claimExpiresAt).toBeUndefined();
    expect(completed.claimedBy).toBeUndefined();
  });

  it('should allow cancellation to override ownership (no claim token required)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'cancel-beats-owner',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Cancel without providing claim token — cancellation overrides ownership
    const cancelled = await repo.cancelTask(task.id);
    expect(cancelled.status).toBe('CANCELLED');
    expect(cancelled.claimToken).toBeUndefined();
    expect(cancelled.claimExpiresAt).toBeUndefined();
    expect(cancelled.claimedBy).toBeUndefined();

    // Worker A can no longer complete with its claim token
    await expect(
      repo.transitionStatus(task.id, processing.version, 'COMPLETED', {
        completedAt: new Date(),
        claimToken: processing.claimToken,
      }),
    ).rejects.toThrow(StaleVersionError);
  });

  it('should ensure only one worker wins concurrent initial claim (QUEUED -> PROCESSING)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'concurrent-claim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const results = await Promise.allSettled([
      repo.transitionStatus(task.id, 1, 'PROCESSING', {
        startedAt: new Date(),
        claimedBy: 'worker-A',
      }),
      repo.transitionStatus(task.id, 1, 'PROCESSING', {
        startedAt: new Date(),
        claimedBy: 'worker-B',
      }),
    ]);

    const fulfilled = results.filter(r => r.status === 'fulfilled');
    const rejected = results.filter(r => r.status === 'rejected');

    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);

    const winner = (fulfilled[0] as PromiseFulfilledResult<typeof task>).value;
    expect(winner.claimToken).toBeDefined();
    expect(winner.claimExpiresAt).toBeDefined();

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimToken).toBe(winner.claimToken);
  });

  it('should reject transition without claim token when task has one', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'no-token-reject',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Try to complete without providing claim token
    await expect(
      repo.transitionStatus(task.id, 2, 'COMPLETED', {
        completedAt: new Date(),
      }),
    ).rejects.toThrow(ClaimTokenMismatchError);
  });
});

describe('Worker lifecycle integration (requires PostgreSQL)', () => {
  it('should simulate full worker lifecycle with ownership protocol', async () => {
    requirePg();

    // 1. Create task
    const { task } = await repo.createTaskWithOutbox({
      name: 'lifecycle-test',
      priority: 'NORMAL',
      payload: { input: 'data' },
      maxRetries: 3,
    });
    expect(task.status).toBe('QUEUED');
    expect(task.claimToken).toBeUndefined();

    // 2. Worker A claims task (QUEUED -> PROCESSING)
    const claimed = await repo.transitionStatus(task.id, task.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });
    expect(claimed.status).toBe('PROCESSING');
    expect(claimed.claimToken).toBeDefined();
    expect(claimed.claimedBy).toBe('worker-A');
    const tokenA = claimed.claimToken!;

    // 3. Worker A completes with its claim token
    const completed = await repo.transitionStatus(task.id, claimed.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-A' },
      claimToken: tokenA,
    });
    expect(completed.status).toBe('COMPLETED');
    expect(completed.claimToken).toBeUndefined();
    expect(completed.claimedBy).toBeUndefined();
  });

  it('should simulate stalled worker recovery with ownership transfer', async () => {
    requirePg();

    // 1. Create and claim
    const { task } = await repo.createTaskWithOutbox({
      name: 'stalled-lifecycle',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const claimedA = await repo.transitionStatus(task.id, task.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });
    const tokenA = claimedA.claimToken!;
    const versionA = claimedA.version;

    // 2. Worker A crashes — claim expires
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    // 3. Worker B reclaims
    const claimedB = await repo.reclaimStalledTask(task.id, versionA, 'worker-B');
    expect(claimedB.claimedBy).toBe('worker-B');
    expect(claimedB.claimToken).not.toBe(tokenA);
    const tokenB = claimedB.claimToken!;

    // 4. Worker A wakes up and tries to fail the task — REJECTED
    await expect(
      repo.transitionStatus(task.id, versionA, 'FAILED', {
        error: 'worker-A late failure',
        claimToken: tokenA,
      }),
    ).rejects.toThrow(); // StaleVersionError (version changed)

    // 5. Worker B completes successfully with its token
    const completed = await repo.transitionStatus(task.id, claimedB.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-B' },
      claimToken: tokenB,
    });
    expect(completed.status).toBe('COMPLETED');
  });

  it('should simulate failure-retry lifecycle with claim token', async () => {
    requirePg();

    // 1. Create and claim
    const { task } = await repo.createTaskWithOutbox({
      name: 'retry-lifecycle',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const claimed = await repo.transitionStatus(task.id, task.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // 2. Worker A fails (intermediate) — requeues with claim token
    const requeued = await repo.transitionStatus(task.id, claimed.version, 'QUEUED', {
      error: 'temporary error',
      retries: 1,
      claimToken: claimed.claimToken,
    });
    expect(requeued.status).toBe('QUEUED');
    expect(requeued.claimToken).toBeUndefined();

    // 3. Worker B picks up retried task
    const claimedB = await repo.transitionStatus(task.id, requeued.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-B',
    });
    expect(claimedB.claimToken).toBeDefined();

    // 4. Worker B completes
    const completed = await repo.transitionStatus(task.id, claimedB.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-B' },
      claimToken: claimedB.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });
});

describe('Early BullMQ redelivery before lease expiry (requires PostgreSQL)', () => {
  it('should throw ClaimNotExpiredError when reclaim attempted before lease expires', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'early-redelivery',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Worker A claims task
    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // BullMQ redelivers before lease expires — worker B tries to reclaim
    const reclaimErr = await repo.reclaimStalledTask(task.id, processing.version, 'worker-B')
      .catch((e: unknown) => e);
    expect(reclaimErr).toBeInstanceOf(ClaimNotExpiredError);

    // Task remains owned by worker-A, version unchanged
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimedBy).toBe('worker-A');
    expect(dbTask!.claimToken).toBe(processing.claimToken);
    expect(dbTask!.version).toBe(processing.version);
  });

  it('should allow reclaim after lease expires following earlier rejection', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'deferred-reclaim',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // First attempt: lease not expired — rejected
    await expect(
      repo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
    ).rejects.toThrow(ClaimNotExpiredError);

    // Expire the lease
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    // Second attempt: lease expired — succeeds
    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version, 'worker-B');
    expect(reclaimed.claimedBy).toBe('worker-B');
    expect(reclaimed.claimToken).toBeDefined();
    expect(reclaimed.claimToken).not.toBe(processing.claimToken);

    // Worker B can complete
    const completed = await repo.transitionStatus(task.id, reclaimed.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-B' },
      claimToken: reclaimed.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });

  it('should not consume version when reclaim is rejected (task remains reclaimable later)', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'version-stable',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Multiple rejected reclaim attempts should not change version
    for (let i = 0; i < 3; i++) {
      await expect(
        repo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
      ).rejects.toThrow(ClaimNotExpiredError);
    }

    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.version).toBe(processing.version);
  });
});

describe('Lease renewal (requires PostgreSQL)', () => {
  it('should renew claim with valid token', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-valid',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const originalExpiry = processing.claimExpiresAt!;

    // Small delay so renewed expiry is measurably later
    await new Promise(r => setTimeout(r, 50));

    const renewed = await repo.renewClaim(task.id, processing.claimToken!);
    expect(renewed.status).toBe('PROCESSING');
    expect(renewed.claimToken).toBe(processing.claimToken);
    expect(renewed.claimExpiresAt).toBeDefined();
    expect(renewed.claimExpiresAt!.getTime()).toBeGreaterThan(originalExpiry.getTime());
  });

  it('should reject renewal with wrong claim token', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-wrong-token',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await expect(
      repo.renewClaim(task.id, 'wrong-token'),
    ).rejects.toThrow(ClaimTokenMismatchError);
  });

  it('should reject renewal with stale token after ownership transfer', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-stale-token',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processingA = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Expire claim and let worker B reclaim
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );
    await repo.reclaimStalledTask(task.id, processingA.version, 'worker-B');

    // Worker A tries to renew with its old token
    await expect(
      repo.renewClaim(task.id, processingA.claimToken!),
    ).rejects.toThrow(ClaimTokenMismatchError);
  });

  it('should reject renewal on CANCELLED task', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-cancelled',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await repo.cancelTask(task.id);

    await expect(
      repo.renewClaim(task.id, processing.claimToken!),
    ).rejects.toThrow('status is CANCELLED, expected PROCESSING');
  });

  it('should reject renewal on COMPLETED task', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-completed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await repo.transitionStatus(task.id, processing.version, 'COMPLETED', {
      completedAt: new Date(),
      claimToken: processing.claimToken,
    });

    await expect(
      repo.renewClaim(task.id, processing.claimToken!),
    ).rejects.toThrow('status is COMPLETED, expected PROCESSING');
  });

  it('should reject renewal on FAILED task', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-failed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    await repo.transitionStatus(task.id, processing.version, 'FAILED', {
      error: 'test failure',
      claimToken: processing.claimToken,
    });

    await expect(
      repo.renewClaim(task.id, processing.claimToken!),
    ).rejects.toThrow('status is FAILED, expected PROCESSING');
  });

  it('should extend expiry from current PG time, not from original expiry', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'renew-extends-from-now',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Wait a bit, then renew
    await new Promise(r => setTimeout(r, 100));

    const renewed = await repo.renewClaim(task.id, processing.claimToken!);
    const newExpiry = renewed.claimExpiresAt!.getTime();
    const originalExpiry = processing.claimExpiresAt!.getTime();

    // New expiry should be later than original by roughly the wait time
    expect(newExpiry).toBeGreaterThan(originalExpiry);
    // New expiry should be approximately NOW + claimTtl (30s), not original + 30s
    const expectedMin = Date.now() + repo.claimTtl - 5000;
    expect(newExpiry).toBeGreaterThan(expectedMin);
  });

  it('should reject renewal on non-existent task', async () => {
    requirePg();

    await expect(
      repo.renewClaim('nonexistent-id', 'some-token'),
    ).rejects.toThrow('not found');
  });
});

describe('Long-running worker with lease renewal (requires PostgreSQL)', () => {
  it('should prevent reclaim while renewal keeps lease fresh', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'long-running-renewed',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Worker A claims task
    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Simulate renewal — extends lease
    const renewed = await repo.renewClaim(task.id, processing.claimToken!);
    expect(renewed.claimExpiresAt!.getTime()).toBeGreaterThan(processing.claimExpiresAt!.getTime() - 100);

    // Worker B cannot reclaim while lease is fresh
    await expect(
      repo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
    ).rejects.toThrow(ClaimNotExpiredError);

    // Worker A completes successfully
    const completed = await repo.transitionStatus(task.id, processing.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-A' },
      claimToken: processing.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });

  it('should allow reclaim only after renewed lease expires', async () => {
    requirePg();

    const shortTtlRepo = new TaskRepository(pool, 100);

    const { task } = await repo.createTaskWithOutbox({
      name: 'renewed-then-expired',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Worker A claims with short TTL
    const processing = await shortTtlRepo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Renew the lease
    await shortTtlRepo.renewClaim(task.id, processing.claimToken!);

    // Still can't reclaim (lease is fresh from renewal)
    await expect(
      shortTtlRepo.reclaimStalledTask(task.id, processing.version, 'worker-B'),
    ).rejects.toThrow(ClaimNotExpiredError);

    // Expire the renewed lease
    await pool.query(
      `UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`,
      [task.id],
    );

    // Now reclaim succeeds
    const reclaimed = await shortTtlRepo.reclaimStalledTask(task.id, processing.version, 'worker-B');
    expect(reclaimed.claimedBy).toBe('worker-B');
    expect(reclaimed.claimToken).not.toBe(processing.claimToken);
  });
});

describe('Retry and failure interactions (requires PostgreSQL)', () => {
  it('should distinguish execution failure from stalled redelivery', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'failure-vs-stall',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    // Worker A claims
    const claimed = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Worker A fails the task (intermediate) — requeues
    const requeued = await repo.transitionStatus(task.id, claimed.version, 'QUEUED', {
      error: 'transient error',
      retries: 1,
      claimToken: claimed.claimToken,
    });
    expect(requeued.status).toBe('QUEUED');
    expect(requeued.claimToken).toBeUndefined();
    expect(requeued.retries).toBe(1);

    // BullMQ retries — worker B picks up from QUEUED
    const claimedB = await repo.transitionStatus(task.id, requeued.version, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-B',
    });
    expect(claimedB.claimToken).toBeDefined();

    // Worker B completes
    const completed = await repo.transitionStatus(task.id, claimedB.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-B' },
      claimToken: claimedB.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
    expect(completed.retries).toBe(1);
  });

  it('should handle final failure correctly', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'final-failure',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 1,
    });

    const claimed = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    const failed = await repo.transitionStatus(task.id, claimed.version, 'FAILED', {
      error: 'permanent error',
      retries: 1,
      claimToken: claimed.claimToken,
    });
    expect(failed.status).toBe('FAILED');
    expect(failed.error).toBe('permanent error');
    expect(failed.retries).toBe(1);
    expect(failed.claimToken).toBeUndefined();
    expect(failed.claimedBy).toBeUndefined();
  });

  it('should not confuse temporarily non-reclaimable with permanent failure', async () => {
    requirePg();

    const { task } = await repo.createTaskWithOutbox({
      name: 'temp-non-reclaimable',
      priority: 'NORMAL',
      payload: {},
      maxRetries: 3,
    });

    const processing = await repo.transitionStatus(task.id, 1, 'PROCESSING', {
      startedAt: new Date(),
      claimedBy: 'worker-A',
    });

    // Reclaim rejected — lease still valid
    const err = await repo.reclaimStalledTask(task.id, processing.version, 'worker-B')
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ClaimNotExpiredError);

    // Task is NOT failed — still PROCESSING
    const dbTask = await repo.getTask(task.id);
    expect(dbTask!.status).toBe('PROCESSING');
    expect(dbTask!.claimedBy).toBe('worker-A');

    // Worker A can still complete
    const completed = await repo.transitionStatus(task.id, processing.version, 'COMPLETED', {
      completedAt: new Date(),
      result: { processedBy: 'worker-A' },
      claimToken: processing.claimToken,
    });
    expect(completed.status).toBe('COMPLETED');
  });
});
