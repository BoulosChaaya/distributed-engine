import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { Pool } from 'pg';
import { TaskRepository, StaleVersionError } from '../db/task-repository';
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

    const completed = await repo.transitionStatus(task.id, 2, 'COMPLETED', {
      completedAt: new Date(),
      result: { output: 'done' },
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

    await repo.transitionStatus(task.id, 1, 'PROCESSING');
    await repo.transitionStatus(task.id, 2, 'COMPLETED', { completedAt: new Date() });

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

    await repo.transitionStatus(task.id, 1, 'PROCESSING');
    await repo.transitionStatus(task.id, 2, 'FAILED', { error: 'temporary error', retries: 1 });

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

    await repo.transitionStatus(task.id, 1, 'PROCESSING');

    const requeued = await repo.transitionStatus(task.id, 2, 'QUEUED', {
      error: 'intermediate failure',
      retries: 1,
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

  it('should reclaim a stalled PROCESSING task', async () => {
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

    const reclaimed = await repo.reclaimStalledTask(task.id, processing.version);
    expect(reclaimed.status).toBe('PROCESSING');
    expect(reclaimed.version).toBe(processing.version + 1);
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
