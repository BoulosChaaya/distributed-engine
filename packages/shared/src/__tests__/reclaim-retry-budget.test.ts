import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import { Pool } from 'pg';
import { runMigrations } from '../db/migrations';
import { TaskRepository, RetryBudgetExhaustedError } from '../db/task-repository';
import { WorkflowRepository } from '../db/workflow-repository';

// A task whose worker dies mid-run (lease expired) is reclaimed by another
// worker. The lost run must count against the retry budget, or a task that
// crashes its worker every time is never bounded by maxRetries.
const TEST_PG_URL = process.env.TEST_POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5432/distributed_engine_test';

let pool: Pool;
let repo: TaskRepository;

beforeAll(async () => {
  pool = new Pool({ connectionString: TEST_PG_URL });
  await runMigrations(pool);
  repo = new TaskRepository(pool);
});

afterAll(async () => {
  await pool.end();
});

async function processing(taskId: string, version: number, worker: string) {
  const t = await repo.transitionStatus(taskId, version, 'PROCESSING', { startedAt: new Date(), claimedBy: worker });
  return t;
}

async function expireLease(taskId: string) {
  await pool.query(`UPDATE tasks SET claim_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, [taskId]);
}

async function status(taskId: string) {
  return (await pool.query('SELECT status, retries, error FROM tasks WHERE id = $1', [taskId])).rows[0];
}

describe('reclaiming a stalled task charges the lost attempt (requires PostgreSQL)', () => {
  it('charges one retry per reclaim and says why', async () => {
    const { task } = await repo.createTaskWithOutbox({ name: 'sleep', priority: 'NORMAL', payload: {}, maxRetries: 3 });
    const p = await processing(task.id, task.version, 'w-dead');
    await expireLease(task.id);

    const reclaimed = await repo.reclaimStalledTask(task.id, p.version, 'w-new');

    expect(reclaimed.status).toBe('PROCESSING');
    expect(reclaimed.retries).toBe(1);
    expect(reclaimed.error).toBe('Worker lost the task lease; attempt 1 charged and the task reclaimed');
  });

  it('fails the task instead of reclaiming it once the budget is spent', async () => {
    const { task } = await repo.createTaskWithOutbox({ name: 'sleep', priority: 'NORMAL', payload: {}, maxRetries: 1 });
    let t = await processing(task.id, task.version, 'w1');
    await expireLease(task.id);
    t = await repo.reclaimStalledTask(task.id, t.version, 'w2'); // lost run 1: retries 1 of 1
    await expireLease(task.id);

    const err = await repo.reclaimStalledTask(task.id, t.version, 'w3').catch((e) => e); // lost run 2
    expect(err).toBeInstanceOf(RetryBudgetExhaustedError);
    expect(await status(task.id)).toEqual({
      status: 'FAILED',
      retries: 2,
      error: 'Worker lost the task lease and the retry budget is exhausted',
    });
    const claim = (await pool.query('SELECT claimed_by, claim_token FROM tasks WHERE id = $1', [task.id])).rows[0];
    expect(claim).toEqual({ claimed_by: null, claim_token: null });
  });

  it('a failed workflow step cancels its downstream, like any other failure', async () => {
    const wf = await new WorkflowRepository(pool).createWorkflow({
      name: 'poison-wf',
      steps: [{ key: 'a', name: 'sleep', maxRetries: 0 }, { key: 'b', name: 'echo', dependsOn: ['a'] }],
    });
    const a = wf.detail.steps.find((s) => s.key === 'a')!.taskId;
    const b = wf.detail.steps.find((s) => s.key === 'b')!.taskId;
    const v = (await pool.query('SELECT version FROM tasks WHERE id = $1', [a])).rows[0].version;
    const t = await processing(a, v, 'w1');
    await expireLease(a);

    await expect(repo.reclaimStalledTask(a, t.version, 'w2')).rejects.toBeInstanceOf(RetryBudgetExhaustedError);

    expect((await status(a)).status).toBe('FAILED');
    expect((await status(b)).status).toBe('CANCELLED');
    const w = (await pool.query('SELECT status FROM workflows WHERE id = $1', [wf.detail.workflow.id])).rows[0];
    expect(w.status).toBe('FAILED');
  });
});
