import { describe, it, expect, afterAll } from '@jest/globals';
import { Queue, Worker, UnrecoverableError } from 'bullmq';
import { buildDefaultJobOptions } from '../queue-options';

// Real Redis: failed jobs must not accumulate without bound.
const connection = {
  host: process.env.TEST_REDIS_HOST || 'localhost',
  port: Number(process.env.TEST_REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD,
  maxRetriesPerRequest: null,
};
const queues: Queue[] = [];

async function failJobs(name: string, defaultJobOptions: object, n: number): Promise<number> {
  const queue = new Queue(name, { connection, defaultJobOptions });
  queues.push(queue);
  await queue.obliterate({ force: true });
  let failed = 0;
  const worker = new Worker(name, async () => { throw new UnrecoverableError('upstream responded 404'); }, { connection });
  const done = new Promise<void>((resolve) => {
    worker.on('failed', () => { if (++failed === n) resolve(); });
  });
  for (let i = 0; i < n; i++) await queue.add('http', { i }, { jobId: `job-${i}` });
  await done;
  await worker.close();
  return queue.getFailedCount();
}

afterAll(async () => {
  for (const q of queues) { await q.obliterate({ force: true }); await q.close(); }
});

describe('task queue job retention (requires Redis)', () => {
  it('keeps every failed job forever with the old options (the problem)', async () => {
    expect(await failJobs('retention-old', { removeOnComplete: 100, removeOnFail: false }, 6)).toBe(6);
  });

  it('caps kept failed jobs at failedJobMaxCount', async () => {
    const opts = buildDefaultJobOptions({ failedJobRetentionSeconds: 86400, failedJobMaxCount: 3 });
    expect(await failJobs('retention-new', opts, 6)).toBe(3);
  });

  it('defaults: one day, at most 1000 failed jobs, 100 completed', () => {
    expect(buildDefaultJobOptions({ failedJobRetentionSeconds: 86400, failedJobMaxCount: 1000 })).toEqual({
      removeOnComplete: 100,
      removeOnFail: { age: 86400, count: 1000 },
    });
  });
});
