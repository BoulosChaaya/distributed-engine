import { describe, it, expect } from '@jest/globals';
import { createWorkerRegistry } from '../metrics';

describe('createWorkerRegistry', () => {
  it('reads the live job counts at scrape time, labelled with the worker id', async () => {
    const counts = { jobsProcessed: 0, jobsCompleted: 0, jobsFailed: 0 };
    const registry = createWorkerRegistry(counts, 'w-1');
    counts.jobsProcessed = 5;
    counts.jobsCompleted = 3;
    counts.jobsFailed = 2;
    const body = await registry.metrics();
    expect(body).toContain('engine_worker_jobs_total{outcome="processed",worker_id="w-1"} 5');
    expect(body).toContain('engine_worker_jobs_total{outcome="completed",worker_id="w-1"} 3');
    expect(body).toContain('engine_worker_jobs_total{outcome="failed",worker_id="w-1"} 2');
    expect(body).toContain('engine_worker_nodejs_eventloop_lag_seconds');

    counts.jobsCompleted = 4;
    expect(await registry.metrics()).toContain('engine_worker_jobs_total{outcome="completed",worker_id="w-1"} 4');
  });
});
