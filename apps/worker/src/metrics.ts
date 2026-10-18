import { Counter, Registry, collectDefaultMetrics } from 'prom-client';

export interface WorkerCounts {
  jobsProcessed: number;
  jobsCompleted: number;
  jobsFailed: number;
}

/**
 * Prometheus registry for one worker process, served on the health port at
 * /metrics. The job counters read the same counts the heartbeat publishes,
 * at scrape time, so there is one source for them. They reset when the
 * process restarts, which rate() handles. Process metrics (event loop lag,
 * heap, CPU) are what show a worker that is alive but stuck.
 */
export function createWorkerRegistry(counts: WorkerCounts, workerId: string): Registry {
  const registry = new Registry();
  registry.setDefaultLabels({ worker_id: workerId });
  collectDefaultMetrics({ register: registry, prefix: 'engine_worker_' });
  new Counter({
    name: 'engine_worker_jobs_total',
    help: 'Jobs this worker process received (processed), finished (completed) or failed.',
    labelNames: ['outcome'],
    registers: [registry],
    collect() {
      this.reset();
      const values: Record<string, number> = { processed: counts.jobsProcessed, completed: counts.jobsCompleted, failed: counts.jobsFailed };
      for (const [outcome, value] of Object.entries(values)) this.inc({ outcome }, value);
    },
  });
  return registry;
}
