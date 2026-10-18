import { Queue } from 'bullmq';
import { Counter, Gauge, Registry, collectDefaultMetrics } from 'prom-client';
import { TaskRepository, OutboxPublisher } from '@repo/shared';
import { settleWithin } from './dependency-check.js';

/** BullMQ states that make up the queue's depth and activity. */
const QUEUE_STATES = ['waiting', 'prioritized', 'delayed', 'active', 'failed'] as const;
const CIRCUIT_STATES = ['CLOSED', 'OPEN', 'HALF_OPEN'] as const;

/** PostgreSQL did not answer a metrics query within the budget. */
export class PostgresTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`PostgreSQL did not answer within ${timeoutMs} ms`);
    this.name = 'PostgresTimeoutError';
  }
}

export class MetricsCollector {
  private requestCount = 0;
  private errorCount = 0;
  private startTime = Date.now();

  // Prometheus view of the same numbers. Gauges are filled on each scrape
  // from PostgreSQL and Redis, so every API replica reports the same
  // engine-wide values: aggregate them with max(), not sum(). The HTTP
  // counters and process metrics are per replica.
  private readonly registry = new Registry();
  private readonly requestsTotal = new Counter({ name: 'engine_http_requests_total', help: 'HTTP requests handled by this API process.', registers: [this.registry] });
  private readonly errorsTotal = new Counter({ name: 'engine_http_errors_total', help: 'HTTP requests that reached the error handler in this API process.', registers: [this.registry] });
  private readonly tasks = new Gauge({ name: 'engine_tasks', help: 'Tasks in PostgreSQL by status.', labelNames: ['status'], registers: [this.registry] });
  private readonly queueUp = new Gauge({ name: 'engine_queue_up', help: '1 when Redis answered the queue counts within the budget, else 0.', registers: [this.registry] });
  private readonly queueJobs = new Gauge({ name: 'engine_queue_jobs', help: 'BullMQ jobs by state. Absent while engine_queue_up is 0.', labelNames: ['state'], registers: [this.registry] });
  private readonly outboxUp = new Gauge({ name: 'engine_outbox_up', help: '1 when the outbox could be read, else 0.', registers: [this.registry] });
  private readonly outboxEvents = new Gauge({ name: 'engine_outbox_events', help: 'Outbox events by status. Absent while engine_outbox_up is 0.', labelNames: ['status'], registers: [this.registry] });
  // Labelled only so it can be absent: prom-client always emits an unlabelled
  // gauge, which would read as "no lag" exactly when the outbox is unreadable.
  private readonly outboxLag = new Gauge({ name: 'engine_outbox_oldest_event_age_seconds', help: 'Age of the oldest outbox event in a status (DB clock); 0 when none. Absent while engine_outbox_up is 0.', labelNames: ['status'], registers: [this.registry] });
  private readonly circuit = new Gauge({ name: 'engine_outbox_circuit_state', help: "1 for this process's outbox publisher circuit state, 0 for the others.", labelNames: ['state'], registers: [this.registry] });

  constructor(
    private taskRepo: TaskRepository,
    private taskQueue: Queue,
    private outboxPublisher: OutboxPublisher,
    /**
     * Budget for every dependency call. Both clients wait instead of failing
     * while their server is unreachable (see dependency-check.ts), so
     * without it the endpoints hang exactly when someone is looking.
     */
    private timeoutMs = 1000,
  ) {
    collectDefaultMetrics({ register: this.registry, prefix: 'engine_api_' });
  }

  /** A PostgreSQL call bounded by the budget; errors pass through unchanged. */
  private pg<T>(query: Promise<T>): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new PostgresTimeoutError(this.timeoutMs)), this.timeoutMs);
    });
    return Promise.race([query, timeout]).finally(() => clearTimeout(timer));
  }

  recordRequest() {
    this.requestCount++;
    this.requestsTotal.inc();
  }

  recordError() {
    this.errorCount++;
    this.errorsTotal.inc();
  }

  async getMetrics() {
    const [statusCounts, totalTasks] = await this.pg(Promise.all([this.taskRepo.getTaskStatusCounts(), this.taskRepo.getTaskCount()]));

    // null (with available: false) means "Redis did not answer in time",
    // which a dashboard must not show as an empty queue.
    const queueCounts = await settleWithin(
      Promise.all([this.taskQueue.count(), this.taskQueue.getActiveCount(), this.taskQueue.getWaitingCount()]),
      this.timeoutMs,
    );
    const [queueTotal, activeCount, waitingCount] = queueCounts.ok ? queueCounts.value : [null, null, null];

    let outboxPending = 0;
    let outboxFailed = 0;
    try {
      [outboxPending, outboxFailed] = await this.pg(Promise.all([this.outboxPublisher.getPendingCount(), this.outboxPublisher.getFailedCount()]));
    } catch {
      // Outbox metrics unavailable
    }

    const uptime = Math.floor((Date.now() - this.startTime) / 1000);

    return {
      timestamp: new Date().toISOString(),
      uptime,
      requests: {
        total: this.requestCount,
        errors: this.errorCount,
        errorRate: this.requestCount > 0 ? (this.errorCount / this.requestCount * 100).toFixed(2) : '0.00',
      },
      tasks: {
        total: totalTasks,
        byStatus: statusCounts,
      },
      queue: {
        available: queueCounts.ok,
        total: queueTotal,
        active: activeCount,
        waiting: waitingCount,
      },
      outbox: {
        pending: outboxPending,
        failed: outboxFailed,
      },
    };
  }

  /**
   * Prometheus text exposition. PostgreSQL failing or not answering within
   * the budget fails the scrape (503, so Prometheus records up=0 at once); Redis or the outbox
   * failing does not: their *_up gauge goes to 0 and their series are
   * dropped, so a graph shows a gap rather than a false zero.
   */
  async prometheus(): Promise<{ contentType: string; body: string }> {
    const statusCounts = await this.pg(this.taskRepo.getTaskStatusCounts());
    this.tasks.reset();
    for (const [status, count] of Object.entries(statusCounts)) this.tasks.set({ status }, count);

    const jobCounts = await settleWithin(this.taskQueue.getJobCounts(...QUEUE_STATES), this.timeoutMs);
    this.queueJobs.reset();
    this.queueUp.set(jobCounts.ok ? 1 : 0);
    if (jobCounts.ok) {
      for (const state of QUEUE_STATES) this.queueJobs.set({ state }, jobCounts.value[state] ?? 0);
    }

    this.outboxEvents.reset();
    this.outboxLag.reset();
    try {
      const stats = await this.pg(this.outboxPublisher.getStats());
      this.outboxEvents.set({ status: 'pending' }, stats.pending);
      this.outboxEvents.set({ status: 'failed' }, stats.failed);
      this.outboxLag.set({ status: 'pending' }, stats.oldestPendingAgeSeconds);
      this.outboxUp.set(1);
    } catch {
      this.outboxUp.set(0);
    }

    const current = this.outboxPublisher.getCircuitState();
    for (const state of CIRCUIT_STATES) this.circuit.set({ state }, state === current ? 1 : 0);

    return { contentType: this.registry.contentType, body: await this.registry.metrics() };
  }
}
