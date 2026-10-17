import { Queue } from 'bullmq';
import { TaskRepository, OutboxPublisher } from '@repo/shared';
import { settleWithin } from './dependency-check.js';

export class MetricsCollector {
  private requestCount = 0;
  private errorCount = 0;
  private startTime = Date.now();

  constructor(
    private taskRepo: TaskRepository,
    private taskQueue: Queue,
    private outboxPublisher: OutboxPublisher,
    /** Budget for the Redis queue counts; they hang while Redis is down. */
    private redisTimeoutMs = 1000,
  ) {}

  recordRequest() {
    this.requestCount++;
  }

  recordError() {
    this.errorCount++;
  }

  async getMetrics() {
    const statusCounts = await this.taskRepo.getTaskStatusCounts();
    const totalTasks = await this.taskRepo.getTaskCount();

    // null (with available: false) means "Redis did not answer in time",
    // which a dashboard must not show as an empty queue.
    const queueCounts = await settleWithin(
      Promise.all([this.taskQueue.count(), this.taskQueue.getActiveCount(), this.taskQueue.getWaitingCount()]),
      this.redisTimeoutMs,
    );
    const [queueTotal, activeCount, waitingCount] = queueCounts.ok ? queueCounts.value : [null, null, null];

    let outboxPending = 0;
    let outboxFailed = 0;
    try {
      outboxPending = await this.outboxPublisher.getPendingCount();
      outboxFailed = await this.outboxPublisher.getFailedCount();
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
}
