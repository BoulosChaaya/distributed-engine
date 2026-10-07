import { Queue } from 'bullmq';
import { TaskRepository, OutboxPublisher } from '@repo/shared';

export class MetricsCollector {
  private requestCount = 0;
  private errorCount = 0;
  private startTime = Date.now();

  constructor(
    private taskRepo: TaskRepository,
    private taskQueue: Queue,
    private outboxPublisher: OutboxPublisher,
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

    let queueTotal = 0;
    let activeCount = 0;
    let waitingCount = 0;
    try {
      queueTotal = await this.taskQueue.count();
      activeCount = await this.taskQueue.getActiveCount();
      waitingCount = await this.taskQueue.getWaitingCount();
    } catch {
      // Queue metrics unavailable
    }

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
