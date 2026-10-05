import { Queue } from 'bullmq';
import { Task, TaskStatus } from '@repo/shared';

// Metrics collector for monitoring system health
export class MetricsCollector {
  private tasksByStatus = new Map<TaskStatus, number>();
  private requestCount = 0;
  private errorCount = 0;
  private startTime = Date.now();

  constructor(private taskStore: Map<string, Task>, private taskQueue: Queue) {
    this.initializeStatusMap();
  }

  private initializeStatusMap() {
    const statuses: TaskStatus[] = ['PENDING', 'QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'];
    statuses.forEach((status) => this.tasksByStatus.set(status, 0));
  }

  // Increment request counter
  recordRequest() {
    this.requestCount++;
  }

  // Record error
  recordError() {
    this.errorCount++;
  }

  // Get current metrics
  async getMetrics() {
    // Recalculate status counts from task store
    this.tasksByStatus.forEach((_, status) => this.tasksByStatus.set(status, 0));

    for (const task of this.taskStore.values()) {
      const current = this.tasksByStatus.get(task.status) || 0;
      this.tasksByStatus.set(task.status, current + 1);
    }

    // Get queue depth
    const queueCount = await this.taskQueue.count();
    const activeCount = await this.taskQueue.getActiveCount();
    const waitingCount = await this.taskQueue.getWaitingCount();

    // Calculate uptime
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
        total: this.taskStore.size,
        byStatus: Object.fromEntries(this.tasksByStatus),
      },
      queue: {
        total: queueCount,
        active: activeCount,
        waiting: waitingCount,
      },
    };
  }

  // Get task completion rate
  getCompletionRate() {
    const completed = this.tasksByStatus.get('COMPLETED') || 0;
    const total = this.taskStore.size;
    return total > 0 ? (completed / total * 100).toFixed(2) : '0.00';
  }

  // Get failure rate
  getFailureRate() {
    const failed = this.tasksByStatus.get('FAILED') || 0;
    const total = this.taskStore.size;
    return total > 0 ? (failed / total * 100).toFixed(2) : '0.00';
  }
}
