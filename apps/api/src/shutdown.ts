import { Queue } from 'bullmq';
import { Pool } from 'pg';
import { log, OutboxPublisher } from '@repo/shared';
import IORedis from 'ioredis';

export class ShutdownManager {
  private isShuttingDown = false;
  private activeRequests = 0;

  constructor(
    private shutdownTimeout: number = 30000,
  ) {}

  get shuttingDown(): boolean {
    return this.isShuttingDown;
  }

  registerHandlers(
    server: NodeJS.Server,
    deps: {
      taskQueue: Queue;
      redisClient: IORedis;
      pgPool: Pool;
      outboxPublisher: OutboxPublisher;
    },
  ) {
    const shutdown = (signal: string) => this.shutdown(server, deps, signal);
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
  }

  incrementRequests() {
    if (this.isShuttingDown) {
      throw new Error('Server is shutting down');
    }
    this.activeRequests++;
  }

  decrementRequests() {
    this.activeRequests--;
  }

  private async shutdown(
    server: NodeJS.Server,
    deps: {
      taskQueue: Queue;
      redisClient: IORedis;
      pgPool: Pool;
      outboxPublisher: OutboxPublisher;
    },
    signal: string,
  ) {
    if (this.isShuttingDown) {
      log('WARN', 'Shutdown already in progress, forcing exit', { signal });
      process.exit(1);
    }

    this.isShuttingDown = true;
    log('INFO', 'Graceful shutdown initiated', { signal, activeRequests: this.activeRequests });

    server.close(() => {
      log('INFO', 'HTTP server closed');
    });

    await deps.outboxPublisher.stop();

    const drainStart = Date.now();
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (this.activeRequests === 0 || Date.now() - drainStart > this.shutdownTimeout) {
          clearInterval(check);
          if (this.activeRequests > 0) {
            log('WARN', 'Shutdown timeout, forcing close', { activeRequests: this.activeRequests });
          }
          resolve();
        }
      }, 100);
    });

    await this.closeConnections(deps);
    process.exit(0);
  }

  private async closeConnections(deps: {
    taskQueue: Queue;
    redisClient: IORedis;
    pgPool: Pool;
  }) {
    log('INFO', 'Closing connections');

    try {
      await deps.taskQueue.close();
      log('INFO', 'Task queue closed');
    } catch (error) {
      log('WARN', 'Error closing task queue', { error: String(error) });
    }

    try {
      deps.redisClient.disconnect();
      log('INFO', 'Redis connection closed');
    } catch (error) {
      log('WARN', 'Error closing Redis', { error: String(error) });
    }

    try {
      await deps.pgPool.end();
      log('INFO', 'PostgreSQL pool closed');
    } catch (error) {
      log('WARN', 'Error closing PostgreSQL pool', { error: String(error) });
    }

    log('INFO', 'Graceful shutdown complete');
  }
}
