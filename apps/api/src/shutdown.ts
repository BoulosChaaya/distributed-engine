import { Queue } from 'bullmq';
import { Pool } from 'pg';
import { OutboxPublisher, SchedulerService, shutdownTelemetry, createLogger, type Logger } from '@repo/shared';
import IORedis from 'ioredis';
import type { Server } from 'http';

const logger: Logger = createLogger({
  service: 'api',
  environment: process.env.NODE_ENV ?? 'development',
  level: process.env.LOG_LEVEL,
});

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
    server: Server,
    deps: {
      taskQueue: Queue;
      redisClient: IORedis;
      pgPool: Pool;
      outboxPublisher: OutboxPublisher;
      schedulerService?: SchedulerService;
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
    server: Server,
    deps: {
      taskQueue: Queue;
      redisClient: IORedis;
      pgPool: Pool;
      outboxPublisher: OutboxPublisher;
      schedulerService?: SchedulerService;
    },
    signal: string,
  ) {
    if (this.isShuttingDown) {
      logger.warn('Shutdown already in progress, forcing exit', { signal });
      process.exit(1);
    }

    this.isShuttingDown = true;
    logger.info('Graceful shutdown initiated', { signal, activeRequests: this.activeRequests });

    server.close(() => {
      logger.info('HTTP server closed');
    });

    if (deps.schedulerService) {
      await deps.schedulerService.stop();
      logger.info('Scheduler stopped');
    }

    await deps.outboxPublisher.stop();

    const drainStart = Date.now();
    let timedOut = false;
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (this.activeRequests === 0 || Date.now() - drainStart > this.shutdownTimeout) {
          clearInterval(check);
          if (this.activeRequests > 0) {
            logger.warn('Shutdown timeout, forcing close', { activeRequests: this.activeRequests });
            timedOut = true;
          }
          resolve();
        }
      }, 100);
    });

    await this.closeConnections(deps);
    await shutdownTelemetry();
    process.exit(timedOut ? 1 : 0);
  }

  private async closeConnections(deps: {
    taskQueue: Queue;
    redisClient: IORedis;
    pgPool: Pool;
  }) {
    logger.info('Closing connections');

    try {
      await deps.taskQueue.close();
      logger.info('Task queue closed');
    } catch (error) {
      logger.warn('Error closing task queue', { reason: String(error) });
    }

    try {
      deps.redisClient.disconnect();
      logger.info('Redis connection closed');
    } catch (error) {
      logger.warn('Error closing Redis', { reason: String(error) });
    }

    try {
      await deps.pgPool.end();
      logger.info('PostgreSQL pool closed');
    } catch (error) {
      logger.warn('Error closing PostgreSQL pool', { reason: String(error) });
    }

    logger.info('Graceful shutdown complete');
  }
}
