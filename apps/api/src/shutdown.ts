import { Express } from 'express';
import { Queue } from 'bullmq';
import { createClient, RedisClientType } from 'redis';
import { log } from '@repo/shared';

// Graceful shutdown manager for production
export class ShutdownManager {
  private isShuttingDown = false;
  private activeRequests = 0;

  constructor(
    private app: Express,
    private redisClient: RedisClientType,
    private taskQueue: Queue,
    private shutdownTimeout: number = 30000 // 30 second timeout
  ) {}

  // Register shutdown handlers (call this once after server start)
  registerHandlers(server: NodeJS.Server) {
    process.on('SIGTERM', () => this.shutdown(server, 'SIGTERM'));
    process.on('SIGINT', () => this.shutdown(server, 'SIGINT'));

    // Handle uncaught exceptions gracefully
    process.on('uncaughtException', (error) => {
      log('ERROR', 'Uncaught exception, initiating shutdown', {
        error: error.message,
        stack: error.stack
      });
      this.shutdown(server, 'uncaughtException');
    });

    // Handle unhandled promise rejections
    process.on('unhandledRejection', (reason) => {
      log('ERROR', 'Unhandled rejection, initiating shutdown', {
        reason: String(reason)
      });
      this.shutdown(server, 'unhandledRejection');
    });
  }

  // Track request lifecycle for graceful draining
  incrementRequests() {
    if (this.isShuttingDown) {
      throw new Error('Server is shutting down, no new requests accepted');
    }
    this.activeRequests++;
  }

  decrementRequests() {
    this.activeRequests--;
  }

  private async shutdown(server: NodeJS.Server, signal: string) {
    if (this.isShuttingDown) {
      log('WARN', 'Shutdown already in progress, forcing exit', { signal });
      process.exit(1);
    }

    this.isShuttingDown = true;
    log('INFO', 'Graceful shutdown initiated', { signal, activeRequests: this.activeRequests });

    // Stop accepting new requests
    server.close(() => {
      log('INFO', 'HTTP server closed, no longer accepting connections');
    });

    // Wait for active requests to drain (with timeout)
    const drainStart = Date.now();
    const drainCheck = setInterval(() => {
      const elapsed = Date.now() - drainStart;
      if (this.activeRequests === 0) {
        clearInterval(drainCheck);
        this.closeConnections();
      } else if (elapsed > this.shutdownTimeout) {
        clearInterval(drainCheck);
        log('WARN', 'Shutdown timeout exceeded, forcing close', {
          activeRequests: this.activeRequests,
          elapsed
        });
        this.closeConnections();
      }
    }, 100);
  }

  private async closeConnections() {
    log('INFO', 'Closing all connections');

    try {
      // Close the queue (which closes its internal connections)
      await this.taskQueue.close();
      log('INFO', 'Task queue closed');
    } catch (error) {
      log('WARN', 'Error closing task queue', { error: String(error) });
    }

    try {
      // Close Redis connection
      await this.redisClient.quit();
      log('INFO', 'Redis connection closed');
    } catch (error) {
      log('WARN', 'Error closing Redis connection', { error: String(error) });
    }

    log('INFO', 'Graceful shutdown complete');
    process.exit(0);
  }
}
