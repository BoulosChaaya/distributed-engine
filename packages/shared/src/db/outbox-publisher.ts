import { Pool } from 'pg';
import { Queue } from 'bullmq';
import { generateId } from '../utils';
import { OutboxEvent } from '../types';
import { tracing } from '../telemetry/spans';
import { TRACE_CONTEXT_KEY, injectTraceContext, extractTraceContext } from '../telemetry/propagation';
import { createLogger, type Logger } from '../logger/index';

const PRIORITY_MAP: Record<string, number> = {
  LOW: 10,
  NORMAL: 5,
  HIGH: 1,
  CRITICAL: 0,
};

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export class OutboxPublisher {
  private running = false;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private activePoll: Promise<number> | null = null;
  private readonly publisherId: string;
  private readonly logger: Logger;

  private circuitState: CircuitState = 'CLOSED';
  private consecutiveFailures = 0;
  private consecutiveSuccesses = 0;
  private lastFailureTime = 0;
  private readonly circuitFailureThreshold: number;
  private readonly circuitSuccessThreshold: number;
  private readonly circuitResetTimeoutMs: number;
  private readonly nowFn: () => number;

  constructor(
    private pool: Pool,
    private taskQueue: Queue,
    private pollIntervalMs: number = 1000,
    private batchSize: number = 10,
    private maxAttempts: number = 5,
    circuitOptions?: {
      failureThreshold?: number;
      successThreshold?: number;
      resetTimeoutMs?: number;
      nowFn?: () => number;
      logger?: Logger;
    },
  ) {
    this.publisherId = generateId().substring(0, 12);
    this.circuitFailureThreshold = circuitOptions?.failureThreshold ?? 5;
    this.circuitSuccessThreshold = circuitOptions?.successThreshold ?? 2;
    this.circuitResetTimeoutMs = circuitOptions?.resetTimeoutMs ?? 30000;
    this.nowFn = circuitOptions?.nowFn ?? (() => Date.now());
    const baseLogger = circuitOptions?.logger ?? createLogger({
      service: 'outbox-publisher',
      environment: process.env.NODE_ENV ?? 'development',
      level: process.env.LOG_LEVEL,
    });
    this.logger = baseLogger.child({ publisherId: this.publisherId });
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.logger.info('Outbox publisher started');
    this.schedulePoll();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.activePoll) {
      try {
        await this.activePoll;
      } catch {
        // drain errors are already logged in processOutbox
      }
      this.activePoll = null;
    }
    this.logger.info('Outbox publisher stopped');
  }

  getCircuitState(): CircuitState {
    if (
      this.circuitState === 'OPEN' &&
      this.nowFn() - this.lastFailureTime > this.circuitResetTimeoutMs
    ) {
      this.circuitState = 'HALF_OPEN';
      this.consecutiveSuccesses = 0;
    }
    return this.circuitState;
  }

  private onBullMQSuccess(): void {
    this.consecutiveFailures = 0;
    if (this.circuitState === 'HALF_OPEN') {
      this.consecutiveSuccesses++;
      if (this.consecutiveSuccesses >= this.circuitSuccessThreshold) {
        this.circuitState = 'CLOSED';
        this.consecutiveSuccesses = 0;
        this.logger.info('Circuit breaker closed');
      }
    }
  }

  private onBullMQFailure(): void {
    this.lastFailureTime = this.nowFn();
    if (this.circuitState === 'HALF_OPEN') {
      this.circuitState = 'OPEN';
      this.consecutiveFailures = 0;
      this.consecutiveSuccesses = 0;
      this.logger.warn('Circuit breaker opened (half-open probe failed)');
      return;
    }
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.circuitFailureThreshold) {
      this.circuitState = 'OPEN';
      this.logger.warn('Circuit breaker opened', { consecutiveFailures: this.consecutiveFailures });
    }
  }

  private schedulePoll(): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(async () => {
      const poll = this.processOutbox();
      this.activePoll = poll;
      try {
        await poll;
      } catch (error) {
        this.logger.error('Outbox poll error', { reason: String(error) });
      }
      this.activePoll = null;
      this.schedulePoll();
    }, this.pollIntervalMs);
  }

  async processOutbox(): Promise<number> {
    const currentCircuitState = this.getCircuitState();
    if (currentCircuitState === 'OPEN') {
      return 0;
    }

    const isHalfOpen = currentCircuitState === 'HALF_OPEN';

    const client = await this.pool.connect();
    let processed = 0;

    try {
      await client.query(
        `UPDATE outbox_events SET status = 'FAILED', processed_at = NOW()
         WHERE status = 'PENDING' AND attempts >= $1`,
        [this.maxAttempts],
      );

      const effectiveBatchSize = isHalfOpen ? 1 : this.batchSize;

      const claimResult = await client.query(
        `UPDATE outbox_events
         SET claimed_by = $1, claimed_at = NOW(), attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM outbox_events
           WHERE status = 'PENDING'
             AND (claimed_by IS NULL OR claimed_at < NOW() - INTERVAL '30 seconds')
             AND attempts < $2
           ORDER BY created_at ASC
           LIMIT $3
           FOR UPDATE SKIP LOCKED
         )
         RETURNING *`,
        [this.publisherId, this.maxAttempts, effectiveBatchSize],
      );

      for (const row of claimResult.rows) {
        const event: OutboxEvent = {
          id: row.id,
          taskId: row.task_id,
          eventType: row.event_type,
          payload: row.payload,
          status: row.status,
          attempts: row.attempts,
          createdAt: new Date(row.created_at),
          claimedBy: row.claimed_by,
          claimedAt: row.claimed_at ? new Date(row.claimed_at) : undefined,
          traceContext: row.trace_context ?? undefined,
        };

        let published: boolean;
        try {
          published = await this.publishEvent(event, client);
        } catch (error) {
          this.onBullMQFailure();

          this.logger.error('Failed to publish outbox event to queue', {
            eventId: event.id,
            taskId: event.taskId,
            reason: String(error),
            attempt: event.attempts,
          });

          if (event.attempts >= this.maxAttempts) {
            await client.query(
              `UPDATE outbox_events SET status = 'FAILED', processed_at = NOW() WHERE id = $1`,
              [event.id],
            );
            this.logger.warn('Outbox event exhausted retries', { eventId: event.id, taskId: event.taskId });
          } else {
            await client.query(
              `UPDATE outbox_events SET claimed_by = NULL, claimed_at = NULL WHERE id = $1`,
              [event.id],
            );
          }

          if (this.getCircuitState() === 'OPEN') {
            break;
          }
          continue;
        }

        try {
          await client.query(
            `UPDATE outbox_events SET status = 'DELIVERED', processed_at = NOW() WHERE id = $1`,
            [event.id],
          );
        } catch (pgError) {
          this.logger.error('Failed to mark outbox event as delivered after successful queue publication', {
            eventId: event.id,
            taskId: event.taskId,
            reason: String(pgError),
          });
        }

        if (published) {
          processed++;
          this.logger.info('Outbox event published', { eventId: event.id, taskId: event.taskId });
        }
      }
    } finally {
      client.release();
    }

    return processed;
  }

  private async publishEvent(event: OutboxEvent, client: import('pg').PoolClient): Promise<boolean> {
    const restoredCtx = extractTraceContext(event.traceContext);
    const span = tracing.startOutboxPublish(event.id, event.taskId, restoredCtx);

    return tracing.withActiveSpan(span, async () => {
      try {
        const payload = event.payload as Record<string, unknown>;
        const priority = PRIORITY_MAP[(payload.priority as string) || 'NORMAL'] ?? 5;
        const maxRetries = (payload.maxRetries as number) ?? 3;

        const taskCheck = await client.query(
          `SELECT status FROM tasks WHERE id = $1`,
          [event.taskId],
        );

        if (taskCheck.rows.length > 0 && taskCheck.rows[0].status === 'CANCELLED') {
          this.logger.info('Skipped publishing cancelled task', { eventId: event.id, taskId: event.taskId });
          span.setAttribute('outbox.skipped', true);
          span.setAttribute('outbox.skip_reason', 'cancelled');
          tracing.setSpanOk(span);
          return false;
        }

        const jobTraceContext = injectTraceContext();

        await this.taskQueue.add(
          payload.taskName as string,
          {
            taskId: event.taskId,
            taskName: payload.taskName,
            payload: payload.payload,
            maxRetries,
            [TRACE_CONTEXT_KEY]: jobTraceContext,
            publishedAt: new Date().toISOString(),
          },
          {
            jobId: event.taskId,
            priority,
            attempts: maxRetries + 1,
            backoff: {
              type: 'exponential',
              delay: 2000,
            },
          },
        );

        this.onBullMQSuccess();
        tracing.setSpanOk(span);
        return true;
      } catch (error) {
        tracing.recordError(span, error);
        throw error;
      } finally {
        tracing.endSpan(span);
      }
    });
  }

  async getPendingCount(): Promise<number> {
    const result = await this.pool.query(
      `SELECT COUNT(*)::int as count FROM outbox_events WHERE status = 'PENDING'`,
    );
    return result.rows[0].count;
  }

  async getFailedCount(): Promise<number> {
    const result = await this.pool.query(
      `SELECT COUNT(*)::int as count FROM outbox_events WHERE status = 'FAILED'`,
    );
    return result.rows[0].count;
  }
}
