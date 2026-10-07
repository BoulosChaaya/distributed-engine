import { Pool } from 'pg';
import { Queue } from 'bullmq';
import { log, generateId } from '../utils';
import { OutboxEvent } from '../types';

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
  private readonly publisherId: string;

  private circuitState: CircuitState = 'CLOSED';
  private consecutiveFailures = 0;
  private consecutiveSuccesses = 0;
  private lastFailureTime = 0;
  private readonly circuitFailureThreshold = 5;
  private readonly circuitSuccessThreshold = 2;
  private readonly circuitResetTimeoutMs = 30000;

  constructor(
    private pool: Pool,
    private taskQueue: Queue,
    private pollIntervalMs: number = 1000,
    private batchSize: number = 10,
    private maxAttempts: number = 5,
  ) {
    this.publisherId = generateId().substring(0, 12);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    log('INFO', 'Outbox publisher started', { publisherId: this.publisherId });
    this.schedulePoll();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    log('INFO', 'Outbox publisher stopped', { publisherId: this.publisherId });
  }

  getCircuitState(): CircuitState {
    if (
      this.circuitState === 'OPEN' &&
      Date.now() - this.lastFailureTime > this.circuitResetTimeoutMs
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
        log('INFO', 'Outbox circuit breaker CLOSED', { publisherId: this.publisherId });
      }
    }
  }

  private onBullMQFailure(): void {
    this.lastFailureTime = Date.now();
    if (this.circuitState === 'HALF_OPEN') {
      this.circuitState = 'OPEN';
      this.consecutiveFailures = 0;
      this.consecutiveSuccesses = 0;
      log('WARN', 'Outbox circuit breaker OPEN (half-open failure)', { publisherId: this.publisherId });
      return;
    }
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= this.circuitFailureThreshold) {
      this.circuitState = 'OPEN';
      log('WARN', 'Outbox circuit breaker OPEN', { publisherId: this.publisherId, failures: this.consecutiveFailures });
    }
  }

  private schedulePoll(): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(async () => {
      try {
        await this.processOutbox();
      } catch (error) {
        log('ERROR', 'Outbox poll error', { error: String(error), publisherId: this.publisherId });
      }
      this.schedulePoll();
    }, this.pollIntervalMs);
  }

  async processOutbox(): Promise<number> {
    const currentCircuitState = this.getCircuitState();
    if (currentCircuitState === 'OPEN') {
      return 0;
    }

    const client = await this.pool.connect();
    let processed = 0;

    try {
      await client.query(
        `UPDATE outbox_events SET status = 'FAILED', processed_at = NOW()
         WHERE status = 'PENDING' AND attempts >= $1`,
        [this.maxAttempts],
      );

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
        [this.publisherId, this.maxAttempts, this.batchSize],
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
        };

        const taskCheck = await client.query(
          `SELECT status FROM tasks WHERE id = $1`,
          [event.taskId],
        );
        if (taskCheck.rows.length > 0 && taskCheck.rows[0].status === 'CANCELLED') {
          await client.query(
            `UPDATE outbox_events SET status = 'DELIVERED', processed_at = NOW() WHERE id = $1`,
            [event.id],
          );
          log('INFO', 'Skipped publishing cancelled task', { eventId: event.id, taskId: event.taskId });
          continue;
        }

        try {
          await this.publishEvent(event);
          this.onBullMQSuccess();

          await client.query(
            `UPDATE outbox_events SET status = 'DELIVERED', processed_at = NOW() WHERE id = $1`,
            [event.id],
          );

          processed++;
          log('INFO', 'Outbox event published', { eventId: event.id, taskId: event.taskId });
        } catch (error) {
          this.onBullMQFailure();

          log('ERROR', 'Failed to publish outbox event', {
            eventId: event.id,
            taskId: event.taskId,
            error: String(error),
            attempt: event.attempts,
          });

          if (event.attempts >= this.maxAttempts) {
            await client.query(
              `UPDATE outbox_events SET status = 'FAILED', processed_at = NOW() WHERE id = $1`,
              [event.id],
            );
            log('WARN', 'Outbox event exhausted retries', { eventId: event.id, taskId: event.taskId });
          } else {
            await client.query(
              `UPDATE outbox_events SET claimed_by = NULL, claimed_at = NULL WHERE id = $1`,
              [event.id],
            );
          }

          if (this.getCircuitState() === 'OPEN') {
            break;
          }
        }
      }
    } finally {
      client.release();
    }

    return processed;
  }

  private async publishEvent(event: OutboxEvent): Promise<void> {
    const payload = event.payload as Record<string, unknown>;
    const priority = PRIORITY_MAP[(payload.priority as string) || 'NORMAL'] ?? 5;
    const maxRetries = (payload.maxRetries as number) ?? 3;

    await this.taskQueue.add(
      payload.taskName as string,
      {
        taskId: event.taskId,
        taskName: payload.taskName,
        payload: payload.payload,
        maxRetries,
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
