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

export class OutboxPublisher {
  private running = false;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly publisherId: string;

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

        try {
          await this.publishEvent(event);

          await client.query(
            `UPDATE outbox_events SET status = 'DELIVERED', processed_at = NOW() WHERE id = $1`,
            [event.id],
          );

          await this.transitionTaskToQueued(event.taskId);

          processed++;
          log('INFO', 'Outbox event published', { eventId: event.id, taskId: event.taskId });
        } catch (error) {
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

    await this.taskQueue.add(
      payload.taskName as string,
      {
        taskId: event.taskId,
        taskName: payload.taskName,
        payload: payload.payload,
        maxRetries: payload.maxRetries,
      },
      {
        jobId: event.taskId,
        priority,
      },
    );
  }

  private async transitionTaskToQueued(taskId: string): Promise<void> {
    try {
      await this.pool.query(
        `UPDATE tasks SET status = 'QUEUED', version = version + 1, updated_at = NOW()
         WHERE id = $1 AND status = 'PENDING'`,
        [taskId],
      );
    } catch (error) {
      log('WARN', 'Failed to transition task to QUEUED after publish', { taskId, error: String(error) });
    }
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
