import { Pool } from 'pg';
import { Queue } from 'bullmq';
import { generateId } from '../utils';
import { OutboxEvent } from '../types';
import { tracing } from '../telemetry/spans';
import { TRACE_CONTEXT_KEY, injectTraceContext, extractTraceContext } from '../telemetry/propagation';
import { createLogger, type Logger } from '../logger/index';
import { WeightedFairScheduler, TenantWeightEntry } from '../fairness';

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
  private readonly fairScheduler?: WeightedFairScheduler;

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
      fairScheduler?: WeightedFairScheduler;
    },
  ) {
    this.publisherId = generateId().substring(0, 12);
    this.circuitFailureThreshold = circuitOptions?.failureThreshold ?? 5;
    this.circuitSuccessThreshold = circuitOptions?.successThreshold ?? 2;
    this.circuitResetTimeoutMs = circuitOptions?.resetTimeoutMs ?? 30000;
    this.nowFn = circuitOptions?.nowFn ?? (() => Date.now());
    this.fairScheduler = circuitOptions?.fairScheduler;
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

      const orderedRows = this.fairScheduler && effectiveBatchSize > 1
        ? await this.claimFairly(client, effectiveBatchSize)
        : await this.claimGlobalFifo(client, effectiveBatchSize);

      for (const row of orderedRows) {
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

  private async claimGlobalFifo(
    client: import('pg').PoolClient,
    batchSize: number,
  ): Promise<Array<Record<string, unknown>>> {
    const result = await client.query(
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
       RETURNING *,
         (SELECT t.tenant_id FROM tasks t WHERE t.id = outbox_events.task_id) as task_tenant_id`,
      [this.publisherId, this.maxAttempts, batchSize],
    );
    return result.rows;
  }

  private async claimFairly(
    client: import('pg').PoolClient,
    batchSize: number,
  ): Promise<Array<Record<string, unknown>>> {
    const tenantResult = await client.query(
      `SELECT t.tenant_id,
         COALESCE(ovr.weight, p.weight, 1)::int as weight,
         COUNT(*)::int as pending_count
       FROM outbox_events oe
       JOIN tasks t ON t.id = oe.task_id
       JOIN tenants tn ON tn.id = t.tenant_id
       JOIN plans p ON tn.plan_id = p.id
       LEFT JOIN tenant_overrides ovr ON ovr.tenant_id = t.tenant_id
       WHERE oe.status = 'PENDING'
         AND (oe.claimed_by IS NULL OR oe.claimed_at < NOW() - INTERVAL '30 seconds')
         AND oe.attempts < $1
       GROUP BY t.tenant_id, COALESCE(ovr.weight, p.weight, 1)`,
      [this.maxAttempts],
    );

    if (tenantResult.rows.length <= 1) {
      return this.claimGlobalFifo(client, batchSize);
    }

    const entries: TenantWeightEntry[] = tenantResult.rows.map((r: Record<string, unknown>) => ({
      tenantId: r.tenant_id as string,
      weight: r.weight as number,
    }));
    const pendingMap = new Map<string, number>(
      tenantResult.rows.map((r: Record<string, unknown>) => [r.tenant_id as string, r.pending_count as number]),
    );

    const selectionOrder: string[] = [];
    for (let i = 0; i < batchSize; i++) {
      const active = entries.filter(e => {
        const count = selectionOrder.filter(id => id === e.tenantId).length;
        return count < (pendingMap.get(e.tenantId) ?? 0);
      });
      if (active.length === 0) break;
      const selected = this.fairScheduler!.selectNext(active);
      if (!selected) break;
      selectionOrder.push(selected);
    }

    const allocation = new Map<string, number>();
    for (const tid of selectionOrder) {
      allocation.set(tid, (allocation.get(tid) ?? 0) + 1);
    }

    const perTenantRows = new Map<string, Array<Record<string, unknown>>>();
    for (const [tenantId, slots] of allocation) {
      const result = await client.query(
        `UPDATE outbox_events
         SET claimed_by = $1, claimed_at = NOW(), attempts = attempts + 1
         WHERE id IN (
           SELECT oe.id FROM outbox_events oe
           JOIN tasks t ON t.id = oe.task_id
           WHERE oe.status = 'PENDING'
             AND (oe.claimed_by IS NULL OR oe.claimed_at < NOW() - INTERVAL '30 seconds')
             AND oe.attempts < $2
             AND t.tenant_id = $3
           ORDER BY oe.created_at ASC
           LIMIT $4
           FOR UPDATE OF oe SKIP LOCKED
         )
         RETURNING *,
           (SELECT t.tenant_id FROM tasks t WHERE t.id = outbox_events.task_id) as task_tenant_id`,
        [this.publisherId, this.maxAttempts, tenantId, slots],
      );
      perTenantRows.set(tenantId, result.rows);
    }

    const result: Array<Record<string, unknown>> = [];
    const tenantCursors = new Map<string, number>();
    for (const tid of selectionOrder) {
      const cursor = tenantCursors.get(tid) ?? 0;
      const rows = perTenantRows.get(tid);
      if (rows && cursor < rows.length) {
        result.push(rows[cursor]);
        tenantCursors.set(tid, cursor + 1);
      }
    }

    const remaining = batchSize - result.length;
    if (remaining > 0) {
      const backfill = await client.query(
        `UPDATE outbox_events
         SET claimed_by = $1, claimed_at = NOW(), attempts = attempts + 1
         WHERE id IN (
           SELECT oe.id FROM outbox_events oe
           LEFT JOIN tasks t ON t.id = oe.task_id
           WHERE oe.status = 'PENDING'
             AND (oe.claimed_by IS NULL OR oe.claimed_at < NOW() - INTERVAL '30 seconds')
             AND oe.attempts < $2
             AND (t.id IS NULL OR t.tenant_id IS NULL)
           ORDER BY oe.created_at ASC
           LIMIT $3
           FOR UPDATE OF oe SKIP LOCKED
         )
         RETURNING *,
           (SELECT t.tenant_id FROM tasks t WHERE t.id = outbox_events.task_id) as task_tenant_id`,
        [this.publisherId, this.maxAttempts, remaining],
      );
      result.push(...backfill.rows);
    }

    return result;
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
