import { Pool } from 'pg';
import { Queue } from 'bullmq';
import { generateId } from '../utils.js';
import { OutboxEvent, OutboxEventType } from '../types.js';
import { tracing } from '../telemetry/spans.js';
import { TRACE_CONTEXT_KEY, injectTraceContext, extractTraceContext } from '../telemetry/propagation.js';
import { createLogger, type Logger } from '../logger/index.js';
import { WeightedFairScheduler, TenantWeightEntry } from '../fairness.js';

interface OutboxRow {
  id: string;
  task_id: string;
  event_type: OutboxEventType;
  payload: Record<string, unknown>;
  status: 'PENDING' | 'DELIVERED' | 'FAILED';
  attempts: number;
  created_at: string;
  claimed_by: string | null;
  claimed_at: string | null;
  trace_context: Record<string, string> | null;
  task_tenant_id: string | null;
}

const PRIORITY_MAP: Record<string, number> = {
  LOW: 10,
  NORMAL: 5,
  HIGH: 1,
  CRITICAL: 0,
};

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

/** Waiting-room order inside a tenant under admission: higher priority first. */
const PRIORITY_RANK_SQL = `CASE t.priority WHEN 'CRITICAL' THEN 3 WHEN 'HIGH' THEN 2 WHEN 'NORMAL' THEN 1 ELSE 0 END`;

export class PublishTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`queue.add did not complete within ${timeoutMs}ms`);
    this.name = 'PublishTimeoutError';
  }
}

/**
 * Bounds a promise in time. The Queue's Redis connection is created with
 * maxRetriesPerRequest: null, so while Redis is unreachable a command is not
 * rejected: it waits in the client's offline queue until Redis returns. A
 * publish that waits forever counts as neither success nor failure, so the
 * circuit breaker can never open and the poll loop stalls on one batch.
 * Racing it against a timer turns "Redis is gone" into a failure the breaker
 * can count.
 *
 * The original promise keeps running. If Redis comes back it may still add
 * the job later; that is harmless, because the event was released and will
 * be published again with the same jobId, which BullMQ deduplicates. Its
 * eventual rejection is swallowed so it cannot surface as an unhandled
 * rejection, which would crash the process.
 */
function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  promise.catch(() => {});
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new PublishTimeoutError(timeoutMs)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

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
  private readonly publishTimeoutMs: number;
  private readonly tenantAdmission: boolean;
  private waitingForRedis = false;

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
      /** Upper bound for one queue.add; a slower add counts as a failure. */
      publishTimeoutMs?: number;
      /**
       * Publish a tenant's events only while it has a free concurrency slot
       * (see admissionSlots). Requires fairScheduler. Off by default in the
       * library; the API turns it on (OUTBOX_TENANT_ADMISSION).
       */
      tenantAdmission?: boolean;
    },
  ) {
    this.publisherId = generateId().substring(0, 12);
    this.circuitFailureThreshold = circuitOptions?.failureThreshold ?? 5;
    this.circuitSuccessThreshold = circuitOptions?.successThreshold ?? 2;
    this.circuitResetTimeoutMs = circuitOptions?.resetTimeoutMs ?? 30000;
    this.nowFn = circuitOptions?.nowFn ?? (() => Date.now());
    this.fairScheduler = circuitOptions?.fairScheduler;
    this.publishTimeoutMs = circuitOptions?.publishTimeoutMs ?? 5000;
    this.tenantAdmission = (circuitOptions?.tenantAdmission ?? false) && !!this.fairScheduler;
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

  /** False while the Queue's Redis connection is down (ioredis status is not "ready"). */
  private async queueConnected(): Promise<boolean> {
    // BullMQ 6 keeps the raw Redis client on the backend (queue.getBackend().client);
    // older versions exposed it as queue.client.
    type WithClient = { client?: Promise<{ status?: string }> };
    const q = this.taskQueue as unknown as WithClient & { getBackend?: () => WithClient };
    const client = q.getBackend?.().client ?? q.client;
    if (!client) return true; // test doubles without a connection
    try {
      const redis = await withTimeout(client, 1000);
      return redis.status === undefined || redis.status === 'ready';
    } catch {
      return false;
    }
  }

  /** Whether the last poll found Redis unreachable (for /health). */
  isWaitingForRedis(): boolean {
    return this.waitingForRedis;
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

    // Redis unreachable: claim nothing. Claiming would charge every event an
    // attempt for a publish that cannot succeed, and the call itself would
    // wait for Redis instead of failing. The events stay PENDING and free,
    // and the next poll after the connection is back publishes them.
    if (!(await this.queueConnected())) {
      if (!this.waitingForRedis) {
        this.waitingForRedis = true;
        this.logger.warn('Redis connection is not ready; outbox publishing paused');
      }
      return 0;
    }
    if (this.waitingForRedis) {
      this.waitingForRedis = false;
      this.logger.info('Redis connection is ready; outbox publishing resumed');
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

      // With admission every tenant event goes through the per-tenant path,
      // even a single event in HALF_OPEN, so no path bypasses the limit.
      const orderedRows = this.fairScheduler && (effectiveBatchSize > 1 || this.tenantAdmission)
        ? await this.claimFairly(client, effectiveBatchSize)
        : await this.claimGlobalFifo(client, effectiveBatchSize);

      for (const [index, row] of orderedRows.entries()) {
        const event: OutboxEvent = {
          id: row.id,
          taskId: row.task_id,
          eventType: row.event_type,
          payload: row.payload,
          status: row.status,
          attempts: row.attempts,
          createdAt: new Date(row.created_at),
          claimedBy: row.claimed_by ?? undefined,
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
            // The rest of the batch was claimed (attempts + 1) but never
            // tried. Hand it back now, uncharged, instead of leaving it
            // claimed for 30 s and spending an attempt on a call never made.
            const untried = orderedRows.slice(index + 1).map((r) => r.id);
            if (untried.length > 0) {
              await client.query(
                `UPDATE outbox_events SET claimed_by = NULL, claimed_at = NULL, attempts = GREATEST(attempts - 1, 0)
                 WHERE id = ANY($1) AND claimed_by = $2 AND status = 'PENDING'`,
                [untried, this.publisherId],
              );
            }
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
  ): Promise<OutboxRow[]> {
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
    return result.rows as OutboxRow[];
  }

  /**
   * Free concurrency slots per tenant, for publish-time admission.
   *
   * A tenant's in-flight work is every task that has left the outbox and not
   * finished: QUEUED or PROCESSING with no PENDING event (waiting in BullMQ,
   * running, or in a retry backoff). Slots = limit - in-flight, and 0 for a
   * SUSPENDED tenant. Publishing only into free slots means BullMQ only holds
   * jobs that can start now, so the outbox (ordered by priority, then age) is
   * the tenant's waiting room. Before, every job was published at once and
   * the ones over the limit were deferred and retried every 2 s by the
   * workers: random start order, and a backlog of N tasks cost ~N/2 deferral
   * round trips per second against PostgreSQL.
   *
   * This is a soft gate: two API replicas can both see a free slot and admit
   * one job each. The worker's concurrency lease stays the hard limit and
   * defers the extra job, as before.
   */
  private async admissionSlots(client: import('pg').PoolClient, tenantIds: string[]): Promise<Map<string, number>> {
    const result = await client.query(
      `SELECT tn.id AS tenant_id,
         CASE WHEN tn.status = 'SUSPENDED' THEN 0
              ELSE COALESCE(o.max_concurrent_executions, p.max_concurrent_executions) END::int AS max_concurrent,
         (SELECT COUNT(*) FROM tasks t
           WHERE t.tenant_id = tn.id AND t.status IN ('QUEUED', 'PROCESSING')
             AND NOT EXISTS (SELECT 1 FROM outbox_events oe WHERE oe.task_id = t.id AND oe.status = 'PENDING')
         )::int AS in_flight
       FROM tenants tn
       JOIN plans p ON p.id = tn.plan_id
       LEFT JOIN tenant_overrides o ON o.tenant_id = tn.id
       WHERE tn.id = ANY($1)`,
      [tenantIds],
    );
    return new Map(result.rows.map((r) => [r.tenant_id as string, Math.max(0, (r.max_concurrent as number) - (r.in_flight as number))]));
  }

  private async claimFairly(
    client: import('pg').PoolClient,
    batchSize: number,
  ): Promise<OutboxRow[]> {
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

    // One tenant (or none) needs no fair ordering, but with admission its
    // events still have to respect its concurrency limit.
    if (tenantResult.rows.length === 0 || (tenantResult.rows.length === 1 && !this.tenantAdmission)) {
      return this.claimGlobalFifo(client, batchSize);
    }

    let tenantRows = tenantResult.rows as Array<{ tenant_id: string; weight: number; pending_count: number }>;
    if (this.tenantAdmission) {
      const slots = await this.admissionSlots(client, tenantRows.map((r) => r.tenant_id));
      tenantRows = tenantRows
        .map((r) => ({ ...r, pending_count: Math.min(r.pending_count, slots.get(r.tenant_id) ?? 0) }))
        .filter((r) => r.pending_count > 0);
    }
    const entries: TenantWeightEntry[] = tenantRows.map((r) => ({
      tenantId: r.tenant_id,
      weight: r.weight,
    }));
    const pendingMap = new Map<string, number>(
      tenantRows.map((r) => [r.tenant_id, r.pending_count]),
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

    const perTenantRows = new Map<string, OutboxRow[]>();
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
           ORDER BY ${this.tenantAdmission ? `${PRIORITY_RANK_SQL} DESC, ` : ''}oe.created_at ASC
           LIMIT $4
           FOR UPDATE OF oe SKIP LOCKED
         )
         RETURNING *,
           (SELECT t.tenant_id FROM tasks t WHERE t.id = outbox_events.task_id) as task_tenant_id`,
        [this.publisherId, this.maxAttempts, tenantId, slots],
      );
      perTenantRows.set(tenantId, result.rows as OutboxRow[]);
    }

    const result: OutboxRow[] = [];
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
      result.push(...(backfill.rows as OutboxRow[]));
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
        await this.removeDeadJob(event);

        await withTimeout(this.taskQueue.add(
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
        ), this.publishTimeoutMs);

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

  /**
   * jobId = taskId makes a second add() a no-op while a job with that id
   * exists, and BullMQ keeps failed jobs (removeOnFail: false). A task sent
   * back to the queue (TASK_REQUEUED: operator retry, reconciler) can
   * therefore meet its own dead job and never be enqueued. A finished job is
   * removed first; a live one (waiting, active, delayed) is left alone, so the
   * add stays a harmless duplicate. The reconciler already removes dead jobs,
   * so for its events this finds nothing.
   */
  private async removeDeadJob(event: OutboxEvent): Promise<void> {
    if (event.eventType !== 'TASK_REQUEUED') return;
    const job = await this.taskQueue.getJob(event.taskId);
    if (!job) return;
    const state = await job.getState();
    if (state === 'failed' || state === 'completed') {
      await job.remove();
      this.logger.info('Removed finished job before requeue', { taskId: event.taskId, state });
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
