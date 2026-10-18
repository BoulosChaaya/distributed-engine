import { Pool, PoolClient } from 'pg';
import { generateId } from '../utils.js';
import { createLogger, type Logger } from '../logger/index.js';
import { lockWorkflowForTask, onTaskTerminal } from './workflow-engine.js';

/**
 * Re-derives the queue from PostgreSQL when Redis has lost work.
 *
 * PostgreSQL is the source of truth; Redis/BullMQ is a delivery mechanism.
 * The outbox guarantees that every task reaches Redis at least once, but
 * nothing guarantees it stays there: a Redis restart without persistence, a
 * FLUSHALL, an eviction under memory pressure, or a failover to a replica
 * that had not received the write all drop jobs silently. BullMQ can also
 * give up on a job by itself (a job that stalls more than `maxStalledCount`
 * times is moved to `failed` without the processor ever running its catch
 * block). In every one of those cases the database row is left in QUEUED or
 * PROCESSING with nothing that will ever pick it up.
 *
 * The reconciler closes that gap. Each cycle it:
 *  1. takes a session-level advisory lock so only one API replica reconciles;
 *  2. reads candidates from PostgreSQL only (no locks held):
 *     - QUEUED tasks untouched for longer than `queuedGraceMs`
 *     - PROCESSING tasks whose lease expired (by the database clock)
 *     that have no PENDING outbox event already in flight;
 *  3. asks BullMQ about each candidate's job (jobId = taskId). A job that is
 *     waiting / delayed / prioritized / active is left alone — BullMQ owns it.
 *     A job that is missing, completed, failed or unknown will never run
 *     again, so the task is orphaned;
 *  4. for each orphan, in its own short transaction with the row locked and
 *     the snapshot version re-checked, emits a TASK_REQUEUED outbox event.
 *     The normal OutboxPublisher then delivers it, so publishing keeps a
 *     single code path (circuit breaker, fairness, tracing).
 *
 * An orphaned PROCESSING task means an attempt was lost, so it consumes one
 * retry. That bounds poison tasks that crash every worker that touches them:
 * once the retry budget is spent the reconciler marks the task FAILED instead
 * of requeueing it forever.
 *
 * Idempotency: the version check means a task that moved after the snapshot
 * is skipped; the NOT EXISTS guard means two cycles can never stack events;
 * and jobId = taskId means even a redundant publish is a no-op in BullMQ.
 * Any Redis error aborts the cycle — when Redis is unreachable we cannot
 * tell "missing" from "unknown", and acting on a guess would double-queue.
 */

export interface ReconcilerJob {
  getState(): Promise<string>;
  remove(): Promise<void>;
}

export interface ReconcilerQueue {
  getJob(jobId: string): Promise<ReconcilerJob | undefined | null>;
}

export interface TaskReconcilerOptions {
  intervalMs?: number;
  queuedGraceMs?: number;
  batchSize?: number;
  /**
   * Delete DELIVERED outbox events older than this. Undefined or 0 keeps them
   * forever (the old behaviour). PENDING and FAILED events are never pruned.
   */
  deliveredOutboxRetentionMs?: number;
  /** Rows per DELETE; up to PRUNE_MAX_BATCHES of them per cycle. */
  pruneBatchSize?: number;
  logger?: Logger;
}

export interface ReconcileResult {
  skipped?: 'locked' | 'redis_unavailable';
  examined: number;
  requeued: number;
  failed: number;
  pruned?: number;
}

interface CandidateRow {
  id: string;
  status: 'QUEUED' | 'PROCESSING';
  version: number;
}

const RECONCILER_LOCK_KEY = 7_340_021;
const PRUNE_MAX_BATCHES = 10;

const LIVE_JOB_STATES = new Set(['waiting', 'delayed', 'prioritized', 'active', 'waiting-children']);
const DEAD_JOB_STATES = new Set(['completed', 'failed', 'unknown']);

export class TaskReconciler {
  private running = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private activeCycle: Promise<ReconcileResult> | null = null;
  private readonly intervalMs: number;
  private readonly queuedGraceMs: number;
  private readonly batchSize: number;
  private readonly deliveredOutboxRetentionMs: number;
  private readonly pruneBatchSize: number;
  private readonly logger: Logger;

  constructor(
    private readonly pool: Pool,
    private readonly queue: ReconcilerQueue,
    options: TaskReconcilerOptions = {},
  ) {
    this.intervalMs = options.intervalMs ?? 30_000;
    this.queuedGraceMs = options.queuedGraceMs ?? 60_000;
    this.batchSize = options.batchSize ?? 100;
    this.deliveredOutboxRetentionMs = options.deliveredOutboxRetentionMs ?? 0;
    this.pruneBatchSize = options.pruneBatchSize ?? 1000;
    this.logger = options.logger ?? createLogger({
      service: 'task-reconciler',
      environment: process.env.NODE_ENV ?? 'development',
      level: process.env.LOG_LEVEL,
    });
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.logger.info('Task reconciler started', {
      intervalMs: this.intervalMs,
      queuedGraceMs: this.queuedGraceMs,
    });
    this.scheduleNext();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.activeCycle) {
      try {
        await this.activeCycle;
      } catch {
        // already logged by the cycle
      }
      this.activeCycle = null;
    }
    this.logger.info('Task reconciler stopped');
  }

  private scheduleNext(): void {
    if (!this.running) return;
    this.timer = setTimeout(async () => {
      const cycle = this.reconcile();
      this.activeCycle = cycle;
      try {
        await cycle;
      } catch (error) {
        this.logger.error('Reconcile cycle failed', { reason: String(error) });
      }
      this.activeCycle = null;
      this.scheduleNext();
    }, this.intervalMs);
  }

  async reconcile(): Promise<ReconcileResult> {
    const result: ReconcileResult = { examined: 0, requeued: 0, failed: 0 };
    const lockClient = await this.pool.connect();
    let locked = false;
    try {
      const lockResult = await lockClient.query(
        'SELECT pg_try_advisory_lock($1) AS locked',
        [RECONCILER_LOCK_KEY],
      );
      locked = lockResult.rows[0].locked === true;
      if (!locked) {
        return { ...result, skipped: 'locked' };
      }

      result.pruned = await this.pruneDeliveredOutbox(lockClient);

      const candidates = await this.findCandidates(lockClient);
      result.examined = candidates.length;
      if (candidates.length === 0) return result;

      const orphans: CandidateRow[] = [];
      try {
        for (const candidate of candidates) {
          const job = await this.queue.getJob(candidate.id);
          if (!job) {
            orphans.push(candidate);
            continue;
          }
          const state = await job.getState();
          if (LIVE_JOB_STATES.has(state)) continue;
          if (DEAD_JOB_STATES.has(state)) {
            // A finished job with the same id would make the next add() a
            // silent no-op, so it has to go before the task is requeued.
            await job.remove();
            orphans.push(candidate);
          }
        }
      } catch (error) {
        this.logger.warn('Redis unavailable, skipping reconcile cycle', { reason: String(error) });
        return { ...result, skipped: 'redis_unavailable' };
      }

      for (const orphan of orphans) {
        const outcome = await this.repairOrphan(orphan);
        if (outcome === 'requeued') result.requeued++;
        if (outcome === 'failed') result.failed++;
      }

      if (result.requeued > 0 || result.failed > 0) {
        this.logger.warn('Reconciled orphaned tasks', {
          examined: result.examined,
          requeued: result.requeued,
          failed: result.failed,
        });
      }
      return result;
    } finally {
      if (locked) {
        await lockClient.query('SELECT pg_advisory_unlock($1)', [RECONCILER_LOCK_KEY]).catch(() => {});
      }
      lockClient.release();
    }
  }

  /**
   * The outbox only needs an event until it is delivered; after that the
   * task row is the record. Without pruning, every task ever accepted left
   * one or more DELIVERED rows behind forever, so the table, its indexes and
   * vacuum work grew with total history rather than with load.
   *
   * Runs under the reconciler's advisory lock, so one replica prunes. Each
   * DELETE is bounded (pruneBatchSize rows, picked with the partial index on
   * processed_at) and short, so it never holds locks long next to the
   * publisher; at most PRUNE_MAX_BATCHES per cycle, the rest waits for the
   * next cycle. A failure is logged and does not stop reconciling.
   */
  private async pruneDeliveredOutbox(client: PoolClient): Promise<number> {
    if (!this.deliveredOutboxRetentionMs) return 0;
    let total = 0;
    try {
      for (let i = 0; i < PRUNE_MAX_BATCHES; i++) {
        const r = await client.query(
          `DELETE FROM outbox_events WHERE id IN (
             SELECT id FROM outbox_events
             WHERE status = 'DELIVERED' AND processed_at < NOW() - $1 * INTERVAL '1 millisecond'
             LIMIT $2)`,
          [this.deliveredOutboxRetentionMs, this.pruneBatchSize],
        );
        total += r.rowCount ?? 0;
        if ((r.rowCount ?? 0) < this.pruneBatchSize) break;
      }
      if (total > 0) this.logger.info('Pruned delivered outbox events', { pruned: total });
    } catch (error) {
      this.logger.warn('Outbox prune failed; will retry next cycle', { reason: String(error), pruned: total });
    }
    return total;
  }

  private async findCandidates(client: PoolClient): Promise<CandidateRow[]> {
    const rows = await client.query(
      `SELECT t.id, t.status, t.version
       FROM tasks t
       WHERE (
           (t.status = 'QUEUED' AND t.updated_at < NOW() - $1 * INTERVAL '1 millisecond')
           OR (t.status = 'PROCESSING' AND (t.claim_expires_at IS NULL OR t.claim_expires_at < NOW()))
         )
         AND NOT EXISTS (
           SELECT 1 FROM outbox_events oe
           WHERE oe.task_id = t.id AND oe.status = 'PENDING'
         )
       ORDER BY t.updated_at ASC
       LIMIT $2`,
      [this.queuedGraceMs, this.batchSize],
    );
    return rows.rows as CandidateRow[];
  }

  private async repairOrphan(orphan: CandidateRow): Promise<'requeued' | 'failed' | 'skipped'> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // Workflow row before task row; the FAILED path below may cancel descendants.
      const workflowId = await lockWorkflowForTask(client, orphan.id);

      const current = await client.query(
        `SELECT id, name, status, priority, payload, max_retries, retries, version,
                claim_expires_at, claim_expires_at < NOW() AS lease_expired
         FROM tasks WHERE id = $1 FOR UPDATE`,
        [orphan.id],
      );

      const row = current.rows[0];
      if (!row || row.status !== orphan.status || row.version !== orphan.version) {
        await client.query('ROLLBACK');
        return 'skipped';
      }

      const pending = await client.query(
        `SELECT 1 FROM outbox_events WHERE task_id = $1 AND status = 'PENDING' LIMIT 1`,
        [orphan.id],
      );
      if (pending.rows.length > 0) {
        await client.query('ROLLBACK');
        return 'skipped';
      }

      if (row.status === 'PROCESSING') {
        if (row.claim_expires_at !== null && row.lease_expired !== true) {
          await client.query('ROLLBACK');
          return 'skipped';
        }

        const retries = (row.retries as number) + 1;
        if (retries > (row.max_retries as number)) {
          await client.query(
            `UPDATE tasks
             SET status = 'FAILED', retries = $2, error = $3,
                 claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
                 version = version + 1, updated_at = NOW()
             WHERE id = $1`,
            [row.id, retries, 'Worker lost the task lease and the retry budget is exhausted'],
          );
          if (workflowId) await onTaskTerminal(client, workflowId, row.id, 'FAILED');
          await client.query('COMMIT');
          this.logger.warn('Orphaned task failed: retry budget exhausted', { taskId: row.id, retries });
          return 'failed';
        }

        await client.query(
          `UPDATE tasks
           SET status = 'QUEUED', retries = $2, error = $3,
               claimed_by = NULL, claim_token = NULL, claim_expires_at = NULL,
               version = version + 1, updated_at = NOW()
           WHERE id = $1`,
          [row.id, retries, 'Worker lost the task lease; requeued by reconciler'],
        );
      } else {
        await client.query(
          `UPDATE tasks SET version = version + 1, updated_at = NOW() WHERE id = $1`,
          [row.id],
        );
      }

      await client.query(
        `INSERT INTO outbox_events (id, task_id, event_type, payload, status, attempts, created_at, trace_context)
         VALUES ($1, $2, 'TASK_REQUEUED', $3, 'PENDING', 0, NOW(),
                 -- keep the requeue on the trace the task was submitted under
                 (SELECT trace_context FROM outbox_events
                  WHERE task_id = $2 ORDER BY created_at DESC LIMIT 1))`,
        [
          generateId(),
          row.id,
          JSON.stringify({
            taskId: row.id,
            taskName: row.name,
            priority: row.priority,
            payload: row.payload,
            maxRetries: row.max_retries,
          }),
        ],
      );

      await client.query('COMMIT');
      this.logger.warn('Orphaned task requeued', { taskId: row.id, previousStatus: orphan.status });
      return 'requeued';
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      this.logger.error('Failed to repair orphaned task', { taskId: orphan.id, reason: String(error) });
      return 'skipped';
    } finally {
      client.release();
    }
  }
}
