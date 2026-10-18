import { Pool } from 'pg';
import { ScheduleRepository, DuplicateOccurrenceError } from '../db/schedule-repository.js';
import { RecurringSchedule } from '../types.js';
import { getNextOccurrence, getMissedOccurrences, getLatestOccurrence } from './cron-utils.js';
import { type Logger } from '../logger/index.js';

export interface SchedulerConfig {
  pollIntervalMs: number;
  scheduledTaskBatchSize: number;
  recurringBatchSize: number;
  catchUpBatchSize: number;
  executionLeaseDurationMs: number;
  /**
   * How late an occurrence may be and still count as on time. Polling alone
   * makes every occurrence a few seconds late; beyond this it was missed
   * (downtime, a pause), and the misfire policy decides what happens.
   */
  misfireThresholdMs: number;
}

const DEFAULT_CONFIG: SchedulerConfig = {
  pollIntervalMs: 5000,
  scheduledTaskBatchSize: 50,
  recurringBatchSize: 20,
  catchUpBatchSize: 10,
  executionLeaseDurationMs: 300000,
  misfireThresholdMs: 60000,
};

/** Shortest sleep before a future due time (timer granularity margin). */
const MIN_WAKE_DELAY_MS = 50;
/** Sleep when work is still overdue right after a poll. */
const OVERDUE_RETRY_DELAY_MS = 1000;

export class SchedulerService {
  private running = false;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private activePoll: Promise<void> | null = null;
  private readonly config: SchedulerConfig;
  private readonly scheduleRepo: ScheduleRepository;

  constructor(
    pool: Pool,
    private logger: Logger,
    config?: Partial<SchedulerConfig>,
  ) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.scheduleRepo = new ScheduleRepository(pool);
  }

  get repository(): ScheduleRepository {
    return this.scheduleRepo;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.logger.info('Scheduler started', {
      pollIntervalMs: this.config.pollIntervalMs,
      scheduledTaskBatchSize: this.config.scheduledTaskBatchSize,
      recurringBatchSize: this.config.recurringBatchSize,
    });
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
        // drain errors are already logged
      }
      this.activePoll = null;
    }
    this.logger.info('Scheduler stopped');
  }

  private schedulePoll(delayMs: number = this.config.pollIntervalMs): void {
    if (!this.running) return;
    this.pollTimer = setTimeout(async () => {
      const poll = this.poll();
      this.activePoll = poll;
      try {
        await poll;
      } catch (error) {
        this.logger.error('Scheduler poll error', { reason: String(error) });
      }
      this.activePoll = null;
      this.schedulePoll(await this.nextPollDelay());
    }, delayMs);
  }

  /**
   * Sleep until the next known due time instead of always the full interval.
   * A fixed 5 s poll would start a task up to 5 s after its time. After each
   * poll the scheduler asks PostgreSQL how long
   * until the earliest SCHEDULED task or ACTIVE schedule is due, on the
   * database clock, and wakes then (at least MIN_WAKE_DELAY_MS, at most the
   * poll interval). Work created during the sleep for a time inside it is
   * still picked up by the regular interval, so the interval stays the bound.
   */
  private async nextPollDelay(): Promise<number> {
    try {
      const msUntilDue = await this.scheduleRepo.msUntilNextDue();
      if (msUntilDue === null) return this.config.pollIntervalMs;
      // Still overdue right after a poll: a batch limit left work behind, or a
      // schedule keeps failing or is held by another replica. Come back soon,
      // but not in a tight loop.
      if (msUntilDue <= 0) return Math.min(this.config.pollIntervalMs, OVERDUE_RETRY_DELAY_MS);
      return Math.max(MIN_WAKE_DELAY_MS, Math.min(this.config.pollIntervalMs, msUntilDue));
    } catch {
      return this.config.pollIntervalMs;
    }
  }

  async poll(): Promise<void> {
    await this.processScheduledTasks();
    await this.processRecurringSchedules();
  }

  async processScheduledTasks(): Promise<number> {
    try {
      const released = await this.scheduleRepo.releaseDueScheduledTasks(
        this.config.scheduledTaskBatchSize,
      );
      if (released.length > 0) {
        this.logger.info('Released due scheduled tasks', { count: released.length });
      }
      return released.length;
    } catch (error) {
      this.logger.error('Error processing scheduled tasks', { reason: String(error) });
      return 0;
    }
  }

  async processRecurringSchedules(): Promise<number> {
    let totalGenerated = 0;
    try {
      const dueSchedules = await this.scheduleRepo.fetchDueSchedules(
        this.config.recurringBatchSize,
      );

      for (const schedule of dueSchedules) {
        try {
          const generated = await this.processOneSchedule(schedule);
          totalGenerated += generated;
        } catch (error) {
          this.logger.error('Error processing recurring schedule', {
            scheduleId: schedule.id,
            scheduleName: schedule.name,
            reason: String(error),
          });
        }
      }
    } catch (error) {
      this.logger.error('Error fetching due schedules', { reason: String(error) });
    }
    return totalGenerated;
  }

  private async processOneSchedule(schedule: RecurringSchedule): Promise<number> {
    let generated = 0;

    await this.scheduleRepo.processScheduleWithLock(
      schedule.id,
      async (lockedSchedule, client) => {
        if (lockedSchedule.status !== 'ACTIVE') {
          this.logger.info('Schedule no longer active, skipping', {
            scheduleId: lockedSchedule.id,
            status: lockedSchedule.status,
          });
          return;
        }

        const dbNowResult = await client.query('SELECT NOW() as db_now');
        const dbNow = new Date(dbNowResult.rows[0].db_now as string);

        if (lockedSchedule.nextRunAt > dbNow) {
          return;
        }

        generated = await this.applyMisfirePolicy(lockedSchedule, dbNow, client);
      },
    );

    return generated;
  }

  private async applyMisfirePolicy(
    schedule: RecurringSchedule,
    dbNow: Date,
    client: import('pg').PoolClient,
  ): Promise<number> {
    const { misfirePolicy } = schedule;
    let generated = 0;

    switch (misfirePolicy) {
      case 'SKIP_MISSED': {
        // Run the most recent occurrence only if it is on time; never make up
        // for missed ones. Running the oldest missed occurrence after downtime
        // is RUN_ONCE's behaviour, not this policy's.
        const nextFuture = getNextOccurrence(schedule.cronExpression, schedule.timezone, dbNow);
        const latest = getLatestOccurrence(schedule.cronExpression, schedule.timezone, dbNow);
        const scheduledFor = latest > schedule.nextRunAt ? latest : schedule.nextRunAt;
        const lateMs = dbNow.getTime() - scheduledFor.getTime();

        if (lateMs > this.config.misfireThresholdMs) {
          await client.query(
            `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
             WHERE id = $1`,
            [schedule.id, nextFuture],
          );
          this.logger.info('Missed occurrences skipped (SKIP_MISSED)', {
            scheduleId: schedule.id,
            scheduleName: schedule.name,
            missedFrom: schedule.nextRunAt.toISOString(),
            lateMs,
            nextRunAt: nextFuture.toISOString(),
          });
          break;
        }

        try {
          await this.scheduleRepo.generateOccurrence(
            schedule.id,
            scheduledFor,
            nextFuture,
            client,
          );
          generated = 1;
          this.logger.info('Recurring occurrence generated (SKIP_MISSED)', {
            scheduleId: schedule.id,
            scheduleName: schedule.name,
            scheduledFor: scheduledFor.toISOString(),
            misfirePolicy: 'SKIP_MISSED',
          });
        } catch (error) {
          if (error instanceof DuplicateOccurrenceError) {
            this.logger.info('Duplicate occurrence skipped', {
              scheduleId: schedule.id,
              scheduledFor: scheduledFor.toISOString(),
            });
            await client.query(
              `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
               WHERE id = $1`,
              [schedule.id, nextFuture],
            );
          } else {
            throw error;
          }
        }
        break;
      }

      case 'RUN_ONCE': {
        const nextFuture = getNextOccurrence(schedule.cronExpression, schedule.timezone, dbNow);
        const scheduledFor = schedule.nextRunAt;

        try {
          await this.scheduleRepo.generateOccurrence(
            schedule.id,
            scheduledFor,
            nextFuture,
            client,
          );
          generated = 1;
          this.logger.info('Recurring occurrence generated (RUN_ONCE)', {
            scheduleId: schedule.id,
            scheduleName: schedule.name,
            scheduledFor: scheduledFor.toISOString(),
            misfirePolicy: 'RUN_ONCE',
          });
        } catch (error) {
          if (error instanceof DuplicateOccurrenceError) {
            this.logger.info('Duplicate occurrence skipped', {
              scheduleId: schedule.id,
              scheduledFor: scheduledFor.toISOString(),
            });
            await client.query(
              `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
               WHERE id = $1`,
              [schedule.id, nextFuture],
            );
          } else {
            throw error;
          }
        }
        break;
      }

      case 'CATCH_UP_ALL': {
        const missed = getMissedOccurrences(
          schedule.cronExpression,
          schedule.timezone,
          new Date(schedule.nextRunAt.getTime() - 1000),
          dbNow,
          this.config.catchUpBatchSize,
        );

        if (missed.length === 0) {
          const nextFuture = getNextOccurrence(schedule.cronExpression, schedule.timezone, dbNow);
          await client.query(
            `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
             WHERE id = $1`,
            [schedule.id, nextFuture],
          );
          break;
        }

        for (const occurrenceTime of missed) {
          const isLast = occurrenceTime === missed[missed.length - 1];
          let nextRunAt: Date;

          if (isLast) {
            const remainingMissed = getMissedOccurrences(
              schedule.cronExpression,
              schedule.timezone,
              occurrenceTime,
              dbNow,
              1,
            );
            if (remainingMissed.length > 0 && remainingMissed[0].getTime() > occurrenceTime.getTime()) {
              nextRunAt = remainingMissed[0];
            } else {
              nextRunAt = getNextOccurrence(schedule.cronExpression, schedule.timezone, dbNow);
            }
          } else {
            nextRunAt = missed[missed.indexOf(occurrenceTime) + 1];
          }

          try {
            await this.scheduleRepo.generateOccurrence(
              schedule.id,
              occurrenceTime,
              nextRunAt,
              client,
            );
            generated++;
            this.logger.info('Catch-up occurrence generated', {
              scheduleId: schedule.id,
              scheduleName: schedule.name,
              scheduledFor: occurrenceTime.toISOString(),
              misfirePolicy: 'CATCH_UP_ALL',
              batchPosition: generated,
            });
          } catch (error) {
            if (error instanceof DuplicateOccurrenceError) {
              this.logger.info('Duplicate catch-up occurrence skipped', {
                scheduleId: schedule.id,
                scheduledFor: occurrenceTime.toISOString(),
              });
              if (isLast) {
                await client.query(
                  `UPDATE recurring_schedules SET next_run_at = $2, version = version + 1, updated_at = NOW()
                   WHERE id = $1`,
                  [schedule.id, nextRunAt],
                );
              }
            } else {
              throw error;
            }
          }
        }

        if (generated > 0) {
          this.logger.info('Catch-up batch processed', {
            scheduleId: schedule.id,
            scheduleName: schedule.name,
            count: generated,
            batchLimit: this.config.catchUpBatchSize,
          });
        }
        break;
      }
    }

    return generated;
  }
}
