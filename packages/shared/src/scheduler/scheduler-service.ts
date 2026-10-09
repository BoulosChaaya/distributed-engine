import { Pool } from 'pg';
import { ScheduleRepository, DuplicateOccurrenceError } from '../db/schedule-repository';
import { RecurringSchedule } from '../types';
import { getNextOccurrence, getMissedOccurrences } from './cron-utils';
import { type Logger } from '../logger/index';

export interface SchedulerConfig {
  pollIntervalMs: number;
  scheduledTaskBatchSize: number;
  recurringBatchSize: number;
  catchUpBatchSize: number;
  executionLeaseDurationMs: number;
}

const DEFAULT_CONFIG: SchedulerConfig = {
  pollIntervalMs: 5000,
  scheduledTaskBatchSize: 50,
  recurringBatchSize: 20,
  catchUpBatchSize: 10,
  executionLeaseDurationMs: 300000,
};

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

  private schedulePoll(): void {
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
      this.schedulePoll();
    }, this.config.pollIntervalMs);
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

        if (lockedSchedule.overlapPolicy === 'FORBID_OVERLAP') {
          const hasActive = await this.scheduleRepo.hasActiveOccurrence(lockedSchedule.id);
          if (hasActive) {
            const leaseResult = await this.scheduleRepo.acquireExecutionLease(
              lockedSchedule.id,
              this.config.executionLeaseDurationMs,
            );
            if (!leaseResult.acquired) {
              this.logger.info('Occurrence deferred due to overlap policy', {
                scheduleId: lockedSchedule.id,
                scheduleName: lockedSchedule.name,
              });
              return;
            }
            await this.scheduleRepo.releaseExecutionLease(
              lockedSchedule.id,
              leaseResult.leaseToken!,
            );
            this.logger.info('Occurrence deferred due to overlap policy', {
              scheduleId: lockedSchedule.id,
              scheduleName: lockedSchedule.name,
            });
            return;
          }
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
