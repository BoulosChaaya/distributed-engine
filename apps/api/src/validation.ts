import { z } from 'zod';

export const SubmitTaskSchema = z.object({
  name: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  payload: z.record(z.unknown()).optional().default({}),
  priority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  maxRetries: z.number().int().min(0).max(10).optional().default(3),
  scheduledFor: z.string().datetime().optional(),
});

export type SubmitTaskInput = z.infer<typeof SubmitTaskSchema>;

export const PaginationSchema = z.object({
  page: z.coerce.number().int().positive().optional().default(1),
  pageSize: z.coerce.number().int().min(1).max(100).optional().default(10),
});

export type PaginationInput = z.infer<typeof PaginationSchema>;

export const CreateScheduleSchema = z.object({
  name: z.string().min(1, 'Schedule name is required').max(255, 'Schedule name too long'),
  taskName: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  taskPriority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  taskPayload: z.record(z.unknown()).optional().default({}),
  taskMaxRetries: z.number().int().min(0).max(10).optional().default(3),
  cronExpression: z.string().min(1, 'Cron expression is required'),
  timezone: z.string().min(1, 'Timezone is required').default('UTC'),
  misfirePolicy: z.enum(['CATCH_UP_ALL', 'SKIP_MISSED', 'RUN_ONCE']).optional().default('SKIP_MISSED'),
  overlapPolicy: z.enum(['ALLOW_OVERLAP', 'FORBID_OVERLAP']).optional().default('ALLOW_OVERLAP'),
});

export type CreateScheduleInput = z.infer<typeof CreateScheduleSchema>;

export const UpdateScheduleSchema = z.object({
  version: z.number().int().positive('Version is required for optimistic concurrency'),
  name: z.string().min(1).max(255).optional(),
  taskName: z.string().min(1).max(255).optional(),
  taskPriority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional(),
  taskPayload: z.record(z.unknown()).optional(),
  taskMaxRetries: z.number().int().min(0).max(10).optional(),
  cronExpression: z.string().min(1).optional(),
  timezone: z.string().min(1).optional(),
  misfirePolicy: z.enum(['CATCH_UP_ALL', 'SKIP_MISSED', 'RUN_ONCE']).optional(),
  overlapPolicy: z.enum(['ALLOW_OVERLAP', 'FORBID_OVERLAP']).optional(),
});

export type UpdateScheduleInputType = z.infer<typeof UpdateScheduleSchema>;

export const SetScheduleStatusSchema = z.object({
  version: z.number().int().positive('Version is required for optimistic concurrency'),
  status: z.enum(['ACTIVE', 'PAUSED', 'DISABLED']),
});

export type SetScheduleStatusInput = z.infer<typeof SetScheduleStatusSchema>;

export class ValidationError extends Error {
  constructor(public errors: z.ZodError) {
    super('Validation error');
    this.name = 'ValidationError';
  }

  toJSON() {
    return {
      errors: this.errors.issues.map((issue) => ({
        field: issue.path.join('.'),
        message: issue.message,
      })),
    };
  }
}
