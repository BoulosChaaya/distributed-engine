import { z } from 'zod';

export const SubmitTaskSchema = z.object({
  name: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  payload: z.record(z.string(), z.unknown()).optional().default({}),
  priority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  maxRetries: z.number().int().min(0).max(10).optional().default(3),
  scheduledFor: z.string().datetime().optional(),
  // Same key + same body returns the first task (200); same key + another
  // body is refused (409). Without it, a client that never saw the 201
  // could only resend and create a second task.
  idempotencyKey: z.string().min(1).max(255).optional(),
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
  taskPayload: z.record(z.string(), z.unknown()).optional().default({}),
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
  taskPayload: z.record(z.string(), z.unknown()).optional(),
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

export const TenantSubmitTaskSchema = z.object({
  name: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  payload: z.record(z.string(), z.unknown()).optional().default({}),
  priority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  maxRetries: z.number().int().min(0).max(10).optional().default(3),
  idempotencyKey: z.string().max(255).optional(),
  scheduledFor: z.string().datetime().optional(),
});

export type TenantSubmitTaskInput = z.infer<typeof TenantSubmitTaskSchema>;

export const TenantCreateScheduleSchema = z.object({
  name: z.string().min(1, 'Schedule name is required').max(255, 'Schedule name too long'),
  taskName: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  taskPriority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  taskPayload: z.record(z.string(), z.unknown()).optional().default({}),
  taskMaxRetries: z.number().int().min(0).max(10).optional().default(3),
  cronExpression: z.string().min(1, 'Cron expression is required'),
  timezone: z.string().min(1, 'Timezone is required').default('UTC'),
  misfirePolicy: z.enum(['CATCH_UP_ALL', 'SKIP_MISSED', 'RUN_ONCE']).optional().default('SKIP_MISSED'),
  overlapPolicy: z.enum(['ALLOW_OVERLAP', 'FORBID_OVERLAP']).optional().default('ALLOW_OVERLAP'),
});

export type TenantCreateScheduleInput = z.infer<typeof TenantCreateScheduleSchema>;

/**
 * Matched by name, not instanceof: a schema built by another copy of zod
 * (a dependency's) throws a ZodError from a different class.
 */
export function isZodError(error: unknown): error is z.ZodError {
  return error instanceof Error && error.name === 'ZodError';
}

export class ValidationError extends Error {
  constructor(public errors: z.ZodError) {
    super('Validation error');
    this.name = 'ValidationError';
  }

  toJSON() {
    return {
      errors: this.errors.issues.map((issue) => ({
        field: issue.path.map(String).join('.'),
        message: issue.message,
      })),
    };
  }
}

export const CreateTenantSchema = z.object({
  name: z.string().trim().min(1, 'Tenant name is required').max(255, 'Tenant name too long'),
  planId: z.string().min(1, 'planId is required'),
});

export const SetTenantStatusSchema = z.object({
  status: z.enum(['ACTIVE', 'SUSPENDED']),
});

const WorkflowStepSchema = z.object({
  key: z.string().regex(/^[A-Za-z0-9_-]{1,64}$/, 'Step key must be 1-64 of A-Z a-z 0-9 _ -'),
  name: z.string().min(1, 'Step name (task type) is required').max(255),
  payload: z.record(z.string(), z.unknown()).optional().default({}),
  priority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  maxRetries: z.number().int().min(0).max(10).optional().default(3),
  dependsOn: z.array(z.string()).max(200).optional().default([]),
});

export const CreateWorkflowSchema = z.object({
  name: z.string().trim().min(1, 'Workflow name is required').max(255),
  steps: z.array(WorkflowStepSchema).min(1).max(200),
  idempotencyKey: z.string().min(1).max(255).optional(),
});
