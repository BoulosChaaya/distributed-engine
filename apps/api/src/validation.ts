import { z } from 'zod';

// Task submission validation
export const SubmitTaskSchema = z.object({
  name: z.string().min(1, 'Task name is required').max(255, 'Task name too long'),
  payload: z.record(z.unknown()).optional().default({}),
  priority: z.enum(['LOW', 'NORMAL', 'HIGH', 'CRITICAL']).optional().default('NORMAL'),
  maxRetries: z.number().int().min(0).max(10).optional().default(3),
});

export type SubmitTaskInput = z.infer<typeof SubmitTaskSchema>;

// Pagination validation
export const PaginationSchema = z.object({
  page: z.coerce.number().int().positive().optional().default(1),
  pageSize: z.coerce.number().int().min(1).max(100).optional().default(10),
});

export type PaginationInput = z.infer<typeof PaginationSchema>;

// Validation error handler
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
