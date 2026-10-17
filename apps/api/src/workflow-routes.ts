import { Router, Request, Response, NextFunction, RequestHandler } from 'express';
import type { Queue } from 'bullmq';
import {
  AppError,
  WorkflowRepository,
  WorkflowStateError,
  WorkflowValidationError,
  TenantRepository,
  QuotaExceededError,
  IdempotencyConflictError,
  computeBillingPeriodStart,
} from '@repo/shared';
import { CreateWorkflowSchema, PaginationSchema, ValidationError } from './validation.js';
import { routeId } from './route-params.js';

/**
 * Workflow (DAG) routes. The same handlers serve the operator surface
 * (/workflows, every tenant's workflows) and the tenant surface
 * (/tenant/workflows, scoped to the caller's tenant and charged against its
 * quota). Which one applies is decided by whether req.tenant was set by
 * tenant auth; it is never taken from the request body.
 */

type JobRemover = Pick<Queue, 'getJob'>;

function wrap(fn: (req: Request, res: Response) => Promise<unknown>): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      await fn(req, res);
    } catch (error) {
      if (error instanceof Error && error.name === 'ZodError') next(new ValidationError(error as any));
      else if (error instanceof WorkflowValidationError) next(new AppError(422, error.message));
      else if (error instanceof WorkflowStateError) next(new AppError(409, error.message));
      else if (error instanceof IdempotencyConflictError) next(new AppError(409, error.message));
      else if (error instanceof QuotaExceededError) next(new AppError(429, error.message));
      else if (error instanceof Error && error.message.includes('not found')) next(new AppError(404, error.message));
      else next(error);
    }
  };
}

export function createWorkflowRouter(
  workflows: WorkflowRepository,
  tenants: TenantRepository,
  queue: JobRemover,
  onCancelError: (taskId: string, error: unknown) => void = () => {},
): Router {
  const router = Router();

  router.post('/', wrap(async (req, res) => {
    const input = CreateWorkflowSchema.parse(req.body);
    let quota: { billingPeriodStart: Date; maxJobsPerPeriod: number } | undefined;
    if (req.tenant) {
      const limits = await tenants.getEffectiveLimits(req.tenant.id);
      quota = {
        billingPeriodStart: computeBillingPeriodStart(req.tenant.createdAt, limits.billingPeriodDays, new Date()),
        maxJobsPerPeriod: limits.maxJobsPerPeriod,
      };
    }
    const { detail, idempotent } = await workflows.createWorkflow({
      name: input.name,
      steps: input.steps,
      idempotencyKey: input.idempotencyKey,
      tenantId: req.tenant?.id,
      ...quota,
    });
    res.status(idempotent ? 200 : 201).json({ success: true, data: detail, idempotent, timestamp: new Date() });
  }));

  router.get('/', wrap(async (req, res) => {
    const { page, pageSize } = PaginationSchema.parse(req.query);
    const { items, total } = await workflows.listWorkflows(page, pageSize, req.tenant?.id);
    res.json({
      success: true,
      data: { items, total, page, pageSize, hasMore: (page - 1) * pageSize + items.length < total },
      timestamp: new Date(),
    });
  }));

  router.get('/:id', wrap(async (req, res) => {
    const detail = await workflows.getWorkflow(routeId(req), req.tenant?.id);
    if (!detail) throw new AppError(404, `Workflow ${routeId(req)} not found`);
    res.json({ success: true, data: detail, timestamp: new Date() });
  }));

  router.put('/:id/cancel', wrap(async (req, res) => {
    const result = await workflows.cancelWorkflow(routeId(req), req.tenant?.id);
    // PostgreSQL is already authoritative; dropping queued jobs only saves
    // workers a no-op (they skip CANCELLED tasks anyway).
    for (const taskId of result.cancelledTaskIds) {
      try {
        const job = await queue.getJob(taskId);
        if (job) await job.remove();
      } catch (error) {
        onCancelError(taskId, error);
      }
    }
    res.json({ success: true, data: result, timestamp: new Date() });
  }));

  return router;
}
