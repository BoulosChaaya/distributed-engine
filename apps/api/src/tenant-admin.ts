import { Router, Request, Response, NextFunction } from 'express';
import { randomBytes } from 'crypto';
import { AppError, TenantRepository, computeBillingPeriodStart } from '@repo/shared';
import { CreateTenantSchema, SetTenantStatusSchema, ValidationError, isZodError } from './validation.js';
import { hashApiKey } from './tenant-middleware.js';
import { routeId } from './route-params.js';

/**
 * Operator-only tenant provisioning (mounted behind operator auth).
 *
 * The tenant API (/tenant/*) authenticates with per-tenant API keys, but
 * until now there was no way to create a tenant or issue a key short of
 * writing SQL by hand. These routes close that gap.
 *
 * Keys are generated server-side (256 bits, base64url, `te_` prefix so a
 * leaked key is recognisable in logs and secret scanners). Only the SHA-256
 * hash is stored. The plaintext key is returned once, at creation or
 * rotation, and can never be read back.
 */
export function generateTenantApiKey(): string {
  return `te_${randomBytes(32).toString('base64url')}`;
}

function wrap(fn: (req: Request, res: Response) => Promise<unknown>) {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      await fn(req, res);
    } catch (error) {
      if (isZodError(error)) {
        next(new ValidationError(error));
      } else if (error instanceof Error && error.message.includes('not found')) {
        next(new AppError(404, error.message));
      } else {
        next(error);
      }
    }
  };
}

export function createTenantAdminRouter(tenantRepo: TenantRepository): Router {
  const router = Router();

  router.get('/plans', wrap(async (_req, res) => {
    res.json({ success: true, data: await tenantRepo.listPlans(), timestamp: new Date() });
  }));

  router.get('/tenants', wrap(async (_req, res) => {
    res.json({ success: true, data: await tenantRepo.listTenants(), timestamp: new Date() });
  }));

  router.post('/tenants', wrap(async (req, res) => {
    const input = CreateTenantSchema.parse(req.body);
    const plan = await tenantRepo.getPlan(input.planId);
    if (!plan) throw new AppError(422, `Unknown plan "${input.planId}"`);
    const apiKey = generateTenantApiKey();
    const tenant = await tenantRepo.createTenant(input.name, plan.id, hashApiKey(apiKey));
    res.status(201).json({
      success: true,
      data: { tenant, apiKey, note: 'Store this key now; it cannot be shown again.' },
      timestamp: new Date(),
    });
  }));

  router.get('/tenants/:id', wrap(async (req, res) => {
    const tenant = await tenantRepo.getTenant(routeId(req));
    if (!tenant) throw new AppError(404, `Tenant ${routeId(req)} not found`);
    const limits = await tenantRepo.getEffectiveLimits(tenant.id);
    const periodStart = computeBillingPeriodStart(tenant.createdAt, limits.billingPeriodDays, new Date());
    const usage = await tenantRepo.getUsage(tenant.id, periodStart);
    const activeConcurrency = await tenantRepo.getActiveConcurrencyCount(tenant.id);
    res.json({
      success: true,
      data: {
        tenant,
        limits,
        usage: usage
          ? { acceptedJobs: usage.acceptedJobs, billingPeriodStart: usage.billingPeriodStart }
          : { acceptedJobs: 0, billingPeriodStart: periodStart },
        activeConcurrency,
      },
      timestamp: new Date(),
    });
  }));

  router.put('/tenants/:id/status', wrap(async (req, res) => {
    const { status } = SetTenantStatusSchema.parse(req.body);
    const tenant = await tenantRepo.setTenantStatus(routeId(req), status);
    res.json({ success: true, data: tenant, timestamp: new Date() });
  }));

  router.post('/tenants/:id/api-key', wrap(async (req, res) => {
    const apiKey = generateTenantApiKey();
    const tenant = await tenantRepo.rotateApiKey(routeId(req), hashApiKey(apiKey));
    res.json({
      success: true,
      data: { tenant, apiKey, note: 'The previous key no longer works. Store this one now.' },
      timestamp: new Date(),
    });
  }));

  return router;
}
