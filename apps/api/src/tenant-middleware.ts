import { Request, Response, NextFunction } from 'express';
import { createHash } from 'crypto';
import { TenantRepository, RedisRateLimiter } from '@repo/shared';
import type { Tenant } from '@repo/shared';

declare global {
  namespace Express {
    interface Request {
      tenant?: Tenant;
    }
  }
}

export function hashApiKey(apiKey: string): string {
  return createHash('sha256').update(apiKey).digest('hex');
}

export function createTenantAuth(tenantRepo: TenantRepository) {
  return async (req: Request, res: Response, next: NextFunction) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        error: 'Missing or invalid Authorization header',
        timestamp: new Date(),
      });
    }

    const apiKey = authHeader.slice(7);
    if (!apiKey) {
      return res.status(401).json({
        success: false,
        error: 'API key is required',
        timestamp: new Date(),
      });
    }

    const apiKeyHash = hashApiKey(apiKey);
    const tenant = await tenantRepo.getTenantByApiKeyHash(apiKeyHash);
    if (!tenant) {
      return res.status(401).json({
        success: false,
        error: 'Invalid API key',
        timestamp: new Date(),
      });
    }

    if (tenant.status === 'SUSPENDED') {
      return res.status(403).json({
        success: false,
        error: 'Tenant is suspended',
        timestamp: new Date(),
      });
    }

    req.tenant = tenant;
    next();
  };
}

export function createRateLimitMiddleware(rateLimiter: RedisRateLimiter, tenantRepo: TenantRepository) {
  return async (req: Request, res: Response, next: NextFunction) => {
    if (!req.tenant) return next();

    try {
      const limits = await tenantRepo.getEffectiveLimits(req.tenant.id);
      const result = await rateLimiter.check(`tenant:${req.tenant.id}`, limits.rateLimit);

      res.setHeader('X-RateLimit-Limit', limits.rateLimit);
      res.setHeader('X-RateLimit-Remaining', result.remaining);
      res.setHeader('X-RateLimit-Reset', Math.ceil(result.resetAtMs / 1000));

      if (!result.allowed) {
        return res.status(429).json({
          success: false,
          error: 'Rate limit exceeded',
          timestamp: new Date(),
        });
      }
    } catch {
      // fail-open
    }

    next();
  };
}
