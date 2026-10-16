import { Request, Response, NextFunction, RequestHandler } from 'express';
import { createHash, timingSafeEqual } from 'crypto';

/**
 * Guards the operator surface (/tasks, /schedules, /workers, /metrics).
 *
 * Tenants have their own keys and their own routes under /tenant/*. The
 * operator routes see every tenant's data and can cancel anything, so they
 * need a separate credential: OPERATOR_API_KEY, sent as
 * `Authorization: Bearer <key>`.
 *
 * Fail closed: when no key is configured the operator routes answer 503
 * instead of being open. Health probes (/health, /ready, /live) and the
 * tenant API are unaffected.
 *
 * Both sides are hashed before comparing so timingSafeEqual always sees
 * equal-length buffers and the comparison leaks nothing about the key length.
 */
export const MIN_OPERATOR_KEY_LENGTH = 32;

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export function createOperatorAuth(apiKey: string | undefined): RequestHandler {
  const expected = apiKey ? digest(apiKey) : null;

  return (req: Request, res: Response, next: NextFunction) => {
    if (!expected) {
      return res.status(503).json({
        success: false,
        error: 'Operator API is disabled: OPERATOR_API_KEY is not configured',
        timestamp: new Date(),
      });
    }

    const header = req.headers.authorization;
    const provided = header && header.startsWith('Bearer ') ? header.slice(7).trim() : '';
    if (!provided || !timingSafeEqual(digest(provided), expected)) {
      res.setHeader('WWW-Authenticate', 'Bearer realm="operator"');
      return res.status(401).json({
        success: false,
        error: 'Operator authentication required',
        timestamp: new Date(),
      });
    }

    next();
  };
}
