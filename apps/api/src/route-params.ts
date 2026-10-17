import type { Request } from 'express';

/**
 * Express 5 types route params as `string | string[]` (an array only for
 * wildcard segments like `*path`). Every route here uses a plain `:id`, so
 * the value is a string; this narrows it in one audited place instead of a
 * cast at every call site.
 */
export function routeId(req: Request): string {
  const value = req.params.id;
  return Array.isArray(value) ? value.join('/') : value;
}
