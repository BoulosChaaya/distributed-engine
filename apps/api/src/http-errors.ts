import { Request, Response, NextFunction } from 'express';
import { AppError } from '@repo/shared';
import { ValidationError, isZodError } from './validation.js';

interface ErrorLogger {
  error(msg: string, meta?: Record<string, unknown>): void;
}

/**
 * Unknown routes answer in the API's JSON envelope. Express's default is an
 * HTML page, which a JSON client cannot parse and which says nothing useful.
 */
export function notFoundHandler(req: Request, res: Response): void {
  res.status(404).json({
    success: false,
    error: `Route ${req.method} ${req.path} not found`,
    timestamp: new Date(),
  });
}

/**
 * Errors raised by express.json() before any route runs. body-parser marks
 * them with `type` and a 4xx `status`; they are the client's fault, so they
 * must not become a 500 (which clients retry and alerts count as outages).
 */
function bodyParserError(err: unknown): { status: number; message: string } | null {
  const e = err as { type?: string; status?: number; limit?: number } | null;
  if (!e || typeof e !== 'object' || typeof e.status !== 'number' || e.status < 400 || e.status >= 500) return null;
  switch (e.type) {
    case 'entity.parse.failed':
      return { status: 400, message: 'Malformed JSON body' };
    case 'entity.too.large':
      return { status: 413, message: `Request body too large (limit ${e.limit ?? 'exceeded'} bytes)` };
    case 'encoding.unsupported':
    case 'charset.unsupported':
      return { status: 415, message: 'Unsupported request body encoding' };
    default:
      return e.type ? { status: e.status, message: 'Bad request body' } : null;
  }
}

export function createErrorHandler(deps: { logger: ErrorLogger; recordError: () => void }) {
  return (err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    deps.recordError();

    // Any route that let a ZodError through (e.g. bad query strings on list
    // routes) is still a client error, never a 500.
    if (isZodError(err)) {
      err = new ValidationError(err);
    }

    if (err instanceof ValidationError) {
      return res.status(422).json({
        success: false,
        error: 'Validation error',
        details: err.toJSON(),
        timestamp: new Date(),
      });
    }

    if (err instanceof AppError) {
      return res.status(err.statusCode).json({
        success: false,
        error: err.message,
        timestamp: new Date(),
      });
    }

    const clientError = bodyParserError(err);
    if (clientError) {
      return res.status(clientError.status).json({
        success: false,
        error: clientError.message,
        timestamp: new Date(),
      });
    }

    deps.logger.error('Unhandled error', {
      errorType: err instanceof Error ? err.name : 'UnknownError',
      reason: err instanceof Error ? err.message : String(err),
    });
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      timestamp: new Date(),
    });
  };
}
