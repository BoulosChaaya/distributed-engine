import pino from 'pino';
import { trace, context } from '@opentelemetry/api';
import { serializeError } from './error-serializer';

export interface LoggerOptions {
  service: string;
  environment?: string;
  level?: string;
  destination?: import('stream').Writable;
}

export interface Logger {
  debug(msg: string, data?: Record<string, unknown>): void;
  info(msg: string, data?: Record<string, unknown>): void;
  warn(msg: string, data?: Record<string, unknown>): void;
  error(msg: string, data?: Record<string, unknown>): void;
  child(bindings: Record<string, unknown>): Logger;
}

const REDACT_PATHS = [
  'password',
  'secret',
  'token',
  'authorization',
  'apiKey',
  'api_key',
  'credential',
  'connectionString',
  'connection_string',
  'cookie',
  'x-api-key',
];

function getActiveTraceContext(): { traceId?: string; spanId?: string } {
  try {
    const activeSpan = trace.getSpan(context.active());
    if (!activeSpan) return {};

    const spanCtx = activeSpan.spanContext();
    if (!spanCtx) return {};

    const { traceId, spanId } = spanCtx;
    const invalidTraceId = '00000000000000000000000000000000';
    const invalidSpanId = '0000000000000000';

    if (!traceId || traceId === invalidTraceId) return {};
    if (!spanId || spanId === invalidSpanId) return { traceId };

    return { traceId, spanId };
  } catch {
    return {};
  }
}

function wrapChild(pinoChild: pino.Logger): Logger {
  return {
    debug(msg, data) {
      const traceCtx = getActiveTraceContext();
      pinoChild.debug({ ...data, ...traceCtx }, msg);
    },
    info(msg, data) {
      const traceCtx = getActiveTraceContext();
      pinoChild.info({ ...data, ...traceCtx }, msg);
    },
    warn(msg, data) {
      const traceCtx = getActiveTraceContext();
      pinoChild.warn({ ...data, ...traceCtx }, msg);
    },
    error(msg, data) {
      const traceCtx = getActiveTraceContext();
      pinoChild.error({ ...data, ...traceCtx }, msg);
    },
    child(bindings) {
      return wrapChild(pinoChild.child(bindings));
    },
  };
}

export function createLogger(options: LoggerOptions): Logger {
  const { service, environment, level, destination } = options;

  const pinoOptions: pino.LoggerOptions = {
    level: (level ?? process.env.LOG_LEVEL ?? 'info').toLowerCase(),
    redact: {
      paths: REDACT_PATHS,
      censor: '[REDACTED]',
    },
    base: {
      service,
      environment: environment ?? process.env.NODE_ENV ?? 'development',
    },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level(label) {
        return { level: label };
      },
    },
    serializers: {
      err: (err) => serializeError(err),
      error: (err) => {
        if (typeof err === 'string') return err;
        return serializeError(err);
      },
    },
  };

  const pinoInstance = destination ? pino(pinoOptions, destination) : pino(pinoOptions);

  return wrapChild(pinoInstance);
}
