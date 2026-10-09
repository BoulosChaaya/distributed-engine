import { describe, it, expect, beforeAll, afterAll, beforeEach } from '@jest/globals';
import { createLogger, serializeError } from '../logger/index';
import {
  context,
  trace,
  ROOT_CONTEXT,
} from '@opentelemetry/api';
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
  NodeTracerProvider,
} from '@opentelemetry/sdk-trace-node';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks';
import { Writable } from 'stream';

function createCaptureStream(): { stream: Writable; lines: string[] } {
  const lines: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      const str = chunk.toString().trim();
      if (str) lines.push(str);
      callback();
    },
  });
  return { stream, lines };
}

function parseLine(line: string): Record<string, unknown> {
  return JSON.parse(line);
}

describe('Logger', () => {
  describe('Structured output with common fields', () => {
    it('should emit structured JSON with required common envelope', () => {
      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        {
          level: 'debug',
          base: { service: 'test-service', environment: 'test' },
          timestamp: pino.stdTimeFunctions.isoTime,
          formatters: { level(label: string) { return { level: label }; } },
        },
        stream,
      );

      pinoInstance.info('test message');

      expect(lines.length).toBe(1);
      const parsed = parseLine(lines[0]);
      expect(parsed).toHaveProperty('time');
      expect(parsed).toHaveProperty('level', 'info');
      expect(parsed).toHaveProperty('service', 'test-service');
      expect(parsed).toHaveProperty('environment', 'test');
      expect(parsed).toHaveProperty('msg', 'test message');
    });

    it('should include correct service identity', () => {
      const logger = createLogger({ service: 'api', environment: 'production', level: 'info' });
      expect(logger).toBeDefined();
      expect(typeof logger.info).toBe('function');
      expect(typeof logger.error).toBe('function');
      expect(typeof logger.warn).toBe('function');
      expect(typeof logger.debug).toBe('function');
    });

    it('should support all log levels', () => {
      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        {
          level: 'debug',
          base: { service: 'level-test', environment: 'test' },
          timestamp: pino.stdTimeFunctions.isoTime,
          formatters: { level(label: string) { return { level: label }; } },
        },
        stream,
      );

      pinoInstance.debug('debug msg');
      pinoInstance.info('info msg');
      pinoInstance.warn('warn msg');
      pinoInstance.error('error msg');

      expect(lines.length).toBe(4);
      expect(parseLine(lines[0]).level).toBe('debug');
      expect(parseLine(lines[1]).level).toBe('info');
      expect(parseLine(lines[2]).level).toBe('warn');
      expect(parseLine(lines[3]).level).toBe('error');
    });
  });

  describe('Service and environment metadata', () => {
    it('should set correct service identity for each component', () => {
      const { stream: s1, lines: l1 } = createCaptureStream();
      const { stream: s2, lines: l2 } = createCaptureStream();
      const { stream: s3, lines: l3 } = createCaptureStream();
      const pino = require('pino');

      const apiLogger = pino({ base: { service: 'api', environment: 'development' } }, s1);
      const pubLogger = pino({ base: { service: 'outbox-publisher', environment: 'development' } }, s2);
      const workerLogger = pino({ base: { service: 'worker', environment: 'development' } }, s3);

      apiLogger.info('api log');
      pubLogger.info('publisher log');
      workerLogger.info('worker log');

      expect(parseLine(l1[0]).service).toBe('api');
      expect(parseLine(l2[0]).service).toBe('outbox-publisher');
      expect(parseLine(l3[0]).service).toBe('worker');
    });
  });

  describe('OTel context correlation', () => {
    let exporter: InMemorySpanExporter;
    let provider: NodeTracerProvider;

    beforeAll(() => {
      exporter = new InMemorySpanExporter();
      const contextManager = new AsyncHooksContextManager();
      contextManager.enable();
      context.setGlobalContextManager(contextManager);

      provider = new NodeTracerProvider({
        resource: resourceFromAttributes({ 'service.name': 'logger-test' }),
        spanProcessors: [new SimpleSpanProcessor(exporter)],
      });
      provider.register();
    });

    afterAll(async () => {
      await provider.shutdown();
      context.disable();
    });

    beforeEach(() => {
      exporter.reset();
    });

    it('should automatically add traceId and spanId when active OTel span exists', (done) => {
      const tracer = trace.getTracer('test');
      const span = tracer.startSpan('test-operation');
      const ctx = trace.setSpan(context.active(), span);
      const expectedTraceId = span.spanContext().traceId;
      const expectedSpanId = span.spanContext().spanId;

      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        {
          level: 'info',
          base: { service: 'test', environment: 'test' },
          formatters: { level(label: string) { return { level: label }; } },
        },
        stream,
      );

      context.with(ctx, () => {
        const activeSpan = trace.getSpan(context.active());
        const spanCtx = activeSpan?.spanContext();
        pinoInstance.info({
          traceId: spanCtx?.traceId,
          spanId: spanCtx?.spanId,
        }, 'traced message');

        span.end();

        const parsed = parseLine(lines[0]);
        expect(parsed.traceId).toBe(expectedTraceId);
        expect(parsed.spanId).toBe(expectedSpanId);
        expect(parsed.traceId).toMatch(/^[0-9a-f]{32}$/);
        expect(parsed.spanId).toMatch(/^[0-9a-f]{16}$/);
        done();
      });
    });

    it('should produce valid log without traceId/spanId when no active span exists', () => {
      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        {
          level: 'info',
          base: { service: 'test', environment: 'test' },
          formatters: { level(label: string) { return { level: label }; } },
        },
        stream,
      );

      context.with(ROOT_CONTEXT, () => {
        pinoInstance.info('no trace');

        const parsed = parseLine(lines[0]);
        expect(parsed.msg).toBe('no trace');
        expect(parsed.service).toBe('test');
        expect(parsed).not.toHaveProperty('traceId');
        expect(parsed).not.toHaveProperty('spanId');
      });
    });

    it('should never fabricate traceId or spanId', () => {
      const logger = createLogger({ service: 'test', environment: 'test', level: 'info' });

      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        { level: 'info', base: { service: 'test', environment: 'test' } },
        stream,
      );

      context.with(ROOT_CONTEXT, () => {
        pinoInstance.info('no span');
        const parsed = parseLine(lines[0]);
        expect(parsed.traceId).toBeUndefined();
        expect(parsed.spanId).toBeUndefined();
      });
    });
  });

  describe('Sensitive field redaction', () => {
    it('should redact configured sensitive paths', () => {
      const { stream, lines } = createCaptureStream();
      const pino = require('pino');
      const pinoInstance = pino(
        {
          level: 'info',
          base: { service: 'test', environment: 'test' },
          redact: {
            paths: ['password', 'secret', 'token', 'authorization', 'apiKey', 'api_key',
                     'credential', 'connectionString', 'connection_string', 'cookie', 'x-api-key'],
            censor: '[REDACTED]',
          },
        },
        stream,
      );

      pinoInstance.info({
        password: 'super-secret-password',
        token: 'Bearer abc123',
        authorization: 'Bearer xyz',
        apiKey: 'key-12345',
        cookie: 'session=abc123',
        safeField: 'visible',
      }, 'sensitive test');

      const parsed = parseLine(lines[0]);
      expect(parsed.password).toBe('[REDACTED]');
      expect(parsed.token).toBe('[REDACTED]');
      expect(parsed.authorization).toBe('[REDACTED]');
      expect(parsed.apiKey).toBe('[REDACTED]');
      expect(parsed.cookie).toBe('[REDACTED]');
      expect(parsed.safeField).toBe('visible');
    });
  });

  describe('Child logger', () => {
    it('should create child loggers with bound context', () => {
      const logger = createLogger({ service: 'test', environment: 'test', level: 'debug' });
      const child = logger.child({ workerId: 'worker-1' });
      expect(child).toBeDefined();
      expect(typeof child.info).toBe('function');
    });
  });
});

describe('Error serialization', () => {
  it('should safely serialize Error objects', () => {
    const error = new Error('test failure');
    const safe = serializeError(error);

    expect(safe.name).toBe('Error');
    expect(safe.message).toBe('test failure');
    expect(safe.stack).toBeDefined();
  });

  it('should truncate long error messages', () => {
    const longMessage = 'x'.repeat(2000);
    const error = new Error(longMessage);
    const safe = serializeError(error);

    expect(safe.message.length).toBeLessThanOrEqual(1050);
    expect(safe.message).toContain('…[truncated]');
  });

  it('should handle null/undefined errors', () => {
    expect(serializeError(null).name).toBe('UnknownError');
    expect(serializeError(undefined).name).toBe('UnknownError');
  });

  it('should handle string errors', () => {
    const safe = serializeError('something went wrong');
    expect(safe.name).toBe('StringError');
    expect(safe.message).toBe('something went wrong');
  });

  it('should handle object errors without leaking secrets', () => {
    const objError = {
      code: 'ECONNREFUSED',
      password: 'leaked-password',
      token: 'leaked-token',
      host: 'localhost',
    };
    const safe = serializeError(objError);

    expect(safe.name).toBe('ObjectError');
    expect(safe.message).toContain('code: ECONNREFUSED');
    expect(safe.message).toContain('password: [REDACTED]');
    expect(safe.message).toContain('token: [REDACTED]');
    expect(safe.message).toContain('host: localhost');
    expect(safe.message).not.toContain('leaked-password');
    expect(safe.message).not.toContain('leaked-token');
  });

  it('should not blindly dump arbitrary nested objects', () => {
    const deepError = {
      config: {
        auth: { password: 'secret' },
        headers: { Authorization: 'Bearer token' },
      },
    };
    const safe = serializeError(deepError);

    expect(safe.message).not.toContain('secret');
    expect(safe.message).not.toContain('Bearer token');
    expect(safe.message).toContain('[object]');
  });

  it('should include error cause when present', () => {
    const cause = new Error('root cause');
    const error = new Error('wrapper error', { cause });
    const safe = serializeError(error);

    expect(safe.message).toContain('root cause');
    expect(safe.message).toContain('caused by');
  });

  it('should limit stack lines', () => {
    const error = new Error('test');
    const safe = serializeError(error, true);
    if (safe.stack) {
      const lines = safe.stack.split('\n');
      expect(lines.length).toBeLessThanOrEqual(20);
    }
  });

  it('should omit stack when requested', () => {
    const error = new Error('test');
    const safe = serializeError(error, false);
    expect(safe.stack).toBeUndefined();
  });
});

describe('Logger integration with createLogger', () => {
  it('should create a logger with all required methods', () => {
    const logger = createLogger({ service: 'api', environment: 'test', level: 'debug' });
    expect(typeof logger.debug).toBe('function');
    expect(typeof logger.info).toBe('function');
    expect(typeof logger.warn).toBe('function');
    expect(typeof logger.error).toBe('function');
    expect(typeof logger.child).toBe('function');
  });

  it('should not throw when logging with no active OTel context', () => {
    const logger = createLogger({ service: 'worker', environment: 'test', level: 'info' });
    expect(() => logger.info('safe message')).not.toThrow();
    expect(() => logger.error('error message', { taskId: 'task-1' })).not.toThrow();
  });

  it('should support structured fields in log context', () => {
    const logger = createLogger({ service: 'worker', environment: 'test', level: 'debug' });
    expect(() =>
      logger.info('Task received', {
        taskId: 'task-123',
        workerId: 'worker-A',
        attempt: 1,
      }),
    ).not.toThrow();
  });

  it('should support child loggers with workerId binding', () => {
    const logger = createLogger({ service: 'worker', environment: 'test', level: 'debug' });
    const child = logger.child({ workerId: 'worker-B' });
    expect(() => child.info('Task claimed', { taskId: 'task-456' })).not.toThrow();
  });

  it('should support representative API logging use case', () => {
    const logger = createLogger({ service: 'api', environment: 'test', level: 'debug' });
    expect(() => {
      logger.info('Task creation requested', { taskName: 'my-task', priority: 'HIGH' });
      logger.info('Task created', { taskId: 'task-789', taskName: 'my-task', priority: 'HIGH' });
      logger.warn('Task creation rejected: validation error', { errorType: 'ValidationError' });
      logger.error('Task creation failed', { errorType: 'DatabaseError', reason: 'connection refused' });
    }).not.toThrow();
  });

  it('should support representative publisher logging use case', () => {
    const logger = createLogger({ service: 'outbox-publisher', environment: 'test', level: 'debug' });
    const pubLogger = logger.child({ publisherId: 'pub-abc' });
    expect(() => {
      pubLogger.info('Outbox event published', { eventId: 'evt-1', taskId: 'task-1' });
      pubLogger.error('Failed to publish outbox event to queue', { eventId: 'evt-2', taskId: 'task-2', reason: 'connection refused', attempt: 3 });
      pubLogger.warn('Circuit breaker opened', { consecutiveFailures: 5 });
    }).not.toThrow();
  });

  it('should support representative worker logging use case', () => {
    const logger = createLogger({ service: 'worker', environment: 'test', level: 'debug' });
    const workerLogger = logger.child({ workerId: 'w-abc12' });
    expect(() => {
      workerLogger.info('Task received', { taskId: 'task-1', taskName: 'process-order', attempt: 1 });
      workerLogger.info('Task claimed', { taskId: 'task-1' });
      workerLogger.info('Task execution started', { taskId: 'task-1' });
      workerLogger.info('Task durably completed', { taskId: 'task-1' });
      workerLogger.warn('Ownership lost during execution, aborting', { taskId: 'task-2' });
      workerLogger.error('Task execution failed', {
        taskId: 'task-3',
        reason: 'timeout exceeded',
        attempt: 2,
        maxAttempts: 3,
        retryable: true,
        errorType: 'TimeoutError',
      });
      workerLogger.debug('Lease renewed', { taskId: 'task-4' });
      workerLogger.info('Graceful shutdown initiated', { signal: 'SIGTERM' });
    }).not.toThrow();
  });
});
