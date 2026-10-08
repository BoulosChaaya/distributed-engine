import {
  SpanKind,
  SpanStatusCode,
  context,
  trace,
  Context,
  Span,
} from '@opentelemetry/api';
import { getTracer } from './setup';

export const tracing = {
  startTaskCreation(taskId: string, taskName: string, priority: string): Span {
    return getTracer().startSpan('task.create', {
      kind: SpanKind.PRODUCER,
      attributes: {
        'task.id': taskId,
        'task.name': taskName,
        'task.priority': priority,
      },
    });
  },

  startOutboxPublish(eventId: string, taskId: string): Span {
    return getTracer().startSpan('outbox.publish', {
      kind: SpanKind.PRODUCER,
      attributes: {
        'outbox.event.id': eventId,
        'task.id': taskId,
        'messaging.system': 'bullmq',
        'messaging.operation': 'publish',
      },
    });
  },

  startTaskProcess(
    taskId: string,
    taskName: string,
    workerId: string,
    attempt: number,
    parentContext?: Context,
  ): Span {
    const ctx = parentContext ?? context.active();
    return getTracer().startSpan(
      'task.process',
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          'task.id': taskId,
          'task.name': taskName,
          'worker.id': workerId,
          'messaging.system': 'bullmq',
          'messaging.operation': 'process',
          'task.attempt': attempt,
        },
      },
      ctx,
    );
  },

  startTaskClaim(taskId: string, workerId: string, claimType: 'initial' | 'reclaim'): Span {
    return getTracer().startSpan('task.claim', {
      attributes: {
        'task.id': taskId,
        'worker.id': workerId,
        'task.claim.type': claimType,
      },
    });
  },

  startTaskComplete(taskId: string, workerId: string): Span {
    return getTracer().startSpan('task.complete', {
      attributes: {
        'task.id': taskId,
        'worker.id': workerId,
      },
    });
  },

  startTaskFail(taskId: string, workerId: string, isFinalAttempt: boolean): Span {
    return getTracer().startSpan('task.fail', {
      attributes: {
        'task.id': taskId,
        'worker.id': workerId,
        'task.final_attempt': isFinalAttempt,
      },
    });
  },

  recordError(span: Span, error: unknown): void {
    try {
      const message = error instanceof Error ? error.message : String(error);
      span.setStatus({ code: SpanStatusCode.ERROR, message });
      span.recordException(error instanceof Error ? error : new Error(message));
    } catch {
      // Must not throw from telemetry code
    }
  },

  endSpan(span: Span): void {
    try {
      span.end();
    } catch {
      // Must not throw from telemetry code
    }
  },

  setSpanOk(span: Span): void {
    try {
      span.setStatus({ code: SpanStatusCode.OK });
    } catch {
      // Must not throw from telemetry code
    }
  },

  withActiveSpan<T>(span: Span, fn: () => T): T {
    return context.with(trace.setSpan(context.active(), span), fn);
  },
};
