import { context, propagation, Context, ROOT_CONTEXT } from '@opentelemetry/api';

export const TRACE_CONTEXT_KEY = 'traceContext';

export function injectTraceContext(ctx?: Context): Record<string, string> {
  const carrier: Record<string, string> = {};
  try {
    propagation.inject(ctx ?? context.active(), carrier);
  } catch {
    // Propagation failure must not affect business operations
  }
  return carrier;
}

export function extractTraceContext(
  carrier: Record<string, string> | null | undefined,
): Context {
  if (!carrier || typeof carrier !== 'object') {
    return ROOT_CONTEXT;
  }
  try {
    return propagation.extract(ROOT_CONTEXT, carrier);
  } catch {
    return ROOT_CONTEXT;
  }
}
