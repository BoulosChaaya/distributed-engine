/**
 * Task handlers: what a worker actually does with a task.
 *
 * A task's `name` is its type, the way a BullMQ job name or a Celery task name
 * is. The worker looks the name up in a registry and runs the matching
 * handler. A name with no handler fails immediately and permanently, because
 * retrying cannot make code appear.
 *
 * Error contract (read by the worker):
 *  - NonRetryableTaskError: the task can never succeed as submitted (unknown
 *    type, invalid payload, HTTP 4xx). The worker fails it at once without
 *    spending the remaining retries.
 *  - Any other error: transient (timeout, HTTP 5xx/429, network). The worker
 *    requeues it with backoff until the retry budget is spent.
 *
 * Every handler receives an AbortSignal. It fires when the worker loses its
 * lease (another worker may already own the task), when the task exceeds its
 * timeout, or when the worker is shutting down and its drain window is over
 * (the task is then handed back without charging a retry). Handlers must stop
 * promptly when it fires.
 */
import net from 'node:net';
import { Agent, fetch as undiciFetch } from 'undici';
import { addressRefusal, findBlocked, guardedLookup } from './http-guard.js';

export class NonRetryableTaskError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NonRetryableTaskError';
  }
}

/** Name check rather than instanceof: survives duplicate module copies. */
export function isNonRetryable(error: unknown): boolean {
  return error instanceof Error && error.name === 'NonRetryableTaskError';
}

export interface TaskContext {
  taskId: string;
  name: string;
  payload: Record<string, unknown>;
  /** 1-based attempt number, from the retry count in PostgreSQL. */
  attempt: number;
  /**
   * Workflow steps only: outputs of the direct dependencies, keyed by step
   * key. This is how data flows along the DAG (fan-in sees all parents).
   */
  upstream?: Record<string, unknown>;
  signal: AbortSignal;
}

export type TaskHandler = (ctx: TaskContext) => Promise<unknown>;

export interface BuiltinHandlerOptions {
  /**
   * Hostnames the `http` handler may call. Exact names or `*.example.com`
   * suffix patterns. Empty means the handler is disabled. An open HTTP
   * handler inside the cluster would let anyone who can submit a task make
   * requests to internal services (SSRF).
   */
  httpAllowedHosts?: string[];
  /**
   * Allow-listed names that may resolve to private or loopback addresses
   * (internal services called on purpose). Any other name that resolves to
   * one is refused; link-local addresses (cloud metadata) are refused always.
   */
  httpPrivateHosts?: string[];
  maxSleepMs?: number;
  fetchImpl?: typeof fetch;
}

export const MAX_RESULT_BYTES = 64 * 1024;
const MAX_HTTP_BODY_CHARS = 16 * 1024;
const HTTP_METHODS = new Set(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE']);

function abortable(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason ?? new Error('aborted'));
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason ?? new Error('aborted'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function intField(payload: Record<string, unknown>, key: string, def: number, min: number, max: number): number {
  const value = payload[key];
  if (value === undefined) return def;
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new NonRetryableTaskError(`payload.${key} must be an integer between ${min} and ${max}`);
  }
  return value;
}

export function hostAllowed(hostname: string, allowed: string[]): boolean {
  const host = hostname.toLowerCase();
  return allowed.some((rule) => {
    const r = rule.trim().toLowerCase();
    if (!r) return false;
    if (r.startsWith('*.')) return host.endsWith(r.slice(1)) && host.length > r.length - 1;
    return host === r;
  });
}

export function createBuiltinHandlers(options: BuiltinHandlerOptions = {}): Map<string, TaskHandler> {
  const maxSleepMs = options.maxSleepMs ?? 300_000;
  const allowed = options.httpAllowedHosts ?? [];
  const privateHosts = options.httpPrivateHosts ?? [];
  const isPrivateAllowed = (host: string) => hostAllowed(host, privateHosts);
  // The address check runs in the connection's own DNS lookup, so the address
  // checked is the address connected to (no second resolution to rebind).
  const agent = new Agent({ connect: { lookup: guardedLookup(isPrivateAllowed) } });
  const doFetch = options.fetchImpl ?? ((url: URL, init: RequestInit) =>
    undiciFetch(url, { ...(init as object), dispatcher: agent }) as unknown as Promise<Response>);

  const handlers = new Map<string, TaskHandler>();

  // Waits for payload.ms (default 1000). Stands in for any slow I/O-bound job.
  handlers.set('sleep', async ({ payload, signal }) => {
    const ms = intField(payload, 'ms', 1000, 0, maxSleepMs);
    await abortable(ms, signal);
    return { sleptMs: ms };
  });

  // Returns its payload (and, in a workflow, what its parents produced).
  // Proves data made the full round trip.
  handlers.set('echo', async ({ payload, upstream }) =>
    upstream && Object.keys(upstream).length > 0 ? { echo: payload, upstream } : { echo: payload });

  // Fails the first payload.failTimes attempts, then succeeds. Retry
  // behaviour can then be shown and tested on purpose instead of hoped for.
  handlers.set('flaky', async ({ payload, attempt }) => {
    const failTimes = intField(payload, 'failTimes', 1, 0, 10);
    if (attempt <= failTimes) {
      throw new Error(`planned failure ${attempt} of ${failTimes}`);
    }
    return { succeededOnAttempt: attempt };
  });

  // Calls an HTTP endpoint on an allow-listed host.
  handlers.set('http', async ({ taskId, payload, signal }) => {
    if (allowed.length === 0) {
      throw new NonRetryableTaskError('http handler is disabled: set HTTP_TASK_ALLOWED_HOSTS on the worker');
    }
    if (typeof payload.url !== 'string') throw new NonRetryableTaskError('payload.url is required');
    let url: URL;
    try {
      url = new URL(payload.url);
    } catch {
      throw new NonRetryableTaskError('payload.url is not a valid URL');
    }
    if (url.protocol !== 'https:' && url.protocol !== 'http:') {
      throw new NonRetryableTaskError('payload.url must be http or https');
    }
    if (url.username || url.password) {
      throw new NonRetryableTaskError('payload.url must not contain credentials');
    }
    if (!hostAllowed(url.hostname, allowed)) {
      throw new NonRetryableTaskError(`host ${url.hostname} is not in HTTP_TASK_ALLOWED_HOSTS`);
    }
    // An IP literal is never looked up, so it is checked here. Written in the
    // allow-list it is taken as meant, except for addresses never called.
    const literal = url.hostname.replace(/^\[|\]$/g, '');
    if (net.isIP(literal)) {
      const why = addressRefusal(literal, literal, true);
      if (why) throw new NonRetryableTaskError(why);
    }
    const method = typeof payload.method === 'string' ? payload.method.toUpperCase() : 'GET';
    if (!HTTP_METHODS.has(method)) throw new NonRetryableTaskError(`payload.method ${method} is not supported`);

    const headers: Record<string, string> = {};
    if (payload.headers !== undefined) {
      if (typeof payload.headers !== 'object' || payload.headers === null || Array.isArray(payload.headers)) {
        throw new NonRetryableTaskError('payload.headers must be an object of strings');
      }
      for (const [k, v] of Object.entries(payload.headers)) {
        if (typeof v !== 'string') throw new NonRetryableTaskError('payload.headers must be an object of strings');
        headers[k] = v;
      }
    }
    // A task runs at least once: if a worker dies mid-call, another worker
    // makes the call again. The task id, the same on every attempt, lets a
    // receiver that honours Idempotency-Key (Stripe-style APIs, the IETF
    // draft) drop the repeat. A key set in the payload is kept.
    if (!Object.keys(headers).some((k) => k.toLowerCase() === 'idempotency-key')) {
      headers['Idempotency-Key'] = taskId;
    }
    let body: string | undefined;
    if (payload.body !== undefined && method !== 'GET' && method !== 'HEAD') {
      body = JSON.stringify(payload.body);
      headers['content-type'] ??= 'application/json';
    }

    const timeoutMs = intField(payload, 'timeoutMs', 10_000, 100, 120_000);
    let response: Response;
    try {
      response = await doFetch(url, {
        method,
        headers,
        body,
        // Following a redirect could leave the allow-list.
        redirect: 'manual',
        signal: AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]),
      });
    } catch (error) {
      const blocked = findBlocked(error);
      if (blocked) throw new NonRetryableTaskError(blocked.message);
      throw error;
    }
    const text = method === 'HEAD' ? '' : await response.text();
    const result = {
      status: response.status,
      contentType: response.headers.get('content-type'),
      body: text.length > MAX_HTTP_BODY_CHARS ? text.slice(0, MAX_HTTP_BODY_CHARS) : text,
      truncated: text.length > MAX_HTTP_BODY_CHARS,
    };
    if (response.status === 429 || response.status >= 500) {
      throw new Error(`upstream responded ${response.status}`);
    }
    // Redirects are never followed (a Location could point outside the
    // allow-list), so a 3xx means the task did not get what it asked for.
    // Reporting it as success hid that. Retrying returns the same redirect,
    // so it fails permanently and names the target so the URL can be fixed.
    // 304 Not Modified is the success answer to a conditional request.
    if (response.status >= 300 && response.status < 400 && response.status !== 304) {
      const location = (response.headers.get('location') ?? 'no Location header').slice(0, 200);
      throw new NonRetryableTaskError(
        `upstream responded ${response.status} (redirect to ${location}); redirects are not followed, update the URL`,
      );
    }
    if (response.status >= 400) {
      throw new NonRetryableTaskError(`upstream responded ${response.status}`);
    }
    return result;
  });

  return handlers;
}

/** Keeps stored results bounded: tasks.result is a JSONB column, not blob storage. */
export function boundResult(output: unknown): unknown {
  if (output === undefined) return null;
  let json: string;
  try {
    json = JSON.stringify(output);
  } catch {
    return { unserializable: true };
  }
  if (json === undefined) return null;
  if (Buffer.byteLength(json, 'utf8') <= MAX_RESULT_BYTES) return output;
  return { truncated: true, preview: json.slice(0, 1024) };
}

/**
 * Looks up and runs the handler for ctx.name with an overall timeout.
 * Unknown task types fail permanently.
 */
export async function runTaskHandler(
  handlers: Map<string, TaskHandler>,
  ctx: TaskContext,
  timeoutMs: number,
): Promise<unknown> {
  const handler = handlers.get(ctx.name);
  if (!handler) {
    throw new NonRetryableTaskError(
      `No handler registered for task type "${ctx.name}". Known types: ${[...handlers.keys()].sort().join(', ')}`,
    );
  }
  const timeout = AbortSignal.timeout(timeoutMs);
  const signal = AbortSignal.any([ctx.signal, timeout]);
  try {
    return boundResult(await handler({ ...ctx, signal }));
  } catch (error) {
    if (timeout.aborted && !ctx.signal.aborted) {
      // The caught error is only the AbortSignal's TimeoutError; this message
      // says everything it did. (`cause` needs lib ES2022; we target ES2020.)
      // eslint-disable-next-line preserve-caught-error
      throw new Error(`task timed out after ${timeoutMs}ms`);
    }
    throw error;
  }
}
