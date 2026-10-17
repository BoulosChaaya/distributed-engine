import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import http from 'http';
import type { AddressInfo } from 'net';
import {
  createBuiltinHandlers,
  runTaskHandler,
  hostAllowed,
  boundResult,
  isNonRetryable,
  NonRetryableTaskError,
  MAX_RESULT_BYTES,
  TaskContext,
} from '../task-handlers';

function ctx(name: string, payload: Record<string, unknown> = {}, attempt = 1, signal = new AbortController().signal): TaskContext {
  return { taskId: 't1', name, payload, attempt, signal };
}

let server: http.Server;
let base: string;
const hits: string[] = [];

beforeAll(async () => {
  server = http.createServer((req, res) => {
    hits.push(`${req.method} ${req.url}`);
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      if (req.url === '/ok') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ got: body ? JSON.parse(body) : null, auth: req.headers['x-token'] ?? null }));
      } else if (req.url === '/missing') {
        res.writeHead(404);
        res.end('nope');
      } else if (req.url === '/busy') {
        res.writeHead(503);
        res.end('busy');
      } else if (req.url === '/redirect') {
        res.writeHead(302, { location: 'http://169.254.169.254/latest/meta-data' });
        res.end();
      } else if (req.url === '/not-modified') {
        res.writeHead(304);
        res.end();
      } else if (req.url === '/slow') {
        setTimeout(() => { res.writeHead(200); res.end('late'); }, 2000);
      } else {
        res.writeHead(500);
        res.end();
      }
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterAll(async () => {
  server.closeAllConnections();
  await new Promise((r) => server.close(r));
});

describe('registry dispatch', () => {
  const handlers = createBuiltinHandlers();

  it('fails unknown task types permanently and names the known ones', async () => {
    const err = await runTaskHandler(handlers, ctx('send-email'), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
    expect(err.message).toMatch(/No handler registered for task type "send-email"/);
    expect(err.message).toMatch(/echo, flaky, http, sleep/);
  });

  it('echo returns the payload', async () => {
    expect(await runTaskHandler(handlers, ctx('echo', { a: 1, b: [2] }), 5000)).toEqual({ echo: { a: 1, b: [2] } });
  });

  it('sleep waits and reports, and rejects bad input permanently', async () => {
    expect(await runTaskHandler(handlers, ctx('sleep', { ms: 20 }), 5000)).toEqual({ sleptMs: 20 });
    const err = await runTaskHandler(handlers, ctx('sleep', { ms: -5 }), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
  });

  it('sleep stops when the worker aborts (lease lost)', async () => {
    const ac = new AbortController();
    const p = runTaskHandler(handlers, ctx('sleep', { ms: 60_000 }, 1, ac.signal), 120_000);
    setTimeout(() => ac.abort(new Error('task lease lost')), 20);
    await expect(p).rejects.toThrow('task lease lost');
  });

  it('enforces the overall task timeout as a retryable error', async () => {
    const err = await runTaskHandler(handlers, ctx('sleep', { ms: 60_000 }), 50).catch((e) => e);
    expect(err.message).toBe('task timed out after 50ms');
    expect(isNonRetryable(err)).toBe(false);
  });

  it('flaky fails exactly failTimes attempts, then succeeds', async () => {
    for (const attempt of [1, 2]) {
      const err = await runTaskHandler(handlers, ctx('flaky', { failTimes: 2 }, attempt), 5000).catch((e) => e);
      expect(err.message).toBe(`planned failure ${attempt} of 2`);
      expect(isNonRetryable(err)).toBe(false);
    }
    expect(await runTaskHandler(handlers, ctx('flaky', { failTimes: 2 }, 3), 5000)).toEqual({ succeededOnAttempt: 3 });
  });
});

describe('http handler', () => {
  it('is disabled unless hosts are allow-listed', async () => {
    const err = await runTaskHandler(createBuiltinHandlers(), ctx('http', { url: `${base}/ok` }), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
    expect(err.message).toMatch(/HTTP_TASK_ALLOWED_HOSTS/);
  });

  const handlers = createBuiltinHandlers({ httpAllowedHosts: ['127.0.0.1'] });

  it('calls an allowed host with method, headers and JSON body', async () => {
    const out: any = await runTaskHandler(handlers, ctx('http', {
      url: `${base}/ok`, method: 'post', headers: { 'x-token': 'abc' }, body: { n: 1 },
    }), 5000);
    expect(out.status).toBe(200);
    expect(JSON.parse(out.body)).toEqual({ got: { n: 1 }, auth: 'abc' });
  });

  it('refuses hosts outside the allow-list without making a request', async () => {
    const before = hits.length;
    const err = await runTaskHandler(handlers, ctx('http', { url: 'http://169.254.169.254/latest/meta-data' }), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
    expect(err.message).toMatch(/not in HTTP_TASK_ALLOWED_HOSTS/);
    expect(hits.length).toBe(before);
  });

  it.each([
    [{ url: 'file:///etc/passwd' }, /http or https/],
    [{ url: 'http://user:pw@127.0.0.1/ok' }, /credentials/],
    [{ url: 'not a url' }, /valid URL/],
    [{}, /url is required/],
    [{ url: 'http://127.0.0.1/ok', method: 'TRACE' }, /not supported/],
  ])('rejects bad payload %j permanently', async (payload, msg) => {
    const err = await runTaskHandler(handlers, ctx('http', payload), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
    expect(err.message).toMatch(msg);
  });

  it('does not follow redirects, and fails permanently naming the target', async () => {
    const err = await runTaskHandler(handlers, ctx('http', { url: `${base}/redirect` }), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(true);
    expect(err.message).toBe(
      'upstream responded 302 (redirect to http://169.254.169.254/latest/meta-data); redirects are not followed, update the URL',
    );
    expect(hits.filter((h) => h.includes('meta-data'))).toHaveLength(0);
  });

  it('treats 304 Not Modified as success', async () => {
    const out: any = await runTaskHandler(handlers, ctx('http', { url: `${base}/not-modified` }), 5000);
    expect(out.status).toBe(304);
  });

  it('treats 4xx as permanent and 5xx as retryable', async () => {
    const e404 = await runTaskHandler(handlers, ctx('http', { url: `${base}/missing` }), 5000).catch((e) => e);
    expect(isNonRetryable(e404)).toBe(true);
    const e503 = await runTaskHandler(handlers, ctx('http', { url: `${base}/busy` }), 5000).catch((e) => e);
    expect(isNonRetryable(e503)).toBe(false);
    expect(e503.message).toBe('upstream responded 503');
  });

  it('applies the per-request timeout as a retryable error', async () => {
    const err = await runTaskHandler(handlers, ctx('http', { url: `${base}/slow`, timeoutMs: 100 }), 5000).catch((e) => e);
    expect(isNonRetryable(err)).toBe(false);
  });
});

describe('helpers', () => {
  it('matches exact hosts and wildcard subdomains only', () => {
    expect(hostAllowed('api.example.com', ['api.example.com'])).toBe(true);
    expect(hostAllowed('API.example.com', ['api.example.com'])).toBe(true);
    expect(hostAllowed('a.b.example.com', ['*.example.com'])).toBe(true);
    expect(hostAllowed('example.com', ['*.example.com'])).toBe(false);
    expect(hostAllowed('evilexample.com', ['*.example.com'])).toBe(false);
    expect(hostAllowed('example.com.evil.io', ['example.com'])).toBe(false);
  });

  it('bounds stored results', () => {
    expect(boundResult(undefined)).toBeNull();
    expect(boundResult({ a: 1 })).toEqual({ a: 1 });
    const big = boundResult({ s: 'x'.repeat(MAX_RESULT_BYTES) }) as any;
    expect(big.truncated).toBe(true);
    expect(big.preview.length).toBe(1024);
  });

  it('recognises permanent errors by name', () => {
    expect(isNonRetryable(new NonRetryableTaskError('x'))).toBe(true);
    expect(isNonRetryable(new Error('x'))).toBe(false);
  });
});
