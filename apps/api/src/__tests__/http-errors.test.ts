import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { AppError } from '@repo/shared';
import { notFoundHandler, createErrorHandler } from '../http-errors';

// The same middleware order as main.ts: body parser, routes, 404, errors.
const logged: string[] = [];
let errors = 0;
let server: Server;
let base: string;

beforeAll(async () => {
  const app = express();
  app.use(express.json({ limit: '1kb' }));
  app.post('/tasks', (req, res) => { res.status(201).json({ success: true, got: req.body }); });
  app.get('/boom', () => { throw new Error('db exploded'); });
  app.get('/gone', (_req, _res, next) => next(new AppError(404, 'Task x not found')));
  app.use(notFoundHandler);
  app.use(createErrorHandler({ logger: { error: (m) => { logged.push(m); } }, recordError: () => { errors++; } }));
  server = await new Promise<Server>((r) => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
afterAll(async () => { await new Promise((r) => server.close(r)); });

async function call(method: string, path: string, body?: string) {
  const res = await fetch(base + path, { method, body, headers: body !== undefined ? { 'content-type': 'application/json' } : {} });
  return { status: res.status, type: res.headers.get('content-type') ?? '', json: await res.json().catch(() => null) };
}

describe('API error responses', () => {
  it('malformed JSON is a 400 the client can read, not a logged 500', async () => {
    const before = logged.length;
    const r = await call('POST', '/tasks', '{bad');
    expect(r.status).toBe(400);
    expect(r.json).toMatchObject({ success: false, error: 'Malformed JSON body' });
    expect(logged.length).toBe(before);
  });

  it('a body over the limit is 413', async () => {
    const r = await call('POST', '/tasks', JSON.stringify({ s: 'x'.repeat(2048) }));
    expect(r.status).toBe(413);
    expect(r.json.error).toMatch(/Request body too large \(limit 1024 bytes\)/);
  });

  it('an unknown route is a JSON 404, not an HTML page', async () => {
    const r = await call('GET', '/nope');
    expect(r.status).toBe(404);
    expect(r.type).toMatch(/application\/json/);
    expect(r.json).toMatchObject({ success: false, error: 'Route GET /nope not found' });
  });

  it('keeps AppError statuses and real failures as a logged 500', async () => {
    expect((await call('GET', '/gone')).status).toBe(404);
    const before = logged.length;
    const r = await call('GET', '/boom');
    expect(r.status).toBe(500);
    expect(r.json.error).toBe('Internal server error');
    expect(logged.length).toBe(before + 1);
  });

  it('valid requests are untouched', async () => {
    const r = await call('POST', '/tasks', JSON.stringify({ name: 'echo' }));
    expect(r.status).toBe(201);
    expect(r.json.got).toEqual({ name: 'echo' });
    expect(errors).toBeGreaterThan(0);
  });
});
