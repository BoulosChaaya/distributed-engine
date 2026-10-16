import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import express from 'express';
import type { Server } from 'http';
import type { AddressInfo } from 'net';
import { createOperatorAuth } from '../operator-auth';

const KEY = 'k'.repeat(16) + '0123456789abcdef0123';

function buildApp(apiKey: string | undefined) {
  const app = express();
  app.use(['/metrics', '/workers', '/tasks', '/schedules'], createOperatorAuth(apiKey));
  const ok = (_req: express.Request, res: express.Response) => res.json({ ok: true });
  app.get('/health', ok);
  app.get('/metrics', ok);
  app.get('/workers', ok);
  app.get('/tasks', ok);
  app.get('/tasks/:id', ok);
  app.put('/tasks/:id/cancel', ok);
  app.get('/schedules', ok);
  app.get('/tenant/tasks', ok);
  return app;
}

async function listen(app: express.Express): Promise<{ server: Server; base: string }> {
  return new Promise((resolve) => {
    const server = app.listen(0, () => {
      const { port } = server.address() as AddressInfo;
      resolve({ server, base: `http://127.0.0.1:${port}` });
    });
  });
}

describe('operator auth', () => {
  let server: Server;
  let base: string;
  let disabledServer: Server;
  let disabledBase: string;

  beforeAll(async () => {
    ({ server, base } = await listen(buildApp(KEY)));
    ({ server: disabledServer, base: disabledBase } = await listen(buildApp(undefined)));
  });

  afterAll(async () => {
    await new Promise((r) => server.close(r));
    await new Promise((r) => disabledServer.close(r));
  });

  it.each(['/metrics', '/workers', '/tasks', '/tasks/abc', '/schedules'])('rejects %s without a key', async (path) => {
    const res = await fetch(base + path);
    expect(res.status).toBe(401);
    expect(res.headers.get('www-authenticate')).toMatch(/Bearer/);
  });

  it('rejects a wrong key, including a prefix of the right one', async () => {
    for (const bad of ['wrong', KEY.slice(0, -1), KEY + 'x', `bearer ${KEY}`]) {
      const res = await fetch(base + '/tasks', { headers: { Authorization: bad.startsWith('bearer') ? bad : `Bearer ${bad}` } });
      expect(res.status).toBe(401);
    }
  });

  it('accepts the right key on reads and writes', async () => {
    const headers = { Authorization: `Bearer ${KEY}` };
    expect((await fetch(base + '/tasks', { headers })).status).toBe(200);
    expect((await fetch(base + '/tasks/abc/cancel', { method: 'PUT', headers })).status).toBe(200);
    expect((await fetch(base + '/metrics', { headers })).status).toBe(200);
  });

  it('leaves health probes and the tenant API alone', async () => {
    expect((await fetch(base + '/health')).status).toBe(200);
    // /tenant/tasks is guarded by tenant auth in the real app, never by the operator key.
    expect((await fetch(base + '/tenant/tasks')).status).toBe(200);
  });

  it('fails closed with 503 when no key is configured', async () => {
    const res = await fetch(disabledBase + '/tasks', { headers: { Authorization: 'Bearer anything' } });
    expect(res.status).toBe(503);
    expect((await fetch(disabledBase + '/health')).status).toBe(200);
  });
});
