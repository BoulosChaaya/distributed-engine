import { describe, it, expect } from '@jest/globals';
import { checkBasicAuth, authFailureResponse } from '../basic-auth';
import { isAllowedEngineCall } from '../engine-routes';
import { isSameOriginRequest } from '../same-origin';

const env = { DASHBOARD_USER: 'ops', DASHBOARD_PASSWORD: 'correct horse battery staple' } as unknown as NodeJS.ProcessEnv;
const basic = (u: string, p: string) => `Basic ${Buffer.from(`${u}:${p}`).toString('base64')}`;

describe('dashboard Basic auth', () => {
  it('accepts the configured credentials', () => {
    expect(checkBasicAuth(basic('ops', 'correct horse battery staple'), env)).toBe('ok');
  });

  it('accepts passwords that contain a colon', () => {
    const e = { DASHBOARD_USER: 'ops', DASHBOARD_PASSWORD: 'a:b:c' } as unknown as NodeJS.ProcessEnv;
    expect(checkBasicAuth(basic('ops', 'a:b:c'), e)).toBe('ok');
  });

  it.each([
    ['no header', null],
    ['wrong scheme', 'Bearer abc'],
    ['wrong user', basic('admin', 'correct horse battery staple')],
    ['wrong password', basic('ops', 'nope')],
    ['password prefix', basic('ops', 'correct horse')],
    ['no colon', `Basic ${Buffer.from('ops').toString('base64')}`],
  ])('rejects %s', (_label, header) => {
    expect(checkBasicAuth(header as string | null, env)).toBe('unauthorized');
  });

  it('fails closed when credentials are not configured', () => {
    expect(checkBasicAuth(basic('ops', 'x'), {} as unknown as NodeJS.ProcessEnv)).toBe('not_configured');
    expect(checkBasicAuth(basic('ops', ''), { DASHBOARD_USER: 'ops', DASHBOARD_PASSWORD: '' } as unknown as NodeJS.ProcessEnv)).toBe('not_configured');
  });

  it('challenges the browser on 401 and explains 503', async () => {
    const r401 = authFailureResponse('unauthorized');
    expect(r401.status).toBe(401);
    expect(r401.headers.get('www-authenticate')).toMatch(/^Basic /);
    const r503 = authFailureResponse('not_configured');
    expect(r503.status).toBe(503);
    expect(await r503.text()).toMatch(/DASHBOARD_USER/);
  });
});

describe('engine call allow-list', () => {
  it.each([
    ['GET', 'metrics'],
    ['GET', 'workers'],
    ['GET', 'tasks'],
    ['GET', 'tasks/abc-123'],
    ['POST', 'tasks'],
    ['PUT', 'tasks/abc-123/cancel'],
    ['GET', 'schedules'],
    ['GET', 'schedules/s_1'],
  ])('allows %s %s', (method, path) => {
    expect(isAllowedEngineCall(method, path)).toBe(true);
  });

  it.each([
    ['DELETE', 'tasks/abc'],
    ['POST', 'schedules'],
    ['PUT', 'schedules/abc/status'],
    ['GET', 'tenant/usage'],
    ['GET', 'tasks/../metrics'],
    ['GET', 'tasks/a/b'],
    ['GET', 'health/../tasks'],
    ['GET', ''],
  ])('refuses %s %s', (method, path) => {
    expect(isAllowedEngineCall(method, path)).toBe(false);
  });
});

describe('same-origin check for state-changing calls', () => {
  const h = (init: Record<string, string>) => new Headers(init);

  it('trusts Fetch Metadata when present', () => {
    expect(isSameOriginRequest(h({ 'sec-fetch-site': 'same-origin' }))).toBe(true);
    expect(isSameOriginRequest(h({ 'sec-fetch-site': 'cross-site', origin: 'https://dash.example', host: 'dash.example' }))).toBe(false);
    expect(isSameOriginRequest(h({ 'sec-fetch-site': 'same-site' }))).toBe(false);
  });

  it('falls back to Origin vs Host', () => {
    expect(isSameOriginRequest(h({ origin: 'https://dash.example', host: 'dash.example' }))).toBe(true);
    expect(isSameOriginRequest(h({ origin: 'https://evil.example', host: 'dash.example' }))).toBe(false);
    expect(isSameOriginRequest(h({ host: 'dash.example' }))).toBe(false);
  });
});
