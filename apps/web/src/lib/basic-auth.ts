import { createHash, timingSafeEqual } from 'node:crypto';

/**
 * HTTP Basic auth for the operator dashboard.
 *
 * The dashboard can see every tenant's tasks and cancel any of them, so it is
 * never served unauthenticated. Credentials come from DASHBOARD_USER and
 * DASHBOARD_PASSWORD; when either is missing the dashboard refuses to serve
 * (fail closed) rather than falling back to open access.
 */
export type AuthResult = 'ok' | 'unauthorized' | 'not_configured';

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

function safeEqual(a: string, b: string): boolean {
  return timingSafeEqual(digest(a), digest(b));
}

export function checkBasicAuth(header: string | null, env: NodeJS.ProcessEnv = process.env): AuthResult {
  const user = env.DASHBOARD_USER;
  const password = env.DASHBOARD_PASSWORD;
  if (!user || !password) return 'not_configured';

  if (!header || !header.startsWith('Basic ')) return 'unauthorized';
  let decoded: string;
  try {
    decoded = Buffer.from(header.slice(6).trim(), 'base64').toString('utf8');
  } catch {
    return 'unauthorized';
  }
  const sep = decoded.indexOf(':');
  if (sep < 0) return 'unauthorized';

  // Evaluate both comparisons so timing does not reveal which half was wrong.
  const userOk = safeEqual(decoded.slice(0, sep), user);
  const passwordOk = safeEqual(decoded.slice(sep + 1), password);
  return userOk && passwordOk ? 'ok' : 'unauthorized';
}

export function authFailureResponse(result: Exclude<AuthResult, 'ok'>): Response {
  if (result === 'not_configured') {
    return new Response('Dashboard is disabled: set DASHBOARD_USER and DASHBOARD_PASSWORD.', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  }
  return new Response('Authentication required.', {
    status: 401,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'WWW-Authenticate': 'Basic realm="Distributed Engine", charset="UTF-8"',
    },
  });
}
