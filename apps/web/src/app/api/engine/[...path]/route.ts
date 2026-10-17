import { authFailureResponse, checkBasicAuth } from '@/lib/basic-auth';
import { isAllowedEngineCall } from '@/lib/engine-routes';
import { isSameOriginRequest } from '@/lib/same-origin';

/**
 * Server-side bridge from the dashboard to the engine API.
 *
 * The operator key (OPERATOR_API_KEY) lives only in this server's
 * environment. The browser talks to /api/engine/* on the dashboard's own
 * origin; this handler checks the operator's Basic credentials, checks the
 * call against an allow-list, and forwards it with the key attached.
 */
export const dynamic = 'force-dynamic';

const TIMEOUT_MS = 10_000;

function json(status: number, error: string): Response {
  return Response.json({ success: false, error }, { status });
}

async function forward(request: Request, path: string[]): Promise<Response> {
  const auth = checkBasicAuth(request.headers.get('authorization'));
  if (auth !== 'ok') return authFailureResponse(auth);

  const method = request.method.toUpperCase();
  if (method !== 'GET' && !isSameOriginRequest(request.headers)) {
    return json(403, 'Cross-site request refused');
  }

  const joined = path.join('/');
  if (!isAllowedEngineCall(method, joined)) {
    return json(404, 'Not found');
  }

  const base = process.env.ENGINE_API_URL;
  const key = process.env.OPERATOR_API_KEY;
  if (!base || !key) {
    return json(503, 'Dashboard is not connected to the engine: set ENGINE_API_URL and OPERATOR_API_KEY');
  }

  const target = new URL(joined, base.endsWith('/') ? base : `${base}/`);
  target.search = new URL(request.url).search;

  const headers: Record<string, string> = { Authorization: `Bearer ${key}`, Accept: 'application/json' };
  let body: string | undefined;
  if (method === 'POST' || method === 'PUT') {
    body = await request.text();
    if (body.length > 64 * 1024) return json(413, 'Request body too large');
    headers['Content-Type'] = 'application/json';
  }

  try {
    const upstream = await fetch(target, {
      method,
      headers,
      body,
      cache: 'no-store',
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const text = await upstream.text();
    return new Response(text, {
      status: upstream.status,
      headers: { 'Content-Type': upstream.headers.get('content-type') ?? 'application/json', 'Cache-Control': 'no-store' },
    });
  } catch {
    return json(502, 'Engine API is unreachable');
  }
}

type Ctx = { params: Promise<{ path: string[] }> };

export async function GET(request: Request, { params }: Ctx) {
  return forward(request, (await params).path);
}

export async function POST(request: Request, { params }: Ctx) {
  return forward(request, (await params).path);
}

export async function PUT(request: Request, { params }: Ctx) {
  return forward(request, (await params).path);
}
