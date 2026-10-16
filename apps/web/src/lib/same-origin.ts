/**
 * Browsers attach Basic credentials to cross-site requests automatically, so
 * a state-changing call must prove it came from the dashboard's own pages.
 * Fetch Metadata (Sec-Fetch-Site) is checked first; older clients fall back
 * to comparing the Origin host with the Host header.
 */
export function isSameOriginRequest(headers: Headers): boolean {
  const site = headers.get('sec-fetch-site');
  if (site) return site === 'same-origin';

  const origin = headers.get('origin');
  const host = headers.get('x-forwarded-host') ?? headers.get('host');
  if (!origin || !host) return false;
  try {
    return new URL(origin).host === host;
  } catch {
    return false;
  }
}
