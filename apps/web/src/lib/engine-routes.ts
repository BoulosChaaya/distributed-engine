/**
 * The only engine API calls the dashboard may make on the operator's behalf.
 * Anything not listed here is refused by the proxy route, so the browser
 * cannot use the dashboard's operator key to reach other endpoints.
 */
const ID = '[A-Za-z0-9_-]{1,128}';

const ALLOWED: Array<{ method: string; pattern: RegExp }> = [
  { method: 'GET', pattern: /^metrics$/ },
  { method: 'GET', pattern: /^workers$/ },
  { method: 'GET', pattern: /^tasks$/ },
  { method: 'GET', pattern: new RegExp(`^tasks/${ID}$`) },
  { method: 'POST', pattern: /^tasks$/ },
  { method: 'PUT', pattern: new RegExp(`^tasks/${ID}/cancel$`) },
  { method: 'GET', pattern: /^schedules$/ },
  { method: 'GET', pattern: new RegExp(`^schedules/${ID}$`) },
  { method: 'GET', pattern: /^plans$/ },
  { method: 'GET', pattern: /^tenants$/ },
  { method: 'GET', pattern: new RegExp(`^tenants/${ID}$`) },
  { method: 'POST', pattern: /^tenants$/ },
  { method: 'PUT', pattern: new RegExp(`^tenants/${ID}/status$`) },
  { method: 'POST', pattern: new RegExp(`^tenants/${ID}/api-key$`) },
];

export function isAllowedEngineCall(method: string, path: string): boolean {
  return ALLOWED.some((rule) => rule.method === method && rule.pattern.test(path));
}
