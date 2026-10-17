import { describe, it, expect } from '@jest/globals';
import { loadConfig, validateConfig } from '../config';

function withEnv<T>(env: Record<string, string | undefined>, fn: () => T): T {
  const saved: Record<string, string | undefined> = {};
  for (const k of Object.keys(env)) {
    saved[k] = process.env[k];
    if (env[k] === undefined) delete process.env[k];
    else process.env[k] = env[k];
  }
  try {
    return fn();
  } finally {
    for (const k of Object.keys(saved)) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
  }
}

describe('API config', () => {
  it('treats an empty OPERATOR_API_KEY as not configured', () => {
    const cfg = withEnv({ OPERATOR_API_KEY: '' }, loadConfig);
    expect(cfg.operator.apiKey).toBeUndefined();
    expect(() => validateConfig(cfg)).not.toThrow();
  });

  it('rejects a short operator key', () => {
    const cfg = withEnv({ OPERATOR_API_KEY: 'short' }, loadConfig);
    expect(() => validateConfig(cfg)).toThrow(/OPERATOR_API_KEY must be at least 32/);
  });

  it('accepts a 32+ character operator key', () => {
    const cfg = withEnv({ OPERATOR_API_KEY: 'a'.repeat(64) }, loadConfig);
    expect(() => validateConfig(cfg)).not.toThrow();
  });

  it('applies reconciler defaults and rejects unsafe values', () => {
    const cfg = withEnv({ RECONCILER_INTERVAL_MS: undefined, RECONCILER_QUEUED_GRACE_MS: undefined }, loadConfig);
    expect(cfg.reconciler).toEqual({ intervalMs: 30000, queuedGraceMs: 60000, batchSize: 100 });

    const tooFast = withEnv({ RECONCILER_INTERVAL_MS: '10' }, loadConfig);
    expect(() => validateConfig(tooFast)).toThrow(/RECONCILER_INTERVAL_MS/);

    const noGrace = withEnv({ RECONCILER_QUEUED_GRACE_MS: '100' }, loadConfig);
    expect(() => validateConfig(noGrace)).toThrow(/RECONCILER_QUEUED_GRACE_MS/);
  });
});
