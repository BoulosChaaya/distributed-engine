import { describe, it, expect } from '@jest/globals';
import type { Pool } from 'pg';
import { waitForPostgres } from '../db/migrations';

function fakePool(failures: number) {
  let calls = 0;
  return {
    pool: { query: async () => { calls++; if (calls <= failures) throw new Error('ECONNREFUSED'); return { rows: [] }; } } as unknown as Pool,
    calls: () => calls,
  };
}

describe('waitForPostgres', () => {
  it('returns once the database answers, retrying with backoff', async () => {
    const { pool, calls } = fakePool(2);
    const retries: number[] = [];
    await waitForPostgres(pool, { timeoutMs: 5000, onRetry: (n) => retries.push(n) });
    expect(calls()).toBe(3);
    expect(retries).toEqual([1, 2]);
  });

  it('gives up with the last error after the timeout', async () => {
    const { pool } = fakePool(1_000);
    await expect(waitForPostgres(pool, { timeoutMs: 300 })).rejects.toThrow('ECONNREFUSED');
  });
});
