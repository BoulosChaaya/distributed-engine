import { describe, it, expect, beforeAll, afterAll } from '@jest/globals';
import IORedis from 'ioredis';
import { RedisRateLimiter } from '../rate-limiter';

// Found by tracing the system map ("twice": a client that retries while over
// its limit). Real Redis.
const host = process.env.TEST_REDIS_HOST || 'localhost';
const port = Number(process.env.TEST_REDIS_PORT || 6379);
let redis: IORedis;

beforeAll(async () => {
  redis = new IORedis({ host, port, password: process.env.TEST_REDIS_PASSWORD, maxRetriesPerRequest: null });
  await redis.ping();
});
afterAll(async () => { await redis.quit(); });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('RedisRateLimiter against real Redis', () => {
  it('a client sending twice its limit still gets its limit through', async () => {
    // 5 per 1 s window, a request every 100 ms (10 per second) for 3 s.
    const limiter = new RedisRateLimiter(redis, 1000, 1000);
    const key = `t-${Math.random()}`;
    let allowed = 0;
    for (let i = 0; i < 30; i++) {
      if ((await limiter.check(key, 5)).allowed) allowed++;
      await sleep(100);
    }
    // About 5 per second over 3 s. Counting the rejected requests in the
    // window would let only the first 5 through, then nothing.
    expect(allowed).toBeGreaterThanOrEqual(12);
  });

  it('never lets more than the limit through inside one window', async () => {
    const limiter = new RedisRateLimiter(redis, 1000, 1000);
    const key = `t-${Math.random()}`;
    const results = await Promise.all(Array.from({ length: 20 }, () => limiter.check(key, 5)));
    expect(results.filter((r) => r.allowed).length).toBe(5);
  });
});
