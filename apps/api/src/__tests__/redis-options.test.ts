import { describe, it, expect } from '@jest/globals';
import { Queue } from 'bullmq';
import { buildRedisOptions, redisRetryStrategy } from '../redis-options';

describe('Redis reconnect strategy', () => {
  it('backs off 250 ms per attempt, capped at 2 s', () => {
    expect([1, 2, 4, 8, 9, 50].map(redisRetryStrategy)).toEqual([250, 500, 1000, 2000, 2000, 2000]);
  });

  it("replaces BullMQ's default (exp(times), up to 20 s) on the Queue's own connection", async () => {
    const queue = new Queue('redis-options-test', {
      connection: { ...buildRedisOptions({ host: '127.0.0.1', port: 1 }), lazyConnect: true },
    });
    // The options BullMQ actually uses for the Queue's connection.
    const opts = (queue.getBackend() as unknown as { connection: { opts: { retryStrategy: (n: number) => number } } }).connection.opts;
    expect(opts.retryStrategy(10)).toBe(2000);
    expect(opts.retryStrategy(30)).toBe(2000);
    await queue.close();
  });

  it("BullMQ's own default would wait 20 s (what the API used before)", async () => {
    const queue = new Queue('redis-options-test-default', {
      connection: { host: '127.0.0.1', port: 1, lazyConnect: true, maxRetriesPerRequest: null },
    });
    const opts = (queue.getBackend() as unknown as { connection: { opts: { retryStrategy: (n: number) => number } } }).connection.opts;
    expect(opts.retryStrategy(30)).toBe(20000);
    await queue.close();
  });
});
