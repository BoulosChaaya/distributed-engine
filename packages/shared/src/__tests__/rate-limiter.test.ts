import { describe, it, expect, jest } from '@jest/globals';
import { RedisRateLimiter } from '../rate-limiter';

// Unit tests with a stand-in client. The real behaviour (one atomic script,
// only allowed requests recorded) is tested against Redis in
// rate-limiter-redis.test.ts.
function makeMockRedis(overrides: any = {}): any {
  return {
    status: 'ready',
    eval: jest.fn<any>().mockResolvedValue([1, 1]),
    ...overrides,
  };
}

describe('RedisRateLimiter', () => {
  it('should allow requests under limit', async () => {
    const redis = makeMockRedis();
    const limiter = new RedisRateLimiter(redis, 60000);

    const result = await limiter.check('tenant:t1', 10);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(9);
  });

  it('should deny requests over limit', async () => {
    const redis = makeMockRedis({ eval: jest.fn<any>().mockResolvedValue([0, 10]) });
    const limiter = new RedisRateLimiter(redis, 60000);
    const result = await limiter.check('tenant:t1', 10);

    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
  });

  it('should fail-open on Redis error', async () => {
    const redis = makeMockRedis({ eval: jest.fn<any>().mockRejectedValue(new Error('Redis unavailable')) });
    const limiter = new RedisRateLimiter(redis, 60000);
    const result = await limiter.check('tenant:t1', 10);

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(10);
  });

  it('passes the key, the window and the limit to one script call', async () => {
    const redis = makeMockRedis();
    const limiter = new RedisRateLimiter(redis, 60000);

    await limiter.check('tenant:abc', 5);

    expect(redis.eval).toHaveBeenCalledTimes(1);
    const args = redis.eval.mock.calls[0];
    expect(args[1]).toBe(1);
    expect(args[2]).toBe('ratelimit:tenant:abc');
    expect(args[5]).toBe(5);
    expect(args[7]).toBe(60000);
  });
});

describe('RedisRateLimiter while Redis is unavailable', () => {
  // The API's Redis client waits for a reconnect instead of failing
  // (maxRetriesPerRequest: null), so "fail open on error" alone never fired:
  // the call simply waited, and every tenant request with it, for as long as
  // Redis was down.
  it('fails open within the timeout when Redis does not answer', async () => {
    const redis = makeMockRedis({ eval: jest.fn<any>().mockReturnValue(new Promise(() => {})) });
    const limiter = new RedisRateLimiter(redis, 60000, 100);

    const started = Date.now();
    const result = await limiter.check('tenant:t1', 10);
    expect(Date.now() - started).toBeLessThan(1000);
    expect(result.allowed).toBe(true);
  });

  it('fails open at once, without queueing a command, while the client is not connected', async () => {
    const redis = makeMockRedis({ status: 'reconnecting' });
    const limiter = new RedisRateLimiter(redis, 60000, 100);

    const started = Date.now();
    const result = await limiter.check('tenant:t1', 10);
    expect(Date.now() - started).toBeLessThan(50);
    expect(result.allowed).toBe(true);
    expect(redis.eval).not.toHaveBeenCalled();
  });
});
