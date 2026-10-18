import { describe, it, expect, jest } from '@jest/globals';
import { RedisRateLimiter } from '../rate-limiter';

function makeMockRedis(overrides: any = {}): any {
  const pipeline = {
    zremrangebyscore: jest.fn().mockReturnThis(),
    zadd: jest.fn().mockReturnThis(),
    zcard: jest.fn().mockReturnThis(),
    pexpire: jest.fn().mockReturnThis(),
    exec: jest.fn<any>().mockResolvedValue([
      [null, 0],
      [null, 1],
      [null, 1],
      [null, 1],
    ]),
  };

  return {
    status: 'ready',
    pipeline: jest.fn().mockReturnValue(pipeline),
    _pipeline: pipeline,
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
    const redis = makeMockRedis();
    redis._pipeline.exec.mockResolvedValue([
      [null, 0],
      [null, 1],
      [null, 11],
      [null, 1],
    ]);

    const limiter = new RedisRateLimiter(redis, 60000);
    const result = await limiter.check('tenant:t1', 10);

    expect(result.allowed).toBe(false);
    expect(result.remaining).toBe(0);
  });

  it('should fail-open on Redis error', async () => {
    const redis = makeMockRedis();
    redis.pipeline.mockImplementation(() => {
      throw new Error('Redis unavailable');
    });

    const limiter = new RedisRateLimiter(redis, 60000);
    const result = await limiter.check('tenant:t1', 10);

    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(10);
  });

  it('should fail-open when pipeline returns null', async () => {
    const redis = makeMockRedis();
    redis._pipeline.exec.mockResolvedValue(null);

    const limiter = new RedisRateLimiter(redis, 60000);
    const result = await limiter.check('tenant:t1', 10);

    expect(result.allowed).toBe(true);
  });

  it('should use correct sliding window key', async () => {
    const redis = makeMockRedis();
    const limiter = new RedisRateLimiter(redis, 60000);

    await limiter.check('tenant:abc', 5);

    const pipeline = redis._pipeline;
    expect(pipeline.zremrangebyscore).toHaveBeenCalled();
  });
});

describe('RedisRateLimiter while Redis is unavailable', () => {
  // The API's Redis client waits for a reconnect instead of failing
  // (maxRetriesPerRequest: null), so "fail open on error" alone never fired:
  // the call simply waited, and every tenant request with it, for as long as
  // Redis was down.
  it('fails open within the timeout when Redis does not answer', async () => {
    const redis = makeMockRedis();
    redis._pipeline.exec.mockReturnValue(new Promise(() => {}));
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
    expect(redis.pipeline).not.toHaveBeenCalled();
  });
});
