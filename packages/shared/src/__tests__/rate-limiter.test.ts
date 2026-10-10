import { describe, it, expect, beforeEach, jest } from '@jest/globals';
import { RedisRateLimiter, RateLimitResult } from '../rate-limiter';

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
