import type IORedis from 'ioredis';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAtMs: number;
}

export class RedisRateLimiter {
  constructor(
    private redis: IORedis,
    private windowMs: number = 60000,
  ) {}

  async check(key: string, limit: number): Promise<RateLimitResult> {
    const now = Date.now();
    const windowStart = now - this.windowMs;
    const redisKey = `ratelimit:${key}`;

    try {
      const pipeline = this.redis.pipeline();
      pipeline.zremrangebyscore(redisKey, '-inf', windowStart);
      pipeline.zadd(redisKey, now, `${now}:${Math.random().toString(36).slice(2, 8)}`);
      pipeline.zcard(redisKey);
      pipeline.pexpire(redisKey, this.windowMs);

      const results = await pipeline.exec();
      if (!results) {
        return { allowed: true, remaining: limit, resetAtMs: now + this.windowMs };
      }

      const count = (results[2]?.[1] as number) ?? 0;
      const allowed = count <= limit;
      const remaining = Math.max(0, limit - count);

      return { allowed, remaining, resetAtMs: now + this.windowMs };
    } catch {
      return { allowed: true, remaining: limit, resetAtMs: now + this.windowMs };
    }
  }
}
