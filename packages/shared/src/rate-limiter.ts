import type { Redis as IORedis } from 'ioredis';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAtMs: number;
}

/**
 * Sliding-window rate limit per key in Redis. It is a guard against bursts,
 * not an accounting of record (quotas are enforced in PostgreSQL when a task
 * is accepted), so it fails open: with Redis unavailable a request is let
 * through rather than refused or held.
 *
 * Failing open has to be fast. The API's Redis client waits for a reconnect
 * instead of failing commands (maxRetriesPerRequest: null), so a command sent
 * during an outage neither errors nor returns: it waits. Without a bound
 * every tenant request waited for Redis to come back, although accepting a
 * task needs only PostgreSQL. So: not connected -> allow at once, without
 * queueing a command; connected but slow -> allow after timeoutMs.
 */
export class RedisRateLimiter {
  constructor(
    private redis: IORedis,
    private windowMs: number = 60000,
    private timeoutMs: number = 1000,
  ) {}

  async check(key: string, limit: number): Promise<RateLimitResult> {
    const now = Date.now();
    const windowStart = now - this.windowMs;
    const redisKey = `ratelimit:${key}`;

    const allowAll = { allowed: true, remaining: limit, resetAtMs: now + this.windowMs };
    if (this.redis.status !== 'ready') return allowAll;

    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const pipeline = this.redis.pipeline();
      pipeline.zremrangebyscore(redisKey, '-inf', windowStart);
      pipeline.zadd(redisKey, now, `${now}:${Math.random().toString(36).slice(2, 8)}`);
      pipeline.zcard(redisKey);
      pipeline.pexpire(redisKey, this.windowMs);

      const timedOut = new Promise<null>((resolve) => { timer = setTimeout(() => resolve(null), this.timeoutMs); });
      const results = await Promise.race([pipeline.exec(), timedOut]);
      if (!results) {
        return { allowed: true, remaining: limit, resetAtMs: now + this.windowMs };
      }

      const count = (results[2]?.[1] as number) ?? 0;
      const allowed = count <= limit;
      const remaining = Math.max(0, limit - count);

      return { allowed, remaining, resetAtMs: now + this.windowMs };
    } catch {
      return allowAll;
    } finally {
      clearTimeout(timer);
    }
  }
}
