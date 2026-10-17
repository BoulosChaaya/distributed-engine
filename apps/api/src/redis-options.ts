/**
 * Delay before reconnect attempt `times` (1, 2, 3, …): 250 ms, 500 ms, …,
 * capped at 2 s.
 *
 * The cap is the point. BullMQ fills in its own strategy when it is given
 * connection options instead of a client: Math.exp(times), capped at 20 s.
 * After a Redis outage of a minute or more, the Queue's connection is already
 * waiting 20 s between attempts, so publishing resumed up to 20 s after Redis
 * was back (17 s in a recorded outage) even though everything else had
 * reconnected. A 2 s cap bounds that, and costs at most one connect attempt
 * every 2 s while Redis is down.
 */
export function redisRetryStrategy(times: number): number {
  return Math.min(times * 250, 2000);
}

/**
 * Options shared by the API's Redis client and its BullMQ Queue. The Queue
 * gets options rather than the shared client so BullMQ owns its connection
 * lifecycle; passing retryStrategy explicitly overrides BullMQ's default.
 */
export function buildRedisOptions(redis: { host: string; port: number; password?: string }) {
  return {
    host: redis.host,
    port: redis.port,
    password: redis.password,
    maxRetriesPerRequest: null as null,
    retryStrategy: redisRetryStrategy,
  };
}
