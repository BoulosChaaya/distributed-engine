/**
 * Health and metrics talk to PostgreSQL and Redis, and both clients wait
 * instead of failing when their server is unreachable: the Redis client uses
 * maxRetriesPerRequest: null (commands queue until Redis returns) and the
 * PostgreSQL pool waits for a connection. An endpoint that awaits them
 * directly hangs for as long as the dependency is down, which is exactly when
 * someone is looking at it. Every dependency call made on behalf of a probe
 * therefore goes through settleWithin: it reports what happened within the
 * budget and never waits longer.
 */
export type Settled<T> = { ok: true; value: T } | { ok: false; reason: 'timeout' | 'error' };

export function settleWithin<T>(promise: Promise<T>, timeoutMs: number): Promise<Settled<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<Settled<T>>((resolve) => {
    timer = setTimeout(() => resolve({ ok: false, reason: 'timeout' }), timeoutMs);
  });
  const settled = promise.then(
    (value): Settled<T> => ({ ok: true, value }),
    (): Settled<T> => ({ ok: false, reason: 'error' }),
  );
  return Promise.race([settled, timeout]).finally(() => clearTimeout(timer));
}

export interface HealthDeps {
  pingPostgres: () => Promise<unknown>;
  pingRedis: () => Promise<unknown>;
  circuitState: () => string;
  timeoutMs: number;
}

export interface HealthReport {
  httpStatus: 200 | 503;
  data: {
    status: 'healthy' | 'degraded' | 'unhealthy';
    timestamp: Date;
    outboxCircuitBreaker: string;
    dependencies: { postgres: 'up' | 'down'; redis: 'up' | 'down' };
  };
}

/**
 * PostgreSQL is required (it holds every decision); Redis is not (work keeps
 * being accepted into the outbox). So: PG down → 503 unhealthy; Redis down or
 * breaker not CLOSED → 200 degraded. Both checks run in parallel, each within
 * the budget.
 */
export async function checkHealth(deps: HealthDeps): Promise<HealthReport> {
  const [pg, redis] = await Promise.all([
    settleWithin(deps.pingPostgres(), deps.timeoutMs),
    settleWithin(deps.pingRedis(), deps.timeoutMs),
  ]);
  const breaker = deps.circuitState();
  const degraded = !redis.ok || breaker !== 'CLOSED';
  return {
    httpStatus: pg.ok ? 200 : 503,
    data: {
      status: pg.ok ? (degraded ? 'degraded' : 'healthy') : 'unhealthy',
      timestamp: new Date(),
      outboxCircuitBreaker: breaker,
      dependencies: { postgres: pg.ok ? 'up' : 'down', redis: redis.ok ? 'up' : 'down' },
    },
  };
}
