# Known limits

What the engine does not do, or does only within stated bounds. Each item
says why it is acceptable here and what would change it. Measured numbers
come from the load test ([load-test.md](load-test.md)).

## Delivery and ordering

- **Execution is at least once.** A task that was running when its worker
  died runs again on another worker. PostgreSQL records exactly one terminal
  state per task (version column plus claim token), but a side effect the
  handler performed before the crash (an HTTP call, an email) can happen
  twice. Handlers with external effects must be idempotent. The `http` task
  type sends `Idempotency-Key: <task id>`, the same on every attempt, so a
  receiver that honours the header drops the repeat; a receiver that ignores
  it still sees the call twice. The load test counts these re-executions
  instead of failing on them.
- **A resend is safe only with an idempotency key.** Both submit routes
  (`POST /tasks` and `POST /tenant/tasks`) and both workflow routes accept
  `idempotencyKey`: the same key and body return the first task (200), the
  same key with another body is refused (409). Without a key, a client that
  never saw its 201 and sends again creates a second task.
- **No global ordering.** Within one tenant, tasks are published by priority
  then age, and admission keeps that order under the tenant's concurrency
  limit. Across tenants, weighted fair publishing interleaves them. With
  several API replicas each running a publisher, and several workers, start
  order is approximate; completion order is not ordered at all. A single
  publisher would give strict publish order at the cost of a single point
  of throughput; it is not built.
- **Tenant admission is a soft gate across API replicas.** Two publishers
  can both see one free slot and publish one job each. The worker's
  concurrency lease in PostgreSQL is the hard limit; the extra job is
  deferred, not run.

## Availability

- **One PostgreSQL primary.** PostgreSQL is the source of truth and a single
  point of failure for writes (`k8s/postgres.yaml` runs one replica). While
  it is down or frozen, nothing is accepted or completed; work resumes when
  it returns. There is no replication or failover in the manifests; backups
  and restore are manual ([backup-restore.md](backup-restore.md)).
- **One Redis.** Redis holds only the queue. Losing it loses no task: the
  reconciler re-publishes from PostgreSQL after `RECONCILER_QUEUED_GRACE_MS`
  (60 s), so a Redis loss costs latency
  ([redis-loss-recovery.md](redis-loss-recovery.md)). While Redis is down,
  submissions are still accepted (the tenant rate limiter fails open) and
  wait in the outbox.
- **The tenant rate limiter fails open.** Without Redis it lets requests
  through. It guards against bursts; quotas are enforced in PostgreSQL in the
  acceptance transaction and keep working.
- **Recovery is bounded, not instant.** A killed worker's jobs are handed
  out again when the job lock expires (15 s with the Kubernetes settings,
  30 s by default); events a killed API had claimed but not published wait
  for the 30 s stale-claim timeout; jobs lost from Redis are re-published
  after the reconciler's grace period (60 s). The load test shows these as
  the longest waits ([load-test.md](load-test.md)).
- **Deploys hand work back, they don't drop it.** On SIGTERM a worker gives
  running tasks 20 s (`WORKER_SHUTDOWN_DRAIN_MS`) to finish, then stops the
  rest and hands them back as QUEUED without charging a retry; another
  worker starts them at once, from the beginning. A handler that ignores its
  abort signal keeps running until the 30 s shutdown guard, is killed, and
  is recovered by its lease (charged). The built-in handlers all stop on the
  signal. Orchestrators must allow at least 30 s (k8s and compose use 40 s).
- **One region.** No multi-region deployment, no cross-region failover.

## Capacity

- **PostgreSQL connections are the scaling ceiling.** At full HPA scale the
  pods can open exactly `max_connections` (260): API 10 × (9 pooled + 1
  LISTEN), workers 20 × 7, 20 headroom. CI fails a change that breaks the
  sum. Going further needs more PostgreSQL memory or a connection pooler in
  session mode; PgBouncer in transaction mode would break the advisory locks
  and `LISTEN`.
- **Autoscaling is on CPU and memory only.** Queue depth is exported
  (`engine_queue_jobs`), but no in-cluster Prometheus or KEDA is set up, so
  workers do not scale on backlog.
- **Request and result sizes are bounded.** JSON bodies up to 1 MB; a task
  result is stored up to 64 KB; `maxRetries` at most 10; `sleep` at most
  5 minutes; page size at most 100.

## Features not built

- Cron schedules start single tasks, not workflows.
- `FORBID_OVERLAP` runs every occurrence, one at a time and oldest first;
  none is dropped. A blocked occurrence asks for the schedule's lease every 5 s
  (`SCHEDULE_OVERLAP_RECHECK_MS`). A schedule whose runs take longer than its
  interval therefore builds a backlog that drains at the speed of the runs.
- No dead-letter queue beyond the FAILED state (and operator or tenant
  retry); no saga or compensation steps in workflows.
- The `http` task type checks the address it connects to, but an internal
  service listed in `HTTP_TASK_PRIVATE_HOSTS` is reachable by anyone who can
  submit a task; keep that list to services meant to be called.

## Security posture of the defaults

- **TLS** is not configured at the Istio Gateway (certificates are
  environment-specific). Inside the mesh, traffic is mTLS.
- **The dashboard** uses one HTTP Basic operator login.
- **Local compose only:** Grafana runs with anonymous admin access, and
  PostgreSQL uses the `postgres/postgres` development password. Neither is in
  the Kubernetes manifests.

## Observability

- Logs (Loki) and traces (Jaeger) are operational telemetry, not records:
  they can be incomplete, and PostgreSQL stays the record of every outcome.
- Prometheus scraping is set up for docker compose only.
