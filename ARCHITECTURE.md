# Architecture

Technical architecture of the distributed task execution engine.

## System Overview

```
┌──────────┐     ┌──────────────────┐     ┌──────────────┐
│  Client  │────▶│  API Server      │────▶│  PostgreSQL   │
└──────────┘     │  (Express)       │     │  (source of   │
                 │                  │     │   truth)      │
                 │  - Validates     │     └──────┬───────┘
                 │  - Creates task  │            │
                 │    + outbox in   │     ┌──────▼───────┐
                 │    one PG txn    │     │  Outbox      │
                 └──────────────────┘     │  Publisher   │
                                          │  (polls PG)  │
                                          └──────┬───────┘
                                                 │ BullMQ.add()
                                          ┌──────▼───────┐
                                          │    Redis     │
                                          │  (BullMQ     │
                                          │   queue)     │
                                          └──────┬───────┘
                                                 │
                                          ┌──────▼───────┐
                                          │   Workers    │
                                          │  (BullMQ     │
                                          │   consumers) │
                                          └──────────────┘
```

## Data Flow

### 1. Task Submission

```
Client                API                 PostgreSQL
  │                    │                      │
  │── POST /tasks ────▶│                      │
  │                    │── BEGIN ─────────────▶│
  │                    │── INSERT task ───────▶│  status='QUEUED', version=1
  │                    │── INSERT outbox ─────▶│  status='PENDING'
  │                    │── COMMIT ───────────▶│
  │◀── 201 Created ───│                      │
```

The task is created as QUEUED (not PENDING — there is no PENDING task state). This eliminates a race condition where a worker could pick up a job from BullMQ before the outbox publisher has enqueued it.

### 2. Outbox Publication

```
Outbox Publisher            PostgreSQL              BullMQ/Redis
      │                         │                       │
      │── poll (SELECT + FOR ──▶│                       │
      │   UPDATE SKIP LOCKED)   │                       │
      │◀── claimed events ─────│                       │
      │                         │                       │
      │── SELECT task FOR ─────▶│                       │
      │   UPDATE (check cancel) │                       │
      │                         │                       │
      │── BullMQ.add(jobId = ──────────────────────────▶│
      │   taskId, attempts =    │                       │
      │   maxRetries + 1)       │                       │
      │                         │                       │
      │── UPDATE outbox ───────▶│  status='DELIVERED'   │
      │   status=DELIVERED      │                       │
```

Key properties:
- **Idempotent publication**: Uses taskId as BullMQ jobId. Re-publishing the same event is a no-op while the job exists in BullMQ. After BullMQ removes completed jobs (retention of 100 jobs), the jobId becomes reusable — but this only matters if outbox events are manually reset, since the outbox marks events DELIVERED on successful publication.
- **Per-job retry config**: Each BullMQ job gets `attempts = maxRetries + 1` from the task's configuration.
- **Cancellation best-effort check**: The publisher checks task status before calling `BullMQ.add()`. Because PostgreSQL and Redis are separate systems, this check is **not** atomic with the publish — a concurrent cancellation can commit between the check and the add. If this race occurs, BullMQ holds a job for a cancelled task, but the worker's status check before processing ensures it is skipped harmlessly. PostgreSQL is the authoritative source of task state.
- **Circuit breaker**: After 5 consecutive BullMQ failures, the publisher enters OPEN state and stops attempting for 30 seconds. In HALF_OPEN state, only a single event is processed as a probe (not the full batch).

### 3. Task Processing

```
Worker                  PostgreSQL              BullMQ/Redis
  │                         │                       │
  │◀── job consumed ───────────────────────────────│
  │                         │                       │
  │── SELECT task ─────────▶│                       │
  │   (check status)        │                       │
  │                         │                       │
  │── transition to ───────▶│  QUEUED → PROCESSING  │
  │   PROCESSING             │  version + 1          │
  │   (SELECT FOR UPDATE +   │                       │
  │    version check)        │                       │
  │                         │                       │
  │── [do work] ───────────▶│                       │
  │                         │                       │
  │── transition to ───────▶│  PROCESSING → COMPLETED│
  │   COMPLETED              │  version + 1          │
```

### 4. Failure and Retry

On task failure, the worker checks whether BullMQ has remaining retry attempts:

- **Intermediate failure** (more BullMQ attempts available): Transitions task PROCESSING → QUEUED in PG, then throws to let BullMQ retry with exponential backoff.
- **Final failure** (no more BullMQ attempts): Transitions task PROCESSING → FAILED in PG.

This keeps PG and BullMQ states synchronized. The PG state always reflects the actual task lifecycle.

### 5. Stalled Job Recovery

If a worker crashes while processing a task:

1. BullMQ detects the stalled job (via `stalledInterval`, default 5 seconds)
2. BullMQ redelivers the job to another worker
3. The new worker finds the task in PROCESSING state with the crashed worker's `claimed_by` and an **expired** `claim_expires_at`
4. The new worker calls `reclaimStalledTask`, which verifies:
   - The task is PROCESSING (status check)
   - The version matches (optimistic concurrency)
   - The caller is not the same worker (self-reclaim rejection)
   - The previous claim has expired (`claim_expires_at < NOW()`)
5. On success, a new `claim_token` and `claim_expires_at` are set, and the new worker's ID replaces `claimed_by`
6. Processing continues from the start

PROCESSING status alone is **not** sufficient evidence that a job stalled. A task in PROCESSING with a non-expired claim is actively being worked on by its owner. Reclaim is only permitted after the claim expires, which provides a bounded recovery window that prevents live-owner theft.

**Execution ownership protocol**: Each PROCESSING transition atomically establishes a unique `claim_token` and a `claim_expires_at` (default 30s TTL). All transitions out of PROCESSING (`COMPLETED`, `FAILED`, `QUEUED`) must present the matching `claim_token` — a stale worker whose ownership was transferred cannot mutate the task. Cancellation (`cancelTask`) overrides ownership without requiring the token, since it is an external administrative action; it clears the token and bumps the version, so the stale worker's subsequent transition attempt fails with `StaleVersionError`.

**Failure handler safety**: The worker's failure handler uses the stored `taskVersion` and `claimToken` from the initial claim — it does **not** re-read the latest task state. If ownership has transferred (version or claim token changed), the transition is rejected harmlessly. This prevents the scenario where a stale worker re-reads the latest version and mutates a task now owned by another execution.

BullMQ's `maxStalledCount` (default 2) limits how many times a single job can be reclaimed. After that limit, BullMQ marks the job as failed and the worker transitions the task to FAILED.

## Component Details

### API Server (`apps/api/`)

- Express HTTP server with Zod input validation
- Creates tasks atomically with outbox events in a single PG transaction
- Runs the outbox publisher as a background process
- Health probes: `/ready` checks PG only (task submission requires only PG), `/live` always returns 200, `/health` checks both PG and Redis and reports outbox circuit breaker state
- `/metrics` returns JSON with task status counts, queue depth, and outbox stats
- Graceful shutdown: stops accepting connections, drains in-flight requests, stops outbox publisher (waits for in-progress poll to complete), closes connections. Exit code 1 if shutdown times out with active requests.

### Worker (`apps/worker/`)

- BullMQ Worker consuming from the "tasks" queue
- Publishes heartbeat to Redis every 10 seconds (`worker:{id}` key with 30s TTL)
- Validates task state in PG before processing (skips cancelled/completed/failed/missing tasks)
- Reclaims stalled PROCESSING tasks when BullMQ redelivers them after a worker crash, but only after the previous execution claim has expired (`claim_expires_at < NOW()`). Reclaim generates a new `claim_token` and sets a new expiry.
- Uses optimistic concurrency (version check) and execution ownership (claim token verification) on all state transitions out of PROCESSING
- Graceful shutdown: stops consuming new jobs, waits for in-flight jobs to complete (30s timeout), removes heartbeat key, closes connections

### Shared Package (`packages/shared/`)

- **State machine**: Defines valid task transitions (QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED) and enforces them at the repository layer
- **Task repository**: PG-backed CRUD with `SELECT FOR UPDATE` + version check + claim token verification for all transitions; includes `reclaimStalledTask` with lease expiry check for crash recovery
- **Outbox publisher**: Polls PG for pending events, publishes to BullMQ with circuit breaker protection. Cancellation check uses `SELECT FOR UPDATE` for atomicity. Stops cleanly by draining in-progress polls.
- **Migrations**: Schema versioning with advisory lock for concurrent startup safety
- **Types**: Task, OutboxEvent, WorkerStatus interfaces

### Dashboard (`apps/web/`)

- Next.js app for viewing system state
- Fetches global task counts from `/metrics` endpoint (not from a single page of `/tasks`)
- Workers page shows live data from `/workers` endpoint (Redis heartbeats)

## Concurrency and Safety

### Optimistic Concurrency Control

Every task has a `version` column, starting at 1. Each state transition:
1. `SELECT ... FOR UPDATE` (row lock)
2. Check `version = expectedVersion`
3. Validate transition against state machine
4. If leaving PROCESSING: verify caller's `claim_token` matches the task's current `claim_token` (`ClaimTokenMismatchError` on mismatch)
5. `UPDATE ... SET version = version + 1`

If two processes race, only the first succeeds. The second gets a `StaleVersionError`. If a stale worker presents an old claim token after ownership transferred, it gets a `ClaimTokenMismatchError`.

### Migration Safety

`runMigrations()` acquires a PostgreSQL advisory lock (`pg_advisory_lock(42)`) before checking and applying migrations. Multiple API/worker replicas can safely start concurrently — only one will run migrations, others will wait.

### At-Least-Once Delivery

The system guarantees at-least-once delivery, not exactly-once. Duplicate processing is possible in edge cases:
- Worker completes work but crashes before writing COMPLETED to PG
- BullMQ retries a job that was already processed

Task handlers should be idempotent to handle these cases safely.

## Scaling Considerations

- **API servers**: Stateless, horizontally scalable. PG pool size is the main constraint.
- **Workers**: Stateless, horizontally scalable. Each worker processes `QUEUE_CONCURRENCY` jobs concurrently.
- **PostgreSQL**: Single instance (StatefulSet). For higher throughput, consider read replicas or connection pooling (PgBouncer).
- **Redis**: Single instance (StatefulSet) with AOF persistence. BullMQ supports Redis Cluster for higher throughput, but this is not configured.

HPA in the K8s manifests scales on CPU and memory. Queue-depth-based worker scaling would require a custom metrics adapter (Prometheus + KEDA or similar), which is not included.
