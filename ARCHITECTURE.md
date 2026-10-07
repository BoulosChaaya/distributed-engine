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

The task is created as QUEUED (not PENDING). This eliminates a race condition where a worker could pick up a job from BullMQ before the outbox publisher has transitioned the task from PENDING to QUEUED.

### 2. Outbox Publication

```
Outbox Publisher            PostgreSQL              BullMQ/Redis
      │                         │                       │
      │── poll (SELECT + FOR ──▶│                       │
      │   UPDATE SKIP LOCKED)   │                       │
      │◀── claimed events ─────│                       │
      │                         │                       │
      │── check task status ───▶│                       │
      │   (skip if CANCELLED)   │                       │
      │                         │                       │
      │── BullMQ.add(jobId = ──────────────────────────▶│
      │   taskId, attempts =    │                       │
      │   maxRetries + 1)       │                       │
      │                         │                       │
      │── UPDATE outbox ───────▶│  status='DELIVERED'   │
      │   status=DELIVERED      │                       │
```

Key properties:
- **Idempotent publication**: Uses taskId as BullMQ jobId. Re-publishing the same event is a no-op in BullMQ.
- **Per-job retry config**: Each BullMQ job gets `attempts = maxRetries + 1` from the task's configuration.
- **Cancellation check**: Skips publishing for tasks that were cancelled between creation and publication.
- **Circuit breaker**: After 5 consecutive BullMQ failures, the publisher enters OPEN state and stops attempting for 30 seconds.

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

## Component Details

### API Server (`apps/api/`)

- Express HTTP server with Zod input validation
- Creates tasks atomically with outbox events in a single PG transaction
- Runs the outbox publisher as a background process
- Health probes: `/ready` checks PG only (task submission requires only PG), `/live` always returns 200, `/health` checks both PG and Redis and reports outbox circuit breaker state
- `/metrics` returns JSON with task status counts, queue depth, and outbox stats
- Graceful shutdown: stops accepting connections, drains in-flight requests, stops outbox publisher, closes connections

### Worker (`apps/worker/`)

- BullMQ Worker consuming from the "tasks" queue
- Publishes heartbeat to Redis every 10 seconds (`worker:{id}` key with 30s TTL)
- Validates task state in PG before processing (skips cancelled/completed/missing tasks)
- Uses optimistic concurrency (version check) on all state transitions
- Graceful shutdown: stops consuming new jobs, waits for in-flight jobs to complete (30s timeout), removes heartbeat key, closes connections

### Shared Package (`packages/shared/`)

- **State machine**: Defines valid task transitions and enforces them at the repository layer
- **Task repository**: PG-backed CRUD with `SELECT FOR UPDATE` + version check for all transitions
- **Outbox publisher**: Polls PG for pending events, publishes to BullMQ with circuit breaker protection
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
4. `UPDATE ... SET version = version + 1`

If two processes race, only the first succeeds. The second gets a `StaleVersionError`.

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
