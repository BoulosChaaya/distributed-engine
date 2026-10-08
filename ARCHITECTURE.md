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

**Early BullMQ redelivery (lease deferral)**: BullMQ's `stalledInterval` (5s) is shorter than the PG execution lease (30s). When BullMQ redelivers a stalled job before the PG lease expires, the receiving worker's `reclaimStalledTask` call throws `ClaimNotExpiredError`. The worker handles this by calling `job.moveToDelayed(claimExpiresAt + margin, job.token)`, which moves the job back to BullMQ's delayed state until approximately when the PG lease expires. This is fundamentally different from an execution retry: `moveToDelayed` does **not** consume a BullMQ attempt, does not increment `attemptsMade`, and does not trigger exponential backoff. When the delayed time elapses, BullMQ re-activates the job and the worker can successfully reclaim the task. Lease deferral and execution retries are completely separate concepts — a `ClaimNotExpiredError` is not a task failure, and deferring the job preserves the full retry budget for actual execution failures.

**Execution ownership protocol**: Each PROCESSING transition atomically establishes a unique `claim_token` and a `claim_expires_at` (default 30s TTL, set via PG `NOW()` to avoid Node/PG clock skew). All transitions out of PROCESSING (`COMPLETED`, `FAILED`, `QUEUED`) must present the matching `claim_token` — a stale worker whose ownership was transferred cannot mutate the task. Cancellation (`cancelTask`) overrides ownership without requiring the token, since it is an external administrative action; it clears the token and bumps the version, so the stale worker's subsequent transition attempt fails with `StaleVersionError`.

**Lease renewal**: The worker renews the execution lease at ~TTL/3 intervals (default 10s) while actively processing. `renewClaim(taskId, expectedClaimToken)` atomically verifies the task is PROCESSING with the matching claim token and extends `claim_expires_at` using PG `NOW()`. If renewal fails (ownership transferred, task cancelled), the renewal timer is cleared. The claim-token-protected transition remains the final safety guard.

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
- When BullMQ redelivers a stalled job before the PG lease expires, the worker defers the job via `job.moveToDelayed()` until approximately the PG claim expiry. This does not consume an execution retry attempt. When the deferred time elapses, BullMQ re-activates the job and the worker can reclaim the task.
- Renews execution lease at ~TTL/3 intervals while actively processing. Renewal requires the matching `claim_token`. If renewal fails (ownership lost, task cancelled), the timer is cleared and an execution-local `ownershipLost` flag is set. The worker checks this flag before attempting to complete or transition the task and cooperatively aborts if ownership was lost. The claim-token-protected transition remains the final guard against stale mutations — even if the flag check is bypassed, the DB rejects the stale token.
- Uses optimistic concurrency (version check) and execution ownership (claim token verification) on all state transitions out of PROCESSING
- Graceful shutdown: stops consuming new jobs, waits for in-flight jobs to complete (30s timeout), removes heartbeat key, closes connections

### Shared Package (`packages/shared/`)

- **State machine**: Defines valid task transitions (QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED) and enforces them at the repository layer
- **Task repository**: PG-backed CRUD with `SELECT FOR UPDATE` + version check + claim token verification for all transitions; includes `reclaimStalledTask` with lease expiry check for crash recovery and `renewClaim` for lease extension by the active owner. All lease timestamps use PG `NOW()` as the single authoritative clock.
- **Outbox publisher**: Polls PG for pending events, publishes to BullMQ with circuit breaker protection. Best-effort cancellation check (plain SELECT, not atomic with BullMQ publish — see Data Flow §2). Stops cleanly by draining in-progress polls.
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

## Distributed Tracing (OpenTelemetry)

### What a Trace Represents

A single trace follows one task through its entire lifecycle:

```
API (task.create)                         [PRODUCER span]
  └── OutboxPublisher (outbox.publish)    [INTERNAL span]
        └── Worker (task.process)         [CONSUMER span]
              ├── task.claim              [INTERNAL span]
              ├── task.complete           [INTERNAL span]
              └── task.fail              [INTERNAL span]
```

Each task submission creates a root span in the API. The trace context propagates through the outbox event (stored in PG as JSONB) into BullMQ job data, then into the worker. The worker's `task.process` span links back to the original trace via W3C `traceparent`, forming a single distributed trace across async service boundaries.

**Task ID vs Trace ID**: A `task.id` is a business identifier (UUIDv4) stored in PG and used throughout the system for lookups, cancellations, and state transitions. A `traceId` is an OpenTelemetry identifier (32 hex chars) that groups related spans for observability. Both appear as span attributes, but they serve different purposes. A single task maps to one trace under normal operation; retries create child spans within the same trace when the trace context survives in BullMQ job data.

### Context Propagation Path

```
API POST /tasks
  │  injectTraceContext() → W3C traceparent carrier
  │  stored in outbox_events.trace_context (JSONB)
  ▼
OutboxPublisher.processOutbox()
  │  reads trace_context from outbox row
  │  includes carrier in BullMQ job.data[TRACE_CONTEXT_KEY]
  ▼
Worker job handler
  │  extractTraceContext(job.data[TRACE_CONTEXT_KEY]) → OTel Context
  │  task.process span created with extracted context as parent
  ▼
Worker processTask()
     child spans (task.claim, task.complete, task.fail) inherit context
```

The `TRACE_CONTEXT_KEY` constant (`'traceContext'`) is the standard key used in BullMQ job data to carry the W3C trace context carrier object. This is a `Record<string, string>` containing at minimum a `traceparent` field.

### Instrumented Components

| Component | Spans Created | SpanKind |
|-----------|--------------|----------|
| API server | `task.create` | PRODUCER |
| Outbox publisher | `outbox.publish` | INTERNAL |
| Worker | `task.process` | CONSUMER |
| Worker | `task.claim` (initial/reclaim) | INTERNAL |
| Worker | `task.complete` | INTERNAL |
| Worker | `task.fail` | INTERNAL |

Auto-instrumentation is enabled for:
- **HTTP** (API only): Incoming requests are traced automatically; health/ready/live probes are excluded via `ignoreIncomingRequestHook`
- **PostgreSQL** (API and worker): All `pg` queries are traced as child spans of the active context

### Configuration

Tracing is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_SDK_DISABLED` | `false` | Disables the OTel SDK entirely (no-op tracer) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | none | OTLP HTTP endpoint (e.g. `http://otel-collector:4318`). If unset, no exporter is configured (spans go to the registered span processor only, useful for testing) |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | Sampling ratio (0.0–1.0). `1.0` traces everything; `0.1` samples 10% of traces |
| `OTEL_LOG_LEVEL` | `warn` | Diagnostic log level for OTel internals (`none`, `error`, `warn`, `info`, `debug`, `verbose`, `all`) |

### Local Development

To trace locally with Jaeger:

1. Add the `otel-collector` service to `docker-compose.yml` (already included as an optional profile)
2. Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` on the `api` and `worker` services
3. Open Jaeger UI at `http://localhost:16686`

Without an OTLP endpoint configured, tracing still works internally (context propagates end-to-end) but spans are not exported to any backend.

### Failure Isolation

**Critical invariant: the business system never depends on tracing for correctness.**

- Every tracing call (`startSpan`, `endSpan`, `recordError`, `injectTraceContext`, `extractTraceContext`) is wrapped in `try/catch`. Failures log a warning and return safe defaults (no-op spans, `ROOT_CONTEXT`, empty carriers).
- If the OTel SDK fails to initialize, `initTelemetry` logs the error and continues. All subsequent `tracing.*` calls use no-op spans from the API's default tracer.
- `shutdownTelemetry` has a configurable timeout (default 5s) so a hung exporter never blocks process exit.
- Missing, null, or malformed trace context in outbox events or BullMQ jobs is handled gracefully: `extractTraceContext` returns `ROOT_CONTEXT`, and the worker creates a new root span instead. This ensures backward compatibility with tasks created before tracing was added.

### Security

- No secrets, credentials, PII, or task payloads are included in span attributes. Spans carry only: task IDs, worker IDs, status values, priority levels, timing measurements, and error messages.
- The OTLP exporter connects to the Collector, which is a cluster-internal service. No traces leave the cluster unless the Collector is explicitly configured to export them.

### Known Limitations

- Trace context propagation depends on BullMQ job data. If BullMQ drops or corrupts job data, the trace chain breaks (worker creates a new root span). The task itself processes correctly regardless.
- The cancellation race window (between outbox publisher's status check and `BullMQ.add()`) means a cancelled task may get a BullMQ job with a valid trace context. The worker skips it harmlessly.
- Queue delay measurement (`task.queue_delay_ms` span attribute) uses wall-clock difference between `publishedAt` in job data and `Date.now()` at the worker. Clock skew between machines introduces measurement error, not correctness issues.
