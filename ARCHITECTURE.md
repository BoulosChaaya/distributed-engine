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

### 6. One-Time Scheduled Tasks

```
Client                API                 PostgreSQL
  │                    │                      │
  │── POST /tasks ────▶│                      │
  │   {scheduledFor}   │── BEGIN ─────────────▶│
  │                    │── INSERT task ───────▶│  status='SCHEDULED', scheduled_for=<ts>
  │                    │── COMMIT ───────────▶│
  │◀── 201 Created ───│                      │
  │                    │                      │
  │       Scheduler Service (polls PG)        │
  │                    │                      │
  │          ┌─────────┴──────────┐           │
  │          │ releaseDueScheduled│           │
  │          │ Tasks(batchSize)   │           │
  │          └─────────┬──────────┘           │
  │                    │── FOR UPDATE ───────▶│
  │                    │   SKIP LOCKED        │
  │                    │── UPDATE status ────▶│  SCHEDULED → QUEUED
  │                    │── INSERT outbox ────▶│  SCHEDULED_TASK_RELEASED
  │                    │── COMMIT ──────────▶│
```

A one-time scheduled task starts in SCHEDULED state and remains there until its `scheduled_for` timestamp passes. The scheduler service polls for due SCHEDULED tasks and atomically transitions them to QUEUED with an outbox event, which the outbox publisher then delivers to BullMQ as with any other task.

### 7. Recurring Schedules

```
Client                API                 PostgreSQL              Scheduler
  │                    │                      │                      │
  │── POST /schedules─▶│                      │                      │
  │   {cron, tz, ...}  │── INSERT schedule ──▶│  status='ACTIVE',    │
  │                    │                      │  next_run_at=<ts>    │
  │◀── 201 Created ───│                      │                      │
  │                    │                      │                      │
  │                    │                      │◀── poll due ────────│
  │                    │                      │   schedules          │
  │                    │                      │                      │
  │                    │                      │── FOR UPDATE ───────▶│
  │                    │                      │   SKIP LOCKED        │
  │                    │                      │                      │
  │                    │                      │── INSERT task ──────▶│  QUEUED + outbox
  │                    │                      │── advance next_run──▶│
  │                    │                      │── COMMIT ──────────▶│
```

A recurring schedule is a separate durable object in `recurring_schedules` that generates task occurrences on its cron cadence. Each occurrence is a standard task (status QUEUED) with `schedule_id` and `scheduled_for` columns linking it back to the schedule. The scheduler never directly publishes to BullMQ — it creates PENDING outbox events that the outbox publisher handles.

**Occurrence uniqueness**: A partial unique index `(schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL` prevents duplicate task generation for the same schedule at the same time, even across concurrent schedulers.

**Misfire policies** (when the scheduler was down and missed occurrences):
- `SKIP_MISSED`: Generate one occurrence for the missed `nextRunAt`, advance to next future time
- `RUN_ONCE`: Same as SKIP_MISSED — generate one catch-up occurrence
- `CATCH_UP_ALL`: Generate all missed occurrences in bounded batches (configurable `catchUpBatchSize`). Remaining missed occurrences are processed on subsequent poll cycles. This provides backpressure without silently discarding missed work.

**Overlap policies**:
- `ALLOW_OVERLAP`: New occurrences generate regardless of in-flight ones
- `FORBID_OVERLAP`: If any occurrence for this schedule is in QUEUED or PROCESSING state, defer the next occurrence until the active one completes

**Multi-scheduler concurrency**: `fetchDueSchedules` selects due schedules, and `processScheduleWithLock` acquires a `FOR UPDATE SKIP LOCKED` row lock per schedule. Multiple scheduler instances can run concurrently — each processes different schedules without contention.

**Schedule versioning**: Optimistic concurrency via a `version` column. Updates require the expected version; stale versions are rejected with `ScheduleStaleVersionError`. Status changes (ACTIVE/PAUSED/DISABLED) also bump the version.

**Pause/resume/disable**: PAUSED schedules retain their `next_run_at` but the scheduler skips them. DISABLED schedules also stop generating occurrences. Neither status retroactively erases already-created occurrences.

## Component Details

### API Server (`apps/api/`)

- Express HTTP server with Zod input validation
- Creates tasks atomically with outbox events in a single PG transaction
- Supports one-time scheduled tasks via `scheduledFor` parameter on `POST /tasks`
- CRUD for recurring schedules: `POST/GET/PUT /schedules`, `PUT /schedules/:id/status`
- Runs the outbox publisher and scheduler service as background processes
- Health probes: `/ready` checks PG only (task submission requires only PG), `/live` always returns 200, `/health` checks both PG and Redis and reports outbox circuit breaker state
- `/metrics` returns JSON with task status counts, queue depth, and outbox stats
- Graceful shutdown: stops accepting connections, stops scheduler, drains in-flight requests, stops outbox publisher (waits for in-progress poll to complete), closes connections. Exit code 1 if shutdown times out with active requests.

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

- **State machine**: Defines valid task transitions (SCHEDULED, QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED) and enforces them at the repository layer. SCHEDULED tasks can only transition to QUEUED (when released by the scheduler) or CANCELLED.
- **Task repository**: PG-backed CRUD with `SELECT FOR UPDATE` + version check + claim token verification for all transitions; includes `reclaimStalledTask` with lease expiry check for crash recovery and `renewClaim` for lease extension by the active owner. All lease timestamps use PG `NOW()` as the single authoritative clock.
- **Schedule repository**: PG-backed CRUD for recurring schedules with optimistic concurrency (version column). Handles one-time scheduled task creation, batch release of due scheduled tasks, occurrence generation (atomic task + outbox + next_run_at advance), execution lease management for overlap prevention, and `FOR UPDATE SKIP LOCKED` multi-scheduler concurrency.
- **Scheduler service**: Polls PG on a configurable interval. Each poll cycle: (1) releases due one-time scheduled tasks in batch, (2) fetches due recurring schedules and processes each with a `FOR UPDATE SKIP LOCKED` row lock, applying the schedule's misfire policy to generate occurrences. Supports graceful shutdown by draining in-progress polls.
- **Cron utilities**: Wraps `cron-parser` for IANA-timezone-aware cron evaluation. `getNextOccurrence`, `getNextOccurrences`, and `getMissedOccurrences` (bounded) handle DST transitions.
- **Outbox publisher**: Polls PG for pending events (including `SCHEDULED_TASK_RELEASED` events), publishes to BullMQ with circuit breaker protection. Best-effort cancellation check (plain SELECT, not atomic with BullMQ publish — see Data Flow §2). Stops cleanly by draining in-progress polls.
- **Migrations**: Schema versioning with advisory lock for concurrent startup safety. Migrations 8–10 add SCHEDULED status, `scheduled_for`/`schedule_id` columns, the `recurring_schedules` table, and the occurrence uniqueness index.
- **Types**: Task, OutboxEvent, WorkerStatus, RecurringSchedule, ScheduleStatus, MisfirePolicy, OverlapPolicy interfaces

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

## Centralized Structured Logging (Pino / Alloy / Loki / Grafana)

### Observability Stack Overview

The engine has two complementary observability systems:

```
                         ┌──────────────────────────────────────────────┐
                         │           Distributed Tracing                │
  API ──┐                │  OpenTelemetry → OTel Collector → Jaeger     │
  Publisher ─┤           │  (causal journey of a single task)           │
  Worker ──┘             └──────────────────────────────────────────────┘

                         ┌──────────────────────────────────────────────┐
                         │           Centralized Logging                │
  API ──┐                │  Pino → stdout → Grafana Alloy → Loki       │
  Publisher ─┤ stdout    │  → Grafana (query/investigate)              │
  Worker ──┘             └──────────────────────────────────────────────┘
```

**Responsibilities:**

| System | Purpose | Source of truth? |
|--------|---------|-----------------|
| PostgreSQL | Durable business state (tasks, outbox) | **Yes** |
| Redis / BullMQ | Queue coordination | No |
| OpenTelemetry / Jaeger | Distributed trace of one task's lifecycle | No |
| Pino / Alloy / Loki / Grafana | Operational logs (debugging, investigation) | No |

**Critical invariant:** Logs are operational telemetry, not business state. Logging infrastructure failure (Alloy, Loki, Grafana) must never affect task correctness, transaction commits, retries, lease renewals, or graceful shutdown. The application writes structured JSON to stdout; Alloy reads container logs and forwards to Loki. Applications have no direct Loki connection.

### Shared Logger (`packages/shared/src/logger/`)

A centralized Pino-based logger providing:

- **Structured JSON output** with consistent common envelope: `time`, `level`, `service`, `environment`, `msg`.
- **Service identity**: `api`, `outbox-publisher`, `worker` — distinguishes runtime components.
- **Environment**: `development`, `test`, `production` — from `NODE_ENV`.
- **OpenTelemetry correlation**: Automatically enriches logs with `traceId` and `spanId` from the active OTel context. If no valid span exists, the log is emitted without trace fields — never fabricated or guessed.
- **Child loggers**: Bind context like `workerId` once; every subsequent log inherits it.
- **Centralized redaction**: Sensitive keys (`password`, `secret`, `token`, `authorization`, `apiKey`, `credential`, `connectionString`, `cookie`) are redacted at the Pino serializer level as defense in depth.
- **Safe error serialization**: Errors are serialized to `{name, message, stack?}` with message truncation, stack line limiting, and sensitive field scrubbing. Arbitrary objects are not blindly dumped.

### Log Schema

Every structured log includes:

```json
{
  "level": "info",
  "time": "2024-01-15T10:30:00.000Z",
  "service": "worker",
  "environment": "development",
  "msg": "Task durably completed",
  "taskId": "abc123",
  "workerId": "w-1234",
  "traceId": "0af7651916cd43dd8448eb211c80319c",
  "spanId": "b7ad6b7169203331"
}
```

Optional event-specific fields (present only where meaningful):

| Field | Meaning |
|-------|---------|
| `taskId` | Business task identity |
| `taskName` | Task name |
| `workerId` | Worker instance ID |
| `publisherId` | Outbox publisher instance ID |
| `eventId` | Outbox event ID |
| `attempt` | Execution/retry attempt number |
| `maxAttempts` | Maximum attempts allowed |
| `retryable` | Whether further retries are available |
| `reason` | Human-readable error/skip reason |
| `errorType` | Error class name |
| `durationMs` | Operation duration |
| `traceId` | OTel trace ID (auto-enriched, never fabricated) |
| `spanId` | OTel span ID (auto-enriched, never fabricated) |

### Correlation Semantics

- **`taskId`** = business task identity across its lifetime
- **`traceId`** = one distributed causal journey (OTel)
- **`spanId`** = one operation within that trace (OTel)
- **`attempt`** = execution/retry attempt number

These are independent concepts. `taskId` is not `traceId`. No fake trace/span correlation is generated.

### Log Levels

| Level | Usage |
|-------|-------|
| `debug` | Diagnostic detail (lease renewals, BullMQ job lifecycle events) |
| `info` | Meaningful operational events (task created, claimed, completed, startup) |
| `warn` | Unusual conditions (ownership lost, circuit breaker opened, lease renewal failed) |
| `error` | Failed operations (task execution failed, connection errors, unhandled errors) |

A failed attempt is `error` even if retry policy permits another attempt. Successful periodic lease renewals are `debug` to avoid noise.

### Instrumented Events

**API:**
- Task creation requested / created / rejected (validation) / failed
- Redis/PostgreSQL connection events
- Graceful shutdown lifecycle

**Outbox Publisher:**
- Event published / failed to publish / exhausted retries
- Circuit breaker state changes
- Cancelled task skip

**Worker:**
- Task received / claimed / execution started / durably completed
- Lease deferral (another valid lease exists)
- Ownership lost
- Execution attempt failed (with attempt/maxAttempts/retryable)
- Task failure persistence failed
- Lease renewal (debug level)
- BullMQ job lifecycle (stalled, failed)
- Graceful shutdown lifecycle

### Sensitive Data Policy

- Passwords, authorization headers, cookies, API keys, credentials, connection strings, and complete task payloads are never logged.
- Pino redaction is configured as defense in depth for known sensitive key paths.
- Error serialization scrubs sensitive patterns from arbitrary thrown objects.
- Redaction does not make arbitrary payload dumping acceptable — sensitive data is prevented from entering the pipeline, not just masked on exit.

### Audit Boundary

Operational logs may be incomplete and are not authoritative business records. If a future security/business requirement demands guaranteed audit history, that belongs in a durable transactional audit mechanism (PostgreSQL), not in Loki.

### Docker Compose Profiles

Logging infrastructure uses Docker Compose profiles to avoid forcing it on developers who only need core services:

| Profile | Services | Command |
|---------|----------|---------|
| (none) | postgres, redis, api, worker | `docker compose up` |
| `tracing` | + jaeger, otel-collector | `docker compose --profile tracing up` |
| `logging` | + loki, alloy, grafana | `docker compose --profile logging up` |
| `observability` | + all tracing + all logging | `docker compose --profile observability up` |

### Infrastructure Components

**Grafana Alloy** (`alloy-config.alloy`):
- Discovers engine containers via Docker socket
- Extracts `service` label from container name
- Parses JSON logs to extract `level` as a Loki label
- Forwards to Loki at `http://loki:3100`

**Grafana Loki** (`loki-config.yaml`):
- Single-node development configuration
- TSDB storage with filesystem backend
- Persistent volume for local evaluation
- Structured metadata enabled for queryable fields
- **Not production-ready** — no replication, no retention policy

**Grafana** (port 3001):
- Auto-provisioned Loki data source via `grafana/provisioning/datasources/`
- Anonymous admin access for local development

### Loki Labels and Cardinality

Low-cardinality labels only:

| Label | Values |
|-------|--------|
| `service` | `api`, `worker-1` (container name derived) |
| `environment` | `development`, `production` |
| `level` | `debug`, `info`, `warn`, `error` |

High-cardinality values (`taskId`, `traceId`, `spanId`, `workerId`, `attempt`) are structured log fields, queryable via LogQL JSON parsing — **never Loki labels**.

### Investigation Workflow

**Find logs for a specific task:**
```logql
{service=~"api|worker.*"} | json | taskId="abc123"
```

**Find error logs from the worker:**
```logql
{service=~"worker.*", level="error"} | json
```

**Correlate logs with traces:**
```logql
{service=~"api|worker.*"} | json | traceId="0af7651916cd43dd8448eb211c80319c"
```

Then open the `traceId` in Jaeger UI (`http://localhost:16686`) to see the distributed trace.

**Find all logs for a task across its lifecycle:**
```logql
{service=~"api|worker.*|outbox-publisher"} | json | taskId="abc123"
```

### Ports

| Service | Port | URL |
|---------|------|-----|
| API | 3000 | `http://localhost:3000` |
| Jaeger UI | 16686 | `http://localhost:16686` |
| OTel Collector gRPC | 4317 | — |
| OTel Collector HTTP | 4318 | — |
| Loki | 3100 | `http://localhost:3100` |
| Grafana | 3001 | `http://localhost:3001` |

### Failure Semantics

The logging pipeline is designed to fail open:

1. Application writes structured JSON to stdout — this never fails (even if the process's stdout is a broken pipe, the application's business logic is unaffected).
2. Alloy reads container logs — if Alloy crashes, logs continue to stdout; when Alloy recovers, it resumes from where it left off.
3. If Loki is unavailable, Alloy buffers and retries — no log data reaches the application.
4. If Grafana is unavailable, logs are still in Loki and can be queried later.

At no point does a failure in steps 2–4 affect API transactions, outbox publishing, worker processing, task state, retry decisions, lease renewal, or graceful shutdown.

### Development vs. Production

This local configuration is a development/evaluation setup. Production deployment would require:

- Loki cluster with replication and appropriate retention policies
- Alloy deployed as a DaemonSet/sidecar with resource limits
- Grafana with proper authentication and RBAC
- Network policies for log transport security
- Alerting rules (out of scope for this slice)
