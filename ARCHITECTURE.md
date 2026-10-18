# Architecture

Technical architecture of the distributed task execution engine.

## System Overview

```
 Operator browser                         Tenant client
       │ Basic auth                            │ Bearer te_… (tenant key)
┌──────▼──────────────┐  Bearer operator key  │
│ Dashboard (Next.js) │──────────────┐        │
│ /api/engine/* proxy │              │        │
└─────────────────────┘        ┌─────▼────────▼──────────────────────┐
                               │ API (Express)                       │
                               │  operator routes  /tasks /schedules │
                               │                   /tenants /metrics │
                               │  tenant routes    /tenant/*         │
                               │  background: outbox publisher,      │
                               │  scheduler, task reconciler         │
                               └──────┬───────────────────┬──────────┘
                 task + outbox in one │                   │ BullMQ.add(jobId = taskId)
                 transaction          ▼                   ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ PostgreSQL       │     │ Redis (BullMQ)  │
                        │ source of truth  │     │ delivery only   │
                        └────────▲─────────┘     └────────┬────────┘
                                 │ claim / renew /        │ consume
                                 │ complete (claim token) │
                        ┌────────┴────────────────────────▼────────┐
                        │ Workers: handler registry (echo, sleep,  │
                        │ flaky, http), leases, retries            │
                        └──────────────────────────────────────────┘
```

PostgreSQL decides what is true; Redis only moves work. Anything Redis loses,
the reconciler re-derives from PostgreSQL (§8).

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
      │── SELECT task status ──▶│                       │
      │   (check cancel)        │                       │
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
  │── run handler for ─────▶│  (lease renewed every │
  │   task.name             │   TTL/3 meanwhile)    │
  │                         │                       │
  │── transition to ───────▶│  PROCESSING → COMPLETED│
  │   COMPLETED              │  version + 1          │
```

### 4. Task Handlers, Failure and Retry

A task's `name` is its **type**. The worker looks it up in a handler registry
(`packages/shared/src/task-handlers.ts`) and runs the handler with
`{ taskId, name, payload, attempt, signal }`. Built-in types: `echo`, `sleep`,
`flaky` (fails N attempts on purpose) and `http` (allow-listed hosts only;
disabled unless `HTTP_TASK_ALLOWED_HOSTS` is set). Redirects are not followed:
a 3xx response (except 304 Not Modified) fails the task permanently with the
`Location` in the error, because the request the caller asked for never
reached its target and retrying would get the same redirect.
The handler's return value is stored as `result.output` (capped at 64 KB).

Errors fall into two classes:

- **Permanent** (`NonRetryableTaskError`: unknown task type, invalid payload,
  HTTP 4xx). The task goes PROCESSING → FAILED on this attempt, and the worker
  throws BullMQ's `UnrecoverableError` so BullMQ stops retrying too.
- **Transient** (timeout, HTTP 5xx/429, network, anything else). The task goes
  PROCESSING → QUEUED with `retries + 1` and BullMQ retries with exponential
  backoff. When the budget is spent it goes PROCESSING → FAILED.

**PostgreSQL owns the retry budget.** "Final attempt" is true if either
BullMQ's `attemptsMade` or PostgreSQL's `retries/max_retries` says so. BullMQ's
counter is per job, and a job re-published by the reconciler (§8) starts again
at zero, so it cannot be the only authority. `attempt` passed to handlers is
`retries + 1` from PostgreSQL for the same reason. On COMPLETED the `error`
column is cleared; earlier failures stay visible through `retries`.

**Cancellation of running work.** Every handler receives an `AbortSignal`. It
fires when lease renewal fails (another worker may own the task now), and when
`TASK_TIMEOUT_MS` (default 5 min) is exceeded. A handler that stops because the
lease was lost makes the worker return `SKIPPED/ownership_lost` without
writing. Any write would be rejected by the claim token anyway.

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
5. The lost run is charged as an attempt (`retries + 1`, with the reason in `error`). If that exceeds `maxRetries`, the task goes to FAILED instead (a workflow step cancels its downstream as for any failure) and is not run again
6. Otherwise a new `claim_token` and `claim_expires_at` are set, and the new worker's ID replaces `claimed_by`
7. Processing continues from the start

Charging the lost run bounds a task that kills its worker every time (a poison task): it fails after `maxRetries + 1` crashes. Before, reclaims were free and only the reconciler charged, after BullMQ's stall limit gave up, so a task with `maxRetries: 2` crashed workers 9 times (recorded). The cost: a task interrupted by a forced kill (OOM, SIGKILL during a rollout) also spends one retry, the same rule the reconciler already applied.

PROCESSING status alone is **not** sufficient evidence that a job stalled. A task in PROCESSING with a non-expired claim is actively being worked on by its owner. Reclaim is only permitted after the claim expires, which provides a bounded recovery window that prevents live-owner theft.

**Early BullMQ redelivery (lease deferral)**: BullMQ's `stalledInterval` (5s) is shorter than the PG execution lease (30s). When BullMQ redelivers a stalled job before the PG lease expires, the receiving worker's `reclaimStalledTask` call throws `ClaimNotExpiredError`. The worker handles this by calling `job.moveToDelayed(claimExpiresAt + margin, job.token)`, which moves the job back to BullMQ's delayed state until approximately when the PG lease expires. This is fundamentally different from an execution retry: `moveToDelayed` does **not** consume a BullMQ attempt, does not increment `attemptsMade`, and does not trigger exponential backoff. When the delayed time elapses, BullMQ re-activates the job and the worker can successfully reclaim the task. Lease deferral and execution retries are completely separate concepts — a `ClaimNotExpiredError` is not a task failure, and deferring the job preserves the full retry budget for actual execution failures.

**Execution ownership protocol**: Each PROCESSING transition atomically establishes a unique `claim_token` and a `claim_expires_at` (default 30s TTL, set via PG `NOW()` to avoid Node/PG clock skew). All transitions out of PROCESSING (`COMPLETED`, `FAILED`, `QUEUED`) must present the matching `claim_token` — a stale worker whose ownership was transferred cannot mutate the task. Cancellation (`cancelTask`) overrides ownership without requiring the token, since it is an external administrative action; it clears the token and bumps the version, so the stale worker's subsequent transition attempt fails with `StaleVersionError`.

**Lease renewal**: The worker renews the execution lease at ~TTL/3 intervals (default 10s) while actively processing. `renewClaim(taskId, expectedClaimToken)` atomically verifies the task is PROCESSING with the matching claim token and extends `claim_expires_at` using PG `NOW()`. If renewal fails (ownership transferred, task cancelled), the renewal timer is cleared. The claim-token-protected transition remains the final safety guard.

**Failure handler safety**: The worker's failure handler uses the stored `taskVersion` and `claimToken` from the initial claim — it does **not** re-read the latest task state. If ownership has transferred (version or claim token changed), the transition is rejected harmlessly. This prevents the scenario where a stale worker re-reads the latest version and mutates a task now owned by another execution.

BullMQ's `maxStalledCount` (2 in this worker) limits how many times a single job can be redelivered after stalling. After that, BullMQ moves the job to `failed` **without running the processor**, so the worker's failure handler never runs and the task is left in PROCESSING. The reconciler (§8) recovers that case. It finds the expired lease and the dead job, charges one retry, and requeues the task, or marks it FAILED if the budget is spent. That bounds poison tasks that crash every worker.

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
An occurrence counts as missed when it is more than `SCHEDULER_MISFIRE_THRESHOLD_MS` late (default 60 s). Polling alone makes every occurrence a few seconds late, so anything within the threshold simply runs, under every policy.

- `SKIP_MISSED`: Missed occurrences are dropped: nothing runs for them and `next_run_at` jumps to the next future time. If the most recent occurrence is within the threshold, it runs.
- `RUN_ONCE`: Run one catch-up occurrence for the first missed time, then jump to the next future time.
- `CATCH_UP_ALL`: Generate all missed occurrences in bounded batches (configurable `catchUpBatchSize`). Remaining missed occurrences are processed on subsequent poll cycles. This provides backpressure without silently discarding missed work.

**Overlap policies**:
- `ALLOW_OVERLAP`: New occurrences generate regardless of in-flight ones. No schedule-level execution lease is used.
- `FORBID_OVERLAP`: The scheduler generates all occurrences regardless of overlap policy (including CATCH_UP_ALL missed occurrences). Overlap is enforced at the **worker execution boundary** via a schedule-level execution lease:
  1. Before claiming a task (QUEUED→PROCESSING), the worker checks if the task belongs to a FORBID_OVERLAP schedule.
  2. If so, the worker attempts to acquire the schedule's execution lease (`acquireExecutionLease`). The lease is a PostgreSQL-backed token with an expiration time, consistent with the existing task claim/token model.
  3. If the lease is held by another occurrence, the worker defers the job using `moveToDelayed(expiresAt + margin)` + `DelayedError`. This does **not** consume a BullMQ retry attempt — the job re-activates after the lease holder finishes.
  4. If the lease is acquired, the worker proceeds with the QUEUED→PROCESSING transition. The lease is renewed alongside the task claim in the same renewal timer.
  5. On completion or failure, the worker releases the schedule execution lease. On crash, the lease expires and another occurrence can acquire it.
  6. If lease renewal fails (token mismatch from crash recovery), the worker sets `ownershipLost` and stops treating itself as the valid execution owner.

  This design ensures CATCH_UP_ALL + FORBID_OVERLAP generates all missed occurrences durably while executing them serially. No occurrences are silently discarded by the scheduler.

**Multi-scheduler concurrency**: `fetchDueSchedules` selects due schedules, and `processScheduleWithLock` acquires a `FOR UPDATE SKIP LOCKED` row lock per schedule. Multiple scheduler instances can run concurrently — each processes different schedules without contention.

**Schedule versioning**: Optimistic concurrency via a `version` column. Updates require the expected version; stale versions are rejected with `ScheduleStaleVersionError`. Status changes (ACTIVE/PAUSED/DISABLED) also bump the version.

**Pause/resume/disable**: PAUSED schedules retain their `next_run_at` but the scheduler skips them. DISABLED schedules also stop generating occurrences. Neither status retroactively erases already-created occurrences.

### 8. Redis Loss and Orphan Reconciliation

The outbox guarantees a task reaches Redis at least once. It cannot guarantee
the job stays there: Redis restarts without persistence, FLUSHALL, eviction,
or failover to a replica that missed the write all drop jobs silently. Before
the reconciler existed, a measured run lost 24 of 30 in-flight tasks for good
when the queue was wiped.

`TaskReconciler` (`packages/shared/src/db/task-reconciler.ts`) runs in the API
every `RECONCILER_INTERVAL_MS` (30s):

```
Reconciler            PostgreSQL                 BullMQ/Redis
    │── pg_try_advisory_lock ─▶│  one API replica only
    │── candidates ───────────▶│  QUEUED idle > grace (60s), or
    │                          │  PROCESSING with an expired lease,
    │                          │  and no PENDING outbox event
    │── getJob(taskId) ──────────────────────────────────▶│
    │   waiting/delayed/active → leave it (BullMQ owns it)
    │   missing/completed/failed → orphan (finished job removed,
    │                                because add() with that jobId would no-op)
    │── per orphan, own txn: lock row, version unchanged? ─▶│
    │   QUEUED     → TASK_REQUEUED outbox event
    │   PROCESSING → retries+1, claim cleared, QUEUED + event
    │                (or FAILED when the budget is spent)
```

Delivery still goes through the normal outbox publisher (circuit breaker,
fairness, tracing). It cannot double-queue: live jobs are never touched, the
version check skips anything that moved, `NOT EXISTS` stops two cycles stacking
events, and `jobId = taskId` makes a redundant publish a no-op. If Redis errors
during a cycle, the cycle is abandoned, because "missing" cannot be told apart
from "unreachable". CI proves it with a chaos run: a worker SIGKILLed and Redis
FLUSHALLed mid-run, and all 60 tasks still complete.

### 9. Workflows (DAG)

A workflow is a set of tasks plus dependency edges. `POST /workflows` (or
`/tenant/workflows`) takes `{ name, steps: [{ key, name, payload, priority,
maxRetries, dependsOn }] }`.

**Validation, before anything is written.**
`validateWorkflowDefinition` runs Kahn's algorithm and returns the steps in
topological order. It rejects:
- duplicate or malformed keys;
- unknown, repeated or self dependencies;
- more than 200 steps;
- cycles. A DFS reports the actual cycle (`a -> b -> c -> a`), not just
  "invalid".

**Creation, one transaction.** Written together:
- the `workflows` row;
- one `tasks` row per step (`workflow_id`, `step_key`);
- the `task_dependencies` edges;
- outbox events for the root steps only.

Steps with parents are created **BLOCKED**, with `pending_dependencies` set to
their parent count. A BLOCKED step has no outbox event and no queue job, so no
worker can run it early. Tenants are charged one job per step against their
quota under the same row lock single tasks use.

**Progress, in the parent's transaction.** Every terminal transition of a
workflow task (`transitionStatus`, `cancelTask`, the reconciler's FAILED path)
calls `onTaskTerminal` inside the transaction that records it:

```
parent COMPLETED   UPDATE children SET pending_dependencies = pending_dependencies - 1
                   child reaching 0 → QUEUED + WORKFLOW_STEP_READY outbox event
parent FAILED or   recursive CTE over the edges → every BLOCKED descendant
CANCELLED          becomes CANCELLED ("Upstream step "x" failed")
then               no unfinished steps left → workflow COMPLETED / FAILED / CANCELLED
```

Why it is safe:

- **No lost release.** "Parent done" and "child released" commit together.
  The release is an outbox row, so a crash right after commit loses nothing.
  The publisher delivers it, and the reconciler covers Redis losing it.
- **Fan-in fires once.** The decrement is a single `UPDATE … RETURNING`.
  Concurrent parents serialise on the workflow row lock, so exactly one of
  them sees the counter reach 0 and writes the one outbox event. Tested with
  parallel completions.
- **No deadlock.** Every terminal path locks the **workflow row first**, then
  task rows (`lockWorkflowForTask`). A parent completing (task → children)
  and a child being cancelled (child → workflow) take locks in the same
  order. The same lock serialises the "is everything finished?" check.
- **Partial failure is contained.** Only descendants of a failed step are
  cancelled; independent branches run to completion. The workflow ends FAILED
  once nothing can still change.
- **Data along edges.** The worker passes each step `upstream`: its direct
  parents' outputs keyed by step key (`getUpstreamOutputs`). `echo` returns it,
  which makes the flow visible.
- **Belt and braces.** The worker refuses any job whose task is not QUEUED or
  PROCESSING. A stray job for a BLOCKED step is skipped (`not_runnable`) rather
  than executed without a claim.

`PUT /workflows/:id/cancel` locks the workflow and marks it CANCELLED. In the
same transaction it cancels every unfinished step, including one a worker
holds; that worker loses on its next write through the claim token. It then
drops queued jobs best-effort.

Not included: workflows started by cron schedules, per-step timeouts other
than `TASK_TIMEOUT_MS`, and conditional edges.

## Component Details

### API Server (`apps/api/`)

- Express HTTP server with Zod input validation
- **Operator authentication**: `/tasks`, `/schedules`, `/workers`, `/metrics`, `/tenants`, `/plans` require `Authorization: Bearer $OPERATOR_API_KEY` (SHA-256 + `timingSafeEqual`). With no key configured these routes answer 503 (fail closed). A key under 32 characters stops startup. Health probes are open.
- **Tenant provisioning** (operator): create tenants with a generated `te_` key (shown once, only the hash stored), list, inspect limits/usage, suspend/activate, rotate keys (old key revoked immediately)
- Creates tasks atomically with outbox events in a single PG transaction
- Supports one-time scheduled tasks via `scheduledFor` parameter on `POST /tasks`
- CRUD for recurring schedules: `POST/GET/PUT /schedules`, `PUT /schedules/:id/status`
- **Tenant-scoped endpoints** (`/tenant/*`): 7 routes behind Bearer token authentication middleware. Tenant identity is derived from the API key hash — never from a client-supplied tenant_id in the request body. Includes task submission with atomic quota + idempotency enforcement, task listing/retrieval/cancellation, schedule creation/listing/retrieval, and usage reporting.
- **Rate limiting middleware**: Per-tenant sliding-window rate limiting with `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` response headers. Fail-open on Redis errors.
- Runs the outbox publisher, scheduler service and task reconciler as background processes
- Startup waits for PostgreSQL with capped backoff (no crash-loop when the database starts later). Redis is not awaited: the client reconnects forever, and the BullMQ queue owns its own connection, so publishing resumes by itself when Redis returns
- Health probes: `/ready` checks PG only (task submission requires only PG), `/live` always returns 200, `/health` checks both PG and Redis and reports outbox circuit breaker state. Every dependency call made for a probe or for `/metrics` is bounded by `HEALTH_DEPENDENCY_TIMEOUT_MS`: the Redis client waits rather than fails while Redis is down (`maxRetriesPerRequest: null`), so an unbounded probe would hang exactly when it is needed. `/metrics` then reports `queue.available: false` with null counts rather than zeros.
- `/metrics` returns JSON with task status counts, queue depth, and outbox stats (the dashboard reads it)
- `/metrics/prometheus` (same operator key) exposes the same numbers for Prometheus: `engine_tasks{status}`, `engine_queue_jobs{state}`, `engine_outbox_events{status}`, `engine_outbox_oldest_event_age_seconds{status="pending"}` (publishing lag), `engine_outbox_circuit_state{state}`, HTTP request/error counters and Node process stats. When Redis or the outbox cannot be read, `engine_queue_up` / `engine_outbox_up` go to 0 and those series are dropped, so a graph shows a gap instead of a false zero; PostgreSQL failing fails the scrape. The gauges are engine-wide (read from PostgreSQL/Redis), so with several API replicas aggregate them with `max()`.
- Each worker serves `/metrics` on its health port: `engine_worker_jobs_total{outcome}` and process stats (event-loop lag shows a worker that is alive but blocked), labelled `worker_id`.
- `docker compose --profile metrics` runs Prometheus (port 9090) scraping both; `--profile observability` adds Grafana (port 3002) with the provisioned "Engine overview" dashboard. CI (`compose-metrics`) checks the scrape, the values after real work, the Redis-down gap and that every metric the dashboard queries exists.
- Graceful shutdown: stops accepting connections, stops scheduler, drains in-flight requests, stops outbox publisher (waits for in-progress poll to complete), closes connections. Exit code 1 if shutdown times out with active requests.

### Worker (`apps/worker/`)

- BullMQ Worker consuming from the "tasks" queue
- Runs the handler registered for the task's type with a per-task timeout (`TASK_TIMEOUT_MS`) and an abort signal tied to lease ownership; see Data Flow §4
- Publishes heartbeat to Redis every 10 seconds (`worker:{id}` key with 30s TTL)
- Validates task state in PG before processing (skips cancelled/completed/failed/missing tasks)
- Reclaims stalled PROCESSING tasks when BullMQ redelivers them after a worker crash, but only after the previous execution claim has expired (`claim_expires_at < NOW()`). Reclaim generates a new `claim_token` and sets a new expiry.
- When BullMQ redelivers a stalled job before the PG lease expires, the worker defers the job via `job.moveToDelayed()` until approximately the PG claim expiry. This does not consume an execution retry attempt. When the deferred time elapses, BullMQ re-activates the job and the worker can reclaim the task.
- Renews execution lease at ~TTL/3 intervals while actively processing. Renewal requires the matching `claim_token`. If renewal fails (ownership lost, task cancelled), the timer is cleared and an execution-local `ownershipLost` flag is set. The worker checks this flag before attempting to complete or transition the task and cooperatively aborts if ownership was lost. The claim-token-protected transition remains the final guard against stale mutations — even if the flag check is bypassed, the DB rejects the stale token.
- **Tenant concurrency enforcement**: Before claiming a task, the worker checks the task's tenant status and acquires a per-tenant concurrency lease. The lease TTL is `min(taskClaimTtl, scheduleLeaseDuration)` so the unified renewal interval (min/3) covers all three lease types (task claim + schedule execution + tenant concurrency). If the concurrency limit is reached, the worker defers the job via `moveToDelayed` + `DelayedError` — this does not consume a retry attempt. A task of a SUSPENDED tenant is deferred the same way, by `TENANT_SUSPENDED_RECHECK_MS` (default 60 s): the job stays live (delayed) in BullMQ, so the reconciler leaves it alone, and the task runs after the tenant is reactivated. (Completing the job as "skipped" instead left a QUEUED row with a dead job, which the reconciler requeued every cycle.) The concurrency lease is renewed in the same renewal timer alongside other leases, and released in a `finally` block on completion, failure, or any error path.
- Uses optimistic concurrency (version check) and execution ownership (claim token verification) on all state transitions out of PROCESSING
- Cancellation of a running task: every `CANCEL_CHECK_INTERVAL_MS` (default 2 s, 0 disables) the worker reads the task's status; on CANCELLED it fires the handler's AbortSignal, and the job ends as skipped: no FAILED write and no BullMQ retry. A handler that ignores the signal still finishes, and its result is discarded.
- Graceful shutdown: stops consuming new jobs, waits for in-flight jobs to complete (30s timeout), removes heartbeat key, closes connections

### Shared Package (`packages/shared/`)

- **State machine**: Defines valid task transitions (SCHEDULED, QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED) and enforces them at the repository layer. SCHEDULED tasks can only transition to QUEUED (when released by the scheduler) or CANCELLED. FAILED → QUEUED has exactly one caller: the operator redrive `POST /tasks/:id/retry`, which resets the retry budget and writes a `TASK_REQUEUED` event in one transaction (workflow steps are refused: their descendants were already cancelled). The publisher removes a finished BullMQ job with the same id before re-adding, since `jobId = taskId` would otherwise make the add a silent no-op.
- **Task repository**: PG-backed CRUD with `SELECT FOR UPDATE` + version check + claim token verification for all transitions; includes `reclaimStalledTask` with lease expiry check for crash recovery and `renewClaim` for lease extension by the active owner. All lease timestamps use PG `NOW()` as the single authoritative clock.
- **Schedule repository**: PG-backed CRUD for recurring schedules with optimistic concurrency (version column). Handles one-time scheduled task creation, batch release of due scheduled tasks, occurrence generation (atomic task + outbox + next_run_at advance), execution lease management for overlap prevention, and `FOR UPDATE SKIP LOCKED` multi-scheduler concurrency.
- **Scheduler service**: Polls PG on a configurable interval. Each poll cycle: (1) releases due one-time scheduled tasks in batch, (2) fetches due recurring schedules and processes each with a `FOR UPDATE SKIP LOCKED` row lock, applying the schedule's misfire policy to generate occurrences. Supports graceful shutdown by draining in-progress polls. Between polls it sleeps until the earliest known due time (one query on the database clock, using the partial indexes), never longer than `SCHEDULER_POLL_INTERVAL_MS` and never in a tight loop: if work is still overdue right after a poll it waits 1 s. So a task scheduled ahead of time is released within milliseconds of its time; only work created during a sleep for a time inside that sleep can wait up to the interval.
- **Cron utilities**: Wraps `cron-parser` for IANA-timezone-aware cron evaluation. `getNextOccurrence`, `getNextOccurrences`, and `getMissedOccurrences` (bounded) handle DST transitions.
- **Task handlers**: registry, built-in types, permanent vs transient error contract, result bounding (see Data Flow §4).
- **Task reconciler**: finds tasks Redis lost and requeues them through the outbox (see Data Flow §8). Under the same advisory lock it prunes `DELIVERED` outbox events older than `OUTBOX_DELIVERED_RETENTION_HOURS` (default 168; 0 keeps them forever), in bounded batches. `PENDING` and `FAILED` events are never pruned; the task row is the record once an event is delivered.
- **Workflow engine / repository**: DAG validation, atomic creation, in-transaction release and failure propagation, cancellation (see Data Flow §9).
- **Outbox publisher**: Polls PG for pending events (including `SCHEDULED_TASK_RELEASED` events), publishes to BullMQ with circuit breaker protection. It claims nothing while the queue's Redis connection is not ready, and bounds each `queue.add` with `OUTBOX_PUBLISH_TIMEOUT_MS` (the connection uses `maxRetriesPerRequest: null`, so an unbounded add would wait for Redis forever instead of failing). Best-effort cancellation check (plain SELECT, not atomic with BullMQ publish — see Data Flow §2). Stops cleanly by draining in-progress polls.
- **Tenant repository**: PG-backed tenant CRUD, plan management, effective limits (plan defaults + tenant overrides via `COALESCE`), concurrency lease management (acquire/renew/release with expiry), billing period computation, and idempotency hash generation.
- **Rate limiter**: Redis sliding-window sorted set rate limiter with fail-open semantics. On any Redis error, requests are allowed through with full remaining count.
- **Fair scheduler**: Deficit-based weighted round-robin scheduler. Each tenant accumulates a deficit proportional to the inverse of its weight; the tenant with the lowest deficit is selected next. Guarantees bounded gap between selections for any eligible tenant, preventing starvation.
- **Migrations**: Schema versioning with advisory lock for concurrent startup safety. Migrations 8–10 add SCHEDULED status, `scheduled_for`/`schedule_id` columns, the `recurring_schedules` table, and the occurrence uniqueness index. Migrations 11–16 add multi-tenancy: plans (with seed data), tenants, tenant_overrides, tenant_usage, tenant_concurrency_leases, and tenant_id columns on tasks/schedules. Migration 17 adds partial indexes for the reconciler's candidate query. Migration 18 adds `workflows`, `task_dependencies`, the `workflow_id`/`step_key`/`pending_dependencies` task columns and the BLOCKED status.
- **Types**: Task, OutboxEvent, WorkerStatus, RecurringSchedule, ScheduleStatus, MisfirePolicy, OverlapPolicy, Plan, Tenant, TenantOverride, TenantUsage, TenantConcurrencyLease, EffectiveLimits interfaces

### Dashboard (`apps/web/`)

- Next.js 16 / React 19 operator dashboard: overview (task counts, queue, outbox), tasks (submit by type with payload, page, cancel, see retries and last error), workers (heartbeats), tenants (create, suspend, rotate keys)
- **The browser never talks to the engine API.** Pages call `/api/engine/*` on the dashboard's own origin. That route handler re-checks Basic auth, allows only a fixed list of method+path pairs, refuses cross-site writes (Fetch Metadata / Origin check), and forwards with `OPERATOR_API_KEY` from server-side env
- Every page and route requires HTTP Basic auth (`DASHBOARD_USER` / `DASHBOARD_PASSWORD`) through `proxy.ts`; with no credentials configured it answers 503
- Strict CSP (`connect-src 'self'`), `frame-ancestors 'none'`, no-referrer; mobile-first layout
- Shipped as a standalone Next build (`Dockerfile.web`), non-root

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

Task handlers should be idempotent to handle these cases safely. What *is* exactly-once is the recorded outcome: COMPLETED is terminal in the state machine, and every transition out of PROCESSING needs the current claim token. A duplicate execution can never record a second result.

## Multi-Tenancy

### Tenant Model

```
┌──────────┐     ┌──────────────┐     ┌──────────────────┐
│  plans   │◀────│   tenants    │────▶│ tenant_overrides  │
│          │     │              │     │ (per-tenant limit │
│ starter  │     │ api_key_hash │     │  overrides)       │
│ profess. │     │ status       │     └──────────────────┘
│ enterpr. │     │ plan_id FK   │
└──────────┘     └──────┬───────┘
                        │
           ┌────────────┼────────────┐
           ▼            ▼            ▼
    ┌────────────┐ ┌──────────┐ ┌───────────────────────┐
    │tenant_usage│ │  tasks   │ │tenant_concurrency_    │
    │(per-period │ │(tenant_id│ │leases (PG-backed      │
    │ quotas)    │ │ FK)      │ │ with expiry)          │
    └────────────┘ └──────────┘ └───────────────────────┘
```

Plans define default limits (rate_limit, max_concurrent_executions, max_jobs_per_period, weight, billing_period_days). Tenant overrides layer on top via `COALESCE` — only overridden fields replace plan defaults. Three seed plans are created by migration 11: starter, professional, enterprise.

### Authentication

```
Client                    API                          PostgreSQL
  │                        │                               │
  │── Authorization: ─────▶│                               │
  │   Bearer <api-key>     │── SHA-256(api-key) ──────────▶│
  │                        │   SELECT * FROM tenants       │
  │                        │   WHERE api_key_hash = $1     │
  │                        │◀── tenant row ───────────────│
  │                        │                               │
  │                        │── check status ──────────────▶│
  │                        │   (reject SUSPENDED w/ 403)   │
```

Tenant identity is **never** supplied in the request body. The API key header is the sole source of tenant identity, preventing cross-tenant impersonation.

**Provisioning.** Operators create tenants with `POST /tenants { name, planId }`
(operator key). The API generates the key (`te_` + 32 random bytes, base64url),
stores only its SHA-256, and returns the plaintext once. `POST
/tenants/:id/api-key` rotates it, and the old key stops working in the same
UPDATE. `PUT /tenants/:id/status` suspends or reactivates. The operator key is
not a tenant key: `/tenant/*` rejects it.

### Task Acceptance (Atomic Transaction)

```
BEGIN
  ├── Check idempotency_key exists for this tenant
  │     ├── EXISTS + same material hash → return existing task (dedup)
  │     └── EXISTS + different material hash → IdempotencyConflictError
  │
  ├── UPSERT tenant_usage for current billing period
  │     └── Check accepted_jobs < max_jobs_per_period → QuotaExceededError
  │
  ├── INCREMENT accepted_jobs
  ├── INSERT task (with tenant_id, idempotency_key, idempotency_hash)
  │     status SCHEDULED if scheduledFor is in the future (database clock), else QUEUED
  ├── INSERT outbox_event   (QUEUED only; a SCHEDULED task gets its event when the scheduler releases it)
COMMIT
```

The entire acceptance is a single PG transaction, making it race-safe under concurrent API servers. The idempotency hash covers `(name, priority, maxRetries, scheduledFor, scheduleId)` — material request properties, not payload content.

### Billing Period

Billing periods are computed relative to each tenant's `created_at` date, not calendar months. `computeBillingPeriodStart(tenantCreatedAt, billingPeriodDays, now)` determines the start of the current period by advancing from the tenant's creation date in `billingPeriodDays` increments.

### Concurrency Leases

Per-tenant concurrency limits are enforced at the worker via PG-backed leases:

```
Worker                     PostgreSQL
  │                            │
  │── acquireConcurrencyLease ▶│  COUNT active leases for tenant
  │   (tenantId, taskId,       │  IF count < maxConcurrent:
  │    workerId, maxConc, ttl) │    INSERT lease → acquired=true
  │                            │  ELSE:
  │◀── {acquired, lease} ─────│    → acquired=false
  │                            │
  │── renewConcurrencyLease ──▶│  UPDATE expires_at WHERE token=$1
  │── releaseConcurrencyLease ▶│  DELETE WHERE lease_token=$1
```

Concurrency leases compose with existing task claim leases and schedule execution leases. All three are renewed in the same timer. The concurrency lease TTL is `min(taskClaimTtl, scheduleLeaseDuration)` so the unified renewal interval (computed as min/3) covers all lease types. Crashed workers' leases expire and are automatically reclaimed.

When the concurrency limit is reached, the worker defers the job via `moveToDelayed` + `DelayedError`. This does **not** consume a BullMQ retry attempt — the same mechanism used for schedule lease deferral.

**Admission at publish time (FIFO by priority under the limit).** The outbox publisher only publishes a tenant's task into a free concurrency slot. Slots = the tenant's `max_concurrent_executions` minus its in-flight tasks, where in flight means QUEUED or PROCESSING with no PENDING outbox event: waiting in BullMQ, running, or waiting out a retry backoff. A SUSPENDED tenant has no slots, so its new work stays in the outbox until it is reactivated. The outbox is the tenant's waiting room, ordered by priority (CRITICAL, HIGH, NORMAL, LOW) and then age, so tasks start in that order. Tasks without a tenant are never held back, and weighted fair publishing between tenants is unchanged; admission only caps how much of a tenant's share it can use right now. `OUTBOX_TENANT_ADMISSION=false` turns it off.

Why at publish time and not in the worker. Before, every task was published at once and the worker deferred the ones over the limit by 2 s, again and again. Whichever deferred job retried right after a slot freed took it, so the start order was random (recorded: limit 1, six tasks, the third submitted ran last) and priority was lost at the first deferral. It also cost work proportional to the backlog: N waiting tasks meant about N/2 deferral round trips per second, each with PostgreSQL queries, for tasks that could not run.

What it costs, stated plainly:
- A task in a retry backoff holds its slot until it runs again. With a limit of 1, the tenant's next task waits for the retry. That is the meaning of a concurrency limit protecting a tenant's downstream (a retry against a struggling upstream should not let more work pile in), but it is stricter than before, when other tasks ran during the backoff.
- A task whose BullMQ job was lost (Redis flush) holds its slot until the reconciler requeues it (`RECONCILER_QUEUED_GRACE_MS`, default 60 s). Bounded, and the reconciler then puts it back in the waiting room.
- It is a soft gate. Two API replicas can see the same free slot and both publish. The worker's concurrency lease (above) stays the hard limit and defers the extra job, so the limit is never exceeded; ordering is exact with one replica and close with several.

### Rate Limiting

Redis sliding-window sorted set rate limiter per tenant. Each request adds a timestamped entry; entries outside the window are pruned; the count of remaining entries determines whether the request is allowed.

**Fail-open**: Any Redis error (connection failure, timeout, pipeline error) returns `{allowed: true, remaining: limit}`. Rate limiting is a best-effort throttle, not a security boundary.

Response headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.

### Weighted Fair Scheduling

Deficit-based round-robin ensures proportional CPU time across tenants with different plan weights:

1. Each tenant starts with deficit = 0
2. On each selection, pick the tenant with the lowest deficit
3. Increment the selected tenant's deficit by `1 / weight`

Higher-weight tenants accumulate deficit more slowly, so they are selected more frequently. The deficit model guarantees a bounded maximum gap between consecutive selections for any tenant — a continuously backlogged high-weight tenant cannot permanently starve a lower-weight tenant.

### Tenant Suspension

- **API**: SUSPENDED tenants receive HTTP 403 on all tenant-scoped endpoints
- **Publisher**: new work of a SUSPENDED tenant is not published (no admission slots); it waits in the outbox and runs after reactivation
- **Worker**: a task of a SUSPENDED tenant that was already published is deferred by `TENANT_SUSPENDED_RECHECK_MS` (default 60 s) without consuming retries or leases, and runs after reactivation
- **In-flight**: Tasks already in PROCESSING continue to completion — suspension does not kill active work

## Service Mesh (Istio Ambient Mode)

### Overview

The engine optionally runs under Istio Ambient mode, which provides transport security, workload identity, and L4 authorization without sidecars. Ambient mode uses ztunnel — a per-node DaemonSet that transparently intercepts pod-to-pod traffic.

```
                              ┌──────────────────────────┐
                              │ Kubernetes Gateway API   │
 External ──── HTTP :80 ────▶ │ engine-gateway           │
 Client                       │ (gatewayClassName: istio)│
                              │ SA: engine-gateway-istio │
                              └────────┬─────────────────┘
                                       │ HTTPRoute
                                       ▼
                              ┌──────────────────────────┐
                              │ API (api-sa)             │
                              │ AuthzPolicy: gateway,    │
                              │ web-sa on :3000          │
                              └────┬──────────┬──────────┘
                                   │          │
                    ┌──────────────▼──┐  ┌────▼──────────────┐
                    │ PostgreSQL      │  │ Redis             │
                    │ (postgres-sa)   │  │ (redis-sa)        │
                    │ AuthzPolicy:    │  │ AuthzPolicy:      │
                    │ api-sa,worker-sa│  │ api-sa,worker-sa  │
                    │ on :5432       │  │ on :6379          │
                    └─────────────────┘  └───────────────────┘
                              ▲                    ▲
                              │                    │
                    ┌─────────┴────────────────────┴──┐
                    │ Worker (worker-sa)               │
                    │ (no inbound AuthzPolicy)         │
                    └─────────────────────────────────┘
```

All connections between workloads are mTLS-encrypted by ztunnel. Authorization is L4 only (identity + port), enforced by ztunnel without waypoint proxies.

### Data Plane

ztunnel runs as a DaemonSet managed by Istio. When the namespace carries `istio.io/dataplane-mode: ambient`, ztunnel intercepts all pod traffic on that node and establishes mTLS tunnels (HBONE) between pods. No application changes are needed — ztunnel operates at the network layer.

### Workload Identities

Each workload's SPIFFE identity is derived from its Kubernetes ServiceAccount:

| Workload   | ServiceAccount         | SPIFFE ID |
|------------|------------------------|-----------|
| API        | api-sa                 | `cluster.local/ns/distributed-engine/sa/api-sa` |
| Worker     | worker-sa              | `cluster.local/ns/distributed-engine/sa/worker-sa` |
| PostgreSQL | postgres-sa            | `cluster.local/ns/distributed-engine/sa/postgres-sa` |
| Redis      | redis-sa               | `cluster.local/ns/distributed-engine/sa/redis-sa` |
| Gateway    | engine-gateway-istio   | `cluster.local/ns/distributed-engine/sa/engine-gateway-istio` |
| Dashboard  | web-sa                 | `cluster.local/ns/distributed-engine/sa/web-sa` |

Istio CA issues X.509 SVIDs to ztunnel for each identity. The certificates are never exposed to the application containers.

### L4 Authorization

AuthorizationPolicies use `selector.matchLabels` to target pods (not `targetRef`, which requires waypoint proxies). ALLOW policy semantics: once an ALLOW policy applies to a workload, requests that don't match any ALLOW rule are implicitly denied. No separate default-deny is needed.

| Policy | Target | Allowed Sources | Port |
|--------|--------|-----------------|------|
| authz-postgres | `app: postgres` | api-sa, worker-sa | 5432 |
| authz-redis | `app: redis` | api-sa, worker-sa | 6379 |
| authz-api | `app: api` | engine-gateway-istio, web-sa | 3000 |

Workers have no inbound AuthorizationPolicy — they consume from Redis (outbound), not receive inbound connections.

### Gateway Ingress

The Kubernetes Gateway API (`gatewayClassName: istio`) creates a mesh-aware ingress point. Istio's controller generates a Deployment, Service, and ServiceAccount for the Gateway, named `<gateway-name>-<gateway-class-name>` (i.e., `engine-gateway-istio`). An HTTPRoute forwards HTTP traffic from the Gateway to the `api` Service on port 80 (a Gateway API backendRef names the Service port; the Service maps 80 → container port 3000).

The `engine-gateway-istio` ServiceAccount is pre-created in `service-accounts.yaml`. Whether Istio adopts a pre-existing SA or always creates its own is not explicitly guaranteed by Istio's documentation — the supported mechanism for controlling Gateway infrastructure is `spec.infrastructure.parametersRef`. The runtime verification script (`verify-runtime.sh gateway-identity`) confirms at runtime that the actual Gateway pod uses the expected ServiceAccount before any authorization policy references it. Static YAML alone does not guarantee the runtime identity.

During initial rollout, the existing API `type: LoadBalancer` service is preserved to avoid disrupting external access. The transition to Gateway-only ingress is a controlled step after verification.

### Headless Services

PostgreSQL and Redis use headless services (`clusterIP: None`), where DNS resolves directly to pod IPs. AuthorizationPolicies target pods by label (`selector.matchLabels`), which works identically regardless of DNS resolution path. ztunnel intercepts traffic to the pod IP and enforces the policy.

### Kubelet Probe Exemption

ztunnel rewrites kubelet probe packets to use source address 169.254.7.127, which is exempted from AuthorizationPolicy evaluation. Liveness and readiness probes continue to work on API pods even with an ALLOW policy restricting port 3000.

### Verified in CI

`.github/workflows/full-stack.yml` (kind-istio job) deploys these manifests to
a kind cluster with Istio 1.30 in ambient mode and runs
`k8s/istio/verify-runtime.sh`. It checks enrollment, ztunnel mTLS
certificates, the Gateway identity against the policy principal, allowed paths,
denied paths (an unauthorized identity cannot reach PostgreSQL, Redis or the
API), and a task completed through the Gateway. kind has no cloud load
balancer, so CI reaches the Gateway Service through `kubectl port-forward`
(overlay `ci/kind`).

### Delivery Semantics

Istio Ambient mode does not change the engine's delivery semantics. The system provides at-least-once delivery (transactional outbox with BullMQ jobId deduplication), not exactly-once. Ambient mode has no waypoint proxies and therefore no mesh-level retries — application-level retry logic (BullMQ attempts, exponential backoff) remains the sole retry mechanism.

### What Is NOT Included

- **Waypoint proxies**: No L7 policy enforcement, no mesh-level retries, no fault injection.
- **Mesh retries or circuit breaking**: Application-level BullMQ retries remain authoritative.
- **HTTP routing between API and Worker**: Workers consume from Redis queues, not HTTP calls from the API.
- **Production TLS**: The Gateway listens on HTTP :80. TLS termination at the edge requires a certificate and Gateway listener configuration, which is environment-specific.
- **Full observability migration**: The existing Pino/Alloy/Loki logging and OTel tracing stacks are independent of the mesh.

## Scaling Considerations

- **API servers**: Stateless, horizontally scalable. PG pool size is the main constraint.
- **Workers**: Stateless, horizontally scalable. Each worker processes `QUEUE_CONCURRENCY` jobs concurrently.
- **PostgreSQL**: Single instance (StatefulSet). For higher throughput, consider read replicas or connection pooling (PgBouncer).
- **Redis**: Single instance (StatefulSet) with AOF persistence. BullMQ supports Redis Cluster for higher throughput, but this is not configured.

HPA in the K8s manifests scales on CPU and memory. Queue-depth-based worker scaling needs a metrics adapter (KEDA or similar) reading `engine_queue_jobs` from Prometheus; the metrics exist (`/metrics/prometheus`) but the adapter and an in-cluster Prometheus are not included.

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
| `metrics` | + prometheus | `docker compose --profile metrics up` |
| `observability` | + all tracing, logging and metrics | `docker compose --profile observability up` |

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

**Grafana** (port 3002):
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
| Grafana | 3002 | `http://localhost:3002` |

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
