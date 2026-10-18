# Distributed Task Execution Engine

A multi-tenant task execution engine in TypeScript: Node.js, PostgreSQL,
Redis/BullMQ, Next.js. Built as a learning project to practise
production-grade backend design. Every reliability claim below is backed by
a test or a CI job that breaks the system on purpose.

**What it does.** Clients submit tasks, or whole workflows (DAGs of tasks),
over HTTP. PostgreSQL records them, and workers execute them through a handler
registry, with retries, leases, quotas and fair scheduling across tenants. An
operator dashboard shows and controls the system.

**What it proves, on every pull request**

| Claim | Proof |
|---|---|
| PostgreSQL is the source of truth; Redis only delivers | Transactional outbox; reconciler requeues anything Redis loses |
| A killed worker or a flushed Redis loses nothing | `compose-chaos` CI job: SIGKILL a worker + FLUSHALL Redis mid-run, all 60 tasks and 5 workflows still COMPLETED, none recorded twice |
| Workflows advance exactly when their dependencies finish | Parent completion and child release commit in one transaction; concurrent parents release a join exactly once (tested) |
| Manifests work on Kubernetes with Istio ambient mTLS and L4 policies | `kind-istio` CI job: runtime verifier passes, including deny paths |
| Production images work end to end | Smoke test on the built images: auth boundaries, retries, unknown types, tenant path |
| Logic is correct under concurrency | 490 tests against real PostgreSQL and Redis (Node 22 and 24) |

## Architecture

```
 Operator browser                         Tenant client
       │ Basic auth                            │ Bearer te_… (tenant key)
┌──────▼──────────────┐  Bearer operator key  │
│ Dashboard (Next.js) │──────────────┐        │
│ /api/engine/* proxy │              │        │
└─────────────────────┘        ┌─────▼────────▼──────────────────────┐
                               │ API (Express)                       │
                               │  outbox publisher · scheduler ·     │
                               │  task reconciler (background)       │
                               └──────┬───────────────────┬──────────┘
                 task + outbox in one │                   │ BullMQ.add(jobId = taskId)
                 transaction          ▼                   ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ PostgreSQL       │     │ Redis (BullMQ)  │
                        │ source of truth  │     │ delivery only   │
                        └────────▲─────────┘     └────────┬────────┘
                                 │ claim / renew /        │
                                 │ complete (claim token) │
                        ┌────────┴────────────────────────▼────────┐
                        │ Workers: handler registry, leases, retry │
                        └──────────────────────────────────────────┘
```

1. `POST /tasks` writes the task (QUEUED) and an outbox event in **one
   transaction**. If either write fails, neither exists.
2. The outbox publisher puts the job on BullMQ with `jobId = taskId`, so
   publishing twice is a no-op.
3. A worker claims the task in PostgreSQL (version check + claim token + lease
   on the database clock), renews the lease while it works, and runs the
   handler for the task's type.
4. Success → COMPLETED with the result. Transient error → QUEUED with backoff.
   Permanent error or spent budget → FAILED.
5. If Redis drops the job, or BullMQ gives up on it, the **reconciler** finds
   the orphan from PostgreSQL and requeues it through the outbox.

Full design: [ARCHITECTURE.md](ARCHITECTURE.md). Operations:
[DEPLOYMENT.md](DEPLOYMENT.md).

## Features

- **Reliability:**
  - transactional outbox
  - optimistic concurrency (version column) plus claim tokens on every
    transition out of PROCESSING
  - leases renewed on the database clock
  - stalled-worker reclaim
  - outbox circuit breaker
  - Redis-loss reconciler
  - graceful shutdown
  - startup that waits for PostgreSQL and heals its Redis connection
- **Workflows (DAG):**
  - fan-out and fan-in, validated up front: cycles rejected with the cycle named
  - a step runs when all its parents have completed, and receives their outputs
  - a failed step cancels only its downstream; independent branches finish
  - cancel a whole workflow; idempotency keys; tenants charged one job per step
- **Task handlers:**
  - `echo`, `sleep`, `flaky` (fails on purpose), and `http` (SSRF-safe
    allow-list; a redirect fails the task permanently instead of
    counting as success)
  - permanent vs transient errors
  - retry budget owned by PostgreSQL
  - per-task timeout
  - abort when the lease is lost
- **Scheduling:**
  - one-time `scheduledFor`
  - cron schedules with IANA time zones, misfire policies (`CATCH_UP_ALL`,
    `SKIP_MISSED`, `RUN_ONCE`) and overlap policies (`ALLOW_OVERLAP`,
    `FORBID_OVERLAP`)
- **Multi-tenancy:**
  - plans and per-tenant overrides
  - per-period quotas, idempotency keys, concurrency leases
  - Redis sliding-window rate limits
  - weighted fair publishing
  - suspension
  - operator provisioning with keys shown once
- **Security:**
  - operator key on every operator route
  - per-tenant keys stored as hashes only
  - dashboard behind Basic auth with a server-side allow-listed proxy,
    CSRF guard and strict CSP
  - fail-closed defaults everywhere
- **Observability:**
  - OpenTelemetry traces from API → outbox → worker
  - Pino JSON logs with trace correlation
  - optional Jaeger, Loki/Alloy/Grafana via compose profiles
- **Deployment:**
  - multi-stage non-root images published to GHCR
  - docker compose
  - Kubernetes manifests with HPA/PDB
  - Istio ambient (mTLS, Gateway API, L4 AuthorizationPolicies)

## Quick Start

Requirements: Node.js 22+, pnpm (via corepack), Docker.

```bash
cp .env.example .env     # set OPERATOR_API_KEY (openssl rand -hex 32) and dashboard login
docker compose up -d --build --scale worker=3 postgres redis api worker web
```

- Dashboard: http://localhost:3001 (log in with `DASHBOARD_USER` / `DASHBOARD_PASSWORD`)
- API: http://localhost:3000

```bash
source .env
curl -X POST http://localhost:3000/tasks \
  -H "Authorization: Bearer $OPERATOR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"echo","priority":"HIGH","maxRetries":3,"payload":{"hello":"world"}}'
```

Guided demo (stack, tenant, every task type, then a killed worker and a wiped
Redis): `bash scripts/demo.sh`.

Development without containers for the app:

```bash
pnpm install
docker compose up -d postgres redis
export OPERATOR_API_KEY="$(openssl rand -hex 32)"
pnpm -F @distributed-engine/api run dev
pnpm -F @distributed-engine/worker run dev
pnpm -F @distributed-engine/web run dev          # needs ENGINE_API_URL, OPERATOR_API_KEY, DASHBOARD_*
```

## Task Types

A task's `name` is its type. Unknown types fail at once, because retrying
cannot make code appear.

| Type | Payload | Does |
|------|---------|------|
| `echo` | anything | Returns the payload as the result |
| `sleep` | `{ "ms": 1000 }` (max 300000) | Waits; stands in for slow I/O |
| `flaky` | `{ "failTimes": 1 }` | Fails the first N attempts, then succeeds |
| `http` | `{ "url", "method", "headers", "body", "timeoutMs" }` | Calls an allow-listed host; disabled unless `HTTP_TASK_ALLOWED_HOSTS` is set |

- **Permanent errors** (unknown type, bad payload, HTTP 4xx): FAILED on that
  attempt.
- **Transient errors** (timeout, 5xx/429, network): retried with exponential
  backoff until `maxRetries` is spent.

## API

**Operator routes.** All require `Authorization: Bearer $OPERATOR_API_KEY`.
They answer 503 if no key is configured.

| Method | Path | Description |
|--------|------|-------------|
| POST | /tasks | Create a task (`scheduledFor` makes it one-time scheduled) |
| GET | /tasks, /tasks/:id | List (paginated) / get |
| PUT | /tasks/:id/cancel | Cancel |
| POST | /tasks/:id/retry | Send a FAILED task back to the queue with a fresh retry budget (not workflow steps) |
| POST | /workflows | Start a workflow `{ name, steps: [{ key, name, payload, dependsOn }], idempotencyKey? }` |
| GET | /workflows, /workflows/:id | List with progress / steps with status, level, dependencies, output |
| PUT | /workflows/:id/cancel | Cancel the workflow and every unfinished step |
| POST/GET/PUT | /schedules, /schedules/:id | Cron schedules |
| PUT | /schedules/:id/status | ACTIVE / PAUSED / DISABLED |
| GET | /metrics | JSON: task counts, queue depth, outbox (read by the dashboard) |
| GET | /metrics/prometheus | The same numbers plus process stats, in Prometheus format |
| GET | /workers | Live workers (heartbeats) |
| GET | /plans | Plans |
| GET/POST | /tenants | List / create (returns the tenant key **once**) |
| GET | /tenants/:id | Limits, usage, live concurrency |
| PUT | /tenants/:id/status | ACTIVE / SUSPENDED |
| POST | /tenants/:id/api-key | Rotate key (old key revoked at once) |

**Tenant routes.** All require `Authorization: Bearer te_…`. They are
rate-limited and quota-checked.

| Method | Path | Description |
|--------|------|-------------|
| POST | /tenant/tasks | Submit (supports `idempotencyKey`; a future `scheduledFor` waits as SCHEDULED) |
| GET | /tenant/tasks, /tenant/tasks/:id | List / get own tasks |
| PUT | /tenant/tasks/:id/cancel | Cancel own task |
| POST | /tenant/tasks/:id/retry | Re-run own FAILED task; counts as one job against the quota (429 when it is used up) |
| POST/GET | /tenant/workflows, /tenant/workflows/:id | Own workflows (quota: one job per step) |
| PUT | /tenant/workflows/:id/cancel | Cancel own workflow |
| POST/GET | /tenant/schedules, /tenant/schedules/:id | Own schedules |
| GET | /tenant/usage | Limits and usage this period |

**Probes** (no auth): `/health` (PG, Redis, breaker), `/ready` (PG), `/live`.

## Workflows

```bash
curl -X POST http://localhost:3000/workflows \
  -H "Authorization: Bearer $OPERATOR_API_KEY" -H "Content-Type: application/json" \
  -d '{"name":"nightly-report","steps":[
        {"key":"fetch","name":"echo","payload":{"source":"orders"}},
        {"key":"by-region","name":"sleep","payload":{"ms":2000},"dependsOn":["fetch"]},
        {"key":"by-product","name":"flaky","payload":{"failTimes":1},"dependsOn":["fetch"]},
        {"key":"merge","name":"echo","dependsOn":["by-region","by-product"]}]}'
```

```
fetch ──┬──▶ by-region ──┐
        └──▶ by-product ─┴──▶ merge      (merge receives both outputs as `upstream`)
```

- Steps with dependencies start BLOCKED: no queue job exists until their last
  parent completes.
- Completion and release commit in the same transaction, so a crash between
  "parent done" and "child released" is impossible.
- If a step fails (after its retries), its downstream steps are CANCELLED with
  "Upstream step … failed". Branches that do not depend on it run to the end,
  and then the workflow is FAILED.
- The dashboard's Workflows page draws the graph by level and updates live.

## Task State Machine

```
SCHEDULED ──▶ QUEUED ──▶ PROCESSING ──▶ COMPLETED
BLOCKED ────▶ QUEUED     (workflow step: last dependency completed)
                ▲            │
                └────────────┤  transient failure, budget left
                             ▼
                           FAILED   (permanent error or budget spent)

QUEUED / SCHEDULED / BLOCKED / PROCESSING ──▶ CANCELLED
```

COMPLETED and CANCELLED are terminal. Leaving PROCESSING requires the current
claim token, so a worker that lost its lease can never record an outcome.

## Delivery Guarantee

**At-least-once execution, exactly-once outcome.**

- A task whose worker dies mid-run executes again on another worker.
- PostgreSQL records exactly one result per task.
- Handlers that call external systems should be idempotent.

## Configuration

**API**

| Variable | Default | Description |
|----------|---------|-------------|
| PORT | 3000 | HTTP port |
| POSTGRES_HOST / PORT / DB / USER / PASSWORD | localhost / 5432 / distributed_engine / postgres / — | PostgreSQL |
| POSTGRES_MAX_CONNECTIONS | 20 | Pool size |
| REDIS_HOST / PORT / PASSWORD | localhost / 6379 / — | Redis |
| OPERATOR_API_KEY | — | Operator Bearer key, 32+ chars; unset = operator routes 503 |
| OUTBOX_POLL_INTERVAL_MS / BATCH_SIZE / MAX_ATTEMPTS | 1000 / 10 / 5 | Outbox publisher. The interval is the idle delay: a full batch is followed by the next poll at once |
| SCHEDULER_POLL_INTERVAL_MS / BATCH_SIZE | 5000 / 50 | Scheduler |
| RECONCILER_INTERVAL_MS / QUEUED_GRACE_MS / BATCH_SIZE | 30000 / 60000 / 100 | Redis-loss reconciler |
| GRACEFUL_SHUTDOWN_TIMEOUT_MS | 30000 | Drain timeout |
| LOG_LEVEL | INFO | DEBUG, INFO, WARN, ERROR |

**Worker**

| Variable | Default | Description |
|----------|---------|-------------|
| POSTGRES_* / REDIS_* | as API | Connections |
| QUEUE_CONCURRENCY | 5 | Jobs in parallel per worker |
| TASK_TIMEOUT_MS | 300000 | Per-task execution limit |
| HTTP_TASK_ALLOWED_HOSTS | — | Comma-separated hosts for `http` tasks (`*.example.com` allowed); empty = disabled |
| SCHEDULE_LEASE_DURATION_MS | 300000 | FORBID_OVERLAP execution lease |

**Dashboard:** `ENGINE_API_URL`, `OPERATOR_API_KEY`, `DASHBOARD_USER`,
`DASHBOARD_PASSWORD`. Missing login → 503.

**Tracing:** standard `OTEL_*` variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_SDK_DISABLED`, `OTEL_TRACES_SAMPLER_ARG`).

## Testing

```bash
docker compose up -d postgres redis
pnpm test            # unit + integration against real PostgreSQL and Redis
pnpm verify          # build + test
bash scripts/chaos-compose.sh       # full stack under a killed worker and a flushed Redis
bash scripts/k8s-kind-e2e.sh        # kind + Istio ambient + runtime verifier (needs images built)
```

Integration tests fail loudly when PostgreSQL or Redis is missing. They never
skip silently.

## Project Structure

```
apps/
  api/        Express API, operator + tenant routes, background loops
  worker/     BullMQ worker, task execution orchestration
  web/        Next.js operator dashboard (server-side proxy to the API)
packages/
  shared/     state machine, repositories, outbox, reconciler, scheduler,
              workflows (DAG engine), task handlers, tenancy, telemetry, logger
k8s/          Kubernetes manifests (+ istio/ for the ambient mesh layer)
ci/kind/      kustomize overlay used by CI on kind
scripts/      smoke, chaos, kind e2e, demo
docs/         runbooks (backup/restore, Redis-loss recovery, deployment)
```

## Not Included (Yet)

- Queue-depth autoscaling (the metrics exist; a KEDA/adapter setup does not)
- A Prometheus scrape setup for Kubernetes (compose has one)
- Workflows started by cron schedules (schedules create single tasks today)
- A dead-letter queue beyond FAILED tasks; saga/compensation
- TLS at the Gateway (environment-specific certificates)
- Load and soak testing
