# Distributed Task Execution Engine

A learning/portfolio distributed task execution engine with production-oriented reliability patterns. Built with TypeScript, Node.js, BullMQ, PostgreSQL, and Redis.

## What This Is

An asynchronous task processing system demonstrating:

- **Transactional outbox pattern** for reliable event publishing
- **PostgreSQL as source of truth** for task state (not Redis)
- **Optimistic concurrency control** with version columns
- **State machine enforcement** on all task transitions
- **At-least-once delivery** with idempotent job publication
- **Circuit breaker** protecting the outbox publisher from Redis failures
- **Stalled job recovery** for worker crash scenarios
- **Graceful shutdown** for both API and worker processes

This is **not** a production SaaS platform. It does not include service mesh, distributed tracing, log aggregation, advanced scheduling, or multi-tenancy. Those would be the next phase.

## Architecture

```
┌─────────────────┐
│   Client App    │
└────────┬────────┘
         │ POST /tasks
┌────────▼────────────┐     ┌──────────────┐
│   API Server (3)    │────▶│  PostgreSQL   │  ← Source of truth
└─────────────────────┘     └──────┬───────┘
                                   │ outbox poll
                            ┌──────▼───────┐
                            │ Outbox       │
                            │ Publisher    │
                            └──────┬───────┘
                                   │ BullMQ add
                            ┌──────▼───────┐
                            │    Redis     │  ← Job queue only
                            └──────┬───────┘
                                   │ consume
                            ┌──────▼───────┐
                            │ Workers (5+) │
                            └──────────────┘
```

**Task submission flow:**

1. API inserts task (status=QUEUED) + outbox event atomically in one PG transaction
2. Outbox publisher polls PG for pending events, publishes to BullMQ using taskId as jobId (idempotent while the job exists in BullMQ)
3. Worker picks up job, transitions task QUEUED → PROCESSING → COMPLETED/FAILED in PG
4. On intermediate failure with remaining BullMQ retries: PROCESSING → QUEUED
5. On final failure (retries exhausted): PROCESSING → FAILED

## Quick Start

### Prerequisites

- Node.js 20+
- pnpm
- Docker & Docker Compose (for PostgreSQL and Redis)

### Local Development

```bash
pnpm install

# Start PostgreSQL and Redis
docker-compose up -d postgres redis

# Run API server
pnpm -F @distributed-engine/api run dev

# Run worker (separate terminal)
pnpm -F @distributed-engine/worker run dev
```

### Submit a Task

```bash
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"name":"hello-world","priority":"HIGH","maxRetries":3}'
```

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /tasks | Create a task |
| GET | /tasks | List tasks (paginated) |
| GET | /tasks/:id | Get task by ID |
| PUT | /tasks/:id/cancel | Cancel a task |
| GET | /health | Health check (PG + Redis + circuit breaker) |
| GET | /ready | Readiness probe (PG connectivity only) |
| GET | /live | Liveness probe (always 200) |
| GET | /metrics | JSON metrics (task counts, queue depth, outbox stats) |
| GET | /workers | Live worker list from Redis heartbeats |

## Task State Machine

```
QUEUED ──▶ PROCESSING ──▶ COMPLETED
  │  ▲        │  │  ▲
  │  │        │  │  │
  │  └────────┘  │  └── (BullMQ retry: intermediate failure)
  │              │
  │              ▼
  │           FAILED
  │              
  ▼
CANCELLED ◀── QUEUED | PROCESSING
```

- **QUEUED**: Task created and waiting for a worker
- **PROCESSING**: Worker has picked up the task
- **COMPLETED**: Task finished successfully (terminal)
- **FAILED**: Task failed after all retries exhausted (terminal). The state machine permits FAILED → QUEUED but no user-facing retry endpoint is exposed.
- **CANCELLED**: Task cancelled by user (terminal)

## Reliability Patterns

### Source of Truth: PostgreSQL

All task state lives in PostgreSQL. Redis/BullMQ is a job queue only — if Redis loses data, the outbox publisher will re-publish pending events. Tasks are never created in Redis first.

### Transactional Outbox

Task creation and the outbox event are inserted in a single PG transaction. The outbox publisher polls for pending events and publishes to BullMQ. This guarantees at-least-once delivery: if the publisher crashes after BullMQ.add() but before marking the event DELIVERED, it will re-publish on the next poll.

**Idempotency boundary:** The outbox publisher uses taskId as the BullMQ jobId. BullMQ rejects duplicate jobIds while the job exists, making re-publication a no-op. However, completed jobs are removed after a retention window (`removeOnComplete: 100`), after which the jobId becomes reusable. The outbox marks events as DELIVERED after successful publication, so this window only matters if the outbox itself is manually reset.

### Cancellation Safety

The outbox publisher performs a best-effort cancellation check before publishing to BullMQ. Because PostgreSQL and Redis are separate systems, the check and the publish are **not** atomic — a task can be cancelled between the check and the BullMQ.add() call. If this race occurs, BullMQ will hold a job for a cancelled task. The worker guards (status check before processing, version-checked state transitions) ensure such a job is skipped harmlessly. PostgreSQL is the authoritative source of task state; workers always verify task status before processing.

### Optimistic Concurrency

Every task has a `version` column. State transitions use `SELECT FOR UPDATE` + version check. If two workers race to process the same task, only one wins — the other gets a `StaleVersionError`.

### Stalled Job Recovery

If a worker crashes while processing a task, BullMQ detects the stalled job and redelivers it. The new worker finds the task in PROCESSING state and reclaims it by verifying the version and bumping it under a row lock. This prevents the task from getting stuck in PROCESSING after a worker crash. BullMQ's `maxStalledCount` limits how many times a single job can be reclaimed before being marked as failed.

### Circuit Breaker (Outbox Publisher)

The outbox publisher has a built-in circuit breaker for BullMQ operations. After 5 consecutive failures, it stops attempting to publish (OPEN state). After 30 seconds, it tries a single operation as a probe (HALF_OPEN) — not the full batch. After 2 consecutive successes, it resumes normal operation (CLOSED). The `/health` endpoint reports this state.

### At-Least-Once, Not Exactly-Once

This system provides **at-least-once delivery**. A task may be processed more than once if:
- The worker crashes after completing work but before writing COMPLETED to PG
- BullMQ retries a job that the worker already processed

Task handlers should be **idempotent** — processing the same task twice should produce the same result.

## Testing

```bash
# Run all tests (requires PostgreSQL + Redis)
docker-compose up -d postgres redis
pnpm test

# Full verification (build + test)
pnpm verify
```

Integration tests for TaskRepository and OutboxPublisher require PostgreSQL and Redis. Tests fail with a clear error when infrastructure is unavailable rather than silently passing.

## Project Structure

```
distributed-engine/
├── apps/
│   ├── api/           # Express REST API server
│   ├── worker/        # BullMQ job processor
│   └── web/           # Next.js dashboard
├── packages/
│   ├── shared/        # Types, state machine, DB layer, outbox
│   └── ui/            # Shared React components
├── k8s/               # Kubernetes manifests
├── docker-compose.yml # Local development services
├── Dockerfile.api     # API container image
└── Dockerfile.worker  # Worker container image
```

## Configuration

All configuration is via environment variables with sensible defaults for local development.

### API Server

| Variable | Default | Description |
|----------|---------|-------------|
| PORT | 3000 | API server port |
| REDIS_HOST | localhost | Redis hostname |
| REDIS_PORT | 6379 | Redis port |
| POSTGRES_HOST | localhost | PostgreSQL hostname |
| POSTGRES_PORT | 5432 | PostgreSQL port |
| POSTGRES_DB | distributed_engine | Database name |
| POSTGRES_USER | postgres | Database user |
| POSTGRES_PASSWORD | (none) | Database password |
| POSTGRES_MAX_CONNECTIONS | 20 | PG pool size |
| OUTBOX_POLL_INTERVAL_MS | 1000 | Outbox polling frequency |
| OUTBOX_BATCH_SIZE | 10 | Events per outbox poll |
| OUTBOX_MAX_ATTEMPTS | 5 | Max outbox publish attempts |
| GRACEFUL_SHUTDOWN_TIMEOUT_MS | 30000 | Shutdown drain timeout |
| LOG_LEVEL | INFO | Logging level |

### Worker

| Variable | Default | Description |
|----------|---------|-------------|
| REDIS_HOST | localhost | Redis hostname |
| REDIS_PORT | 6379 | Redis port |
| POSTGRES_HOST | localhost | PostgreSQL hostname |
| POSTGRES_PORT | 5432 | PostgreSQL port |
| POSTGRES_DB | distributed_engine | Database name |
| POSTGRES_USER | postgres | Database user |
| POSTGRES_PASSWORD | (none) | Database password |
| QUEUE_CONCURRENCY | 5 | Concurrent jobs per worker |

## Kubernetes Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for the full deployment guide. The K8s manifests in `k8s/` provide:

- API: 3 replicas with HPA (3-10), readiness probe on `/ready` (PG only), liveness on `/live`
- Workers: 5 replicas with HPA (5-20), graceful shutdown with 40s termination grace period
- Redis: StatefulSet with AOF persistence
- PostgreSQL: StatefulSet with persistent volume

HPA scales on CPU and memory utilization. Queue-depth-based scaling requires a custom metrics adapter (not included).

## What's Not Included (Future Work)

- Service mesh (Istio)
- Distributed tracing (OpenTelemetry/Jaeger)
- Log aggregation (ELK/Loki)
- Prometheus-format metrics endpoint
- Advanced scheduling (cron, dependencies, DAGs)
- Multi-tenancy
- Authentication/authorization on the API
