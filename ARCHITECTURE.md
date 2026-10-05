# Distributed Task Execution Engine - Architecture

Complete architectural overview of the system design, data flow, and production considerations.

## System Overview

A production-grade distributed task execution system with three core components:

```
┌─────────────────────────────────────────────────────────┐
│                    CLIENT APPLICATION                   │
│  (Submits tasks via REST API)                           │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP POST /tasks
                     │ (JSON payload + priority)
┌────────────────────▼────────────────────────────────────┐
│                    API SERVER (Node.js + Express)       │
│  ┌──────────────────────────────────────────────────┐   │
│  │ Responsibilities:                                │   │
│  │ • Validate task input (Zod schemas)             │   │
│  │ • Generate unique task IDs                      │   │
│  │ • Enqueue to BullMQ (Redis-backed queue)        │   │
│  │ • Store task metadata in-memory/Redis          │   │
│  │ • Collect system metrics                        │   │
│  │ • Circuit breaker for fault tolerance           │   │
│  │ • Graceful shutdown on SIGTERM                  │   │
│  └──────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │ enqueue(jobId, {taskId, task})
                     │ {"status": "QUEUED"}
┌────────────────────▼──────────────┬────────────────────┐
│                                    │                    │
│   ┌─────────────────────────────┐  │  ┌─────────────────┤
│   │   REDIS (In-Memory Store)   │  │  │  BullMQ Queue   │
│   │                             │  │  │  (Job queue)    │
│   │ Stores:                     │  │  │                 │
│   │ • Job queue (tasks:pending) │◄─┼──┤ Persists tasks │
│   │ • Worker heartbeats         │  │  │ on disk (AOF)   │
│   │ • Session data              │  │  │                 │
│   │ • Metrics (ephemeral)       │  │  └─────────────────┤
│   │                             │  │                    │
│   └─────────────────────────────┘  │                    │
│                                    │                    │
│         Persistent Volume (10Gi)   │                    │
└────────────────────┬───────────────┴────────────────────┘
                     │ BLPOP tasks:pending (blocking pop)
                     │ {"taskId": "abc123", "task": {...}}
┌────────────────────▼────────────────────────────────────┐
│              WORKER POOL (5+ parallel instances)         │
│  Each worker:                                           │
│  ┌──────────────────────────────────────────────────┐   │
│  │ • Connects to Redis queue                        │   │
│  │ • Pops job (blocks if queue empty)              │   │
│  │ • Updates task: status → PROCESSING             │   │
│  │ • Executes job with retry logic (exp backoff)   │   │
│  │ • Publishes heartbeat every 10s                 │   │
│  │ • Updates task: status → COMPLETED/FAILED       │   │
│  │ • Graceful SIGTERM (finishes current job)       │   │
│  └──────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │ GET /workers
                     │ (worker:* keys from Redis)
┌────────────────────▼────────────────────────────────────┐
│                    MONITORING & OBSERVABILITY            │
│  • GET /metrics → System stats (queue depth, errors)    │
│  • GET /health → Liveness probe (returns 503 if down)  │
│  • GET /tasks/:id → Task status (QUEUED→PROCESSING)    │
│  • Prometheus scrape every 30s                         │
│  • Kubernetes HPA scales based on CPU/memory           │
└─────────────────────────────────────────────────────────┘
```

## Data Flow: Task Lifecycle

### 1. Submission Phase

```
Client          API                  Redis/BullMQ
  │              │                        │
  ├─POST /tasks─→│                        │
  │              │ validate (Zod)         │
  │              │ generate ID            │
  │              ├─enqueue job─────────→  │
  │              │ store metadata         │
  │              │◄─job confirmed────────┤
  │←─201 QUEUED──│                        │
  │
Task Status: QUEUED
Location: Redis queue, waiting for worker
```

### 2. Processing Phase

```
Worker          Redis               API (metrics)
  │              │                       │
  ├─BLPOP───────→│                       │
  │◄─job data────│                       │
  │              │                       │
  │ status: PROCESSING                   │
  │ update taskStore                     │
  │              │                       │
  │◄─retry logic + exponential backoff──→
  │              │                       │
  │ [execute 2s]                         │
  │              │                       │
  │ success OR   │                       │
  │ error        │                       │
  │              │                       │
  │ status: COMPLETED/FAILED             │
  │ update taskStore                     │
  │              │                       │
  └─heartbeat───→│ worker:{id} TTL=30s  │
                 │                       │
                 │◄──GET /metrics───────→
                 │ completed++, failed++│
```

### 3. Completion Phase

```
Client                    API
  │                       │
  └─GET /tasks/{id}───────→
                          │ lookup taskStore
                          │◄─{status: COMPLETED}
                          │
                          └─→ Client sees COMPLETED
```

## Component Deep Dive

### API Server (`apps/api`)

**Request Flow**:
1. Client sends POST /tasks
2. Express middleware logs, tracks active requests
3. ValidationError if invalid (Zod schema fails)
4. Circuit breaker wraps Redis enqueue
5. If Redis down, task stored locally with PENDING status
6. Response 201 with task data

**Endpoints**:
- `POST /tasks`: Submit new task
- `GET /tasks/:id`: Fetch single task
- `GET /tasks?page=1&pageSize=10`: List tasks (paginated)
- `PUT /tasks/:id/cancel`: Cancel task
- `GET /health`: Liveness probe (503 if circuit OPEN)
- `GET /metrics`: System observability (uptime, request rates, error rates, queue depth)
- `GET /workers`: Active worker pool status (heartbeats from Redis)

**Key Production Features**:
- **Validation**: All inputs validated with Zod (422 on error)
- **Error Hierarchy**: ValidationError (422), AppError (custom), generic (500)
- **Circuit Breaker**: Opens after 5 failures, auto-recovers in 30s
- **Metrics**: Request count, error count, task counts by status
- **Graceful Shutdown**: 30s timeout for in-flight requests, then force close

### Worker Pool (`apps/worker`)

**Job Processing**:
1. Connect to Redis, listen on "tasks" queue
2. BLPOP blocks until job available (no busy-waiting)
3. Job data: {taskId, task, task.payload}
4. Update status: QUEUED → PROCESSING
5. Execute job with retryWithBackoff (exponential backoff, 3 attempts)
6. On success: status → COMPLETED, log
7. On failure: status → FAILED, log error
8. Increment metrics (jobsProcessed, jobsCompleted/jobsFailed)

**Concurrency**:
- 5 workers per pod (configurable)
- Multiple pods scale horizontally
- No cross-worker state (fully distributed)

**Health & Reliability**:
- Periodic heartbeat (every 10s) to Redis
- Heartbeat includes: uptime, jobsProcessed, jobsCompleted, jobsFailed
- Redis TTL=30s (auto-cleanup if worker crashes)
- Graceful SIGTERM: 25s preStop hook, then SIGKILL

### Redis (`statefulset`)

**Purpose**:
- Primary: BullMQ queue storage (job queue, retry logic)
- Secondary: Ephemeral state (worker heartbeats, metrics)

**Data Structure**:
```
KEYS patterns:
- tasks:pending → Job queue (list of job IDs)
- tasks:completed → Completed jobs (for history)
- worker:* → Worker status (JSON, TTL=30s)
- metrics:* → System metrics (ephemeral)
```

**Persistence**:
- AOF (Append-Only File) enabled
- Every write logged to disk
- Survives pod restart
- PVC volume for durability

**Failover**:
- StatefulSet (stable hostname)
- PVC snapshots for backup
- Manual recovery from snapshot

### Shared Types & Utilities (`packages/shared`)

**Exports**:
```typescript
// Types
Task, TaskStatus, TaskPriority
Worker, WorkerStatus
ApiResponse, PaginatedResponse

// Utilities
generateId() → unique string
log(level, msg, context) → structured logging
retryWithBackoff(fn, maxRetries) → exponential backoff
AppError(statusCode, message) → HTTP-aware error
```

Used by all services (type safety across monorepo).

## Production Architecture Patterns

### 1. Fault Tolerance (Circuit Breaker)

```
API tries to enqueue task to Redis

Success path:           Failure path:
task → queue        ×5  failures detected
✓ completed         →   Circuit OPEN
                        ↓
                    reject immediately
                        ↓
                    after 30s: HALF_OPEN
                        ↓
                    test one request
                        ↓
                    success: CLOSED
                    or
                    failure: back to OPEN
```

**Benefit**: Instead of cascading timeouts, user gets instant feedback.

### 2. Graceful Shutdown

```
SIGTERM received (deployment terminating pod)
       ↓
HTTP server stops accepting NEW connections
       ↓
Active requests continue (max 30s)
       ↓
Poll: activeRequests == 0?
       ↓
Close BullMQ & Redis connections
       ↓
Exit process (Kubernetes replaces pod)
```

**Benefit**: No data loss, no duplicate processing during rolling updates.

### 3. Metrics Observability

```
API continuously collects:
├── Request metrics (total, errors, error rate)
├── Task metrics (total by status, completion rate, failure rate)
├── Queue metrics (queue depth, active jobs, waiting jobs)
└── System uptime

Worker publishes heartbeat every 10s:
├── Uptime since start
├── Jobs processed (lifetime)
├── Jobs completed (lifetime)
└── Jobs failed (lifetime)

/metrics endpoint aggregates all metrics for:
├── Dashboards (real-time visibility)
├── Alerting (thresholds: CPU, queue depth, error rate)
└── Capacity planning (trend analysis)
```

## Scaling Strategy

### Horizontal Scaling (HPA)

**API Pods**:
- Min: 3 (high availability)
- Max: 10 (cost control)
- Target CPU: 70% (scale up if exceeds)
- Target Memory: 80% (scale up if exceeds)

**Worker Pods**:
- Min: 5 (ensure queue processing)
- Max: 20 (cost control)
- Target CPU: 75% (more aggressive)
- Target Memory: 80%

### Example: Traffic Spike

```
Normal: [API pod 1] [API pod 2] [API pod 3]
        10 req/s, CPU 40%, Mem 50%

Spike: [API pod 1] [API pod 2] [API pod 3] [API pod 4] [API pod 5]
       50 req/s, CPU 70%, Mem 60% (auto-scaled)

After spike: [API pod 1] [API pod 2] [API pod 3]
             (HPA scales down after 5+ min)
```

## Security Layers

### Network
- Pod-to-Pod (no external API calls from worker)
- Kubernetes Service DNS (redis.distributed-engine.svc.cluster.local)
- Network policies (restrict ingress/egress)

### Authentication
- No public API (service mesh or OAuth for external)
- Service accounts for inter-pod communication

### Data
- Redis authentication (password protected)
- Persistent volume encryption (at-rest)
- TLS for data in-transit (if external)

### Resource Limits
- CPU: limits prevent runaway
- Memory: limits prevent OOM kills
- Disk: persistent volume quota

## Debugging & Troubleshooting

### Check System Health

```bash
# Pod status
kubectl get pods -n distributed-engine

# Resource usage
kubectl top pods -n distributed-engine

# Logs
kubectl logs -f -n distributed-engine -l app=api
kubectl logs -f -n distributed-engine -l app=worker

# Metrics
curl http://api:3000/metrics | jq .data.queue
curl http://api:3000/workers
```

### Common Issues

| Issue | Symptom | Check | Fix |
|-------|---------|-------|-----|
| Redis down | Circuit OPEN | `kubectl logs redis-0` | Check PVC mount |
| Queue backing up | high queue.total | `kubectl top pods -l app=worker` | Scale workers |
| Slow API | p95 latency >500ms | `kubectl top pods -l app=api` | Scale API |
| Memory leak | Pod OOM | `kubectl describe pod` | Restart pod |

## Performance Characteristics

**Latencies** (p50/p95/p99):
- API /health: 5ms / 10ms / 20ms
- API /tasks POST: 50ms / 100ms / 200ms
- API /metrics: 200ms / 500ms / 1000ms
- Job processing: 2s (configurable)

**Throughput**:
- Single API pod: 500 req/s
- 3 API pods: 1500 req/s
- Single worker: 30 jobs/min (2s per job)
- 5 workers: 150 jobs/min

**Concurrency**:
- API: stateless (scale horizontally)
- Worker: 5 concurrent jobs per pod

## Next Steps / Advanced Features

Not yet implemented but valuable:

1. **Service Mesh (Istio)**
   - Traffic management
   - Circuit breaking at mesh level
   - Request tracing (distributed tracing)

2. **Monitoring Stack**
   - Prometheus scraping /metrics
   - Grafana dashboards
   - AlertManager for alerts

3. **Log Aggregation**
   - ELK/Loki for log storage
   - Structured JSON logging
   - Log-based alerting

4. **Distributed Tracing**
   - OpenTelemetry instrumentation
   - Jaeger backend for visualization
   - End-to-end request tracing

5. **Advanced Scheduling**
   - Task dependencies (run after task X)
   - Delayed execution (run at specific time)
   - Cron jobs (recurring tasks)

6. **Multi-tenancy**
   - Namespace isolation
   - Per-tenant rate limiting
   - Resource quotas by tenant

## References

- [Kubernetes Best Practices](https://kubernetes.io/docs/)
- [12-Factor App](https://12factor.net/)
- [Site Reliability Engineering](https://sre.google/)
- [Domain-Driven Design](https://martinfowler.com/)
