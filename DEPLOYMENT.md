# Deployment & Operations

Guide for deploying and operating the distributed task execution engine.

## Prerequisites

- Kubernetes cluster (1.24+)
- Docker registry access
- `kubectl` configured
- PostgreSQL password stored in a Kubernetes Secret named `postgres-secret`

## Local Development

```bash
pnpm install

# Start infrastructure
docker-compose up -d postgres redis

# Run API + worker
pnpm -F @distributed-engine/api run dev
pnpm -F @distributed-engine/worker run dev

# Verify
curl http://localhost:3000/health
curl http://localhost:3000/ready
```

## Docker Build

```bash
# Build images
docker build -f Dockerfile.api -t distributed-engine-api:0.1.0 .
docker build -f Dockerfile.worker -t distributed-engine-worker:0.1.0 .

# Tag for your registry
docker tag distributed-engine-api:0.1.0 <registry>/distributed-engine-api:0.1.0
docker tag distributed-engine-worker:0.1.0 <registry>/distributed-engine-worker:0.1.0

# Push
docker push <registry>/distributed-engine-api:0.1.0
docker push <registry>/distributed-engine-worker:0.1.0
```

## Kubernetes Deployment

### 1. Create the namespace and secrets

```bash
kubectl create namespace distributed-engine
kubectl create secret generic postgres-secret \
  --namespace distributed-engine \
  --from-literal=password=<your-password>
```

### 2. Deploy infrastructure

```bash
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml

# Wait for readiness
kubectl -n distributed-engine wait --for=condition=ready pod -l app=postgres --timeout=120s
kubectl -n distributed-engine wait --for=condition=ready pod -l app=redis --timeout=120s
```

### 3. Update image references

Edit `k8s/api.yaml` and `k8s/worker.yaml` to point to your registry images.

### 4. Deploy application

```bash
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml

# Verify
kubectl -n distributed-engine get pods
kubectl -n distributed-engine logs -l app=api --tail=20
```

### 5. Verify deployment

```bash
# Port-forward to test
kubectl -n distributed-engine port-forward svc/api 3000:80

# Health check
curl http://localhost:3000/health
curl http://localhost:3000/ready

# Submit a test task
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"name":"deploy-test","priority":"NORMAL"}'
```

## Health Probes

| Endpoint | What it checks | When it returns 503 |
|----------|---------------|---------------------|
| `/ready` | PostgreSQL connectivity | PG unreachable |
| `/live` | Nothing (always 200) | Never |
| `/health` | PG + Redis + outbox circuit breaker state | PG unreachable |

`/ready` only requires PostgreSQL because task submission (the core API function) writes to PG only. Redis unavailability degrades the outbox publisher but does not prevent task creation. `/health` returns 200 with `status: "degraded"` when Redis is down or the circuit breaker is not CLOSED, and 503 only when PostgreSQL is unreachable.

## Graceful Shutdown

### API Server

1. K8s sends SIGTERM
2. `preStop` hook sleeps 5 seconds (lets load balancer stop routing)
3. Server stops accepting new connections
4. Outbox publisher stops (waits for any in-progress poll to complete)
5. In-flight requests drain (up to GRACEFUL_SHUTDOWN_TIMEOUT_MS)
6. Connections close (BullMQ queue, Redis, PG pool)
7. Process exits with code 0 (clean drain) or code 1 (timeout with active requests still pending)

`terminationGracePeriodSeconds: 40` = 5s preStop + 30s drain + 5s buffer.

### Worker

1. K8s sends SIGTERM
2. `preStop` hook sleeps 5 seconds
3. Worker stops consuming new jobs
4. In-flight jobs complete (up to 30s timeout)
5. Worker heartbeat key removed from Redis
6. Connections close
7. Process exits

`terminationGracePeriodSeconds: 40` = 5s preStop + 30s worker shutdown + 5s buffer.

## Scaling

### Manual Scaling

```bash
kubectl -n distributed-engine scale deployment/worker --replicas=10
kubectl -n distributed-engine scale deployment/api --replicas=5
```

### HPA (Automatic)

Both API and worker deployments have HPA configured:

- **API**: 3-10 replicas, scales on CPU (70%) and memory (80%)
- **Workers**: 5-20 replicas, scales on CPU (75%) and memory (80%)

HPA uses CPU and memory utilization. Queue-depth-based scaling is not configured — it would require a custom metrics adapter (e.g., Prometheus Adapter or KEDA with a Redis scaler).

## Operations

### Viewing Metrics

```bash
curl http://localhost:3000/metrics | jq
```

Returns JSON with:
- `tasks.total`, `tasks.byStatus` — task counts from PostgreSQL
- `queue.total`, `queue.active`, `queue.waiting` — BullMQ queue depth
- `outbox.pending`, `outbox.failed` — outbox event counts
- `requests.total`, `requests.errors`, `requests.errorRate`

Note: The `/metrics` endpoint returns JSON, not Prometheus exposition format. To scrape with Prometheus, you would need to add a Prometheus client library (e.g., `prom-client`) and expose a `/metrics/prometheus` endpoint.

### Checking Worker Status

```bash
curl http://localhost:3000/workers | jq
```

Shows live workers (from Redis heartbeats with 30s TTL). A worker that hasn't sent a heartbeat in 30 seconds is not listed.

### Viewing Logs

```bash
kubectl -n distributed-engine logs -l app=api --tail=50 -f
kubectl -n distributed-engine logs -l app=worker --tail=50 -f
```

Logs are structured JSON with ISO timestamps, level, message, and context fields.

### Rolling Update

```bash
# Build and push new image
docker build -f Dockerfile.api -t <registry>/distributed-engine-api:0.2.0 .
docker push <registry>/distributed-engine-api:0.2.0

# Update deployment
kubectl -n distributed-engine set image deployment/api api=<registry>/distributed-engine-api:0.2.0

# Watch rollout
kubectl -n distributed-engine rollout status deployment/api
```

Rolling updates are configured with `maxSurge: 1, maxUnavailable: 0` for zero-downtime deploys.

### Rollback

```bash
kubectl -n distributed-engine rollout undo deployment/api
kubectl -n distributed-engine rollout undo deployment/worker
```

## Troubleshooting

| Symptom | Check | Action |
|---------|-------|--------|
| `/ready` returns 503 | PG connectivity | Check PG pod logs, verify PG Secret |
| `/health` shows degraded | Redis or circuit breaker | Check Redis pod, check outbox circuit state |
| Tasks stuck in QUEUED | Worker pods, Redis connectivity | Verify workers are running, check Redis |
| Outbox events piling up | Outbox circuit breaker | Check `/health` for circuit state, check Redis |
| Workers not listed | Redis heartbeat | Check worker logs for heartbeat errors |
| Stale version errors | Concurrent processing | Normal under contention, only one wins |

## Backup

### PostgreSQL

```bash
# Manual backup
kubectl -n distributed-engine exec postgres-0 -- pg_dump -U postgres distributed_engine > backup.sql

# Restore
kubectl -n distributed-engine exec -i postgres-0 -- psql -U postgres distributed_engine < backup.sql
```

### Redis

Redis is configured with AOF persistence (`appendonly yes`, `appendfsync everysec`). Data loss window is at most 1 second. However, Redis is not the source of truth — if Redis data is lost, the outbox publisher will re-publish pending events from PostgreSQL.
