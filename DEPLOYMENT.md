# Production Deployment & Operations Runbook

Complete guide for deploying and operating the distributed task execution engine in production.

## Table of Contents

1. [Pre-Deployment Checklist](#pre-deployment-checklist)
2. [Local Development Setup](#local-development-setup)
3. [Docker Build & Registry](#docker-build--registry)
4. [Kubernetes Deployment](#kubernetes-deployment)
5. [Verification & Testing](#verification--testing)
6. [Monitoring & Alerting](#monitoring--alerting)
7. [Incident Response](#incident-response)
8. [Scaling Operations](#scaling-operations)
9. [Backup & Disaster Recovery](#backup--disaster-recovery)

## Pre-Deployment Checklist

### Code Quality
- [ ] All tests passing: `npm test`
- [ ] Coverage above 80%: `npm run test:coverage`
- [ ] No TypeScript errors: `npm run build`
- [ ] Code reviewed and approved
- [ ] Commits signed: `git log --show-signature`

### Configuration
- [ ] `.env.production` created with all required variables
- [ ] Redis password configured (not default)
- [ ] API port not conflicting with other services
- [ ] Log level set to INFO (not DEBUG in prod)
- [ ] Graceful shutdown timeout appropriate for your workload

### Infrastructure
- [ ] Kubernetes cluster ready (nodes healthy)
- [ ] Persistent volume available for Redis (10Gi minimum)
- [ ] Docker registry access configured
- [ ] Load balancer configured
- [ ] DNS entries created for API endpoints

### Security
- [ ] Container images scanned for vulnerabilities
- [ ] Secrets not in code repository
- [ ] Network policies defined
- [ ] RBAC roles configured
- [ ] TLS certificates obtained (for HTTPS)

## Local Development Setup

### Requirements

- Node.js 20+
- Docker & Docker Compose
- kubectl 1.24+
- pnpm 8+

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/distributed-engine.git
cd distributed-engine

# Install dependencies
pnpm install

# Start local development
docker-compose up -d

# Run API
pnpm -F @distributed-engine/api run dev

# Run worker (in another terminal)
pnpm -F @distributed-engine/worker run dev

# Run tests
npm test
```

### Verifying Local Setup

```bash
# Health check
curl http://localhost:3000/health

# Submit a task
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"name":"test-task","priority":"HIGH"}'

# Check system metrics
curl http://localhost:3000/metrics

# View Redis data
docker exec engine-redis redis-cli
> KEYS *
> GET worker:*
```

## Docker Build & Registry

### Building Images

```bash
# Build API image
docker build -f Dockerfile.api \
  -t your-registry.com/distributed-engine-api:0.2.0 \
  -t your-registry.com/distributed-engine-api:latest \
  .

# Build worker image
docker build -f Dockerfile.worker \
  -t your-registry.com/distributed-engine-worker:0.2.0 \
  -t your-registry.com/distributed-engine-worker:latest \
  .

# Verify image size
docker image ls | grep distributed-engine
```

### Publishing to Registry

```bash
# Login to registry
docker login your-registry.com

# Push images
docker push your-registry.com/distributed-engine-api:0.2.0
docker push your-registry.com/distributed-engine-api:latest
docker push your-registry.com/distributed-engine-worker:0.2.0
docker push your-registry.com/distributed-engine-worker:latest

# Verify push
docker inspect your-registry.com/distributed-engine-api:0.2.0
```

### Image Versioning Strategy

- **Semver tags** (0.2.0): Production releases, pinned in deployment
- **Latest tag**: Always points to newest image
- **Branch tags** (main-abc1234): Development builds

Never update production deployments with 'latest' tag. Always use specific version.

## Kubernetes Deployment

### Prerequisites

```bash
# Verify cluster access
kubectl cluster-info
kubectl get nodes

# Verify context
kubectl config current-context

# Create namespace (if not exists)
kubectl create namespace distributed-engine --dry-run=client -o yaml | kubectl apply -f -
```

### Step 1: Deploy Redis

```bash
# Deploy
kubectl apply -f k8s/redis.yaml

# Wait for readiness
kubectl wait --for=condition=ready pod -l app=redis \
  -n distributed-engine --timeout=120s

# Verify
kubectl get statefulset -n distributed-engine
kubectl describe pvc -n distributed-engine

# Test connectivity
kubectl run -it --rm debug --image=redis:7-alpine \
  --restart=Never -n distributed-engine -- \
  redis-cli -h redis.distributed-engine.svc.cluster.local ping
```

### Step 2: Update Image References

Edit `k8s/api.yaml` and `k8s/worker.yaml`:

```yaml
# k8s/api.yaml
containers:
- name: api
  image: your-registry.com/distributed-engine-api:0.2.0  # Update this
```

### Step 3: Deploy API

```bash
# Deploy
kubectl apply -f k8s/api.yaml

# Watch rollout
kubectl rollout status deployment/api -n distributed-engine --timeout=300s

# Verify
kubectl get pods -n distributed-engine -l app=api
kubectl logs -f -n distributed-engine -l app=api

# Test health check
kubectl port-forward -n distributed-engine svc/api 3000:80 &
curl http://localhost:3000/health
```

### Step 4: Deploy Workers

```bash
# Deploy
kubectl apply -f k8s/worker.yaml

# Verify
kubectl get pods -n distributed-engine -l app=worker
kubectl logs -f -n distributed-engine -l app=worker

# Check heartbeat status
kubectl exec -it <api-pod-name> -n distributed-engine -- \
  curl http://localhost:3000/workers
```

## Verification & Testing

### Post-Deployment Tests

```bash
# 1. API Health
kubectl get pods -n distributed-engine -l app=api
kubectl logs -n distributed-engine -l app=api | tail -20

# 2. Worker Health
kubectl get pods -n distributed-engine -l app=worker
kubectl logs -n distributed-engine -l app=worker | tail -20

# 3. Redis Connectivity
kubectl exec redis-0 -n distributed-engine -- redis-cli info server

# 4. Metrics Collection
PODS=$(kubectl get pods -n distributed-engine -l app=api -o name | head -1)
kubectl port-forward $PODS 3000:3000 &
curl http://localhost:3000/metrics

# 5. Task Submission
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-task",
    "payload": {"test": "data"},
    "priority": "HIGH"
  }'
```

### Load Testing

```bash
# Install load testing tool
go install github.com/rakyll/hey@latest

# Run load test
hey -n 1000 -c 50 \
  -m POST \
  -H "Content-Type: application/json" \
  -d '{"name":"load-test","priority":"NORMAL"}' \
  http://api-endpoint:3000/tasks

# Expected: <200ms p95 latency, <1% error rate
```

## Monitoring & Alerting

### Prometheus Metrics (if configured)

```bash
# Metrics scraped from /metrics endpoint every 30s

# Key metrics to watch:
# - http_requests_total: Total API requests
# - http_request_errors: Request errors
# - tasks_total: Total tasks in system
# - queue_depth: Jobs waiting in queue
# - circuit_breaker_status: 0=CLOSED, 1=OPEN
```

### Key Dashboards to Create

1. **System Health**
   - API uptime (SLA target: 99.9%)
   - Worker availability
   - Redis connection status

2. **Performance**
   - Request latency (p50, p95, p99)
   - Task processing time
   - Queue depth and job processing rate

3. **Errors**
   - HTTP error rates by code (4xx, 5xx)
   - Circuit breaker open events
   - Worker job failure rate

### Alerting Rules

```yaml
# Alert if API is down (no healthy pods)
- alert: APIDown
  expr: count(up{job="api"}) == 0
  for: 5m
  severity: critical

# Alert if queue is backing up (>1000 jobs waiting)
- alert: QueueBacklog
  expr: queue_depth > 1000
  for: 10m
  severity: warning

# Alert if error rate is high (>5%)
- alert: HighErrorRate
  expr: (http_request_errors / http_requests_total) > 0.05
  for: 5m
  severity: warning
```

## Incident Response

### Scenario: High Latency

**Symptoms**: API responses slow, users complaining

**Diagnosis**:
```bash
# 1. Check API logs
kubectl logs -f -n distributed-engine -l app=api | grep ERROR

# 2. Check resource usage
kubectl top pods -n distributed-engine -l app=api

# 3. Check Redis connectivity
kubectl exec redis-0 -n distributed-engine -- redis-cli info stats

# 4. Check queue depth
curl http://api:3000/metrics | jq .data.queue

# 5. Check worker status
curl http://api:3000/workers
```

**Resolution**:
- If CPU high: scale up replicas
- If memory high: increase resource limits
- If queue backing up: scale workers
- If Redis slow: check persistent volume I/O

### Scenario: Circuit Breaker Open

**Symptoms**: /health returns 503, new tasks not enqueueing

**Diagnosis**:
```bash
# Check circuit breaker state
curl http://api:3000/health | jq .data.circuitBreaker

# Check Redis connectivity from pod
kubectl exec -it <api-pod> -n distributed-engine -- \
  redis-cli -h redis.distributed-engine.svc.cluster.local ping
```

**Resolution**:
- Circuit breaker auto-recovers after 30s (see config.ts)
- If persists: check Redis pod status
- If Redis down: check persistent volume mount
- Manual recovery: Restart API pods (fresh circuit breaker state)

### Scenario: Worker Jobs Failing

**Symptoms**: High task.FAILED count, queue not draining

**Diagnosis**:
```bash
# Check worker logs for error patterns
kubectl logs -f -n distributed-engine -l app=worker | grep "Task failed"

# Check specific job details
TASK_ID="abc123"  # from API logs
curl http://api:3000/tasks/$TASK_ID | jq .data.error
```

**Resolution**:
- If application error: fix business logic, redeploy
- If transient error: jobs auto-retry (configurable in config.ts)
- If resource exhaustion: increase worker resource limits
- If deadlock: scale down and restart workers

## Scaling Operations

### Manual Scaling

```bash
# Scale API to handle more traffic
kubectl scale deployment api -n distributed-engine --replicas=10

# Scale workers to process faster
kubectl scale deployment worker -n distributed-engine --replicas=15

# Verify
kubectl get hpa -n distributed-engine  # Show current HPA status
```

### Automatic Scaling (HPA)

HPA configuration already in place. Customize for your workload:

```yaml
# Edit k8s/api.yaml
metrics:
- type: Resource
  resource:
    name: cpu
    target:
      type: Utilization
      averageUtilization: 70  # Increase for more aggressive scaling
```

## Backup & Disaster Recovery

### Redis Persistence

Redis is configured with AOF (Append-Only File):

```bash
# Verify AOF enabled
kubectl exec redis-0 -n distributed-engine -- redis-cli CONFIG GET appendonly

# AOF file location: /data/appendonly.aof on persistent volume
```

### Backup Strategy

```bash
# Manual backup
kubectl exec redis-0 -n distributed-engine -- redis-cli BGSAVE

# Automated backup (via volume snapshots)
# Configure your cloud provider's volume snapshot scheduler
```

### Recovery from Backup

```bash
# 1. Stop API and workers
kubectl scale deployment api -n distributed-engine --replicas=0
kubectl scale deployment worker -n distributed-engine --replicas=0

# 2. Restore volume from snapshot
# (varies by cloud provider)

# 3. Verify Redis data
kubectl exec redis-0 -n distributed-engine -- redis-cli DBSIZE

# 4. Restart services
kubectl scale deployment api -n distributed-engine --replicas=3
kubectl scale deployment worker -n distributed-engine --replicas=5
```

## Rollback Procedure

If new version causes issues:

```bash
# 1. Identify previous working version
kubectl rollout history deployment/api -n distributed-engine

# 2. Rollback
kubectl rollout undo deployment/api -n distributed-engine --to-revision=1

# 3. Verify
kubectl rollout status deployment/api -n distributed-engine

# 4. Check logs
kubectl logs -f -n distributed-engine -l app=api
```

## Support & Escalation

For production issues:

1. **Tier 1**: Check dashboards, run diagnostics
2. **Tier 2**: Check logs, restart services if needed
3. **Tier 3**: Scale resources, investigate code issues
4. **Tier 4**: Consult architecture, plan larger changes

Document all incidents in post-mortems for continuous improvement.
