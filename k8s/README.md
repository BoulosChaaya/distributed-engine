# Kubernetes Deployment Guide

Production deployment of the distributed task execution engine on Kubernetes.

## Architecture

Three deployments working together:
1. **Redis** (StatefulSet) - Persistent state and message queue
2. **API** (Deployment, 3 replicas) - REST endpoints for task management
3. **Worker** (Deployment, 5 replicas) - Background job processors

## Prerequisites

- Kubernetes cluster (v1.24+)
- kubectl configured to access your cluster
- Docker images built and pushed to registry

## Building and Publishing Images

```bash
# Build API image
docker build -f Dockerfile.api -t your-registry/distributed-engine-api:0.1.0 .
docker push your-registry/distributed-engine-api:0.1.0

# Build Worker image
docker build -f Dockerfile.worker -t your-registry/distributed-engine-worker:0.1.0 .
docker push your-registry/distributed-engine-worker:0.1.0
```

Update image references in `api.yaml` and `worker.yaml` to match your registry.

## Deployment Steps

### 1. Deploy Redis (persistent data store)

```bash
kubectl apply -f k8s/redis.yaml
```

Wait for Redis to be ready:
```bash
kubectl wait --for=condition=ready pod -l app=redis -n distributed-engine --timeout=60s
```

Verify:
```bash
kubectl get statefulset -n distributed-engine
kubectl get svc -n distributed-engine
```

### 2. Deploy API Server

```bash
kubectl apply -f k8s/api.yaml
```

Verify deployment:
```bash
kubectl get pods -n distributed-engine -l app=api
kubectl logs -n distributed-engine -l app=api --tail=20 -f
```

Wait for readiness:
```bash
kubectl wait --for=condition=ready pod -l app=api -n distributed-engine --timeout=60s
```

### 3. Deploy Workers

```bash
kubectl apply -f k8s/worker.yaml
```

Verify:
```bash
kubectl get pods -n distributed-engine -l app=worker
```

## Checking System Health

```bash
# Check all pods are running
kubectl get pods -n distributed-engine

# Check service endpoints
kubectl get svc -n distributed-engine

# Test API health
kubectl port-forward -n distributed-engine svc/api 3000:80
curl http://localhost:3000/health

# Get API metrics
curl http://localhost:3000/metrics

# Check worker status
curl http://localhost:3000/workers
```

## Scaling

### Manual Scaling

```bash
# Scale API to 5 replicas
kubectl scale deployment api -n distributed-engine --replicas=5

# Scale workers to 10 replicas
kubectl scale deployment worker -n distributed-engine --replicas=10
```

### Automatic Scaling (via HorizontalPodAutoscaler)

The deployments include HPA that automatically scales based on:
- CPU utilization (target: 70-75%)
- Memory utilization (target: 80%)

Check autoscaling status:
```bash
kubectl get hpa -n distributed-engine
kubectl describe hpa api-hpa -n distributed-engine
kubectl describe hpa worker-hpa -n distributed-engine
```

## Rolling Updates (Zero-Downtime Deployments)

Update image and redeploy:

```bash
# Update API image
kubectl set image deployment/api api=your-registry/distributed-engine-api:0.2.0 -n distributed-engine

# Watch rollout progress
kubectl rollout status deployment/api -n distributed-engine

# Verify
kubectl get pods -n distributed-engine -l app=api
```

The `RollingUpdate` strategy ensures:
- maxUnavailable: 0 (always have available pods)
- maxSurge: 1 (one extra pod during update)
- Graceful shutdown timeout: 30s for API, 25s for workers

## Monitoring

### Resource Usage

```bash
# Check current resource usage
kubectl top pods -n distributed-engine
kubectl top nodes
```

### Pod Logs

```bash
# Stream API logs
kubectl logs -f -n distributed-engine -l app=api

# Stream worker logs
kubectl logs -f -n distributed-engine -l app=worker

# Specific pod logs
kubectl logs -f -n distributed-engine pod/api-abc123-xyz789
```

### Events

```bash
# Watch cluster events
kubectl get events -n distributed-engine --sort-by='.lastTimestamp'
```

## Troubleshooting

### Pod stuck in Pending

```bash
kubectl describe pod <pod-name> -n distributed-engine
# Check node resources, PVC availability, image pull errors
```

### Pod keeps restarting

```bash
# Check logs for errors
kubectl logs <pod-name> -n distributed-engine --previous

# Check readiness/liveness probe configuration
kubectl describe pod <pod-name> -n distributed-engine
```

### Connection issues to Redis

```bash
# Verify Redis is running
kubectl get statefulset redis -n distributed-engine
kubectl logs redis-0 -n distributed-engine

# Test connection from API pod
kubectl exec -it <api-pod-name> -n distributed-engine -- \
  redis-cli -h redis.distributed-engine.svc.cluster.local ping
```

## Cleanup

```bash
# Delete entire namespace (all resources)
kubectl delete namespace distributed-engine

# Or delete individual resources
kubectl delete deployment api -n distributed-engine
kubectl delete deployment worker -n distributed-engine
kubectl delete statefulset redis -n distributed-engine
kubectl delete svc api redis -n distributed-engine
kubectl delete pvc -l app=redis -n distributed-engine
```

## Production Hardening Checklist

- [ ] Images pushed to private registry with authentication
- [ ] Resource requests/limits tuned for your workload
- [ ] Persistent volume configured for Redis with backup strategy
- [ ] Network policies set up for pod-to-pod communication
- [ ] RBAC roles configured for least privilege
- [ ] Monitoring and alerting configured (Prometheus, DataDog, etc.)
- [ ] Log aggregation set up (ELK, Splunk, Loki, etc.)
- [ ] Secret management for Redis password (use Sealed Secrets or HashiCorp Vault)
- [ ] Ingress controller configured for external API access
- [ ] DNS entries created for API endpoints

## References

- [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
- [Horizontal Pod Autoscaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Pod Disruption Budgets](https://kubernetes.io/docs/tasks/run-application/configure-pdb/)
- [Health Checks](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
