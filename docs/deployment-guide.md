# Deployment Guide

Operational deployment procedures for the distributed-engine Kubernetes deployment.

## Prerequisites

- Kubernetes cluster with `kubectl` configured
- Access to GHCR (`ghcr.io/bouloschaaya/distributed-engine-*`)
- `distributed-engine` namespace exists (`kubectl apply -f k8s/namespace.yaml`)
- Secrets provisioned (see Secret Provisioning below)
- Istio ambient mesh verified if transitioning API to ClusterIP (Topic 16 gate)

## Secret Provisioning

### PostgreSQL

```bash
kubectl create secret generic postgres-secret \
  --namespace distributed-engine \
  --from-literal=password='<POSTGRES_PASSWORD>'
```

### Redis

```bash
kubectl create secret generic redis-secret \
  --namespace distributed-engine \
  --from-literal=password='<REDIS_PASSWORD>'
```

Verify secrets exist before deploying workloads:

```bash
kubectl get secrets -n distributed-engine postgres-secret redis-secret
```

## Image Publication

Images are published by CI on merge to `main`. Each image is tagged with the full
Git commit SHA:

```
ghcr.io/bouloschaaya/distributed-engine-api:<full-git-sha>
ghcr.io/bouloschaaya/distributed-engine-worker:<full-git-sha>
```

No `latest` tag is published. Each deployment references a specific commit SHA.

## Manual Deployment

### 1. Identify the target commit

```bash
git log --oneline -1 main
```

Record the full SHA:

```bash
DEPLOY_SHA=$(git rev-parse main)
echo "Deploying: $DEPLOY_SHA"
```

### 2. Update manifests with the target image

Edit both `k8s/api.yaml` and `k8s/worker.yaml`:

```bash
# api.yaml
image: ghcr.io/bouloschaaya/distributed-engine-api:${DEPLOY_SHA}

# worker.yaml
image: ghcr.io/bouloschaaya/distributed-engine-worker:${DEPLOY_SHA}
```

API and Worker from one commit are **one release unit**. Both must reference
the same commit SHA.

### 3. Apply manifests

```bash
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml
```

### 4. Wait for rollout

```bash
kubectl rollout status deployment/api -n distributed-engine --timeout=120s
kubectl rollout status deployment/worker -n distributed-engine --timeout=120s
```

### 5. Runtime image identity verification

After pods are running, verify the actual pulled image matches the intended
deployment:

```bash
# Record the expected OCI digest from GHCR push output or:
# docker inspect --format='{{index .RepoDigests 0}}' ghcr.io/.../api:<sha>

# Verify API pods
kubectl get pods -n distributed-engine -l app=api \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}'

# Verify Worker pods
kubectl get pods -n distributed-engine -l app=worker \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

The pod `imageID` digest must match the OCI digest published to GHCR for the
deployed Git SHA. The Git SHA tag is a mutable registry pointer; the OCI digest
is the immutable content-addressed identity.

A mismatch is an acceptance failure requiring investigation — do not change the
expected digest to match an unexpected runtime image.

## Post-Deployment Validation

```bash
# PostgreSQL is ready
kubectl exec -n distributed-engine postgres-0 -- pg_isready -U postgres

# Redis accepts authenticated connections
kubectl exec -n distributed-engine redis-0 -- redis-cli ping

# Redis rejects unauthenticated connections
kubectl exec -n distributed-engine redis-0 -- env -u REDISCLI_AUTH redis-cli ping
# Expected: NOAUTH error

# API responds
kubectl exec -n distributed-engine deploy/api -- wget -qO- http://localhost:3000/live

# Worker health
kubectl exec -n distributed-engine deploy/worker -- wget -qO- http://localhost:9000/healthz

# All worker pods pass probes
kubectl get pods -n distributed-engine -l app=worker
# All should show READY 1/1

# PostgreSQL max_connections
kubectl exec -n distributed-engine postgres-0 -- \
  psql -U postgres -c "SHOW max_connections;"
# Expected: 150
```

## Rollback

### Method 1: Explicit SHA rollback (preferred)

Identify the previous known-good SHA and its verified OCI digest:

```bash
ROLLBACK_SHA=<previous-known-good-sha>
```

Update both `k8s/api.yaml` and `k8s/worker.yaml` to the rollback SHA, then:

```bash
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml
kubectl rollout status deployment/api -n distributed-engine --timeout=120s
kubectl rollout status deployment/worker -n distributed-engine --timeout=120s
```

Verify both deployments show the intended previous image SHA/digest.
API and Worker must be rolled back together — the system must not run
mismatched versions.

### Method 2: kubectl rollout undo (emergency)

```bash
kubectl rollout undo deployment/api -n distributed-engine
kubectl rollout undo deployment/worker -n distributed-engine
```

This uses Kubernetes revision history. It is acceptable as an emergency shortcut
but the explicit SHA method is stronger because it is deterministic and auditable.

After rollback: verify both deployments show the intended previous image
SHA/digest using the image identity verification commands above.

## Failure Gates

- Secrets must exist before workload deployment
- Both API and Worker must reference the same commit SHA
- Pod `imageID` digest must match GHCR published digest
- `verify-runtime.sh all` exit 0 required before API Service type change to ClusterIP (Topic 16 gate)
