# Istio Ambient Mesh — Deployment & Rollout Guide

Staged rollout of Istio Ambient mode for the distributed task execution engine.

## What Istio Ambient Mode Adds

- **mTLS everywhere**: ztunnel encrypts all pod-to-pod traffic with mutual TLS. No sidecar injection required.
- **Workload identity**: Each ServiceAccount gets a SPIFFE identity (`cluster.local/ns/<ns>/sa/<sa-name>`) issued by Istio CA.
- **L4 authorization**: AuthorizationPolicies restrict which identities can reach which ports.
- **Gateway ingress**: Kubernetes Gateway API replaces direct LoadBalancer access to internal services.

## What This Does NOT Add

- Sidecars (ambient mode uses ztunnel, a per-node DaemonSet)
- Waypoint proxies (L7 policies, retries, circuit breaking — not in scope)
- HTTP-level routing between internal services
- Default-deny namespace policy (ALLOW policies implicitly deny non-matching requests)
- New inter-service communication patterns

## Prerequisites

- Kubernetes cluster (v1.28+ recommended for Gateway API support)
- Istio installed in ambient mode (`istioctl install --set profile=ambient`)
- Kubernetes Gateway API CRDs installed (`kubectl get crd gateways.gateway.networking.k8s.io`)
- `istioctl` on PATH (for verification)
- Existing engine deployment operational (PostgreSQL, Redis, API, Worker all healthy)

## Manifest Inventory

| File | Purpose |
|------|---------|
| `k8s/namespace.yaml` | Namespace with `istio.io/dataplane-mode: ambient` label |
| `k8s/istio/service-accounts.yaml` | ServiceAccounts for all workloads + Gateway |
| `k8s/istio/gateway.yaml` | Gateway (HTTP :80) + HTTPRoute → api:3000 |
| `k8s/istio/authz-postgres.yaml` | ALLOW policy: api-sa, worker-sa → postgres:5432 |
| `k8s/istio/authz-redis.yaml` | ALLOW policy: api-sa, worker-sa → redis:6379 |
| `k8s/istio/authz-api.yaml` | ALLOW policy: engine-gateway-istio → api:3000 |
| `k8s/istio/verify-runtime.sh` | Runtime verification script |

## Staged Rollout

### Stage 1 — Namespace & ServiceAccounts

**Prerequisites**: Istio ambient mode installed. Engine namespace may or may not exist yet.

**Apply**:
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/istio/service-accounts.yaml
```

**Expected behavior**: The namespace is created (or updated) with the `istio.io/dataplane-mode: ambient` label. Five ServiceAccounts are created. No traffic disruption — ztunnel enrolls existing pods but enforces no policy yet.

**Positive verification**:
```bash
kubectl get ns distributed-engine -o jsonpath='{.metadata.labels.istio\.io/dataplane-mode}'
# Expected: ambient

kubectl get sa -n distributed-engine
# Expected: api-sa, worker-sa, postgres-sa, redis-sa, engine-gateway-istio (plus default)
```

**Rollback**:
```bash
kubectl label ns distributed-engine istio.io/dataplane-mode-
kubectl delete -f k8s/istio/service-accounts.yaml
```

**GO/NO-GO**: Namespace label reads `ambient`. All five ServiceAccounts exist. Existing workloads remain healthy.

---

### Stage 2 — Workload SA Binding & Enrollment

**Prerequisites**: Stage 1 complete. Existing workloads running.

**Apply**:
```bash
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml
```

**Expected behavior**: Deployments and StatefulSets pick up `serviceAccountName` references. Pods roll to bind their ServiceAccounts. ztunnel enrolls the new pods and establishes mTLS between them.

**Positive verification**:
```bash
# SA assignments
kubectl get deploy api -n distributed-engine -o jsonpath='{.spec.template.spec.serviceAccountName}'
# Expected: api-sa

kubectl get deploy worker -n distributed-engine -o jsonpath='{.spec.template.spec.serviceAccountName}'
# Expected: worker-sa

kubectl get statefulset postgres -n distributed-engine -o jsonpath='{.spec.template.spec.serviceAccountName}'
# Expected: postgres-sa

kubectl get statefulset redis -n distributed-engine -o jsonpath='{.spec.template.spec.serviceAccountName}'
# Expected: redis-sa

# ztunnel enrollment (requires istioctl)
istioctl ztunnel-config workloads | grep distributed-engine

# Functional check: API health
kubectl exec -n distributed-engine deploy/api -- wget -qO- http://localhost:3000/health
# Expected: postgres=up, redis=up
```

**Rollback**: Revert `serviceAccountName` lines from the four workload manifests and re-apply.

**GO/NO-GO**: All pods are Running and Ready. API reports postgres=up and redis=up. ztunnel shows enrolled workloads.

Run automated enrollment verification:
```bash
./k8s/istio/verify-runtime.sh enrollment
```

---

### Stage 3 — Gateway

**Prerequisites**: Stage 2 complete. All workloads enrolled.

**Apply**:
```bash
kubectl apply -f k8s/istio/gateway.yaml
```

**Expected behavior**: Istio creates a Gateway pod using ServiceAccount `engine-gateway-istio`. An HTTPRoute forwards HTTP traffic from the Gateway to `api:3000`.

**Positive verification**:
```bash
# Gateway programmed
kubectl get gateway engine-gateway -n distributed-engine
# STATUS column should show "Programmed"

# Gateway pod identity (CRITICAL CHECK)
kubectl get pods -n distributed-engine -l istio.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].spec.serviceAccountName}'
# MUST be: engine-gateway-istio
# If different, update authz-api.yaml before proceeding to Stage 5.

# Gateway external access
kubectl get svc -n distributed-engine -l istio.io/gateway-name=engine-gateway
# Note the EXTERNAL-IP
```

**Negative verification**: N/A — Gateway has no authorization policy yet.

**Rollback**:
```bash
kubectl delete -f k8s/istio/gateway.yaml
```

**GO/NO-GO**: Gateway pod uses `engine-gateway-istio` ServiceAccount. Gateway is Programmed. External IP is assigned. The existing API LoadBalancer continues to work in parallel.

Run automated identity verification:
```bash
./k8s/istio/verify-runtime.sh gateway-identity
```

---

### Stage 4 — Data Store Authorization Policies

**Prerequisites**: Stage 3 complete. Gateway identity verified.

**Apply**:
```bash
kubectl apply -f k8s/istio/authz-postgres.yaml
kubectl apply -f k8s/istio/authz-redis.yaml
```

**Expected behavior**: PostgreSQL accepts connections only from api-sa and worker-sa on port 5432. Redis accepts connections only from api-sa and worker-sa on port 6379. All other identities are denied by ALLOW policy semantics (once an ALLOW policy applies, non-matching requests are implicitly denied).

**Positive verification**:
```bash
# API health (proves api-sa reaches both data stores)
kubectl exec -n distributed-engine deploy/api -- wget -qO- http://localhost:3000/health
# postgres=up, redis=up

# Worker connectivity (prove workers process tasks)
kubectl exec -n distributed-engine deploy/api -- \
  wget -qO- --post-data='{"name":"authz-test","priority":"NORMAL"}' \
  --header='Content-Type: application/json' http://localhost:3000/tasks
# Task should reach COMPLETED status
```

**Negative verification**:
```bash
# Unauthorized identity should be denied access to PostgreSQL
kubectl run pg-test -n distributed-engine \
  --image=postgres:16-alpine \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never \
  -- sh -c "timeout 5 pg_isready -h postgres -U postgres; echo EXIT_CODE:\$?"
# Expected: non-zero exit code (connection refused/timeout)

# Unauthorized identity should be denied access to Redis
kubectl run redis-test -n distributed-engine \
  --image=redis:7-alpine \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never \
  -- sh -c "timeout 5 redis-cli -h redis ping; echo EXIT_CODE:\$?"
# Expected: non-zero exit code (connection refused/timeout)
```

A negative test passes **only when** the destination is independently reachable by an authorized identity AND the unauthorized attempt is rejected.

**Rollback**:
```bash
kubectl delete -f k8s/istio/authz-postgres.yaml
kubectl delete -f k8s/istio/authz-redis.yaml
```

**GO/NO-GO**: API health shows postgres=up and redis=up. Unauthorized test pods cannot reach data stores. End-to-end task lifecycle works.

Run automated authorization verification:
```bash
./k8s/istio/verify-runtime.sh authz-positive
./k8s/istio/verify-runtime.sh authz-negative
```

---

### Stage 5 — API Authorization Policy

**Prerequisites**: Stage 4 complete. Stage 3 confirmed Gateway pod uses `engine-gateway-istio`. Data store policies verified.

**Apply**:
```bash
kubectl apply -f k8s/istio/authz-api.yaml
```

**Expected behavior**: API port 3000 accepts connections only from the Gateway's `engine-gateway-istio` identity. Kubelet probes continue to work — ztunnel rewrites kubelet probe packets to a link-local address (169.254.7.127), exempting them from policy enforcement.

**Positive verification**:
```bash
# Kubelet probes still pass (pods remain Ready)
kubectl get pods -n distributed-engine -l app=api
# All pods should be 1/1 Ready

# Gateway → API routing works
# Get Gateway external IP:
GW_IP=$(kubectl get svc -n distributed-engine -l istio.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
curl http://${GW_IP}/live
# Expected: {"success":true}
```

**Negative verification**:
```bash
# Unauthorized identity should be denied access to API
kubectl run api-test -n distributed-engine \
  --image=busybox:latest \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never \
  -- sh -c "timeout 5 wget -qO- http://api:3000/live; echo EXIT_CODE:\$?"
# Expected: non-zero exit code (connection refused/timeout)
```

**Rollback**:
```bash
kubectl delete -f k8s/istio/authz-api.yaml
```

**GO/NO-GO**: All API pods remain Ready. Gateway routes to API successfully. Unauthorized test pod cannot reach API. End-to-end task lifecycle still works via Gateway path.

---

### Stage 6 — LoadBalancer → ClusterIP Transition (Post-Verification)

**Prerequisites**: All Stages 1–5 complete and verified. Gateway path confirmed operational.

This stage transitions external traffic from the direct API LoadBalancer to the Gateway. It is a **controlled transition**, not part of the initial rollout.

**Steps**:

1. Verify Gateway path is operational:
   ```bash
   GW_IP=$(kubectl get svc -n distributed-engine -l istio.io/gateway-name=engine-gateway \
     -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
   curl http://${GW_IP}/health
   # Must return postgres=up, redis=up
   ```

2. Update DNS or load balancer to point to the Gateway external IP.

3. Verify traffic flows through Gateway.

4. Change API service from LoadBalancer to ClusterIP:
   ```yaml
   # In k8s/api.yaml, change:
   spec:
     type: ClusterIP    # was: LoadBalancer
   ```
   ```bash
   kubectl apply -f k8s/api.yaml
   ```

5. Verify no disruption:
   ```bash
   curl http://${GW_IP}/health
   curl http://${GW_IP}/tasks
   ```

**Rollback**: Change API service back to `type: LoadBalancer` and re-apply.

**GO/NO-GO**: All traffic flows through Gateway. Direct API LoadBalancer is no longer needed.

## Full Automated Verification

Run all stages:
```bash
./k8s/istio/verify-runtime.sh all
```

The script exits with code 1 if any check fails. Critical negative tests fail the suite if unauthorized access unexpectedly succeeds.

## AuthorizationPolicy Design Notes

### Why selector.matchLabels (not targetRef)

In Istio Ambient mode without waypoints, ztunnel enforces L4 policies. `targetRef` (GAMMA `targetRef` on Service) requires a waypoint proxy for enforcement. `selector.matchLabels` attaches the policy directly to pods, which ztunnel enforces.

### Why No Namespace-Wide Default Deny

ALLOW policy semantics in Istio: once any ALLOW policy applies to a workload (matched by selector), all requests to that workload that don't match an ALLOW rule are implicitly denied. A separate default-deny policy is unnecessary and adds operational risk.

### Headless Services

PostgreSQL and Redis use headless services (`clusterIP: None`). DNS resolves directly to pod IPs. The AuthorizationPolicies use `selector.matchLabels` which targets pods by their labels regardless of DNS resolution path — the policies work identically for headless and ClusterIP services.

### Gateway ServiceAccount

Istio's Kubernetes Gateway implementation creates a Deployment for each Gateway resource. The deployment uses a ServiceAccount named `<gateway-name>-istio` by convention. We pre-create the `engine-gateway-istio` SA so Istio adopts it. The Stage 3 verification gate confirms this before any policy references it.

If the Gateway pod uses a different SA name, the `authz-api.yaml` principal must be updated to match before applying the API authorization policy in Stage 5.

### Kubelet Probe Exemption

ztunnel rewrites kubelet probe packets to use source address 169.254.7.127, which bypasses AuthorizationPolicy evaluation. This means liveness and readiness probes continue to work even with ALLOW policies on the API pods.
