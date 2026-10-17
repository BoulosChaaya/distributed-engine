# Istio Ambient Mesh — Deployment & Rollout Guide

Staged rollout of Istio Ambient mode for the distributed task execution engine.

## What Istio Ambient Mode Adds

- **mTLS everywhere**: ztunnel encrypts all pod-to-pod traffic with mutual TLS. No sidecar injection required.
- **Workload identity**: Each ServiceAccount gets a SPIFFE identity (`cluster.local/ns/<ns>/sa/<sa-name>`) issued by Istio CA.
- **L4 authorization**: AuthorizationPolicies restrict which identities can reach which ports.
- **Gateway ingress**: Kubernetes Gateway API provides mesh-aware ingress alongside the existing LoadBalancer.

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
- `istioctl` on PATH (for mTLS and ztunnel diagnostics)
- Existing engine deployment operational (PostgreSQL, Redis, API, Worker all healthy)

## Manifest Inventory

| File | Purpose |
|------|---------|
| `k8s/namespace.yaml` | Namespace with `istio.io/dataplane-mode: ambient` label |
| `k8s/istio/service-accounts.yaml` | ServiceAccounts for all workloads + Gateway |
| `k8s/istio/gateway.yaml` | Gateway (HTTP :80) + HTTPRoute → `api` Service port 80 (→ container 3000) |
| `k8s/istio/authz-postgres.yaml` | ALLOW policy: api-sa, worker-sa → postgres:5432 |
| `k8s/istio/authz-redis.yaml` | ALLOW policy: api-sa, worker-sa → redis:6379 |
| `k8s/istio/authz-api.yaml` | ALLOW policy: engine-gateway-istio and web-sa (dashboard) → api:3000 |
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

# mTLS identity verification (requires istioctl)
istioctl ztunnel-config certificates | grep distributed-engine

# Component diagnostic: API health (API-local, not Gateway path)
kubectl exec -n distributed-engine deploy/api -- wget -qO- http://localhost:3000/health
# Expected: postgres=up, redis=up
```

**Rollback**: Revert `serviceAccountName` lines from the four workload manifests and re-apply.

**GO/NO-GO**: All pods are Running and Ready. API-local health reports postgres=up and redis=up. ztunnel shows enrolled workloads and active certificates.

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

**Expected behavior**: Istio creates a Gateway Deployment and Service. The generated resources are named `engine-gateway-istio` (convention: `<gateway-name>-<gateway-class-name>`). The Gateway pod uses the `engine-gateway-istio` ServiceAccount. An HTTPRoute forwards HTTP traffic from the Gateway to the `api` Service on port 80. A Gateway API backendRef names the *Service* port; pointing it at 3000 (the container port) never routes.

**Positive verification**:
```bash
# Gateway programmed
kubectl get gateway engine-gateway -n distributed-engine
# STATUS column should show "Programmed"

# Gateway pod identity (CRITICAL CHECK)
# Current Istio (1.22+) uses the standard Gateway API label:
kubectl get pods -n distributed-engine \
  -l gateway.networking.k8s.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].spec.serviceAccountName}'
# If no pods found, try the legacy label:
kubectl get pods -n distributed-engine \
  -l istio.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].spec.serviceAccountName}'
# MUST be: engine-gateway-istio
# If the SA does not match, DO NOT proceed to Stage 5.
# See "Gateway ServiceAccount" below for resolution.

# To see all labels on Gateway pods:
kubectl get pods -n distributed-engine --show-labels | grep gateway

# Gateway Service external access
# Current label:
kubectl get svc -n distributed-engine \
  -l gateway.networking.k8s.io/gateway-name=engine-gateway
# Legacy label:
kubectl get svc -n distributed-engine \
  -l istio.io/gateway-name=engine-gateway
# Note the EXTERNAL-IP or HOSTNAME
```

**Negative verification**: N/A — Gateway has no authorization policy yet.

**Rollback**:
```bash
kubectl delete -f k8s/istio/gateway.yaml
```

**GO/NO-GO**: Gateway pod uses `engine-gateway-istio` ServiceAccount. Gateway is Programmed. External address is assigned. The existing API LoadBalancer continues to work in parallel.

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

**Positive verification** (component diagnostics — API-local health):
```bash
# API-local health check (proves api-sa reaches both data stores)
kubectl exec -n distributed-engine deploy/api -- wget -qO- http://localhost:3000/health
# Expected: postgres=up, redis=up

# Submit a task to verify Worker → PG and Worker → Redis paths
# (Schema: name is required; payload, priority, maxRetries are optional)
KEY=$(kubectl -n distributed-engine get secret operator-secret -o jsonpath='{.data.api-key}' | base64 -d)
kubectl exec -n distributed-engine deploy/api -- \
  wget -qO- --post-data='{"name":"echo","payload":{"from":"authz-verify"}}' \
  --header='Content-Type: application/json' --header="Authorization: Bearer $KEY" \
  http://localhost:3000/tasks
# Task should reach COMPLETED status
```

**Negative verification**:

First, create the test ServiceAccount used by negative tests:
```bash
kubectl create sa test-unauthorized -n distributed-engine
```

Then run the unauthorized access tests:
```bash
# Unauthorized identity → PostgreSQL (should be denied)
kubectl run pg-test -n distributed-engine \
  --image=postgres:16-alpine \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never --timeout=20s \
  -- sh -c "timeout 5 pg_isready -h postgres -U postgres; echo EXIT_CODE:\$?"
# Expected: non-zero EXIT_CODE (connection refused/timeout)

# Unauthorized identity → Redis (should be denied)
kubectl run redis-test -n distributed-engine \
  --image=redis:7-alpine \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never --timeout=20s \
  -- sh -c "timeout 5 redis-cli -h redis ping; echo EXIT_CODE:\$?"
# Expected: non-zero EXIT_CODE (connection refused/timeout)
```

Clean up the test ServiceAccount:
```bash
kubectl delete sa test-unauthorized -n distributed-engine
```

A negative test passes **only when** the destination is independently reachable by an authorized identity AND the unauthorized attempt is rejected. If the destination itself is unreachable by authorized identities, the negative test is inconclusive.

**Rollback**:
```bash
kubectl delete -f k8s/istio/authz-postgres.yaml
kubectl delete -f k8s/istio/authz-redis.yaml
```

**GO/NO-GO**: API-local health shows postgres=up and redis=up. Unauthorized test pods cannot reach data stores. End-to-end task lifecycle works.

Run automated authorization verification:
```bash
./k8s/istio/verify-runtime.sh authz-positive
./k8s/istio/verify-runtime.sh authz-negative
```

---

### Stage 5 — API Authorization Policy

**Prerequisites**: Stage 4 complete. **Stage 3 MUST have confirmed the Gateway pod uses `engine-gateway-istio`**. If the Gateway pod uses a different ServiceAccount, DO NOT apply this policy — see "Gateway ServiceAccount" below.

**Apply**:
```bash
kubectl apply -f k8s/istio/authz-api.yaml
```

**Expected behavior**: API port 3000 accepts connections only from the Gateway's `engine-gateway-istio` identity and the dashboard's `web-sa`. Kubelet probes continue to work — ztunnel rewrites kubelet probe packets to a link-local address (169.254.7.127), exempting them from policy enforcement.

**Positive verification** (Gateway-path tests):
```bash
# Kubelet probes still pass (pods remain Ready)
kubectl get pods -n distributed-engine -l app=api
# All pods should be 1/1 Ready

# Gateway → API routing works (authorized Gateway identity)
# Resolve Gateway address:
GW_ADDR=$(kubectl get svc -n distributed-engine \
  -l gateway.networking.k8s.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')
# If empty, try hostname:
GW_ADDR=${GW_ADDR:-$(kubectl get svc -n distributed-engine \
  -l gateway.networking.k8s.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}')}
# If still empty, try legacy label:
GW_ADDR=${GW_ADDR:-$(kubectl get svc -n distributed-engine \
  -l istio.io/gateway-name=engine-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}')}

curl http://${GW_ADDR}/live
# Expected: {"success":true}
```

**Negative verification**:
```bash
# First establish positive control: Gateway → API succeeds
curl http://${GW_ADDR}/live
# Must return {"success":true} before proceeding

# Then: unauthorized identity should be denied
kubectl create sa test-unauthorized -n distributed-engine
kubectl run api-test -n distributed-engine \
  --image=busybox:1.37 \
  --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
  --rm -i --restart=Never --timeout=20s \
  -- sh -c "timeout 5 wget -qO- http://api/live; echo EXIT_CODE:\$?"
# Expected: non-zero EXIT_CODE (connection refused/timeout)
kubectl delete sa test-unauthorized -n distributed-engine
```

Use the Service port (`http://api/live`, port 80). A port the Service does not expose fails whether or not the policy works, which would make the test meaningless. The API negative test establishes a valid control: the authorized Gateway identity succeeds, then the unauthorized workload identity is denied.

**Rollback**:
```bash
kubectl delete -f k8s/istio/authz-api.yaml
```

**GO/NO-GO**: All API pods remain Ready. Gateway routes to API successfully. Unauthorized test pod cannot reach API. End-to-end task lifecycle works via the Gateway path.

---

### Stage 6 — LoadBalancer → ClusterIP Transition (Post-Verification)

**Prerequisites**: All Stages 1–5 complete and verified. Gateway path confirmed operational.

This stage transitions external traffic from the direct API LoadBalancer to the Gateway. It is a **controlled transition**, not part of the initial rollout.

**Steps**:

1. Verify Gateway path is operational:
   ```bash
   curl http://${GW_ADDR}/health
   # Must return postgres=up, redis=up
   ```

2. Update DNS or load balancer to point to the Gateway external address.

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
   curl http://${GW_ADDR}/health
   curl -H "Authorization: Bearer $KEY" http://${GW_ADDR}/tasks
   ```

**Rollback**: Change API service back to `type: LoadBalancer` and re-apply.

**GO/NO-GO**: All traffic flows through Gateway. Direct API LoadBalancer is no longer needed.

## Full Automated Verification

Run all stages (the e2e stage submits a task with the operator key):
```bash
OPERATOR_API_KEY=$KEY GW_ADDR=<gateway address> ./k8s/istio/verify-runtime.sh all
```

This exact run happens on every pull request (`.github/workflows/full-stack.yml`,
job `kind-istio`, via `scripts/k8s-kind-e2e.sh`) on kind with Istio 1.30
ambient, and must end in RUNTIME ACCEPTANCE PASS.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | **RUNTIME ACCEPTANCE PASS** — all critical checks ran and passed |
| 1 | **RUNTIME ACCEPTANCE FAIL** — one or more checks failed |
| 2 | **RUNTIME ACCEPTANCE INCOMPLETE** — no failures, but critical checks were skipped due to missing infrastructure |

The script distinguishes between non-critical skips (informational) and critical skips (checks that must pass for runtime acceptance). A skipped critical check prevents the suite from reporting full PASS.

### Overriding Gateway Address

If the Gateway LoadBalancer is not directly reachable from the verification runner (e.g., in a private cluster), use a port-forward:

```bash
# In one terminal:
kubectl -n distributed-engine port-forward svc/$(kubectl get svc -n distributed-engine \
  -l gateway.networking.k8s.io/gateway-name=engine-gateway -o name | head -1) 8080:80

# In another terminal:
GW_ADDR=localhost:8080 OPERATOR_API_KEY=$KEY ./k8s/istio/verify-runtime.sh e2e
```

The `GW_ADDR` environment variable overrides the auto-detected address. The request still traverses the Gateway proxy and HTTPRoute.

## AuthorizationPolicy Design Notes

### Why selector.matchLabels (not targetRef)

In Istio Ambient mode without waypoints, ztunnel enforces L4 policies. `targetRef` (GAMMA `targetRef` on Service) requires a waypoint proxy for enforcement. `selector.matchLabels` attaches the policy directly to pods, which ztunnel enforces.

### Why No Namespace-Wide Default Deny

ALLOW policy semantics in Istio: once any ALLOW policy applies to a workload (matched by selector), all requests to that workload that don't match an ALLOW rule are implicitly denied. A separate default-deny policy is unnecessary and adds operational risk.

### Headless Services

PostgreSQL and Redis use headless services (`clusterIP: None`). DNS resolves directly to pod IPs. The AuthorizationPolicies use `selector.matchLabels` which targets pods by their labels regardless of DNS resolution path — the policies work identically for headless and ClusterIP services.

### Gateway ServiceAccount

Istio's Kubernetes Gateway implementation creates a Deployment, Service, ServiceAccount, and related resources for each Gateway. Generated resources are named `<gateway-name>-<gateway-class-name>` (e.g., `engine-gateway-istio` for a Gateway named `engine-gateway` with `gatewayClassName: istio`).

We pre-create the `engine-gateway-istio` ServiceAccount in `service-accounts.yaml` so it exists before the Gateway is applied. Istio's controller is expected to use this existing SA rather than fail on a naming collision. However, this pre-creation behavior is not explicitly documented as a guaranteed adoption mechanism in Istio's Gateway API documentation. The supported way to control Gateway infrastructure (including the ServiceAccount) is through `spec.infrastructure.parametersRef` on the Gateway resource, referencing a ConfigMap with overlay values.

**The critical safeguard is runtime verification, not static YAML alone.** The Stage 3 identity check (`verify-runtime.sh gateway-identity`) confirms at runtime that the actual Gateway pod uses the expected ServiceAccount before any authorization policy references it. If there is a mismatch:

- **Do NOT apply `authz-api.yaml`** — the policy would reference the wrong identity.
- **Do NOT automatically widen the policy** (no wildcards, no namespace-wide allowances).
- **Report the mismatch** and stop Stage 5.
- **Reconcile deliberately**: either update `authz-api.yaml` to match the actual SA, or configure the Gateway to use the expected SA via `parametersRef`.

### Kubelet Probe Exemption

ztunnel rewrites kubelet probe packets to use source address 169.254.7.127, which bypasses AuthorizationPolicy evaluation. This means liveness and readiness probes continue to work even with ALLOW policies on the API pods.

### Gateway Service Labels

Istio Gateway-generated resources carry the label `gateway.networking.k8s.io/gateway-name=<name>` (current Istio 1.22+). Older Istio versions used `istio.io/gateway-name=<name>`. The verification script and this documentation try both labels for compatibility. If neither matches, inspect the generated resources with `kubectl get pods,svc -n distributed-engine --show-labels`.
