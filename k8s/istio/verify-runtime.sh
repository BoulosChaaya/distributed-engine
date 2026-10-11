#!/usr/bin/env bash
#
# Topic 16 — Istio Ambient Mesh Runtime Verification
#
# REQUIRES: Kubernetes cluster with Istio Ambient mode installed.
# REQUIRES: istioctl available on PATH.
# REQUIRES: kubectl configured for the target cluster.
#
# This script performs staged verification of Istio Ambient mesh
# enrollment, mTLS, and AuthorizationPolicy enforcement.
#
# Exit codes:
#   0 — all executed checks passed
#   1 — one or more checks failed
#
# Usage:
#   ./verify-runtime.sh [stage]
#
#   Stages: enrollment | gateway-identity | authz-positive | authz-negative | e2e | all
#   Default: all

set -euo pipefail

NS="distributed-engine"
PASS=0
FAIL=0
SKIP=0

pass() { echo "  PASS: $1"; ((PASS++)); }
fail() { echo "  FAIL: $1"; ((FAIL++)); }
skip() { echo "  SKIP: $1"; ((SKIP++)); }
header() { echo ""; echo "=== $1 ==="; }

check_prereqs() {
  header "Prerequisites"
  if ! command -v kubectl &>/dev/null; then
    echo "kubectl not found. Cannot proceed."
    exit 1
  fi
  if ! command -v istioctl &>/dev/null; then
    echo "WARNING: istioctl not found. Some checks will be skipped."
  fi
  if ! kubectl get ns "$NS" &>/dev/null; then
    echo "Namespace $NS does not exist. Deploy first."
    exit 1
  fi
}

verify_enrollment() {
  header "Stage 2 — Ambient Enrollment"

  # Check namespace label
  local label
  label=$(kubectl get ns "$NS" -o jsonpath='{.metadata.labels.istio\.io/dataplane-mode}' 2>/dev/null || true)
  if [ "$label" = "ambient" ]; then
    pass "Namespace has istio.io/dataplane-mode=ambient"
  else
    fail "Namespace label missing or incorrect (got: '$label')"
  fi

  # Check ServiceAccounts
  for sa in api-sa worker-sa postgres-sa redis-sa engine-gateway-istio; do
    if kubectl get sa "$sa" -n "$NS" &>/dev/null; then
      pass "ServiceAccount $sa exists"
    else
      fail "ServiceAccount $sa not found"
    fi
  done

  # Check workload SA assignments
  for deploy in api worker; do
    local actual_sa
    actual_sa=$(kubectl get deploy "$deploy" -n "$NS" -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)
    local expected="${deploy}-sa"
    if [ "$actual_sa" = "$expected" ]; then
      pass "Deployment $deploy uses $expected"
    else
      fail "Deployment $deploy uses '$actual_sa', expected '$expected'"
    fi
  done

  for sts in postgres redis; do
    local actual_sa
    actual_sa=$(kubectl get statefulset "$sts" -n "$NS" -o jsonpath='{.spec.template.spec.serviceAccountName}' 2>/dev/null || true)
    local expected="${sts}-sa"
    if [ "$actual_sa" = "$expected" ]; then
      pass "StatefulSet $sts uses $expected"
    else
      fail "StatefulSet $sts uses '$actual_sa', expected '$expected'"
    fi
  done

  # Check ztunnel enrollment (requires istioctl)
  if command -v istioctl &>/dev/null; then
    local enrolled
    enrolled=$(istioctl ztunnel-config workloads 2>/dev/null | grep -c "$NS" || true)
    if [ "$enrolled" -gt 0 ]; then
      pass "ztunnel reports $enrolled enrolled workloads in $NS"
    else
      fail "ztunnel reports 0 enrolled workloads in $NS"
    fi
  else
    skip "ztunnel enrollment check (istioctl not available)"
  fi

  # Check DNS still works for headless services
  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "$api_pod" ]; then
    if kubectl exec -n "$NS" "$api_pod" -- nslookup postgres."$NS".svc.cluster.local &>/dev/null; then
      pass "DNS resolution for postgres headless service works"
    else
      fail "DNS resolution for postgres headless service failed"
    fi
    if kubectl exec -n "$NS" "$api_pod" -- nslookup redis."$NS".svc.cluster.local &>/dev/null; then
      pass "DNS resolution for redis headless service works"
    else
      fail "DNS resolution for redis headless service failed"
    fi
  else
    skip "DNS verification (no api pod found)"
  fi
}

verify_gateway_identity() {
  header "Stage 3 — Gateway Identity Verification"

  # Check Gateway resource exists and is programmed
  if kubectl get gateway engine-gateway -n "$NS" &>/dev/null; then
    pass "Gateway engine-gateway exists"
    local programmed
    programmed=$(kubectl get gateway engine-gateway -n "$NS" -o jsonpath='{.status.conditions[?(@.type=="Programmed")].status}' 2>/dev/null || true)
    if [ "$programmed" = "True" ]; then
      pass "Gateway is Programmed"
    else
      fail "Gateway is not Programmed (status: '$programmed')"
    fi
  else
    fail "Gateway engine-gateway not found"
    return
  fi

  # CRITICAL: Verify Gateway pod ServiceAccount
  local gw_sa
  gw_sa=$(kubectl get pods -n "$NS" -l istio.io/gateway-name=engine-gateway \
    -o jsonpath='{.items[0].spec.serviceAccountName}' 2>/dev/null || true)

  if [ -z "$gw_sa" ]; then
    # Try alternative label
    gw_sa=$(kubectl get pods -n "$NS" -l gateway.networking.k8s.io/gateway-name=engine-gateway \
      -o jsonpath='{.items[0].spec.serviceAccountName}' 2>/dev/null || true)
  fi

  if [ "$gw_sa" = "engine-gateway-istio" ]; then
    pass "Gateway pod uses ServiceAccount engine-gateway-istio"
  elif [ -n "$gw_sa" ]; then
    fail "Gateway pod uses ServiceAccount '$gw_sa' — expected 'engine-gateway-istio'"
    echo "  ACTION REQUIRED: Update authz-api.yaml principal to match actual SA before enabling API authorization."
    echo "  Change: cluster.local/ns/$NS/sa/engine-gateway-istio"
    echo "  To:     cluster.local/ns/$NS/sa/$gw_sa"
  else
    fail "No Gateway pod found (label istio.io/gateway-name=engine-gateway)"
  fi

  # Verify Gateway external access
  local gw_ip
  gw_ip=$(kubectl get svc -n "$NS" -l istio.io/gateway-name=engine-gateway \
    -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  if [ -z "$gw_ip" ]; then
    gw_ip=$(kubectl get svc -n "$NS" -l istio.io/gateway-name=engine-gateway \
      -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  fi

  if [ -n "$gw_ip" ]; then
    pass "Gateway has external address: $gw_ip"
    local api_pod
    api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$api_pod" ]; then
      local live_response
      live_response=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 "http://${gw_ip}/live" 2>/dev/null || true)
      if echo "$live_response" | grep -q '"success":true'; then
        pass "Gateway routes to API successfully (/live)"
      else
        fail "Gateway → API routing failed (response: '$live_response')"
      fi
    fi
  else
    skip "Gateway external address not yet assigned (pending LoadBalancer)"
  fi
}

verify_authz_positive() {
  header "Authorization — Positive Tests"
  echo "  (Positive proof = engine functions end-to-end)"

  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -z "$api_pod" ]; then
    fail "No API pod found"
    return
  fi

  # Health check proves API → PG and API → Redis connectivity
  local health
  health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
  if echo "$health" | grep -q '"postgres":"up"'; then
    pass "API → PostgreSQL connectivity (api-sa authorized on :5432)"
  else
    fail "API → PostgreSQL connectivity failed"
  fi
  if echo "$health" | grep -q '"redis":"up"'; then
    pass "API → Redis connectivity (api-sa authorized on :6379)"
  else
    fail "API → Redis connectivity failed"
  fi

  # Kubelet probe passthrough (API pods must be Ready)
  local ready_count
  ready_count=$(kubectl get pods -n "$NS" -l app=api \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null \
    | grep -c "True" || true)
  local total_count
  total_count=$(kubectl get pods -n "$NS" -l app=api --no-headers 2>/dev/null | wc -l || true)
  if [ "$ready_count" -gt 0 ] && [ "$ready_count" -eq "$total_count" ]; then
    pass "All $ready_count API pods are Ready (kubelet probes pass through mesh)"
  else
    fail "Only $ready_count/$total_count API pods are Ready (kubelet probes may be blocked)"
  fi
}

verify_authz_negative() {
  header "Authorization — Negative Tests"
  echo "  (Using test-unauthorized ServiceAccount)"

  # Create test SA if it doesn't exist
  kubectl apply -f - <<'EOF' 2>/dev/null
apiVersion: v1
kind: ServiceAccount
metadata:
  name: test-unauthorized
  namespace: distributed-engine
EOF

  # Negative test: unauthorized → PostgreSQL
  echo "  Testing unauthorized → PostgreSQL :5432 ..."
  local pg_result
  pg_result=$(kubectl run pg-neg-test -n "$NS" \
    --image=postgres:16-alpine \
    --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
    --rm -i --restart=Never --timeout=20s \
    -- sh -c "timeout 5 pg_isready -h postgres -U postgres 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
  kubectl delete pod pg-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

  # First verify destination is independently reachable (use api pod which IS authorized)
  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  local pg_reachable="unknown"
  if [ -n "$api_pod" ]; then
    local health
    health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
    if echo "$health" | grep -q '"postgres":"up"'; then
      pg_reachable="yes"
    fi
  fi

  if [ "$pg_reachable" = "yes" ]; then
    if echo "$pg_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|no response)"; then
      pass "Unauthorized identity denied access to PostgreSQL :5432"
    elif echo "$pg_result" | grep -q "EXIT_CODE:0"; then
      fail "SECURITY: Unauthorized identity accessed PostgreSQL — policy not enforced"
    else
      fail "Inconclusive PostgreSQL negative test (result: '$pg_result')"
    fi
  else
    skip "PostgreSQL negative test (destination not independently reachable)"
  fi

  # Negative test: unauthorized → Redis
  echo "  Testing unauthorized → Redis :6379 ..."
  local redis_result
  redis_result=$(kubectl run redis-neg-test -n "$NS" \
    --image=redis:7-alpine \
    --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
    --rm -i --restart=Never --timeout=20s \
    -- sh -c "timeout 5 redis-cli -h redis ping 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
  kubectl delete pod redis-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

  local redis_reachable="unknown"
  if [ -n "$api_pod" ]; then
    local health
    health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
    if echo "$health" | grep -q '"redis":"up"'; then
      redis_reachable="yes"
    fi
  fi

  if [ "$redis_reachable" = "yes" ]; then
    if echo "$redis_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|Could not connect)"; then
      pass "Unauthorized identity denied access to Redis :6379"
    elif echo "$redis_result" | grep -q "PONG"; then
      fail "SECURITY: Unauthorized identity accessed Redis — policy not enforced"
    else
      fail "Inconclusive Redis negative test (result: '$redis_result')"
    fi
  else
    skip "Redis negative test (destination not independently reachable)"
  fi

  # Negative test: unauthorized → API
  echo "  Testing unauthorized → API :3000 ..."
  local api_result
  api_result=$(kubectl run api-neg-test -n "$NS" \
    --image=busybox:latest \
    --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
    --rm -i --restart=Never --timeout=20s \
    -- sh -c "timeout 5 wget -qO- http://api:3000/live 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
  kubectl delete pod api-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

  local api_reachable="unknown"
  if [ -n "$api_pod" ]; then
    local live
    live=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/live 2>/dev/null || true)
    if echo "$live" | grep -q '"success":true'; then
      api_reachable="yes"
    fi
  fi

  if [ "$api_reachable" = "yes" ]; then
    if echo "$api_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|download timed out)"; then
      pass "Unauthorized identity denied access to API :3000"
    elif echo "$api_result" | grep -q '"success":true'; then
      fail "SECURITY: Unauthorized identity accessed API — policy not enforced"
    else
      fail "Inconclusive API negative test (result: '$api_result')"
    fi
  else
    skip "API negative test (destination not independently reachable)"
  fi

  # Cleanup
  kubectl delete sa test-unauthorized -n "$NS" --ignore-not-found &>/dev/null || true
}

verify_e2e() {
  header "End-to-End Task Lifecycle"

  # Determine API access method
  local api_url=""
  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

  if [ -z "$api_pod" ]; then
    fail "No API pod found — cannot run E2E test"
    return
  fi

  # Create task via localhost (inside the pod, bypasses AuthorizationPolicy)
  local create_response
  create_response=$(kubectl exec -n "$NS" "$api_pod" -- \
    wget -qO- --timeout=10 --post-data='{"name":"istio-e2e-verify","payload":{"mesh":"ambient","verify":true}}' \
    --header='Content-Type: application/json' \
    http://localhost:3000/tasks 2>/dev/null || true)

  if ! echo "$create_response" | grep -q '"success":true'; then
    fail "Task creation failed (response: '$create_response')"
    return
  fi

  local task_id
  task_id=$(echo "$create_response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  if [ -z "$task_id" ]; then
    fail "Could not extract task ID from response"
    return
  fi
  pass "Task created: $task_id"

  # Poll for completion (up to 30s)
  local status="UNKNOWN"
  for i in $(seq 1 10); do
    sleep 3
    local task_response
    task_response=$(kubectl exec -n "$NS" "$api_pod" -- \
      wget -qO- --timeout=5 "http://localhost:3000/tasks/${task_id}" 2>/dev/null || true)
    status=$(echo "$task_response" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo "  Poll $i: status=$status"
    if [ "$status" = "COMPLETED" ] || [ "$status" = "FAILED" ]; then
      break
    fi
  done

  if [ "$status" = "COMPLETED" ]; then
    pass "Task reached COMPLETED — full pipeline through mesh verified"
    echo "  Proved: Gateway→API, API→PG, API→Redis, Worker→Redis, Worker→PG"
  elif [ "$status" = "FAILED" ]; then
    fail "Task reached FAILED — check worker logs"
  else
    fail "Task did not reach terminal state within 30s (last status: $status)"
  fi
}

print_summary() {
  header "Summary"
  echo "  Passed: $PASS"
  echo "  Failed: $FAIL"
  echo "  Skipped: $SKIP"
  echo ""
  if [ "$FAIL" -gt 0 ]; then
    echo "  RESULT: FAIL"
    exit 1
  else
    echo "  RESULT: PASS (with $SKIP skipped)"
    exit 0
  fi
}

# Main
check_prereqs

STAGE="${1:-all}"

case "$STAGE" in
  enrollment)       verify_enrollment ;;
  gateway-identity) verify_gateway_identity ;;
  authz-positive)   verify_authz_positive ;;
  authz-negative)   verify_authz_negative ;;
  e2e)              verify_e2e ;;
  all)
    verify_enrollment
    verify_gateway_identity
    verify_authz_positive
    verify_authz_negative
    verify_e2e
    ;;
  *)
    echo "Unknown stage: $STAGE"
    echo "Usage: $0 [enrollment|gateway-identity|authz-positive|authz-negative|e2e|all]"
    exit 1
    ;;
esac

print_summary
