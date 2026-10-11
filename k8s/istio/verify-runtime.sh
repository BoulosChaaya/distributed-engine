#!/usr/bin/env bash
#
# Topic 16 — Istio Ambient Mesh Runtime Verification
#
# REQUIRES: Kubernetes cluster with Istio Ambient mode installed.
# REQUIRES: kubectl configured for the target cluster.
# OPTIONAL: istioctl on PATH (for mTLS/enrollment diagnostics).
#
# This script performs staged verification of Istio Ambient mesh
# enrollment, mTLS, Gateway identity, AuthorizationPolicy enforcement,
# and end-to-end task lifecycle through the Gateway ingress.
#
# Exit codes:
#   0 — all critical checks passed, none skipped
#   1 — one or more checks failed
#   2 — no checks failed, but critical checks were skipped
#       (RUNTIME ACCEPTANCE INCOMPLETE)
#
# Exit code semantics:
#   0 = RUNTIME ACCEPTANCE PASS — every critical check ran and passed.
#   1 = RUNTIME ACCEPTANCE FAIL — at least one check produced a hard failure.
#   2 = RUNTIME ACCEPTANCE INCOMPLETE — no failures, but one or more critical
#       checks could not execute (missing infrastructure, pending LoadBalancer,
#       istioctl not available, etc.). The operator must resolve the missing
#       prerequisites and re-run before accepting the deployment.
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
CRITICAL_SKIP=0

# Gateway pod label — current Istio (1.22+) uses the standard Gateway API label.
# Older Istio versions used istio.io/gateway-name. We try both.
GW_LABEL_CURRENT="gateway.networking.k8s.io/gateway-name=engine-gateway"
GW_LABEL_LEGACY="istio.io/gateway-name=engine-gateway"

# Expected principal in authz-api.yaml
EXPECTED_GW_SA="engine-gateway-istio"
EXPECTED_GW_PRINCIPAL="cluster.local/ns/${NS}/sa/${EXPECTED_GW_SA}"

pass() { echo "  PASS: $1"; ((PASS++)); }
fail() { echo "  FAIL: $1"; ((FAIL++)); }
skip() { echo "  SKIP: $1"; ((SKIP++)); }
critical_skip() { echo "  SKIP (CRITICAL): $1"; ((SKIP++)); ((CRITICAL_SKIP++)); }
header() { echo ""; echo "=== $1 ==="; }

# Resolve Gateway address (IP or hostname) from the Gateway-created Service.
# Tries both current and legacy Istio labels for Service discovery.
# Sets GW_ADDR or leaves it empty.
resolve_gateway_addr() {
  local gw_addr=""
  # Try current label first
  gw_addr=$(kubectl get svc -n "$NS" -l "$GW_LABEL_CURRENT" \
    -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  if [ -z "$gw_addr" ]; then
    gw_addr=$(kubectl get svc -n "$NS" -l "$GW_LABEL_CURRENT" \
      -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  fi
  # Fallback to legacy label
  if [ -z "$gw_addr" ]; then
    gw_addr=$(kubectl get svc -n "$NS" -l "$GW_LABEL_LEGACY" \
      -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)
  fi
  if [ -z "$gw_addr" ]; then
    gw_addr=$(kubectl get svc -n "$NS" -l "$GW_LABEL_LEGACY" \
      -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  fi
  echo "$gw_addr"
}

# Discover Gateway pod SA. Tries both label conventions.
discover_gateway_sa() {
  local gw_sa=""
  gw_sa=$(kubectl get pods -n "$NS" -l "$GW_LABEL_CURRENT" \
    -o jsonpath='{.items[0].spec.serviceAccountName}' 2>/dev/null || true)
  if [ -z "$gw_sa" ]; then
    gw_sa=$(kubectl get pods -n "$NS" -l "$GW_LABEL_LEGACY" \
      -o jsonpath='{.items[0].spec.serviceAccountName}' 2>/dev/null || true)
  fi
  echo "$gw_sa"
}

check_prereqs() {
  header "Prerequisites"
  if ! command -v kubectl &>/dev/null; then
    echo "kubectl not found. Cannot proceed."
    exit 1
  fi
  if ! command -v istioctl &>/dev/null; then
    echo "WARNING: istioctl not found. mTLS and ztunnel diagnostics will be skipped."
  fi
  if ! kubectl get ns "$NS" &>/dev/null; then
    echo "Namespace $NS does not exist. Deploy first."
    exit 1
  fi
}

verify_enrollment() {
  header "Stage 2 — Ambient Enrollment & mTLS"

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

  # ztunnel enrollment and mTLS diagnostics (requires istioctl)
  if command -v istioctl &>/dev/null; then
    # Check workload enrollment
    local enrolled
    enrolled=$(istioctl ztunnel-config workloads 2>/dev/null | grep -c "$NS" || true)
    if [ "$enrolled" -gt 0 ]; then
      pass "ztunnel reports $enrolled enrolled workloads in $NS"
    else
      fail "ztunnel reports 0 enrolled workloads in $NS"
    fi

    # Check ztunnel certificate/identity state for workloads
    local certs_output
    certs_output=$(istioctl ztunnel-config certificates 2>/dev/null || true)
    if [ -n "$certs_output" ]; then
      local ns_certs
      ns_certs=$(echo "$certs_output" | grep -c "$NS" || true)
      if [ "$ns_certs" -gt 0 ]; then
        pass "ztunnel holds $ns_certs certificate entries for $NS workloads (mTLS identity active)"
      else
        fail "ztunnel holds no certificates for $NS workloads"
      fi
    else
      critical_skip "ztunnel certificate check (istioctl ztunnel-config certificates returned no output)"
    fi
  else
    critical_skip "ztunnel enrollment check (istioctl not available)"
    critical_skip "ztunnel mTLS certificate check (istioctl not available)"
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
    critical_skip "DNS verification (no api pod found)"
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

  # CRITICAL: Verify Gateway pod ServiceAccount matches authz-api.yaml principal
  local gw_sa
  gw_sa=$(discover_gateway_sa)

  if [ "$gw_sa" = "$EXPECTED_GW_SA" ]; then
    pass "Gateway pod uses ServiceAccount '$EXPECTED_GW_SA' — matches authz-api.yaml principal"
  elif [ -n "$gw_sa" ]; then
    fail "Gateway pod uses ServiceAccount '$gw_sa' — authz-api.yaml expects '$EXPECTED_GW_SA'"
    echo ""
    echo "  !! STAGE 5 BLOCKED !!"
    echo "  Do NOT apply authz-api.yaml until this mismatch is resolved."
    echo "  The runtime Gateway identity does not match the configured principal."
    echo "  Expected principal: $EXPECTED_GW_PRINCIPAL"
    echo "  Actual SA:          $gw_sa"
    echo "  Actual principal:   cluster.local/ns/$NS/sa/$gw_sa"
    echo ""
    echo "  Resolution: reconcile the manifest (authz-api.yaml) or the Gateway"
    echo "  configuration to align. Do not apply the policy with a mismatched identity."
  else
    fail "No Gateway pod found (tried labels: $GW_LABEL_CURRENT, $GW_LABEL_LEGACY)"
    echo "  Verify Gateway pod labels with: kubectl get pods -n $NS --show-labels"
  fi

  # Verify Gateway Service exists and has external address
  local gw_addr
  gw_addr=$(resolve_gateway_addr)
  if [ -n "$gw_addr" ]; then
    pass "Gateway has external address: $gw_addr"
  else
    critical_skip "Gateway external address not yet assigned (pending LoadBalancer or no Service found)"
    echo "  Verify Gateway Service with: kubectl get svc -n $NS --show-labels"
  fi
}

verify_authz_positive() {
  header "Authorization — Positive Tests (Component Diagnostics)"
  echo "  (API-local health checks verify authorized outbound connectivity)"

  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -z "$api_pod" ]; then
    fail "No API pod found"
    return
  fi

  # API-local health: proves api-sa → PostgreSQL and api-sa → Redis connectivity
  local health
  health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
  if echo "$health" | grep -q '"postgres":"up"'; then
    pass "API-local health: api-sa → PostgreSQL :5432 authorized"
  else
    fail "API-local health: api-sa → PostgreSQL connectivity failed"
  fi
  if echo "$health" | grep -q '"redis":"up"'; then
    pass "API-local health: api-sa → Redis :6379 authorized"
  else
    fail "API-local health: api-sa → Redis connectivity failed"
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

  # Gateway → API authorized path (requires Gateway address)
  local gw_addr
  gw_addr=$(resolve_gateway_addr)
  if [ -n "$gw_addr" ]; then
    echo ""
    echo "  Gateway authorization positive test (Gateway → API via mesh):"
    local gw_live
    gw_live=$(wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
    if echo "$gw_live" | grep -q '"success":true'; then
      pass "Gateway → API :3000 authorized (engine-gateway-istio → api via /live)"
    else
      # Try from inside the cluster if external wget unavailable
      if [ -n "$api_pod" ]; then
        gw_live=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
        if echo "$gw_live" | grep -q '"success":true'; then
          pass "Gateway → API :3000 authorized (tested via in-cluster request to Gateway address)"
          echo "    Note: request traversed Gateway proxy at $gw_addr, not API localhost"
        else
          fail "Gateway → API routing failed (address: $gw_addr, response: '$gw_live')"
        fi
      else
        fail "Gateway → API routing failed and no API pod available for in-cluster test"
      fi
    fi
  else
    critical_skip "Gateway → API positive authorization test (no Gateway address available)"
  fi
}

verify_authz_negative() {
  header "Authorization — Negative Tests"
  echo "  (Using test-unauthorized ServiceAccount)"

  # Create test SA
  kubectl apply -f - <<'EOF' 2>/dev/null
apiVersion: v1
kind: ServiceAccount
metadata:
  name: test-unauthorized
  namespace: distributed-engine
EOF

  local api_pod
  api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

  # --- PostgreSQL negative test ---
  echo ""
  echo "  Testing unauthorized → PostgreSQL :5432 ..."

  # Positive control: verify PostgreSQL is reachable by authorized identity
  local pg_reachable="unknown"
  if [ -n "$api_pod" ]; then
    local health
    health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
    if echo "$health" | grep -q '"postgres":"up"'; then
      pg_reachable="yes"
    fi
  fi

  if [ "$pg_reachable" = "yes" ]; then
    local pg_result
    pg_result=$(kubectl run pg-neg-test -n "$NS" \
      --image=postgres:16-alpine \
      --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
      --rm -i --restart=Never --timeout=20s \
      -- sh -c "timeout 5 pg_isready -h postgres -U postgres 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
    kubectl delete pod pg-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

    if echo "$pg_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|no response)"; then
      pass "Unauthorized identity denied access to PostgreSQL :5432"
    elif echo "$pg_result" | grep -q "EXIT_CODE:0"; then
      fail "SECURITY: Unauthorized identity accessed PostgreSQL — policy not enforced"
    else
      fail "Inconclusive PostgreSQL negative test (result: '$pg_result')"
    fi
  else
    critical_skip "PostgreSQL negative test (destination not independently reachable by authorized identity)"
  fi

  # --- Redis negative test ---
  echo ""
  echo "  Testing unauthorized → Redis :6379 ..."

  local redis_reachable="unknown"
  if [ -n "$api_pod" ]; then
    local health
    health=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 http://localhost:3000/health 2>/dev/null || true)
    if echo "$health" | grep -q '"redis":"up"'; then
      redis_reachable="yes"
    fi
  fi

  if [ "$redis_reachable" = "yes" ]; then
    local redis_result
    redis_result=$(kubectl run redis-neg-test -n "$NS" \
      --image=redis:7-alpine \
      --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
      --rm -i --restart=Never --timeout=20s \
      -- sh -c "timeout 5 redis-cli -h redis ping 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
    kubectl delete pod redis-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

    if echo "$redis_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|Could not connect)"; then
      pass "Unauthorized identity denied access to Redis :6379"
    elif echo "$redis_result" | grep -q "PONG"; then
      fail "SECURITY: Unauthorized identity accessed Redis — policy not enforced"
    else
      fail "Inconclusive Redis negative test (result: '$redis_result')"
    fi
  else
    critical_skip "Redis negative test (destination not independently reachable by authorized identity)"
  fi

  # --- API negative test ---
  echo ""
  echo "  Testing unauthorized → API :3000 ..."

  # Positive control: verify API is reachable through the authorized Gateway path
  local api_reachable="unknown"
  local gw_addr
  gw_addr=$(resolve_gateway_addr)
  if [ -n "$gw_addr" ]; then
    local gw_live
    gw_live=$(wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
    if echo "$gw_live" | grep -q '"success":true'; then
      api_reachable="yes"
      echo "    Positive control: Gateway ($gw_addr) → API :3000 succeeded"
    else
      # Try from inside the cluster
      if [ -n "$api_pod" ]; then
        gw_live=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
        if echo "$gw_live" | grep -q '"success":true'; then
          api_reachable="yes"
          echo "    Positive control: in-cluster request to Gateway ($gw_addr) → API :3000 succeeded"
        fi
      fi
    fi
  fi

  if [ "$api_reachable" = "yes" ]; then
    local api_result
    api_result=$(kubectl run api-neg-test -n "$NS" \
      --image=busybox:latest \
      --overrides='{"spec":{"serviceAccountName":"test-unauthorized"}}' \
      --rm -i --restart=Never --timeout=20s \
      -- sh -c "timeout 5 wget -qO- http://api:3000/live 2>&1; echo EXIT_CODE:\$?" 2>/dev/null || true)
    kubectl delete pod api-neg-test -n "$NS" --ignore-not-found &>/dev/null || true

    if echo "$api_result" | grep -qE "(EXIT_CODE:[^0]|timed out|connection refused|download timed out)"; then
      pass "Unauthorized identity denied access to API :3000"
    elif echo "$api_result" | grep -q '"success":true'; then
      fail "SECURITY: Unauthorized identity accessed API — policy not enforced"
    else
      fail "Inconclusive API negative test (result: '$api_result')"
    fi
  else
    critical_skip "API negative test (destination not reachable through authorized Gateway path — cannot establish positive control)"
  fi

  # Cleanup
  kubectl delete sa test-unauthorized -n "$NS" --ignore-not-found &>/dev/null || true
}

verify_e2e() {
  header "End-to-End Task Lifecycle (Gateway Path)"
  echo "  This test submits and polls a task through the Gateway ingress."
  echo "  Tested flow: Client → Gateway → API → PG/Outbox → Redis/BullMQ → Worker → PG → API via Gateway"

  # Resolve Gateway address
  local gw_addr
  gw_addr=$(resolve_gateway_addr)

  if [ -z "$gw_addr" ]; then
    critical_skip "Gateway E2E test: no Gateway address available (pending LoadBalancer or no Gateway Service)"
    echo "  The Gateway E2E test requires a reachable Gateway address."
    echo "  This test cannot fall back to API localhost — that would bypass the Gateway."
    return
  fi

  echo "  Gateway address: $gw_addr"

  # Determine how to reach the Gateway.
  # Prefer direct access from the verification runner; fall back to in-cluster
  # request if the runner cannot reach the Gateway (e.g. no external LB).
  local via_kubectl=""
  local test_live
  test_live=$(wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
  if echo "$test_live" | grep -q '"success":true'; then
    echo "  Access method: direct (verification runner → Gateway)"
  else
    # Try from inside the cluster via a disposable pod
    local api_pod
    api_pod=$(kubectl get pod -n "$NS" -l app=api -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$api_pod" ]; then
      test_live=$(kubectl exec -n "$NS" "$api_pod" -- wget -qO- --timeout=5 "http://${gw_addr}/live" 2>/dev/null || true)
      if echo "$test_live" | grep -q '"success":true'; then
        via_kubectl="$api_pod"
        echo "  Access method: in-cluster via $api_pod → Gateway at $gw_addr"
        echo "    Note: request traverses Gateway proxy (not API localhost)"
      fi
    fi
    if [ -z "$via_kubectl" ] && ! echo "$test_live" | grep -q '"success":true'; then
      critical_skip "Gateway E2E test: Gateway address $gw_addr not reachable from runner or cluster"
      echo "  Consider port-forwarding the Gateway Service for local verification:"
      echo "    kubectl -n $NS port-forward svc/\$(kubectl get svc -n $NS -l $GW_LABEL_CURRENT -o name | head -1) 8080:80"
      echo "    GW_ADDR=localhost:8080 ./verify-runtime.sh e2e"
      return
    fi
  fi

  # Allow overriding the Gateway address via environment variable
  # (e.g., when using port-forward: GW_ADDR=localhost:8080 ./verify-runtime.sh e2e)
  gw_addr="${GW_ADDR:-$gw_addr}"

  # Create task through the Gateway
  # Schema from apps/api/src/validation.ts — SubmitTaskSchema:
  #   name: string (required, 1-255 chars)
  #   payload: Record<string,unknown> (optional, default {})
  #   priority: 'LOW'|'NORMAL'|'HIGH'|'CRITICAL' (optional, default 'NORMAL')
  #   maxRetries: int 0-10 (optional, default 3)
  local create_response
  if [ -z "$via_kubectl" ]; then
    create_response=$(wget -qO- --timeout=10 \
      --post-data='{"name":"istio-e2e-verify"}' \
      --header='Content-Type: application/json' \
      "http://${gw_addr}/tasks" 2>/dev/null || true)
  else
    create_response=$(kubectl exec -n "$NS" "$via_kubectl" -- \
      wget -qO- --timeout=10 \
      --post-data='{"name":"istio-e2e-verify"}' \
      --header='Content-Type: application/json' \
      "http://${gw_addr}/tasks" 2>/dev/null || true)
  fi

  if ! echo "$create_response" | grep -q '"success":true'; then
    fail "Gateway E2E: task creation through Gateway failed (response: '$create_response')"
    return
  fi

  local task_id
  task_id=$(echo "$create_response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  if [ -z "$task_id" ]; then
    fail "Gateway E2E: could not extract task ID from response"
    return
  fi
  pass "Gateway E2E: task created through Gateway: $task_id"

  # Poll for completion through the Gateway (up to 30s)
  local status="UNKNOWN"
  for i in $(seq 1 10); do
    sleep 3
    local task_response
    if [ -z "$via_kubectl" ]; then
      task_response=$(wget -qO- --timeout=5 "http://${gw_addr}/tasks/${task_id}" 2>/dev/null || true)
    else
      task_response=$(kubectl exec -n "$NS" "$via_kubectl" -- \
        wget -qO- --timeout=5 "http://${gw_addr}/tasks/${task_id}" 2>/dev/null || true)
    fi
    status=$(echo "$task_response" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo "  Poll $i (via Gateway): status=$status"
    if [ "$status" = "COMPLETED" ] || [ "$status" = "FAILED" ]; then
      break
    fi
  done

  if [ "$status" = "COMPLETED" ]; then
    pass "Gateway E2E: task completed — full pipeline through mesh verified"
    echo "  Verified path: Gateway($gw_addr) → API → PG/Outbox → Redis/BullMQ → Worker → PG → API(via Gateway)"
    echo "  This is functional evidence that all mesh-authorized paths are operational."
    echo "  Individual mTLS properties require ztunnel-level diagnostics (see enrollment stage)."
  elif [ "$status" = "FAILED" ]; then
    fail "Gateway E2E: task reached FAILED — check worker logs"
  else
    fail "Gateway E2E: task did not reach terminal state within 30s (last status: $status)"
  fi
}

verify_observability() {
  header "Observability Preservation"
  echo "  Topic 16 does not deploy or modify the observability stack."
  echo "  Observability verification is not in scope for this runtime suite."
  critical_skip "Observability preservation (not tested — requires separate observability stack deployment)"
}

print_summary() {
  header "Summary"
  echo "  Passed:          $PASS"
  echo "  Failed:          $FAIL"
  echo "  Skipped:         $SKIP (of which $CRITICAL_SKIP critical)"
  echo ""
  if [ "$FAIL" -gt 0 ]; then
    echo "  RESULT: RUNTIME ACCEPTANCE FAIL"
    echo "  One or more checks failed. Review failures above."
    exit 1
  elif [ "$CRITICAL_SKIP" -gt 0 ]; then
    echo "  RESULT: RUNTIME ACCEPTANCE INCOMPLETE"
    echo "  No failures, but $CRITICAL_SKIP critical check(s) could not execute."
    echo "  Resolve missing prerequisites and re-run before accepting the deployment."
    exit 2
  else
    echo "  RESULT: RUNTIME ACCEPTANCE PASS"
    echo "  All critical checks executed and passed."
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
    verify_observability
    ;;
  *)
    echo "Unknown stage: $STAGE"
    echo "Usage: $0 [enrollment|gateway-identity|authz-positive|authz-negative|e2e|all]"
    exit 1
    ;;
esac

print_summary
