#!/usr/bin/env bash
# Deploys the real k8s manifests to a throwaway kind cluster with Istio in
# ambient mode, then proves the deployment works:
#   - k8s/istio/verify-runtime.sh: ambient enrollment and mTLS identities,
#     Gateway identity, AuthorizationPolicy allow and deny paths, and a task
#     driven end to end through the Gateway;
#   - the dashboard (web-sa) reaches the API through the mesh and refuses
#     unauthenticated requests.
#
# Expects images distributed-engine-{api,worker,web}:ci to exist locally.
# Run in CI by .github/workflows/full-stack.yml; works the same on a laptop
# with docker, kubectl and curl.
set -euo pipefail

cd "$(dirname "$0")/.."

KIND_VERSION="${KIND_VERSION:-v0.31.0}"
ISTIO_VERSION="${ISTIO_VERSION:-1.30.5}"
GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-v1.4.0}"
CLUSTER=engine
NS=distributed-engine
TOOLS="${RUNNER_TEMP:-/tmp}/engine-tools"
mkdir -p "$TOOLS"
export PATH="$TOOLS:$PATH"

pf_pids=()
cleanup() {
  local result=$?
  for pid in "${pf_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  if [ "$result" -ne 0 ]; then
    echo "----- failure diagnostics -----" >&2
    kubectl get pods -A -o wide >&2 || true
    kubectl -n "$NS" get events --sort-by=.lastTimestamp >&2 | tail -60 || true
    for app in api worker web postgres redis; do
      echo "--- logs: $app" >&2
      kubectl -n "$NS" logs -l app="$app" --tail=80 --all-containers >&2 || true
    done
  fi
  if [ "${KEEP_CLUSTER:-}" != 1 ]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
  exit "$result"
}
trap cleanup EXIT

echo "== Tools: kind ${KIND_VERSION}, istioctl ${ISTIO_VERSION}"
if ! command -v kind >/dev/null; then
  curl -fsSLo "$TOOLS/kind" "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64"
  chmod +x "$TOOLS/kind"
fi
if ! command -v istioctl >/dev/null; then
  curl -fsSL "https://github.com/istio/istio/releases/download/${ISTIO_VERSION}/istioctl-${ISTIO_VERSION}-linux-amd64.tar.gz" \
    | tar -xz -C "$TOOLS"
fi
kind version
istioctl version --remote=false

echo "== Cluster"
kind create cluster --name "$CLUSTER" --wait 180s
kubectl apply --server-side -f \
  "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"
istioctl install --set profile=ambient --skip-confirmation
kubectl -n istio-system rollout status ds/ztunnel --timeout=180s

echo "== Images"
for image in api worker web; do
  kind load docker-image "distributed-engine-${image}:ci" --name "$CLUSTER"
done

echo "== Secrets (generated for this run only)"
OPERATOR_API_KEY="$(openssl rand -hex 32)"
DASHBOARD_PASSWORD="$(openssl rand -hex 16)"
kubectl apply -f k8s/namespace.yaml
kubectl -n "$NS" create secret generic redis-secret --from-literal=password="$(openssl rand -hex 16)"
kubectl -n "$NS" create secret generic postgres-secret --from-literal=password="$(openssl rand -hex 16)"
kubectl -n "$NS" create secret generic operator-secret --from-literal=api-key="$OPERATOR_API_KEY"
kubectl -n "$NS" create secret generic dashboard-secret \
  --from-literal=username=ops --from-literal=password="$DASHBOARD_PASSWORD"

echo "== Deploy (kustomize overlay ci/kind = k8s/ + k8s/istio/, scaled for one node)"
kubectl apply -k ci/kind
for workload in statefulset/postgres statefulset/redis deployment/api deployment/worker deployment/web; do
  kubectl -n "$NS" rollout status "$workload" --timeout=300s
done
kubectl -n "$NS" wait gateway/engine-gateway --for=condition=Programmed --timeout=180s

echo "== Port-forwards"
kubectl -n "$NS" port-forward svc/engine-gateway-istio 8080:80 >/dev/null 2>&1 &
pf_pids+=($!)
kubectl -n "$NS" port-forward svc/web 3001:80 >/dev/null 2>&1 &
pf_pids+=($!)
for _ in $(seq 1 30); do
  curl -s -o /dev/null http://localhost:8080/live && curl -s -o /dev/null http://localhost:3001/ && break
  sleep 1
done

echo "== Istio ambient runtime verification"
GW_ADDR=localhost:8080 OPERATOR_API_KEY="$OPERATOR_API_KEY" bash k8s/istio/verify-runtime.sh all

echo "== Dashboard through the mesh (web-sa -> api)"
status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
[ "$(status http://localhost:3001/)" = 401 ] || { echo "FAIL: dashboard served without credentials" >&2; exit 1; }
echo "ok: dashboard without credentials -> 401"
[ "$(status -u "ops:${DASHBOARD_PASSWORD}" http://localhost:3001/api/engine/metrics)" = 200 ] \
  || { echo "FAIL: dashboard could not reach the API through the mesh" >&2; exit 1; }
echo "ok: dashboard -> api metrics through the mesh -> 200"
[ "$(status http://localhost:8080/tasks)" = 401 ] || { echo "FAIL: API served /tasks without the operator key via the Gateway" >&2; exit 1; }
echo "ok: Gateway -> api /tasks without operator key -> 401"

echo "== PASS: manifests deploy on Kubernetes with Istio ambient, policies enforce, tasks complete"
