#!/usr/bin/env bash
# Runs the production images (api, worker, web) against the CI PostgreSQL and
# Redis services and proves, end to end:
#   1. each image starts and passes its own health check;
#   2. the operator API refuses requests without the operator key;
#   3. the dashboard refuses requests without Basic credentials;
#   4. a task submitted through the dashboard is executed by the worker and
#      reaches COMPLETED.
set -euo pipefail

# Linux host networking reuses the PostgreSQL and Redis services from CI.
containers=()
cleanup() {
  local result=$?
  for container in "${containers[@]}"; do
    if [ "$result" -ne 0 ]; then
      echo "----- logs: $container -----" >&2
      docker logs "$container" >&2 || true
    fi
    docker rm -f "$container" >/dev/null || true
  done
  exit "$result"
}
trap cleanup EXIT

# Throwaway credentials for this run only.
OPERATOR_API_KEY="$(openssl rand -hex 32)"
DASHBOARD_USER=smoke
DASHBOARD_PASSWORD="$(openssl rand -hex 16)"

common_env=(
  -e NODE_ENV=production
  -e POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
  -e POSTGRES_PORT="${POSTGRES_PORT:-5432}"
  -e POSTGRES_DB="${POSTGRES_DB:-distributed_engine_test}"
  -e POSTGRES_USER="${POSTGRES_USER:-postgres}"
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
  -e REDIS_HOST="${REDIS_HOST:-localhost}"
  -e REDIS_PORT="${REDIS_PORT:-6379}"
  -e REDIS_PASSWORD="${REDIS_PASSWORD:-}"
)

# start <service> [docker run args...]; the container name is left in $started.
# (Not returned via $(...): a subshell could not register it for cleanup.)
start() {
  local service=$1
  shift
  started="distributed-engine-smoke-${service}-$$"
  containers+=("$started")
  docker run -d --name "$started" --network host "$@" "distributed-engine-${service}:ci" >/dev/null
}

# wait_for <container> <label> <node ESM snippet that exits 0 when healthy>
wait_for() {
  local container=$1 label=$2 probe=$3
  for _ in $(seq 1 60); do
    if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
      echo "$label exited before becoming healthy" >&2
      exit 1
    fi
    if docker exec "$container" node --input-type=module -e "$probe" >/dev/null 2>&1; then
      echo "$label production image started and passed its health check"
      return 0
    fi
    sleep 1
  done
  echo "$label did not become healthy" >&2
  exit 1
}

start api "${common_env[@]}" -e OPERATOR_API_KEY="$OPERATOR_API_KEY" \
  -e RECONCILER_INTERVAL_MS=5000 -e RECONCILER_QUEUED_GRACE_MS=10000
api=$started
wait_for "$api" api '
  const r = await fetch("http://localhost:3000/health", { signal: AbortSignal.timeout(2000) });
  const b = await r.json();
  process.exit(r.ok && b.data.status === "healthy" ? 0 : 1);
'

start worker "${common_env[@]}"
worker=$started
wait_for "$worker" worker '
  const r = await fetch("http://localhost:9000/healthz", { signal: AbortSignal.timeout(2000) });
  const b = await r.json();
  process.exit(r.ok && b.status === "alive" ? 0 : 1);
'

start web -e ENGINE_API_URL=http://localhost:3000 -e OPERATOR_API_KEY="$OPERATOR_API_KEY" \
  -e DASHBOARD_USER="$DASHBOARD_USER" -e DASHBOARD_PASSWORD="$DASHBOARD_PASSWORD"
web=$started
wait_for "$web" web '
  const r = await fetch("http://localhost:3001/", { signal: AbortSignal.timeout(2000) });
  process.exit(r.status === 401 ? 0 : 1);
'

status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
expect() {
  local want=$1 label=$2
  shift 2
  local got
  got=$(status "$@")
  if [ "$got" != "$want" ]; then
    echo "FAIL: $label returned $got, expected $want" >&2
    exit 1
  fi
  echo "ok: $label -> $got"
}

op=(-H "Authorization: Bearer ${OPERATOR_API_KEY}")
dash=(-u "${DASHBOARD_USER}:${DASHBOARD_PASSWORD}")

expect 200 "api /health without key" http://localhost:3000/health
expect 401 "api /tasks without key" http://localhost:3000/tasks
expect 401 "api /tasks with wrong key" -H 'Authorization: Bearer wrong' http://localhost:3000/tasks
expect 200 "api /tasks with operator key" "${op[@]}" http://localhost:3000/tasks
expect 401 "dashboard without credentials" http://localhost:3001/
expect 401 "dashboard with wrong password" -u "${DASHBOARD_USER}:wrong" http://localhost:3001/
expect 200 "dashboard with credentials" "${dash[@]}" http://localhost:3001/
expect 200 "dashboard -> engine metrics" "${dash[@]}" http://localhost:3001/api/engine/metrics
expect 404 "dashboard refuses non-allow-listed call" "${dash[@]}" http://localhost:3001/api/engine/tenant/usage
expect 403 "dashboard refuses cross-site write" "${dash[@]}" -X POST -H 'Sec-Fetch-Site: cross-site' \
  -H 'Content-Type: application/json' -d '{"name":"smoke"}' http://localhost:3001/api/engine/tasks

# Submit through the dashboard exactly as its own page would, then wait for the worker.
response=$(curl -sf "${dash[@]}" -X POST -H 'Sec-Fetch-Site: same-origin' -H 'Content-Type: application/json' \
  -d '{"name":"smoke-production-images","priority":"HIGH"}' http://localhost:3001/api/engine/tasks)
task_id=$(node -e 'console.log(JSON.parse(process.argv[1]).data.id)' "$response")
echo "submitted task ${task_id} through the dashboard"

for _ in $(seq 1 60); do
  task_status=$(curl -s "${op[@]}" "http://localhost:3000/tasks/${task_id}" \
    | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{console.log(JSON.parse(s).data.status)}catch{console.log("?")}})')
  if [ "$task_status" = COMPLETED ]; then
    echo "ok: task ${task_id} COMPLETED (api -> outbox -> redis -> worker -> postgres)"
    exit 0
  fi
  sleep 1
done
echo "FAIL: task ${task_id} did not complete (last status: ${task_status})" >&2
exit 1
