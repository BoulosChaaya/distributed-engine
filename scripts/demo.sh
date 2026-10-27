#!/usr/bin/env bash
# A guided tour of the engine on docker compose, for a screen recording or a
# reviewer. It:
#   1. starts the stack (PostgreSQL, Redis, API, 3 workers, dashboard);
#   2. provisions a tenant and shows its one-time key;
#   3. runs every task type, including a planned retry, an unknown type and an
#      http call;
#   4. kills a worker and wipes Redis mid-run, then shows every task still
#      completing.
# The stack is left running for the dashboard. Stop it with:
#   docker compose down -v
#
# Usage: scripts/demo.sh [--no-pause]   (--no-pause for CI / unattended runs)
set -euo pipefail
cd "$(dirname "$0")/.."

PAUSE=true
[ "${1:-}" = "--no-pause" ] && PAUSE=false
pause() { if $PAUSE; then read -r -p "  ↵ $1 " _; else echo "  → $1"; fi; }
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

if [ ! -f .env ]; then
  {
    echo "OPERATOR_API_KEY=$(openssl rand -hex 32)"
    echo "DASHBOARD_USER=ops"
    echo "DASHBOARD_PASSWORD=$(openssl rand -hex 12)"
  } > .env
  echo "Created .env with fresh credentials (git-ignored)."
fi
set -a
# shellcheck disable=SC1091
source .env
set +a
export HTTP_TASK_ALLOWED_HOSTS=api          # lets the http task call the API's own /live
export HTTP_TASK_PRIVATE_HOSTS=api          # api is a compose service, so its address is private
export RECONCILER_INTERVAL_MS=5000 RECONCILER_QUEUED_GRACE_MS=10000

API=http://localhost:3000
op() { curl -sf -H "Authorization: Bearer ${OPERATOR_API_KEY}" -H 'Content-Type: application/json' "$@"; }
json() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const o=JSON.parse(s);console.log(process.argv[1].split(".").reduce((a,k)=>a?.[k],o))})' "$1"; }
sql() { docker compose exec -T postgres psql -U postgres -d distributed_engine -tAc "$1"; }
submit() { op -X POST -d "$1" "$API/tasks" | json data.id; }
status_of() { op "$API/tasks/$1" | json data.status; }
wait_for() {
  local id=$1 want=$2
  for _ in $(seq 1 90); do
    [ "$(status_of "$id")" = "$want" ] && return 0
    sleep 1
  done
  echo "task $id did not reach $want" >&2
  return 1
}

step "1. Start the stack: PostgreSQL, Redis, API, 3 workers, dashboard"
docker compose up -d --build --wait --scale worker=3 postgres redis api worker web
echo "  Dashboard: http://localhost:3001  (user: ${DASHBOARD_USER}, password in .env)"
pause "Open the dashboard, then continue"

step "2. Security boundaries"
echo "  /tasks without the operator key -> $(curl -s -o /dev/null -w '%{http_code}' $API/tasks)"
echo "  /tasks with the operator key    -> $(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer ${OPERATOR_API_KEY}" $API/tasks)"
echo "  dashboard without login         -> $(curl -s -o /dev/null -w '%{http_code}' http://localhost:3001/)"

step "3. Provision a tenant (the key is shown once; only its hash is stored)"
tenant=$(op -X POST -d '{"name":"Demo Corp","planId":"plan_starter"}' "$API/tenants")
tenant_key=$(echo "$tenant" | json data.apiKey)
echo "  tenant: $(echo "$tenant" | json data.tenant.name)   key: ${tenant_key:0:12}…"
tenant_task=$(curl -sf -H "Authorization: Bearer ${tenant_key}" -H 'Content-Type: application/json' \
  -d '{"name":"echo","payload":{"from":"Demo Corp"}}' "$API/tenant/tasks" | json data.id)
wait_for "$tenant_task" COMPLETED
echo "  tenant task ${tenant_task:0:8} COMPLETED via the tenant's own key"

step "4. Every task type"
echo_id=$(submit '{"name":"echo","payload":{"hello":"world"}}')
flaky_id=$(submit '{"name":"flaky","maxRetries":3,"payload":{"failTimes":2}}')
http_id=$(submit '{"name":"http","payload":{"url":"http://api:3000/live"}}')
unknown_id=$(submit '{"name":"send-invoice","maxRetries":5}')
wait_for "$echo_id" COMPLETED
wait_for "$http_id" COMPLETED
wait_for "$unknown_id" FAILED
wait_for "$flaky_id" COMPLETED
printf '  %-8s %-10s %s\n' echo COMPLETED "result: $(op "$API/tasks/$echo_id" | json data.result.output.echo.hello)"
printf '  %-8s %-10s %s\n' flaky COMPLETED "after $(op "$API/tasks/$flaky_id" | json data.retries) planned failures"
printf '  %-8s %-10s %s\n' http COMPLETED "upstream status $(op "$API/tasks/$http_id" | json data.result.output.status)"
printf '  %-8s %-10s %s\n' unknown FAILED "$(op "$API/tasks/$unknown_id" | json data.error | cut -c1-60)…"
pause "See them on the Tasks page, then continue to the chaos test"

step "5. A workflow (DAG): fan-out, a planned retry, fan-in, data passed along the edges"
WF='{"name":"nightly-report","steps":[{"key":"fetch","name":"echo","payload":{"source":"orders"}},'\
'{"key":"by-region","name":"sleep","payload":{"ms":2000},"dependsOn":["fetch"]},'\
'{"key":"by-product","name":"flaky","payload":{"failTimes":1},"dependsOn":["fetch"]},'\
'{"key":"merge","name":"echo","dependsOn":["by-region","by-product"]}]}'
wf_id=$(op -X POST -d "$WF" "$API/workflows" | json data.workflow.id)
for _ in $(seq 1 90); do
  [ "$(op "$API/workflows/$wf_id" | json data.workflow.status)" = RUNNING ] || break
  sleep 1
done
op "$API/workflows/$wf_id" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s).data;
  console.log("  workflow " + d.workflow.status);
  for (const x of d.steps) console.log(`    L${x.level} ${x.key.padEnd(11)} ${x.status.padEnd(10)} ${x.dependsOn.length ? "after " + x.dependsOn.join(", ") : ""}`);
  const m = d.steps.find(x => x.key === "merge"); console.log("  merge received: " + JSON.stringify(m.output.upstream));})'
broken=$(op -X POST -d '{"name":"broken","steps":[{"key":"a","name":"echo"},{"key":"b","name":"no-such-type","dependsOn":["a"]},{"key":"c","name":"echo","dependsOn":["b"]},{"key":"side","name":"echo","dependsOn":["a"]}]}' "$API/workflows" | json data.workflow.id)
for _ in $(seq 1 60); do
  [ "$(op "$API/workflows/$broken" | json data.workflow.status)" = RUNNING ] || break
  sleep 1
done
op "$API/workflows/$broken" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const d=JSON.parse(s).data;
  console.log("  failing workflow " + d.workflow.status + ": " + d.steps.map(x => x.key + "=" + x.status).join("  "));})'
pause "See both on the Workflows page, then continue to the chaos test"

step "6. Chaos: 45 tasks in flight, then SIGKILL a worker and FLUSHALL Redis"
ids=()
for _ in $(seq 1 45); do ids+=("$(submit '{"name":"sleep","payload":{"ms":3000}}')"); done
sleep 3
victim=$(docker compose ps -q worker | awk 'NR == 1')  # not head: see chaos-compose.sh
docker kill --signal KILL "$victim" >/dev/null
echo "  killed worker ${victim:0:12}"
docker compose exec -T redis redis-cli FLUSHALL >/dev/null
echo "  flushed Redis: $(sql "SELECT COUNT(*) FROM tasks WHERE status IN ('QUEUED','PROCESSING')") tasks had nothing left in the queue"
echo "  waiting for the reconciler to rebuild the queue from PostgreSQL…"
for id in "${ids[@]}"; do wait_for "$id" COMPLETED; done
echo "  all 45 COMPLETED; reconciler requeued $(sql "SELECT COUNT(DISTINCT task_id) FROM outbox_events WHERE event_type='TASK_REQUEUED'") of them"
docker compose up -d --scale worker=3 worker >/dev/null 2>&1
echo "  worker pool restored to 3"

step "Done. The stack is still running: http://localhost:3001. Stop it with: docker compose down -v"
