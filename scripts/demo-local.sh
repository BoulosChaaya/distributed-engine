#!/usr/bin/env bash
# Starts the engine on this machine and checks every part answers.
#
#   bash scripts/demo-local.sh          start (or restart) and check
#   bash scripts/demo-local.sh --check  only check a running stack
#   bash scripts/demo-local.sh --down   stop and delete everything (volumes too)
#
# Runs PostgreSQL, Redis, the API, 3 workers, the dashboard, Prometheus and
# Grafana with docker compose, all built from this checkout. Needs Docker
# (Docker Desktop on macOS/Windows) and nothing else on the host: every check
# below uses curl and Docker only. Credentials go to .env (git-ignored) on
# the first run and are reused afterwards. See docs/RUN-LOCALLY.md.
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose --profile metrics)
SERVICES=(postgres redis api worker web prometheus grafana)

if [ "${1:-}" = "--down" ]; then
  "${compose[@]}" down -v --remove-orphans
  echo "Stopped and removed (data volumes deleted)."
  exit 0
fi

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

if [ "${1:-}" != "--check" ]; then
  echo "== Building and starting (first run builds the images: a few minutes)"
  "${compose[@]}" up -d --build --wait --scale worker=3 "${SERVICES[@]}"
fi

echo "== Checking"
failed=0
check() { # check "name" expected_code curl-args...
  local name=$1 want=$2; shift 2
  local got
  got=$(curl -s -o /dev/null -m 5 -w '%{http_code}' "$@" || true)
  if [ "$got" = "$want" ]; then
    printf '   ok    %-46s %s\n' "$name" "$got"
  else
    printf '   FAIL  %-46s %s (expected %s)\n' "$name" "$got" "$want"
    failed=1
  fi
}
check "API /health (PostgreSQL + Redis)" 200 http://localhost:3000/health
check "API rejects /tasks without the operator key" 401 http://localhost:3000/tasks
check "API accepts the operator key" 200 -H "Authorization: Bearer ${OPERATOR_API_KEY}" http://localhost:3000/tasks
check "Dashboard asks for a login" 401 http://localhost:3001/
check "Dashboard with the login from .env" 200 -u "${DASHBOARD_USER}:${DASHBOARD_PASSWORD}" http://localhost:3001/
check "Prometheus is ready" 200 http://localhost:9090/-/ready
check "Grafana is up" 200 http://localhost:3002/api/health

# Prometheus needs one scrape after start; give it up to 30 s.
up=""
for _ in $(seq 1 30); do
  up=$(curl -s --get --data-urlencode 'query=sum(up)' http://localhost:9090/api/v1/query \
    | sed -n 's/.*"value":\[[^,]*,"\([0-9.]*\)"\].*/\1/p')
  [ "$up" = 4 ] && break
  sleep 1
done
if [ "$up" = 4 ]; then
  printf '   ok    %-46s %s\n' "Prometheus scrapes the API and 3 workers" "4/4 up"
else
  printf '   FAIL  %-46s %s\n' "Prometheus scrapes the API and 3 workers" "${up:-0}/4 up"
  failed=1
fi

workers=$("${compose[@]}" ps -q worker | wc -l | tr -d ' ')
printf '   %-5s %-46s %s\n' "$([ "$workers" = 3 ] && echo ok || echo FAIL)" "Worker containers running" "$workers"
[ "$workers" = 3 ] || failed=1

cat <<EOF

   Dashboard   http://localhost:3001   user ${DASHBOARD_USER}, password in .env
   Grafana     http://localhost:3002   dashboard "Engine overview" (folder Engine)
   Prometheus  http://localhost:9090
   API         http://localhost:3000   operator key in .env (OPERATOR_API_KEY)

   Stop and delete everything: bash scripts/demo-local.sh --down
EOF

if [ "$failed" -ne 0 ]; then
  echo
  echo "Something did not answer. Logs: docker compose logs --tail=50 <service>"
  exit 1
fi
