#!/usr/bin/env bash
set -euo pipefail

# Linux host networking reuses the PostgreSQL and Redis services from CI.
containers=()
cleanup() {
  local result=$?
  for container in "${containers[@]}"; do
    if [ "$result" -ne 0 ]; then
      docker logs "$container" >&2 || true
    fi
    docker rm -f "$container" >/dev/null || true
  done
  exit "$result"
}
trap cleanup EXIT

for service in api worker; do
  container="distributed-engine-smoke-${service}-$$"
  containers+=("$container")
  docker run -d --name "$container" --network host \
    -e NODE_ENV=production \
    -e POSTGRES_HOST="${POSTGRES_HOST:-localhost}" \
    -e POSTGRES_PORT="${POSTGRES_PORT:-5432}" \
    -e POSTGRES_DB="${POSTGRES_DB:-distributed_engine_test}" \
    -e POSTGRES_USER="${POSTGRES_USER:-postgres}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}" \
    -e REDIS_HOST="${REDIS_HOST:-localhost}" \
    -e REDIS_PORT="${REDIS_PORT:-6379}" \
    -e REDIS_PASSWORD="${REDIS_PASSWORD:-}" \
    "distributed-engine-${service}:ci" >/dev/null

  if [ "$service" = api ]; then
    url=http://localhost:3000/health
  else
    url=http://localhost:9000/healthz
  fi

  healthy=false
  for attempt in $(seq 1 60); do
    if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then
      echo "$service exited before becoming healthy" >&2
      exit 1
    fi
    if docker exec "$container" node --input-type=module -e '
      const response = await fetch(process.argv[1], { signal: AbortSignal.timeout(2000) });
      const body = await response.json();
      if (!response.ok || (body.data ? body.data.status !== "healthy" : body.status !== "alive")) {
        process.exit(1);
      }
    ' "$url" >/dev/null 2>&1; then
      healthy=true
      break
    fi
    sleep 1
  done
  if [ "$healthy" != true ]; then
    echo "$service did not become healthy" >&2
    exit 1
  fi
  echo "$service production image started and passed its health check"
done
