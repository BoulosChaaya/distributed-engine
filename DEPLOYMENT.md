# Deployment & Operations

How to run the engine locally, with docker compose, and on Kubernetes (with or
without Istio ambient), and how to operate it.

Two of these paths are exercised on every pull request: the compose stack
(`full-stack.yml` → `compose-chaos`) and the Kubernetes + Istio manifests
(`full-stack.yml` → `kind-istio`). The commands below are the same ones CI runs.

## Credentials

| Secret | Used by | How to create |
|---|---|---|
| `OPERATOR_API_KEY` | API (operator routes), dashboard | `openssl rand -hex 32` (32+ characters) |
| `DASHBOARD_USER` / `DASHBOARD_PASSWORD` | dashboard Basic auth | any user; a long random password |
| PostgreSQL password | API, worker, PostgreSQL | `openssl rand -hex 16` |
| Redis password | API, worker, Redis (k8s) | `openssl rand -hex 16` |
| Tenant keys (`te_…`) | tenants | issued by `POST /tenants`, shown once |

Without `OPERATOR_API_KEY`, operator routes answer 503. Without dashboard
credentials, the dashboard answers 503. Nothing falls back to open access.
Never commit real values: `.env` is git-ignored, and `k8s/redis-secret.yaml`
is a template only.

## Local Development (processes)

```bash
pnpm install
docker compose up -d postgres redis

export OPERATOR_API_KEY="$(openssl rand -hex 32)"
pnpm -F @distributed-engine/api run dev        # :3000
pnpm -F @distributed-engine/worker run dev     # health :9000

curl http://localhost:3000/health
curl -H "Authorization: Bearer $OPERATOR_API_KEY" http://localhost:3000/metrics
```

## Docker Compose (whole stack)

```bash
cp .env.example .env            # fill OPERATOR_API_KEY, DASHBOARD_USER, DASHBOARD_PASSWORD
docker compose up -d --build --scale worker=3 postgres redis api worker web
open http://localhost:3001      # dashboard (Basic auth)
```

Optional profiles: `--profile tracing` (OTel collector + Jaeger on :16686),
`--profile logging` (Alloy + Loki + Grafana), `--profile observability` (both).

Chaos run (kills a worker and flushes Redis mid-run, then checks every task
completed): `bash scripts/chaos-compose.sh`.

## Images

CI builds all three images on every PR and runs them in
`scripts/smoke-production-images.sh`. On every push to `main` it publishes
them to GHCR:

```
ghcr.io/<owner>/distributed-engine-api:<git sha>
ghcr.io/<owner>/distributed-engine-worker:<git sha>
ghcr.io/<owner>/distributed-engine-web:<git sha>
```

To build them yourself:

```bash
docker build -f Dockerfile.api    -t distributed-engine-api:0.1.0 .
docker build -f Dockerfile.worker -t distributed-engine-worker:0.1.0 .
docker build -f Dockerfile.web    -t distributed-engine-web:0.1.0 .
```

All three run as a non-root user (uid 1001) under `dumb-init`.

## Kubernetes

Manifests are plain YAML with kustomizations:

- `k8s/`: namespace, ServiceAccounts, PostgreSQL, Redis, API, worker, web
- `k8s/istio/`: Gateway, HTTPRoute, L4 AuthorizationPolicies (needs Istio ambient)
- `ci/kind/`: the overlay CI uses on a single kind node (fewer replicas, `:ci` tags, ClusterIP)

### 1. Namespace and secrets

```bash
kubectl apply -f k8s/namespace.yaml
NS=distributed-engine
kubectl -n $NS create secret generic postgres-secret --from-literal=password="$(openssl rand -hex 16)"
kubectl -n $NS create secret generic redis-secret    --from-literal=password="$(openssl rand -hex 16)"
kubectl -n $NS create secret generic operator-secret --from-literal=api-key="$(openssl rand -hex 32)"
kubectl -n $NS create secret generic dashboard-secret \
  --from-literal=username=ops --from-literal=password="$(openssl rand -hex 16)"
```

### 2. Point at your images

```bash
cd k8s
kustomize edit set image \
  distributed-engine-api=ghcr.io/<owner>/distributed-engine-api:<sha> \
  distributed-engine-worker=ghcr.io/<owner>/distributed-engine-worker:<sha> \
  distributed-engine-web=ghcr.io/<owner>/distributed-engine-web:<sha>
cd ..
```

(Or write your own overlay next to `ci/kind/`. A kustomize overlay cannot live
inside the `k8s/` base directory.)

### 3. Deploy

```bash
kubectl apply -k k8s
for w in statefulset/postgres statefulset/redis deployment/api deployment/worker deployment/web; do
  kubectl -n $NS rollout status $w --timeout=300s
done
```

API and worker wait up to 60s for PostgreSQL at startup, so apply order does
not matter and they do not crash-loop while the database starts. They also
reconnect to Redis on their own.

### 4. Verify

```bash
KEY=$(kubectl -n $NS get secret operator-secret -o jsonpath='{.data.api-key}' | base64 -d)
kubectl -n $NS port-forward svc/api 3000:80 &
curl http://localhost:3000/health
curl -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"echo","payload":{"hello":"k8s"}}' http://localhost:3000/tasks

kubectl -n $NS port-forward svc/web 3001:80 &   # dashboard
```

### 5. Istio ambient (optional)

Requires Istio installed with `--set profile=ambient` and the Gateway API CRDs.
The namespace already carries `istio.io/dataplane-mode: ambient`.

```bash
kubectl apply -k k8s/istio
kubectl -n $NS wait gateway/engine-gateway --for=condition=Programmed --timeout=180s
GW_ADDR=<gateway address> OPERATOR_API_KEY=$KEY ./k8s/istio/verify-runtime.sh all
```

The verifier exits 0 only when every critical check ran and passed: enrollment,
mTLS certificates, Gateway identity, allowed and denied paths, and a task
through the Gateway. `k8s/istio/README.md` has the staged rollout with
rollback steps. `scripts/k8s-kind-e2e.sh` runs the whole sequence on a
throwaway kind cluster, exactly as CI does.

## Health Probes

| Endpoint | Auth | What it checks | 503 when |
|----------|------|----------------|----------|
| `/ready` | none | PostgreSQL | PG unreachable |
| `/live` | none | nothing | never |
| `/health` | none | PG, Redis, outbox circuit breaker, each within `HEALTH_DEPENDENCY_TIMEOUT_MS` (default 1000) | PG unreachable (`degraded` with 200 if only Redis/breaker) |
| worker `:9000/healthz` | none | process alive | — |
| dashboard `:3001/` | Basic | auth proxy up | expects 401 unauthenticated; 503 means credentials not configured |

`/ready` needs only PostgreSQL because accepting a task only writes to
PostgreSQL. With Redis down, tasks wait in the outbox and are delivered when
it returns.

## Graceful Shutdown

### API

1. SIGTERM; `preStop` sleeps 5s so the Service stops routing.
2. Server stops accepting connections.
3. Scheduler and task reconciler stop (each waits for an in-progress cycle).
4. Outbox publisher stops (waits for an in-progress poll).
5. In-flight requests drain (up to `GRACEFUL_SHUTDOWN_TIMEOUT_MS`).
6. Queue, Redis and PG connections close. Exit 0, or 1 if the drain timed out.

### Worker

1. SIGTERM: the worker stops taking new jobs.
2. In-flight jobs finish (up to 30s). A job cut off by the timeout keeps its
   lease until it expires, then is reclaimed by another worker or the
   reconciler.
3. Heartbeat key removed; connections close.

`terminationGracePeriodSeconds: 40` on both.

## Failure detection timing

A worker that dies (crash, OOM kill, node loss) is replaced after two clocks
run out: BullMQ's job lock, checked by a stall scan, hands the job to another
worker, and the task's lease in PostgreSQL lets that worker reclaim it. The
handler then starts again from the beginning.

| Variable (worker) | Default | Meaning |
|---|---|---|
| `WORKER_LOCK_DURATION_MS` | 30000 | BullMQ job lock; renewed by the live worker every half of it |
| `WORKER_STALLED_INTERVAL_MS` | 5000 | How often each worker scans for expired locks |
| `TASK_CLAIM_TTL_MS` | 30000 | Lease on the task row; renewed every third of it |

Defaults give about 35 s from a crash to the reclaim. Recorded with 10 s /
2 s / 10 s: 10 s. Lower values cost more renewal writes (Redis and
PostgreSQL), and a lock shorter than the longest event-loop pause marks a
healthy job as stalled, so keep the lock well above any synchronous work in a
handler. Keep the lock and the claim TTL equal.

## Scaling

- API: stateless; HPA 3–10 replicas on CPU/memory. Only one replica runs the
  reconciler at a time (advisory lock). Scheduler instances share work via
  `FOR UPDATE SKIP LOCKED`.
- Workers: stateless; HPA 5–20 replicas; `QUEUE_CONCURRENCY` jobs each.
- Queue-depth autoscaling is not configured. It needs Prometheus-format
  metrics and an adapter such as KEDA, and `/metrics` is JSON today.
- PostgreSQL connections: every pod has its own pool, so the worst case is
  `HPA maxReplicas × POSTGRES_MAX_CONNECTIONS` per service. The manifests set
  api 10 × 10 and worker 20 × 7 (`QUEUE_CONCURRENCY` + 2) = 240, and
  PostgreSQL `max_connections` = 260 (20 headroom), with 1 Gi of memory. CI
  runs `node scripts/check-db-connection-budget.mjs`, which fails if a change
  to replicas, pools or `max_connections` breaks that sum. PgBouncer in
  transaction mode is not a drop-in alternative: the reconciler and the
  migration runner rely on session-level advisory locks.

## Operations

```bash
H="Authorization: Bearer $OPERATOR_API_KEY"
curl -H "$H" http://localhost:3000/metrics | jq     # task counts, queue depth, outbox
curl -H "$H" http://localhost:3000/workers | jq     # live workers (heartbeats, 30s TTL)
curl -H "$H" http://localhost:3000/tenants | jq     # tenants
curl -H "$H" -X POST -H 'Content-Type: application/json' \
  -d '{"name":"Acme","planId":"plan_starter"}' http://localhost:3000/tenants   # returns the key once
```

Rolling update: `kubectl -n $NS set image deployment/api api=<image>` (and the
same for `worker` and `web`), then `kubectl -n $NS rollout status
deployment/api`. Deployments use `maxSurge: 1, maxUnavailable: 0`. Roll back
with `kubectl -n $NS rollout undo deployment/<name>`.

Logs are structured JSON (Pino). See ARCHITECTURE.md for the schema and the
Loki/Grafana setup.

## Troubleshooting

| Symptom | Check | Action |
|---------|-------|--------|
| `/tasks` returns 503 | `OPERATOR_API_KEY` set on the API? | Create `operator-secret` / set the env var |
| `/tasks` returns 401 | Bearer key matches? | Use the operator key; tenant keys only work on `/tenant/*` |
| Dashboard returns 503 | `DASHBOARD_USER` / `DASHBOARD_PASSWORD` | Create `dashboard-secret` |
| Dashboard "not connected to the engine" | `ENGINE_API_URL`, `OPERATOR_API_KEY` on web | Set both; key must equal the API's |
| `/health` degraded | Redis or circuit breaker | Restore Redis; delivery resumes on its own |
| Tasks sit in QUEUED | Workers running? Redis reachable? | If Redis lost jobs, the reconciler requeues them after the grace period ([docs/redis-loss-recovery.md](docs/redis-loss-recovery.md)) |
| Task FAILED at once with "No handler registered" | Task `name` | Use a registered type (`echo`, `sleep`, `flaky`, `http`) |
| `http` task FAILED "not in HTTP_TASK_ALLOWED_HOSTS" | Worker env | Add the host to `HTTP_TASK_ALLOWED_HOSTS` |
| StaleVersion / ClaimTokenMismatch in logs | — | Expected under contention or after a lease loss; only one writer wins |

## Backup

```bash
kubectl -n $NS exec postgres-0 -- pg_dump -U postgres distributed_engine > backup.sql
kubectl -n $NS exec -i postgres-0 -- psql -U postgres distributed_engine < backup.sql
```

See [docs/backup-restore.md](docs/backup-restore.md). Redis needs no backup:
it holds no state PostgreSQL cannot rebuild. AOF (`everysec`) limits churn
after a restart, and the reconciler requeues anything that was lost.
