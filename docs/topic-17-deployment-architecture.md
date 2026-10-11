# Topic 17 — Deployment Architecture Contract

Implementation contract for the distributed-engine Kubernetes deployment hardening.
Frozen after architecture assessment, corrections, adversarial review, and closure pass.

Base commit: `6f62776325d9e3c8356868001c20f493d81f1a1f`

---

## 1. Scope

Harden the existing distributed-engine for Kubernetes deployment:
authenticated Redis, worker health monitoring, strict configuration validation,
container security, CI pipeline with GHCR publishing, backup/restore drill,
and operational runbooks — within the bounds of a learning project.

**Out of scope:** HA, DR, exactly-once execution, automated CD, PgBouncer,
Prometheus/Grafana in K8s, enterprise secret management, public HTTPS.

---

## 2. Frozen Architecture Decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Redis auth | `--requirepass $(REDISCLI_AUTH)` with Secret. `/proc/1/cmdline` exposure documented. |
| 2 | Redis memory | `--maxmemory 400mb --maxmemory-policy noeviction` |
| 3 | PG SecurityContext | Root start (gosu drops to postgres). `allowPrivilegeEscalation: false` (proposed — requires live acceptance gate). Drop `NET_RAW`. No `runAsNonRoot`. No `readOnlyRootFilesystem`. |
| 4 | API/Worker SecurityContext | `runAsNonRoot: true`, uid 1001, `readOnlyRootFilesystem: true`, drop ALL, emptyDir `/tmp`. |
| 5 | Redis SecurityContext | `runAsNonRoot: true`, uid 999, `readOnlyRootFilesystem: true`, drop ALL. |
| 6 | Worker startup order | Config → clients → migrations → health server → BullMQ Worker → handlers → heartbeat. |
| 7 | Worker probes | startupProbe + livenessProbe on `/healthz:9000`. No readinessProbe. |
| 8 | `/healthz` semantics | Event-loop/process liveness only. No dependency checks. |
| 9 | Config validation | Strict: regex integer parse, range check, throw on malformed. Unset → default. |
| 10 | PG connections | `max_connections=150`. Pools: API=20, Worker=10. |
| 11 | Recovery runbook | Guarded manual reset with inline duplicate-execution danger notice. |
| 12 | CI (PR) | `pnpm install --frozen-lockfile` → `pnpm run build` → configure Redis auth → `pnpm test` (PG+Redis services, `REDIS_PASSWORD` set) → Docker build verify. |
| 13 | CI (main) | Same as PR + GHCR publish with Git SHA tag. |
| 14 | Image identity | `ghcr.io/bouloschaaya/distributed-engine-{api,worker}:<full-git-sha>` (commit-addressed tag, mutable). Deployment verified by pod `imageID` (OCI digest, immutable). No `latest`. |
| 15 | Deployment | Manual operator: edit manifest image → `kubectl apply` → verify. |
| 16 | Backup | `pg_dump` custom format → restore drill to temp DB → verify representative invariants. |
| 17 | Topic 16 gate | `verify-runtime.sh all` exit 0 required before LoadBalancer → ClusterIP. |

---

## 3. Application Changes

All in `apps/worker/src/index.ts` unless noted.

### 3.1 Strict config validation

Add `parseStrictInt(value, default, name)` using `/^-?\d+$/` regex. Malformed explicit
values throw at startup. Apply to `POSTGRES_PORT`, `REDIS_PORT`,
`SCHEDULE_LEASE_DURATION_MS`, `QUEUE_CONCURRENCY` with range enforcement.

### 3.2 Worker startup reordering

Move BullMQ Worker construction, event handlers, and heartbeat interval inside
the async IIFE, after `runMigrations` succeeds. Add HTTP health server
(port 9000, `/healthz`) started between migrations and Worker creation.

### 3.3 Partial-initialization ownership

Resources created inside the async IIFE use nullable references:

```typescript
let worker: Worker | undefined;
let healthServer: Server | undefined;
let heartbeatInterval: NodeJS.Timeout | undefined;
```

Shutdown checks existence before cleanup. `isShuttingDown` guard prevents
overlapping cleanup from concurrent SIGTERM/SIGINT. Second signal during
shutdown forces `process.exit(1)` (existing behavior, preserved).

### 3.4 Fatal initialization failure

```
runMigrations throws
  ↓
.catch(err):
  log original error
  ↓
  set 10s fatal-cleanup timeout (safety net)
  ↓
  best-effort: redisClient.disconnect(), pgPool.end() — each in try/catch
  ↓
  clearTimeout(fatal-cleanup)
  process.exit(1)
```

The 10-second fatal-cleanup timeout prevents a hung `pgPool.end()` from keeping
a broken process alive. Original initialization error is always logged first;
cleanup errors are logged separately, never hiding the root cause.

Kubernetes sees exit code 1, applies `restartPolicy: Always`, and the pod
enters CrashLoopBackOff with exponential backoff if PG remains unavailable.

### 3.5 Shutdown sequence

```
SIGTERM received by dumb-init (PID 1) → forwarded to node (PID 2)
  ↓
gracefulShutdown():
  guard: isShuttingDown → force exit(1)
  isShuttingDown = true
  ↓
  if (heartbeatInterval) clearInterval(heartbeatInterval)
  best-effort: redisClient.del(worker status key)
  ↓
  30s forced-exit timeout
  ↓
  if (healthServer) healthServer.close()        — stop accepting probes, fire-and-forget
  if (worker) await worker.close()              — drains in-flight jobs (up to lockDuration)
  redisClient.disconnect()
  await pgPool.end()
  await shutdownTelemetry()
  ↓
  clearTimeout(forced-exit)
  process.exit(0)
```

`healthServer.close()` is non-blocking (no `await`). It stops accepting new
connections but does not wait for existing ones to drain — the liveness probe
is the only client, and once shutdown begins the pod is already terminating.
This consumes negligible time from the termination budget.

### 3.6 preStop removal

The existing `preStop: sleep 5` in `k8s/worker.yaml` is removed. Its purpose is
traffic draining when Kubernetes removes a pod's endpoint from a Service. Workers
have no Service — they pull from BullMQ. The 5-second sleep delays SIGTERM
delivery for no benefit and reduces the available shutdown budget.

After removal: `terminationGracePeriodSeconds` remains 40. The worker's 30s
internal forced-exit timeout runs entirely within that budget with 10s margin.

The `terminationGracePeriodSeconds` countdown includes preStop execution time.
Kubernetes documentation states: "The grace period countdown begins before the
PreStop hook is executed." With preStop removed, the full 40s is available for
graceful shutdown.

### 3.7 Health server semantics

`/healthz` → HTTP 200 `{"status":"alive"}`. Any other path → 404.

No dependency checks. The probe tests whether the Node.js event loop can
respond to an HTTP request. A blocked event loop, a deadlocked process, or
a crashed process all fail this check.

---

## 4. Kubernetes Changes

### 4.1 redis.yaml

- Add `REDISCLI_AUTH` env from `redis-secret` Secret
- Extend command: `--requirepass $(REDISCLI_AUTH) --maxmemory 400mb --maxmemory-policy noeviction`
- Probes unchanged (`redis-cli ping` auto-authenticates via `REDISCLI_AUTH` env)
- Add securityContext: `runAsNonRoot: true`, `runAsUser: 999`, `runAsGroup: 1000`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, drop ALL capabilities

### 4.2 worker.yaml

- Add `REDIS_PASSWORD` env from `redis-secret`
- Remove `preStop: sleep 5`
- Add startupProbe: httpGet `/healthz:9000`, periodSeconds 5, failureThreshold 12, timeoutSeconds 3
- Add livenessProbe: httpGet `/healthz:9000`, periodSeconds 10, failureThreshold 3, timeoutSeconds 3
- Add securityContext: `runAsNonRoot: true`, `runAsUser: 1001`, `runAsGroup: 1001`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, drop ALL capabilities
- Add emptyDir volume mounted at `/tmp`

### 4.3 api.yaml

- Add `REDIS_PASSWORD` env from `redis-secret`
- Add securityContext: same as worker (uid 1001, readOnlyRootFilesystem, drop ALL)
- Add emptyDir volume mounted at `/tmp`

### 4.4 postgres.yaml

- Add command: `["postgres", "-c", "max_connections=150"]`
- Add securityContext: `allowPrivilegeEscalation: false` (proposed), drop `NET_RAW` capability

**`allowPrivilegeEscalation: false` rationale:** This sets the Linux `no_new_privs` bit
(via `prctl PR_SET_NO_NEW_PRIVS`). `no_new_privs` only constrains what `execve()` can
grant — it prevents setuid/setgid bits on binaries from elevating privileges during exec.
It does NOT block direct `setuid(2)`/`setgid(2)` syscalls from a process already running
as root. `gosu` works by calling `setuid()`/`setgid()` to DROP from root to the postgres
user (UID 999), then calling `execve()` to replace itself with the target process. Since
the privilege change is a voluntary drop via syscall (not a setuid-binary escalation),
`no_new_privs` does not intercept it. The subsequent exec runs as the already-switched
UID 999 and cannot re-escalate — which is the desired security outcome.

**Evidence:** Linux kernel `no_new_privs` documentation states "it does not prevent
privilege changes that do not involve execve(). An appropriately privileged task can
still call setuid(2)." `gosu` source confirms it uses `syscall.Setgroups`/`Setgid`/`Setuid`
then `syscall.Exec`, and explicitly rejects running with the setuid bit set on its own binary.

**Classification:** Strong inference from kernel documentation and gosu source analysis.
Not yet verified in live Kubernetes runtime.

**Live acceptance gate (required before freeze):** Deploy `postgres:16-alpine` pod with
`allowPrivilegeEscalation: false`, `drop: [NET_RAW]`, no `runAsNonRoot`, fresh PVC.
Verify: postgres starts, accepts connections, `initdb` completes on fresh volume.
If the gate fails, fall back to `allowPrivilegeEscalation: true` and document why.

### 4.5 New: k8s/redis-secret.yaml

Template with placeholder password value. Actual password created by operator before deployment.

---

## 5. CI / GHCR Artifact Flow

### 5.1 Workflow: `.github/workflows/ci.yml`

**PR trigger** (`pull_request` to `main`):

```
checkout
→ pnpm install --frozen-lockfile
→ pnpm run build
→ pnpm test                         # root jest.config.cjs, maxWorkers:1
→ docker build -f Dockerfile.api    # verify only, no push
→ docker build -f Dockerfile.worker # verify only, no push
```

**Main trigger** (`push` to `main`):

```
same as PR
→ docker login ghcr.io (GITHUB_TOKEN)
→ docker build + tag with ${GITHUB_SHA}
→ docker push ghcr.io/bouloschaaya/distributed-engine-api:<sha>
→ docker push ghcr.io/bouloschaaya/distributed-engine-worker:<sha>
→ output: image digests
```

### 5.2 CI environment

Service containers:

| Service | Image | Credentials | Env vars |
|---|---|---|---|
| PostgreSQL | `postgres:16-alpine` | user `postgres`, password `postgres`, db `distributed_engine_test` | Match `TEST_POSTGRES_URL` default |
| Redis | `redis:7-alpine` | Password set via `CONFIG SET requirepass` after startup | `REDIS_PASSWORD=testpassword123` |

Tests connect via `TEST_POSTGRES_URL` (default `postgresql://postgres:postgres@localhost:5432/distributed_engine_test`) and
`TEST_REDIS_HOST`/`TEST_REDIS_PORT` (default `localhost:6379`).

### 5.2.1 CI Redis authentication

CI Redis is configured with a password to exercise the authenticated connection path:

1. Redis service starts without auth (GitHub Actions `services` does not support `command:`)
2. A CI step runs `docker exec ${{ job.services.redis.id }} redis-cli CONFIG SET requirepass testpassword123`
3. `REDIS_PASSWORD=testpassword123` is set as an environment variable for subsequent steps
4. `pnpm test` runs with `REDIS_PASSWORD` available to the application code

**Existing test IORedis constructors** (in `outbox-publisher.test.ts`, `bullmq-integration.test.ts`,
`telemetry.test.ts`) currently create connections without a `password` option. These must be
updated during implementation to read the password from the environment:

```typescript
const TEST_REDIS_PASSWORD = process.env.REDIS_PASSWORD;
// in IORedis constructor:
redis = new IORedis({
  host: TEST_REDIS_HOST,
  port: TEST_REDIS_PORT,
  password: TEST_REDIS_PASSWORD,   // undefined when unset (local dev without auth)
  maxRetriesPerRequest: null,
});
```

IORedis accepts `password: undefined` and skips AUTH — so the same constructor works
both with CI Redis auth and local development without auth.

This ensures CI exercises the same `REDIS_PASSWORD` → IORedis `password` path that
production Kubernetes uses. The wiring gap is closed without test-specific Redis
configuration diverging from the application path.

### 5.3 Node/pnpm versions

Dockerfiles use `node:20-alpine`. Runtime environment: Node v22.22.0, pnpm 10.28.0.
No `.nvmrc` or `.node-version` file exists. CI should pin `node: 20` to match
the Docker build image. The discrepancy between development (22) and Docker (20)
is pre-existing and outside Topic 17 scope.

### 5.4 CI commands rationale

- `pnpm install --frozen-lockfile`: Reproducible dependency resolution.
- `pnpm run build`: TypeScript compilation (`tsc` in each workspace). Catches type errors.
- `pnpm test`: Runs root `jest.config.cjs` which aggregates all workspace jest configs with `maxWorkers: 1`. Sequential execution prevents integration test interference.
- Docker build: Verifies images build successfully. PR builds are not pushed.
- `pnpm run lint` is NOT included. Only `apps/web` has a lint script (Next.js). API, worker, and shared have no linter configured. Root `pnpm -r run lint` would fail on packages without a lint script.
- `pnpm run verify` (`build && test`) could replace separate build+test steps but obscures CI reporting. Separate steps preferred for clearer failure diagnosis.

---

## 6. Worker Startup / Shutdown Lifecycle

### 6.1 Startup

```
process starts
  │
  ├─ initTelemetry()                    sync, module level
  ├─ create logger                      sync
  ├─ parseStrictInt / validate config   sync, throws on malformed
  ├─ new Pool(...)                      lazy (no connection yet)
  ├─ new IORedis(...)                   eager connect, retries internally
  ├─ construct repositories             sync
  ├─ validateScheduleLeaseDuration()    sync, throws on invalid
  │
  └─ async IIFE with .catch(fatal):
       │
       ├─ await runMigrations(pgPool)    first PG connection
       ├─ start healthServer on :9000    startupProbe can now pass
       ├─ new Worker('tasks', ...)       begins consuming from BullMQ
       ├─ register Worker event handlers
       ├─ heartbeatInterval = setInterval(updateWorkerStatus, 10000)
       ├─ await updateWorkerStatus()
       └─ log "Worker initialized"
```

### 6.2 Probe timing

**startupProbe:** periodSeconds=5, failureThreshold=12, timeoutSeconds=3.
Maximum startup budget: 60s. Before the health server starts, probes get
connection refused — Kubernetes counts these as failures against the startup
budget. Once `/healthz` responds 200, the startupProbe passes and the
livenessProbe takes over. The startupProbe prevents the livenessProbe from
killing the container during initialization.

**livenessProbe:** periodSeconds=10, failureThreshold=3, timeoutSeconds=3.
After startup completes, if the event loop blocks, the HTTP server cannot
respond. Three consecutive failures (worst case ~30s) trigger container restart.

### 6.3 Shutdown timeline

```
K8s sends SIGTERM to dumb-init (PID 1)
  │  terminationGracePeriodSeconds countdown begins (40s)
  │
  ├─ dumb-init forwards SIGTERM to node (PID 2)
  │
  ├─ gracefulShutdown() begins
  │    30s internal forced-exit timeout starts
  │    ├─ clear heartbeat, delete worker key
  │    ├─ healthServer.close()                  non-blocking
  │    ├─ await worker.close()                  up to lockDuration (30s)
  │    ├─ redisClient.disconnect()
  │    ├─ await pgPool.end()
  │    ├─ await shutdownTelemetry()
  │    └─ process.exit(0)
  │
  ├─ if 30s timer fires: process.exit(1)
  │
  └─ if 40s K8s grace fires: SIGKILL
```

Budget: 30s for worker.close + cleanup. 10s margin before K8s SIGKILL.

---

## 7. Redis-Loss Recovery Contract

### 7.1 PostgreSQL invariants protected

These guards exist in current code and compose to prevent PG state corruption:

- `SELECT ... FOR UPDATE` row lock (task-repository.ts:154)
- Version guard: `current.version !== expectedVersion` → StaleVersionError (line 166)
- State machine: `assertValidTransition()` — terminal states cannot be re-entered (line 171)
- Claim token: PROCESSING tasks require matching token for transition (lines 173-178)
- Reclaim expiry: unexpired claims cannot be stolen (lines 283-286)

### 7.2 What is NOT protected

- **Duplicate external side effects.** Work executes before completion guards. Real side effects (API calls, emails) would execute and cannot be rolled back.
- **Ownership-loss timing gap.** Between `ownershipLost = true` (async interval) and the cooperative check at process-task.ts:302, work already done is committed.
- **Operator TOCTOU.** Between checking Redis (job absent) and resetting PG, another publisher could create a new job.
- **BullMQ retention window.** `removeOnComplete: 100` — after eviction, jobId becomes reusable and Queue.add creates a new job.

### 7.3 Runbook policy

Guarded manual reset. The runbook provides diagnostic queries and a reset procedure.

**Every reset command must be immediately preceded by:**

> **DANGER — DUPLICATE EXECUTION POSSIBLE**
>
> This manual recovery action may cause the task to execute again and may
> duplicate external side effects. PostgreSQL version/claim guards protect
> task-state transitions; they do NOT provide exactly-once execution.
> BullMQ jobId deduplication does not protect a job that has already
> disappeared from Redis.
>
> Proceed only after verifying the task is non-terminal and the BullMQ
> job is absent.

**Every reset command must be immediately followed by:**

1. Inspect affected-row count. `0 rows` → STOP, do not retry.
2. Expected row count must exactly match intended event count.
3. Re-inspect task and outbox state.
4. Observe resulting BullMQ publication.
5. Observe resulting task execution.
6. Record that duplicate external side effects remain possible.

The phrase "safe recovery" must not appear. Use: "guarded manual recovery with
duplicate-execution risk."

---

## 8. PostgreSQL Capacity Boundary

### 8.1 Theoretical connection budgets (static, not observed)

| Topology | API pools | Worker pools | Total |
|---|---|---|---|
| Default (3 API, 5 Worker) | 60 | 50 | 110 |
| HPA max (10 API, 20 Worker) | 200 | 200 | 400 |

### 8.2 Configuration

`max_connections=150` — default topology (110) plus headroom for monitoring,
migrations, operator sessions, and rolling-update connection churn.

PG default `max_connections` is 100, which is below the default topology's 110.
Explicit configuration is mandatory.

### 8.3 Unsupported capacity

HPA maximum (400 connections) exceeds `max_connections=150`. Scaling beyond
default topology requires capacity review: increase `max_connections` with
PG memory assessment, reduce pool sizes, or add PgBouncer.

No claim is made about what 512Mi "safely supports" in connection count or
memory consumption. First indicator of trouble: OOMKill on PG pod.

### 8.4 Required live measurements

- `SELECT count(*) FROM pg_stat_activity;` under normal load
- PG pod memory usage (RSS) under sustained task processing

---

## 9. Backup / Restore Drill

### 9.1 Procedure

Backup: `pg_dump -U postgres distributed_engine --format=custom`
Restore: `pg_restore` to temporary database `distributed_engine_restore_test`

### 9.2 Verification queries

Record source values BEFORE backup, then compare against the restored database:

| Invariant | Source query (before backup) | Restored query | Pass condition |
|---|---|---|---|
| Schema migration count | `SELECT count(*) FROM schema_migrations` | Same query on restored DB | Exact match |
| Task count | `SELECT count(*) FROM tasks` | Same query on restored DB | Exact match |
| Task status distribution | `SELECT status, count(*) FROM tasks GROUP BY status` | Same query on restored DB | Exact match per status |
| Outbox event count | `SELECT count(*) FROM outbox_events` | Same query on restored DB | Exact match |
| Tenant count (if populated) | `SELECT count(*) FROM tenants` | Same query on restored DB | Exact match |
| Schedule count (if populated) | `SELECT count(*) FROM recurring_schedules` | Same query on restored DB | Exact match |
| CHECK constraints | `SELECT conname FROM pg_constraint WHERE contype='c' AND conrelid='tasks'::regclass` | Same query on restored DB | Same constraint names |
| Indexes | `SELECT indexname FROM pg_indexes WHERE tablename='tasks'` | Same query on restored DB | Same index names |
| Foreign keys | `SELECT conname FROM pg_constraint WHERE contype='f'` | Same query on restored DB | Same constraint names |

### 9.3 What the drill demonstrates

Successful logical backup/restore and verification of selected representative
schema and data invariants. It does not prove exhaustive data fidelity,
and backup/restore duration is recorded only after the live drill.

### 9.4 What the drill does NOT prove

HA, disaster recovery, point-in-time recovery, backup scheduling,
cross-region backup, guaranteed RTO, or guaranteed RPO.

---

## 10. Topic 16 Dependency

Istio Ambient mTLS verification (`verify-runtime.sh all` exit 0) is required
before changing API Service from LoadBalancer to ClusterIP. All Istio behavior
referenced in this project is documented upstream behavior, not behavior verified
in our live cluster, until that script passes.

---

## 11. Acceptance Matrix

### Static

- No raw `parseInt` in worker/index.ts
- BullMQ Worker constructed after `runMigrations` in async IIFE
- `.catch()` on async IIFE
- No readinessProbe in worker manifest
- Health server port matches probe port
- No `preStop` in worker manifest
- Nullable resource references in shutdown path

### CI

- `pnpm install --frozen-lockfile` succeeds
- `pnpm run build` succeeds (TypeScript compilation)
- Redis `CONFIG SET requirepass` step succeeds
- `pnpm test` passes with PG+Redis (authenticated) service containers
- `docker build -f Dockerfile.api .` succeeds
- `docker build -f Dockerfile.worker .` succeeds

### Docker

- Worker health: `curl localhost:9000/healthz` → HTTP 200
- Worker bad config: `docker run -e QUEUE_CONCURRENCY=abc ...` → exit 1 with error message

### Kubernetes Live

- Redis auth: `kubectl exec redis-0 -- redis-cli ping` → PONG (via REDISCLI_AUTH)
- Redis reject unauth: `kubectl exec redis-0 -- env -u REDISCLI_AUTH redis-cli ping` → NOAUTH
- Redis uid: `kubectl exec redis-0 -- id` → uid=999 (verify Alpine tag)
- PG max_connections: `psql -c "SHOW max_connections;"` → 150
- PG securityContext: fresh-PVC startup succeeds with `allowPrivilegeEscalation: false` (if gate fails, revert to `true` and update §4.4)
- Worker pods: all pass startupProbe, livenessProbe active
- Worker liveness (event-loop hang): `kubectl exec <pod> -- kill -STOP 2` → liveness probe fails within ~30s, container restarts within ~70s (see §11.1)
- Worker crash recovery: `kubectl exec <pod> -- kill -9 2` → container restarts, startupProbe passes
- API securityContext: verify non-root, read-only filesystem
- Image identity: pod status `imageID` digest matches GHCR published digest for deployed SHA

### §11.1 Liveness probe acceptance test detail

**Test: SIGSTOP on the node process (PID 2, not dumb-init PID 1).**

`dumb-init` is PID 1 in the worker container. Node.js is PID 2. The liveness
probe targets the Node HTTP server, so stopping PID 2 is the correct test.

```bash
kubectl exec <worker-pod> -- kill -STOP 2
```

PID 2 (node) freezes. The HTTP server cannot respond. The liveness probe
(httpGet, timeoutSeconds=3) times out. After `failureThreshold=3` consecutive
failures (~30s), Kubernetes restarts the container.

When Kubernetes terminates the container, it sends SIGTERM to PID 1 (dumb-init).
dumb-init forwards to PID 2, but PID 2 is SIGSTOP'd — SIGTERM remains pending.
After `terminationGracePeriodSeconds` (40s), Kubernetes sends SIGKILL, which
terminates the stopped process.

Total time from SIGSTOP to restart: ~70s worst case (30s probe failures +
40s termination grace). This is operationally acceptable for an acceptance test.

**This tests:** a process that is alive but unresponsive causes the liveness
probe to fail and Kubernetes to restart the container.

**This does NOT test:** event-loop blocking specifically (SIGSTOP is stronger —
it freezes at the kernel level). For this acceptance gate, the distinction
is immaterial: both produce an unresponsive HTTP server.

**Separate test — process crash restart:**

```bash
kubectl exec <worker-pod> -- kill -9 2
```

dumb-init detects child exit, re-exits. Container restarts. startupProbe passes.
This tests crash recovery, not liveness probe detection.

### Topic 16 Ambient Live

- `verify-runtime.sh all` → exit 0
- Only then: API Service type → ClusterIP

### Backup / Restore

- `pg_dump` → non-empty file
- `pg_restore` to temp DB → exit 0
- Representative verification queries pass
- Record measured backup/restore duration
- `dropdb` temp DB

### GHCR / Deployment Identity

Four distinct identifiers:

| Identifier | Type | Example | Mutable? |
|---|---|---|---|
| Git SHA | Source commit identity | `a2c3085...` | No (git object) |
| GHCR tag | Commit-addressed registry tag | `ghcr.io/.../worker:a2c3085...` | Yes (registry pointer) |
| OCI digest | Content-addressed image identity | `sha256:abc123...` | No (content hash) |
| Pod `imageID` | Runtime evidence of pulled image | `ghcr.io/...@sha256:abc123...` | No (kubelet-reported) |

- After GHCR push: record both the Git SHA tag and the OCI digest from the push output
- After deployment: `kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].imageID}'`
- Verify: the pod `imageID` digest matches the OCI digest published to GHCR for the deployed Git SHA
- The Git SHA tag is a mutable registry pointer — it can be overwritten. The OCI digest is the immutable content-addressed identity. Deployment verification must compare digests, not tags.

### Rollback

- Authoritative method: re-apply manifests with previous known-good SHA, then verify
- `kubectl rollout undo` is acceptable as emergency shortcut (uses K8s revision history)
- Recorded previous SHA+digest is stronger because it is deterministic and auditable
- API and Worker from one commit are one release unit — both must be rolled back together if either fails
- After rollback: verify both deployments show the intended previous image SHA/digest
- The system must not run mismatched API/Worker versions after partial rollout failure

---

## 12. Explicit Non-Guarantees

1. No exactly-once execution — at-least-once delivery by design
2. No PostgreSQL high availability — single StatefulSet replica
3. No Redis high availability — single StatefulSet replica
4. No automatic Redis-loss reconciliation — recovery is a guarded manual operator procedure with explicit duplicate-execution and duplicate-side-effect risk
5. No proven HPA-max capacity — requires capacity review before scaling
6. No cross-region or multi-zone disaster recovery
7. No public HTTPS — no domain/certificate supplied
8. No enterprise secret management — K8s Secrets only
9. No proven memory capacity — starting configuration, not measured guarantee
10. No automated continuous deployment — manual operator deployment
11. No monitoring dashboards or alerting — OTel traces only, no Prometheus in K8s
12. No SLA/SLO/SLI commitments
13. Redis password visible in `/proc/1/cmdline` inside Redis container
14. CI Redis uses `CONFIG SET requirepass` (not `--requirepass` startup flag) — exercises the application auth path but not the Redis server's startup-time authentication configuration

**Accurate characterization:** Production-oriented deployment architecture
demonstrating transactional outbox with at-least-once delivery, authenticated
infrastructure, container security hardening, CI-gated image publishing,
verified backup/restore, and Kubernetes health monitoring — with explicitly
documented single-replica, recovery, and capacity limitations.

---

## 13. Implementation Phases

### Phase 1: Worker application source

1. Add `parseStrictInt` / `parseStrictIntInRange`
2. Replace raw `parseInt` calls with validated versions
3. Reorder startup: move BullMQ Worker + handlers + heartbeat into async IIFE after migrations
4. Add nullable resource references for partial-initialization safety
5. Add `.catch()` with fatal-cleanup timeout and bounded teardown
6. Add HTTP health server (port 9000, `/healthz`)
7. Add health server close to shutdown sequence
8. Write `apps/worker/src/__tests__/config.test.ts`
9. Update test IORedis constructors to pass `password: process.env.REDIS_PASSWORD` (undefined-safe)

### Phase 2: Kubernetes manifests

1. Create `k8s/redis-secret.yaml` (template)
2. Update `k8s/redis.yaml` (auth, maxmemory, securityContext)
3. Update `k8s/worker.yaml` (REDIS_PASSWORD, probes, securityContext, remove preStop, emptyDir)
4. Update `k8s/api.yaml` (REDIS_PASSWORD, securityContext, emptyDir)
5. Update `k8s/postgres.yaml` (max_connections, securityContext with `allowPrivilegeEscalation: false`)

### Phase 3: CI

1. Create `.github/workflows/ci.yml`

### Phase 4: Documentation

1. Recovery runbook with inline danger notices
2. Deployment guide (GHCR SHA → manifest → apply → verify → rollback)
3. Backup/restore procedure

### Phase 5: Live verification (requires cluster)

1. Deploy and run acceptance matrix
2. Record image digests and pod imageIDs
3. Run liveness and crash-recovery tests
4. Run backup/restore drill and record duration
5. Topic 16 ambient verification (if cluster has Istio)
