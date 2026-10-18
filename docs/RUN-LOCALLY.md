# Run it on your laptop

Every command below runs as written, in order, in one terminal at the
repository root. CI runs this same file top to bottom on every change
(`scripts/check-run-locally.sh`), so the commands keep working. The outputs
shown come from those runs; IDs and timings will differ on your machine.

Blocks marked *(in the browser)* are for you to look at; CI skips them.

## 0. What you need

- Docker Desktop (or Docker Engine with the compose plugin), with about
  4 GB of memory for Docker.
- git, curl and openssl (all present on macOS and Linux).
- Node.js 22 or newer, only for the short JSON one-liners below.

## 1. Start everything

```bash
bash scripts/demo-local.sh
```

<!-- output:start -->
```text
== Building and starting (first run builds the images: a few minutes)
== Checking
   ok    API /health (PostgreSQL + Redis)               200
   ok    API rejects /tasks without the operator key    401
   ok    API accepts the operator key                   200
   ok    Dashboard asks for a login                     401
   ok    Dashboard with the login from .env             200
   ok    Prometheus is ready                            200
   ok    Grafana is up                                  200
   ok    Prometheus scrapes the API and 3 workers       4/4 up
   ok    Worker containers running                      3
   Dashboard   http://localhost:3001   user ops, password in .env
   Grafana     http://localhost:3002   dashboard "Engine overview" (folder Engine)
   Prometheus  http://localhost:9090
   API         http://localhost:3000   operator key in .env (OPERATOR_API_KEY)
   Stop and delete everything: bash scripts/demo-local.sh --down

== Checking
   ok    API /health (PostgreSQL + Redis)               200
   ok    API rejects /tasks without the operator key    401
   ok    API accepts the operator key                   200
   ok    Dashboard asks for a login                     401
   ok    Dashboard with the login from .env             200
   ok    Prometheus is ready                            200
   ok    Grafana is up                                  200
   ok    Prometheus scrapes the API and 3 workers       4/4 up
   ok    Worker containers running                      3

   Dashboard   http://localhost:3001   user ops, password in .env
   Grafana     http://localhost:3002   dashboard "Engine overview" (folder Engine)
   Prometheus  http://localhost:9090
   API         http://localhost:3000   operator key in .env (OPERATOR_API_KEY)

   Stop and delete everything: bash scripts/demo-local.sh --down
```
<!-- output:end -->

*(in the browser)* Open the dashboard at http://localhost:3001 (user `ops`,
password in `.env`), and Grafana at http://localhost:3002 → Dashboards →
Engine → **Engine overview**. Keep Grafana open: every step below shows up
there within a few seconds.

## 2. Shell helpers for this session

```bash
set -a; source .env; set +a
API=http://localhost:3000
api()   { curl -s -H "Authorization: Bearer $OPERATOR_API_KEY" -H 'Content-Type: application/json' "$@"; }
field() { node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{let v=JSON.parse(s);for(const k of process.argv[1].split("."))v=v?.[k];console.log(typeof v==="object"?JSON.stringify(v):v)})' "$1"; }
sql()   { docker compose exec -T postgres psql -U postgres -d distributed_engine -c "$1"; }
wait_status() { for _ in $(seq 1 60); do s=$(api $API/tasks/$1 | field data.status); [ "$s" = "$2" ] && { echo "$1 -> $s"; return 0; }; sleep 1; done; echo "$1 is $s, not $2"; return 1; }
echo ready
```

<!-- output:start -->
```text
ready
```
<!-- output:end -->

## 3. One task, end to end

Submit, wait for it, then read the row the API answered from:

```bash
ID=$(api -X POST $API/tasks -d '{"name":"echo","payload":{"hello":"laptop"}}' | field data.id)
wait_status "$ID" COMPLETED
sql "SELECT status, retries, version, result FROM tasks WHERE id = '$ID'"
sql "SELECT event_type, status, attempts FROM outbox_events WHERE task_id = '$ID'"
```

<!-- output:start -->
```text
9664da412d0b4facfcfe01cdf373eaf7 -> COMPLETED
  status   | retries | version |                                result                                
-----------+---------+---------+----------------------------------------------------------------------
 COMPLETED |       0 |       3 | {"output": {"echo": {"hello": "laptop"}}, "processedBy": "dd4c2f0d"}
(1 row)

  event_type  |  status   | attempts 
--------------+-----------+----------
 TASK_CREATED | DELIVERED |        1
(1 row)
```
<!-- output:end -->

`version` counts every state change (QUEUED → PROCESSING → COMPLETED). The
outbox row is how the task reached Redis: written in the same transaction
as the task, published afterwards.

## 4. A retry you planned

`flaky` fails its first `failTimes` attempts on purpose:

```bash
ID=$(api -X POST $API/tasks -d '{"name":"flaky","payload":{"failTimes":2},"maxRetries":3}' | field data.id)
wait_status "$ID" COMPLETED
sql "SELECT status, retries, result FROM tasks WHERE id = '$ID'"
```

<!-- output:start -->
```text
e0eda8b2b38ff094a525311b701b0bca -> COMPLETED
  status   | retries |                              result                              
-----------+---------+------------------------------------------------------------------
 COMPLETED |       2 | {"output": {"succeededOnAttempt": 3}, "processedBy": "1bb10846"}
(1 row)
```
<!-- output:end -->

## 5. A tenant, with its own key, quota and idempotency

```bash
TENANT=$(api -X POST $API/tenants -d '{"name":"laptop-co","planId":"plan_starter"}')
TENANT_ID=$(echo "$TENANT" | field data.tenant.id)
TENANT_KEY=$(echo "$TENANT" | field data.apiKey)
tenant() { curl -s -H "Authorization: Bearer $TENANT_KEY" -H 'Content-Type: application/json' "$@"; }

BODY='{"name":"echo","payload":{"order":42},"idempotencyKey":"order-42"}'
FIRST=$(tenant -X POST $API/tenant/tasks -d "$BODY")
AGAIN=$(tenant -X POST $API/tenant/tasks -d "$BODY")
echo "first:  $(echo "$FIRST" | field data.id)  idempotent=$(echo "$FIRST" | field idempotent)"
echo "again:  $(echo "$AGAIN" | field data.id)  idempotent=$(echo "$AGAIN" | field idempotent)"
tenant $API/tenant/usage | field data.usage
```

<!-- output:start -->
```text
first:  1925ca2e342cfb6c0478d5eb50ceb495  idempotent=false
again:  1925ca2e342cfb6c0478d5eb50ceb495  idempotent=true
{"acceptedJobs":1,"computeUnits":0,"storageMb":0,"billingPeriodStart":"2026-10-18T00:00:00.000Z"}
```
<!-- output:end -->

The same key twice gives the same task, and the job is counted once.

## 6. A workflow (a diamond)

`start` runs first, `left` and `right` run in parallel, `join` waits for
both and receives their outputs:

```bash
WF=$(api -X POST $API/workflows -d '{"name":"diamond","steps":[
  {"key":"start","name":"echo","payload":{"n":1}},
  {"key":"left","name":"sleep","payload":{"ms":1500},"dependsOn":["start"]},
  {"key":"right","name":"echo","payload":{"side":"right"},"dependsOn":["start"]},
  {"key":"join","name":"echo","dependsOn":["left","right"]}]}' | field data.workflow.id)
for _ in $(seq 1 30); do [ "$(api $API/workflows/$WF | field data.workflow.status)" = COMPLETED ] && break; sleep 1; done
sql "SELECT step_key, status, started_at::time(0) AS started, completed_at::time(0) AS completed FROM tasks WHERE workflow_id = '$WF' ORDER BY started_at"
```

<!-- output:start -->
```text
step_key |  status   | started  | completed 
----------+-----------+----------+-----------
 start    | COMPLETED | 19:35:57 | 19:35:57
 left     | COMPLETED | 19:35:57 | 19:35:58

 right    | COMPLETED | 19:35:57 | 19:35:57
 join     | COMPLETED | 19:35:58 | 19:35:58
(4 rows)
```
<!-- output:end -->

## 7. A task scheduled for later

```bash
WHEN=$(node -e 'console.log(new Date(Date.now() + 10000).toISOString())')
ID=$(api -X POST $API/tasks -d "{\"name\":\"echo\",\"payload\":{},\"scheduledFor\":\"$WHEN\"}" | field data.id)
echo "now:   $(api $API/tasks/$ID | field data.status)"
wait_status "$ID" COMPLETED
```

<!-- output:start -->
```text
now:   SCHEDULED
04c32c9090c4f5c50e61614ddb4a9a2a -> COMPLETED
```
<!-- output:end -->

## 8. Break it: kill a worker in the middle of work

Thirty 4-second tasks, then a SIGKILL (no clean shutdown) to one worker.
Its tasks' leases expire and the other workers take them over:

```bash
for _ in $(seq 1 30); do api -X POST $API/tasks -d '{"name":"sleep","payload":{"ms":4000}}' > /dev/null; done
sleep 3
VICTIM=$(docker compose ps -q worker | awk 'NR == 1')
docker kill "$VICTIM" > /dev/null && echo "killed worker ${VICTIM:0:12}"
for _ in $(seq 1 90); do
  left=$(docker compose exec -T postgres psql -U postgres -d distributed_engine -tAc "SELECT count(*) FROM tasks WHERE status NOT IN ('COMPLETED','FAILED','CANCELLED')")
  [ "$left" = 0 ] && break; sleep 2
done
sql "SELECT status, count(*) FROM tasks WHERE name = 'sleep' GROUP BY status"
echo "reclaimed after the kill: $(docker compose logs worker 2>/dev/null | grep -c 'Reclaimed stalled task')"
docker start "$VICTIM" > /dev/null && echo "worker restarted"
```

<!-- output:start -->
```text
killed worker 612ebd94f8fb
  status   | count 
-----------+-------
 COMPLETED |    31
(1 row)

reclaimed after the kill: 5
worker restarted
```
<!-- output:end -->

31 `sleep` tasks: these 30 and the workflow step from step 6. Each
reclaim is a task the dead worker held, taken over once its lease expired
(and charged one retry). In Grafana: **Workers up** drops to 2, **Job
outcomes per worker** shows the others picking up the work.

## 9. Break it: stop Redis

The queue is gone for 20 seconds. Tasks are still accepted (PostgreSQL is
the source of truth); they wait in the outbox and go out when Redis is back.
`/health` says `degraded` but stays 200: the API still serves, so a load
balancer must keep sending it traffic:

```bash
docker compose stop redis > /dev/null && echo "Redis stopped"
for i in 1 2 3; do
  curl -s -o /dev/null -w "submit while Redis is down: %{http_code} in %{time_total}s\n" \
    -H "Authorization: Bearer $OPERATOR_API_KEY" -H 'Content-Type: application/json' \
    -X POST $API/tasks -d '{"name":"echo","payload":{}}'
done
curl -s -o /dev/null -w "/health: %{http_code} " $API/health; curl -s $API/health | field data
sql "SELECT status, count(*) FROM outbox_events WHERE status <> 'DELIVERED' GROUP BY status"
sleep 15
docker compose start redis > /dev/null && echo "Redis started"
for _ in $(seq 1 30); do
  pending=$(docker compose exec -T postgres psql -U postgres -d distributed_engine -tAc "SELECT count(*) FROM outbox_events WHERE status = 'PENDING'")
  [ "$pending" = 0 ] && break; sleep 1
done
echo "outbox PENDING after recovery: $pending"
```

<!-- output:start -->
```text
Redis stopped
submit while Redis is down: 201 in 0.003916s
submit while Redis is down: 201 in 0.002481s
submit while Redis is down: 201 in 0.003923s
/health: 200 {"status":"degraded","timestamp":"2026-10-18T19:36:48.718Z","outboxCircuitBreaker":"CLOSED","dependencies":{"postgres":"up","redis":"down"}}
 status  | count 
---------+-------
 PENDING |     3
(1 row)

Redis started
outbox PENDING after recovery: 0
```
<!-- output:end -->

In Grafana: **Queue (Redis)** goes DOWN and the queue panels show a gap (not
zeros), **Outbox lag** rises, then both recover.

## 10. Break it: freeze PostgreSQL

`docker pause` freezes the database without closing connections, like a
long stall. Readiness reports it within the 1-second budget instead of
hanging:

```bash
docker compose pause postgres > /dev/null && echo "PostgreSQL frozen"
curl -s -o /dev/null -m 5 -w "/ready while frozen: %{http_code} in %{time_total}s\n" $API/ready
curl -s -o /dev/null -m 5 -w "/live while frozen:  %{http_code}\n" $API/live
sleep 5
docker compose unpause postgres > /dev/null && echo "PostgreSQL resumed"
curl -s -o /dev/null -w "/ready after:        %{http_code}\n" $API/ready
```

<!-- output:start -->
```text
PostgreSQL frozen
/ready while frozen: 503 in 1.000984s
/live while frozen:  200
PostgreSQL resumed
/ready after:        200
```
<!-- output:end -->

`/live` stays 200 (the process is fine, so Kubernetes must not restart it);
`/ready` is 503 (so it gets no traffic) until the database answers.

## 11. Suspend a tenant

A suspended tenant cannot submit (403), and work it already has waits
instead of running; nothing is lost. Here a task is scheduled 6 seconds
ahead, the tenant is suspended, and the task falls due while suspended:

```bash
WHEN=$(node -e 'console.log(new Date(Date.now() + 6000).toISOString())')
HELD=$(tenant -X POST $API/tenant/tasks -d "{\"name\":\"echo\",\"payload\":{\"held\":true},\"scheduledFor\":\"$WHEN\"}" | field data.id)
api -X PUT $API/tenants/$TENANT_ID/status -d '{"status":"SUSPENDED"}' | field data.status
tenant -o /dev/null -w "new submission while suspended: %{http_code}\n" -X POST $API/tenant/tasks -d '{"name":"echo","payload":{}}'
sleep 12
echo "due 6 s ago, tenant suspended: $(api $API/tasks/$HELD | field data.status)"
api -X PUT $API/tenants/$TENANT_ID/status -d '{"status":"ACTIVE"}' | field data.status
wait_status "$HELD" COMPLETED
```

<!-- output:start -->
```text
SUSPENDED
new submission while suspended: 403
due 6 s ago, tenant suspended: QUEUED
ACTIVE
0667fe69c92172122f39520e246abfc9 -> COMPLETED
```
<!-- output:end -->

`QUEUED` while suspended: the scheduler released it on time, and the
outbox holds it back because a suspended tenant has no free slots.

## 12. A short load test

The load test brings up its own copy of the stack on the same ports, so stop
this one first. Three minutes of three tenants with a worker kill, a Redis
stop, a PostgreSQL freeze and an API kill in the middle:

```bash
bash scripts/demo-local.sh --down
DURATION_MIN=3 OUT=/tmp/load-out bash scripts/load-test.sh 2>&1 | tail -15
```

<!-- output:start -->
```text
Stopped and removed (data volumes deleted).

== Draining (up to 900 s for every accepted task to finish)
   drained in 57 s
== Checking
   ok: no accepted task lost
   ok: every accepted task finished
   ok: every task ended in its expected state
   ok: every workflow completed
   ok: every completed http task reached the sink
   ok: idempotent replays returned the same task
   ok: outbox has no PENDING events
   ok: outbox has no FAILED events
   ok: PostgreSQL connections stayed under max_connections
   ok: k6 completed
   RESULT: 2777 accepted, 15.26/s; submit p95 229.1 ms; drained in 57 s
== LOAD TEST PASSED
```
<!-- output:end -->

The full report is `/tmp/load-out/results.md`. The 60-minute run's numbers
are in [load-test.md](load-test.md).

## 13. Clean up

The load test removes its own stack. If you stopped before step 12:

```bash
bash scripts/demo-local.sh --down
```

<!-- output:start -->
```text
Stopped and removed (data volumes deleted).
```
<!-- output:end -->

## When something does not work

- `docker compose ps` shows what is running; `docker compose logs --tail=50 api`
  (or `worker`, `postgres`, …) shows why something is not.
- Port already in use: another service on 3000, 3001, 3002, 5432, 6379 or 9090.
  Stop it, or stop an earlier run with `bash scripts/demo-local.sh --down`.
- Start from zero: `bash scripts/demo-local.sh --down`, delete `.env`, run step 1.
