# Redis-Loss Recovery

Redis is the delivery layer, not the record. When it loses jobs, the engine
recovers **automatically**. This page explains what happens, how to watch it,
and the few cases that still need an operator.

## What can lose work

- Redis restarts without persistence, or the AOF is lost or corrupted
- `FLUSHALL` / `FLUSHDB` (operator error)
- Eviction under memory pressure (the k8s Redis uses `noeviction` to prevent this)
- Failover to a replica that never received the write
- BullMQ itself giving up on a job: after `maxStalledCount` stalls the job is
  moved to `failed` without running the worker's failure handler

In every case the PostgreSQL row is left in `QUEUED` or `PROCESSING`, and no
job exists that will ever move it.

## What recovers it

`TaskReconciler` runs inside the API (one replica at a time, via an advisory
lock) every `RECONCILER_INTERVAL_MS`:

1. **Candidates (PostgreSQL only).** `QUEUED` tasks untouched for longer than
   `RECONCILER_QUEUED_GRACE_MS`, plus `PROCESSING` tasks whose lease has
   expired. Tasks with a `PENDING` outbox event are excluded, because the
   publisher already owns those.
2. **Ask BullMQ** about each candidate's job (`jobId = taskId`):
   - `waiting`, `delayed`, `prioritized`, `active`: alive, left alone.
   - missing, `completed`, `failed`, `unknown`: orphan. A finished job is
     removed first, because re-adding the same jobId would otherwise be a
     silent no-op.
   - Redis error: the whole cycle is skipped. "Missing" and "unreachable"
     cannot be told apart, so the reconciler does not guess.
3. **Repair each orphan** in its own transaction, with the row locked and the
   version re-checked:
   - `QUEUED` → write a `TASK_REQUEUED` outbox event.
   - `PROCESSING` → an attempt was lost, so charge one retry, clear the claim,
     go back to `QUEUED` and write the event. If the retry budget is already
     spent, mark it `FAILED` instead, so a poison task cannot loop forever.
4. The normal outbox publisher delivers the event, with the same circuit
   breaker, fairness and tracing as any other task.

| Setting | Default | Meaning |
|---|---|---|
| `RECONCILER_INTERVAL_MS` | 30000 | Time between cycles (min 1000) |
| `RECONCILER_QUEUED_GRACE_MS` | 60000 | How long a QUEUED task may sit before its job is checked (min 5000) |
| `RECONCILER_BATCH_SIZE` | 100 | Tasks checked per cycle |

Worst-case time to recovery is roughly grace + interval, plus the lease TTL
(30s) for tasks that were PROCESSING.

## How it is proven

- `packages/shared/src/__tests__/task-reconciler.test.ts`: every branch above
  against real PostgreSQL and Redis, plus a chaos test that wipes the queue
  mid-run. All 30 tasks complete, each executed once. With the reconciler off,
  24 of 30 stay stuck.
- CI `full-stack.yml` → `compose-chaos`: the full compose stack with three
  workers. One worker is SIGKILLed and Redis is FLUSHALLed while 60 tasks are
  in flight. All 60 still complete, none twice in PostgreSQL, and nothing is
  left in the outbox.

## Watching it

```bash
# API logs (one line per repaired task, one summary per cycle)
kubectl -n distributed-engine logs deploy/api | grep -E 'Orphaned task|Reconciled orphaned'
# or: docker compose logs api | grep -E 'Orphaned task|Reconciled orphaned'
```

```sql
-- tasks the reconciler requeued, most recent first
SELECT task_id, created_at, status
FROM outbox_events
WHERE event_type = 'TASK_REQUEUED'
ORDER BY created_at DESC
LIMIT 50;

-- anything still waiting longer than you expect
SELECT id, name, status, retries, max_retries, updated_at, claim_expires_at
FROM tasks
WHERE status IN ('QUEUED', 'PROCESSING')
ORDER BY updated_at
LIMIT 50;
```

## When an operator is still needed

- **Redis is down, not just emptied.** The reconciler skips its cycles and the
  outbox circuit breaker opens. Nothing is lost: tasks wait in PostgreSQL.
  Restore Redis and both resume on their own. `/health` shows
  `redis: down` and the breaker state. The API retries the connection every
  250 ms, backing off to every 2 s at most, so publishing resumes within about
  3 s of Redis coming back, however long the outage lasted (BullMQ's own
  default backs off to 20 s, which delayed recovery by ~17 s after a
  one-minute outage).
- **Outbox events marked `FAILED`** (publishing exhausted `OUTBOX_MAX_ATTEMPTS`,
  e.g. a long Redis outage). Their tasks are still `QUEUED`, so the reconciler
  picks them up after the grace period. No manual reset is needed. The FAILED
  event rows stay as history.
- **External side effects.** PostgreSQL records each outcome exactly once, but
  a task that was mid-execution when its worker died runs again. Handlers that
  call other systems must be idempotent: send an idempotency key, or check
  before acting. The engine cannot undo an email that was already sent.
- **A task you believe is wrong to retry.** Cancel it (`PUT /tasks/:id/cancel`
  or the dashboard) before the next cycle. Cancelled tasks are never requeued.
