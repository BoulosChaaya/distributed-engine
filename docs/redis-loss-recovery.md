# Redis-Loss Recovery Runbook

Guarded manual recovery procedure for tasks stranded after Redis data loss.

This is NOT an automatic reconciliation procedure. Every step requires
operator judgment and verification.

## Context

When Redis loses data (restart without persistence, corruption, or operator
error), BullMQ jobs that were in-flight or queued disappear. Tasks in PostgreSQL
may remain in non-terminal states (PENDING, PROCESSING) with no corresponding
BullMQ job to drive them to completion.

Additionally, outbox events marked DELIVERED may have lost their corresponding
BullMQ job. The outbox publisher considers these events done, so no automatic
re-publication occurs. The task remains in whatever state the worker left it —
PENDING if the worker never picked it up, or PROCESSING if the worker started
but did not complete it.

### What PostgreSQL protects

- `SELECT ... FOR UPDATE` row lock prevents concurrent modification
- Version guard prevents stale-version writes
- State machine prevents re-entering terminal states
- Claim token prevents unauthorized state transitions
- Reclaim expiry prevents stealing unexpired claims

### What PostgreSQL does NOT protect

- **Duplicate external side effects.** Work executes before completion guards.
  Real side effects (API calls, emails) would execute again and cannot be
  rolled back.
- **BullMQ jobId deduplication.** Does not protect a job that has already
  disappeared from Redis. A new job with the same ID can be created after the
  original is lost.

## Step 1: Identify suspected affected records

### 1a: Tasks stuck in PROCESSING with expired claims

```sql
SELECT t.id, t.name, t.status, t.version, t.claimed_by, t.claim_expires_at
FROM tasks t
WHERE t.status = 'PROCESSING'
  AND t.claim_expires_at < NOW()
ORDER BY t.updated_at;
```

### 1b: Outbox events stuck in PENDING or FAILED

```sql
SELECT oe.id, oe.task_id, oe.event_type, oe.status, oe.attempts, oe.created_at
FROM outbox_events oe
WHERE oe.status IN ('PENDING', 'FAILED')
ORDER BY oe.created_at;
```

### 1c: DELIVERED outbox events with missing BullMQ jobs

After Redis data loss, outbox events marked DELIVERED may no longer have a
corresponding BullMQ job in Redis. The outbox publisher will not re-publish
these because it considers them complete.

```sql
-- DELIVERED outbox events whose tasks are still non-terminal
SELECT oe.id, oe.task_id, oe.event_type, oe.status, oe.processed_at,
       t.status AS task_status, t.claimed_by, t.claim_expires_at
FROM outbox_events oe
JOIN tasks t ON t.id = oe.task_id
WHERE oe.status = 'DELIVERED'
  AND t.status NOT IN ('COMPLETED', 'FAILED')
ORDER BY oe.processed_at;
```

Cross-reference each task_id against Redis to confirm the BullMQ job is absent:

```bash
# For each task_id from the query above, check if a job exists
kubectl exec -n distributed-engine redis-0 -- redis-cli EXISTS "bull:tasks:<task_id>"
```

Only tasks where the BullMQ job is confirmed absent AND the task is non-terminal
are candidates for recovery.

## Step 2: Inspect PostgreSQL and Redis evidence

```sql
-- Count of PROCESSING tasks with expired claims
SELECT count(*) FROM tasks WHERE status = 'PROCESSING' AND claim_expires_at < NOW();

-- Count of stuck outbox events (PENDING or FAILED)
SELECT count(*) FROM outbox_events WHERE status IN ('PENDING', 'FAILED');

-- Count of DELIVERED outbox events with non-terminal tasks
SELECT count(*)
FROM outbox_events oe
JOIN tasks t ON t.id = oe.task_id
WHERE oe.status = 'DELIVERED'
  AND t.status NOT IN ('COMPLETED', 'FAILED');
```

Verify from Redis that corresponding BullMQ jobs are absent:

```bash
# Check if any jobs exist for the task queue
kubectl exec -n distributed-engine redis-0 -- redis-cli KEYS "bull:tasks:*"
```

## Step 3: Validate the exact intended affected set

Record the exact task IDs and outbox event IDs that will be reset.
Compare against the queries above.

**STOP if:**
- Affected count is zero → nothing to recover
- Affected count is unexpected → investigate before proceeding
- Any task is in a terminal state (COMPLETED, FAILED) → do not reset

## Step 4: Reset approved records

### 4a: Reset stuck PROCESSING tasks

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

```sql
-- Reset stuck PROCESSING tasks to PENDING so they re-enter the outbox
-- Replace <task_ids> with the exact validated set from Step 3
UPDATE tasks
SET status = 'PENDING',
    claimed_by = NULL,
    claim_expires_at = NULL,
    version = version + 1,
    updated_at = NOW()
WHERE id IN (<task_ids>)
  AND status = 'PROCESSING'
  AND claim_expires_at < NOW();
```

**Immediately after executing:**

1. Inspect affected-row count. `0 rows` → STOP, do not retry.
2. Expected row count must exactly match intended task count from Step 3.

### 4b: Reset stuck PENDING/FAILED outbox events

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

```sql
-- Reset stuck outbox events to PENDING for re-publication
-- Replace <event_ids> with the exact validated set from Step 3
UPDATE outbox_events
SET status = 'PENDING',
    attempts = 0,
    claimed_by = NULL,
    claimed_at = NULL
WHERE id IN (<event_ids>)
  AND status IN ('PENDING', 'FAILED');
```

**Immediately after executing:**

1. Inspect affected-row count. `0 rows` → STOP, do not retry.
2. Expected row count must exactly match intended event count from Step 3.

### 4c: Reset DELIVERED outbox events with missing BullMQ jobs

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

```sql
-- Reset DELIVERED outbox events whose BullMQ jobs are confirmed absent
-- and whose tasks are still non-terminal
-- Replace <delivered_event_ids> with the exact validated set from Step 3
UPDATE outbox_events
SET status = 'PENDING',
    attempts = 0,
    claimed_by = NULL,
    claimed_at = NULL,
    processed_at = NULL
WHERE id IN (<delivered_event_ids>)
  AND status = 'DELIVERED';
```

**Immediately after executing:**

1. Inspect affected-row count. `0 rows` → STOP, do not retry.
2. Expected row count must exactly match intended event count from Step 3.

If the corresponding task is PROCESSING with an expired claim, reset the task
first (Step 4a) before resetting the outbox event. The outbox publisher will
then re-publish the event and the worker will re-process the task.

## Step 5: Reinspect resulting state

```sql
-- Verify tasks were reset
SELECT id, status, version, claimed_by FROM tasks WHERE id IN (<task_ids>);

-- Verify outbox events were reset
SELECT id, task_id, status, attempts FROM outbox_events WHERE id IN (<event_ids>);

-- Verify DELIVERED outbox events were reset
SELECT id, task_id, status, attempts FROM outbox_events WHERE id IN (<delivered_event_ids>);
```

## Step 6: Observe republication and execution

The outbox publisher will pick up the reset events on its next polling cycle
and publish new BullMQ jobs. Monitor:

```bash
# Watch worker logs for task processing
kubectl logs -n distributed-engine -l app=worker --tail=50 -f

# Verify jobs appear in Redis
kubectl exec -n distributed-engine redis-0 -- redis-cli KEYS "bull:tasks:*"
```

## Step 7: Record duplicate-execution risk

After recovery, record:

- Which tasks were reset and re-executed
- That duplicate external side effects are possible for these tasks
- The timestamp of the recovery operation
- The operator who performed the recovery

This is a guarded manual recovery with duplicate-execution risk — not a safe
or exactly-once recovery procedure.
