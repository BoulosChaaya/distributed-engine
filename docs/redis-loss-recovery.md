# Redis-Loss Recovery Runbook

Guarded manual recovery procedure for tasks stranded after Redis data loss.

This is NOT an automatic reconciliation procedure. Every step requires
operator judgment and verification.

## Context

When Redis loses data (restart without persistence, corruption, or operator
error), BullMQ jobs that were in-flight or queued disappear. Tasks in PostgreSQL
may remain in non-terminal states (PENDING, PROCESSING) with no corresponding
BullMQ job to drive them to completion.

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

## Step 1: Identify suspected affected outbox records

```sql
-- Tasks stuck in PROCESSING with no active BullMQ job
SELECT t.id, t.name, t.status, t.version, t.claimed_by, t.claim_expires_at
FROM tasks t
WHERE t.status = 'PROCESSING'
  AND t.claim_expires_at < NOW()
ORDER BY t.updated_at;

-- Outbox events stuck in PENDING or FAILED
SELECT oe.id, oe.task_id, oe.event_type, oe.status, oe.attempts, oe.created_at
FROM outbox_events oe
WHERE oe.status IN ('PENDING', 'FAILED')
ORDER BY oe.created_at;
```

## Step 2: Inspect PostgreSQL and Redis evidence

```sql
-- Count of affected tasks
SELECT count(*) FROM tasks WHERE status = 'PROCESSING' AND claim_expires_at < NOW();

-- Count of stuck outbox events
SELECT count(*) FROM outbox_events WHERE status IN ('PENDING', 'FAILED');
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

## Step 5: Reinspect resulting state

```sql
-- Verify tasks were reset
SELECT id, status, version, claimed_by FROM tasks WHERE id IN (<task_ids>);

-- Verify outbox events were reset
SELECT id, task_id, status, attempts FROM outbox_events WHERE id IN (<event_ids>);
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
