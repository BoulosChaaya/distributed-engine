# Redis-Loss Recovery Runbook

Guarded manual recovery procedure for tasks stranded after Redis data loss.

This is NOT an automatic reconciliation procedure. Every step requires
operator judgment and verification.

## Context

When Redis loses data (restart without persistence, corruption, or operator
error), BullMQ jobs that were in-flight or queued disappear. Tasks in PostgreSQL
may remain in non-terminal states (QUEUED, PROCESSING) with no corresponding
BullMQ job to drive them to completion.

Additionally, outbox events marked DELIVERED may have lost their corresponding
BullMQ job. The outbox publisher considers these events done, so no automatic
re-publication occurs. The task remains in whatever state the worker left it —
QUEUED if the worker never picked it up, or PROCESSING if the worker started
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

Cross-reference each task_id against Redis using the BullMQ API to confirm the
job is absent. Do NOT rely on raw Redis key inspection (`EXISTS`, `KEYS`) —
BullMQ's internal key layout is an implementation detail that can change between
versions.

Use the following Node.js script from a pod that has access to the `bullmq`
package (e.g. the worker container) and to the Redis instance. The script uses
the same queue name (`tasks`) and job ID semantics (`jobId = task_id`) as the
application. Replace `<task_id_1>,<task_id_2>,...` with the exact task IDs from
the query above.

```bash
# Redis password must be available as an environment variable.
# In the Topic 17 deployment model this is sourced from the redis-secret
# Kubernetes Secret via the REDIS_PASSWORD environment variable.
# Do NOT embed the password directly in this command.

kubectl exec -n distributed-engine deploy/worker -- node --input-type=commonjs -e "
const { Queue, Job } = require('bullmq');
const IORedis = require('ioredis');

const taskIds = process.argv.slice(1);
if (taskIds.length === 0) { console.error('Usage: provide task IDs as arguments'); process.exit(1); }

(async () => {
  const connection = new IORedis({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: 3,
  });

  connection.on('error', (err) => {
    console.error('ERROR   Redis connection: ' + err.message);
    process.exit(1);
  });

  const queue = new Queue('tasks', { connection });

  for (const taskId of taskIds) {
    try {
      const job = await Job.fromId(queue, taskId);
      if (job) {
        console.log('EXISTS  ' + taskId + '  state=' + (await job.getState()));
      } else {
        console.log('ABSENT  ' + taskId);
      }
    } catch (err) {
      console.error('ERROR   ' + taskId + '  ' + err.message);
    }
  }

  await queue.close();
  connection.disconnect();
})();
" -- <task_id_1> <task_id_2>
```

Interpret the output for each task_id:

- **EXISTS** — The BullMQ job is present. STOP for this task. Do NOT reset the
  corresponding outbox event. Investigate the actual job state before taking
  any action.

- **ABSENT** — The BullMQ job is confirmed absent for this exact job ID. This
  task_id is a candidate for the guarded manual recovery procedure, provided
  the task is also non-terminal in PostgreSQL.

- **ERROR** — The lookup failed (Redis unavailable, authentication failure,
  connection refused, or any other error). STOP. Do NOT interpret an inability
  to query Redis as proof that the job is absent. Resolve the error before
  proceeding with recovery for this task_id.

> **IMPORTANT — Point-in-time observation.** Job absence is a point-in-time
> result, not a lock. Between the BullMQ lookup and the SQL recovery mutation,
> another publisher or operator could create a new BullMQ job for the same
> task ID. This TOCTOU (time-of-check/time-of-use) gap cannot be eliminated
> without stopping all publishers. To reduce the window: stop the outbox
> publisher before beginning recovery, repeat the BullMQ lookup immediately
> before executing each recovery UPDATE, and restart the publisher only after
> recovery and verification are complete.

> **IMPORTANT — Job absence does not prove non-execution.** BullMQ is
> configured with `removeOnComplete: 100` — completed jobs are evicted after
> 100 newer completions. A task whose BullMQ job was evicted after successful
> completion will show ABSENT even though the task executed. The recovery
> decision must rely on PostgreSQL task state (non-terminal) combined with
> BullMQ job absence, not on job absence alone.

Only tasks where the BullMQ lookup returned **ABSENT** AND the task is
non-terminal in PostgreSQL are candidates for recovery.

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

Verify from Redis that corresponding BullMQ jobs are absent using the
BullMQ-aware lookup from Step 1c. Run the `Job.fromId` script above with the
candidate task IDs from the count queries. Every candidate must return
**ABSENT**; any **EXISTS** result removes that task from the recovery set, and
any **ERROR** result means STOP — do not proceed until the lookup succeeds.

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
-- Reset stuck PROCESSING tasks to QUEUED so they re-enter the outbox
-- Replace <task_ids> with the exact validated set from Step 3
UPDATE tasks
SET status = 'QUEUED',
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
UPDATE outbox_events oe
SET status = 'PENDING',
    attempts = 0,
    claimed_by = NULL,
    claimed_at = NULL,
    processed_at = NULL
WHERE oe.id IN (<delivered_event_ids>)
  AND oe.status = 'DELIVERED'
  AND EXISTS (
    SELECT 1 FROM tasks t
    WHERE t.id = oe.task_id
      AND t.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
  );
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
```

Verify that new BullMQ jobs were created for the recovered tasks using the
BullMQ-aware lookup from Step 1c. Run the `Job.fromId` script with the
recovered task IDs. Each should now return **EXISTS** with an active state.
If any returns **ABSENT**, investigate why the outbox publisher did not
re-publish the event.

## Step 7: Record duplicate-execution risk

After recovery, record:

- Which tasks were reset and re-executed
- That duplicate external side effects are possible for these tasks
- The timestamp of the recovery operation
- The operator who performed the recovery

This is a guarded manual recovery with duplicate-execution risk — not a safe
or exactly-once recovery procedure.
