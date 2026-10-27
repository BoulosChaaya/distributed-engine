# State machines

Every lifecycle in the engine, with the only moves the code allows. A move that
isn't drawn is refused before anything is written.

## Task

Source: `packages/shared/src/state-machine.ts` (`VALID_TRANSITIONS`).

```mermaid
stateDiagram-v2
  direction TB
  [*] --> QUEUED: submitted
  [*] --> SCHEDULED: scheduledFor in the future
  [*] --> BLOCKED: workflow step with parents

  SCHEDULED --> QUEUED: scheduler, when due
  BLOCKED --> QUEUED: last parent completed

  QUEUED --> PROCESSING: worker claims (new token, lease)
  PROCESSING --> COMPLETED: handler succeeded
  PROCESSING --> FAILED: permanent error or retries spent
  PROCESSING --> QUEUED: transient error (retry) · reconciler requeue · handed back on shutdown
  FAILED --> QUEUED: operator / tenant retry (not workflow steps)

  SCHEDULED --> CANCELLED
  BLOCKED --> CANCELLED: cancel, or a parent failed
  QUEUED --> CANCELLED
  PROCESSING --> CANCELLED

  COMPLETED --> [*]
  CANCELLED --> [*]
```

Every move adds 1 to `version`; a write that names an older version is refused.
A reclaim (lease expired) keeps the task PROCESSING but changes the owner and
the token, adds 1 to `version` and charges one retry.

## Outbox event

```mermaid
stateDiagram-v2
  [*] --> PENDING: written with its task
  PENDING --> PENDING: claimed (attempts + 1) · claim older than 30 s is free again
  PENDING --> DELIVERED: job added to Redis
  PENDING --> FAILED: attempts reach OUTBOX_MAX_ATTEMPTS (5)
  DELIVERED --> [*]: pruned after 7 days (OUTBOX_DELIVERED_RETENTION_HOURS)
  FAILED --> [*]: kept as a record · the reconciler re-emits the task
```

Event types: `TASK_CREATED`, `TASK_REQUEUED`, `WORKFLOW_STEP_READY`, `SCHEDULED_TASK_RELEASED`.

## Workflow

```mermaid
stateDiagram-v2
  [*] --> RUNNING: created with all its steps
  RUNNING --> COMPLETED: every step COMPLETED
  RUNNING --> FAILED: nothing open, at least one step FAILED or CANCELLED
  RUNNING --> CANCELLED: workflow cancelled
  COMPLETED --> [*]
  FAILED --> [*]
  CANCELLED --> [*]
```

## Tenant

```mermaid
stateDiagram-v2
  [*] --> ACTIVE: provisioned with a plan and an API key
  ACTIVE --> SUSPENDED: operator
  SUSPENDED --> ACTIVE: operator
  note right of SUSPENDED
    new submissions: 403
    work already accepted waits
    (0 publish slots, parked jobs)
  end note
```

## Recurring schedule

The operator can set any of the three statuses (`PUT /schedules/:id/status`);
each change is versioned like a task's.

```mermaid
stateDiagram-v2
  [*] --> ACTIVE
  ACTIVE --> PAUSED
  PAUSED --> ACTIVE
  ACTIVE --> DISABLED
  DISABLED --> ACTIVE
  PAUSED --> DISABLED
  DISABLED --> PAUSED
  note right of ACTIVE
    only ACTIVE schedules make occurrences
    misfire: SKIP_MISSED · RUN_ONCE · CATCH_UP_ALL
    overlap: ALLOW_OVERLAP · FORBID_OVERLAP
  end note
```
