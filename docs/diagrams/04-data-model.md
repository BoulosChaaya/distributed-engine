# Data model

Every table in PostgreSQL (21 migrations, `packages/shared/src/db/migrations.ts`),
with the columns that carry the engine's guarantees. Timestamps and a few
descriptive columns are left out.

```mermaid
erDiagram
  PLANS ||--o{ TENANTS : "limits for"
  TENANTS ||--o| TENANT_OVERRIDES : "may override"
  TENANTS ||--o{ TENANT_USAGE : "one row per billing period"
  TENANTS ||--o{ TENANT_CONCURRENCY_LEASES : "one per running task"
  TENANTS ||--o{ TASKS : submits
  TENANTS ||--o{ WORKFLOWS : submits
  TENANTS ||--o{ RECURRING_SCHEDULES : owns
  RECURRING_SCHEDULES ||--o{ TASKS : "one occurrence per time"
  WORKFLOWS ||--o{ TASKS : "steps"
  TASKS ||--o{ TASK_DEPENDENCIES : "child of"
  TASKS ||--o{ TASK_DEPENDENCIES : "parent of"
  TASKS ||--o{ OUTBOX_EVENTS : "announced by"
  TASKS ||--o{ TENANT_CONCURRENCY_LEASES : "held while running"

  TASKS {
    text id PK "also the BullMQ jobId"
    text status "SCHEDULED BLOCKED QUEUED PROCESSING COMPLETED FAILED CANCELLED"
    int version "optimistic lock: +1 per change"
    int retries "attempts used (PostgreSQL owns the count)"
    int max_retries
    text claimed_by "worker holding it"
    text claim_token "fencing token, new per claim"
    timestamptz claim_expires_at "lease, on the DB clock"
    jsonb payload
    jsonb result
    text tenant_id FK "null for operator tasks"
    text idempotency_key "unique per tenant; and per key when tenant is null"
    text idempotency_hash "same key + other body → 409"
    text workflow_id FK
    text step_key "unique per workflow"
    int pending_dependencies "open parents; 0 → QUEUED"
    timestamptz scheduled_for
    text schedule_id FK
  }
  OUTBOX_EVENTS {
    text id PK
    text task_id FK
    text event_type "TASK_CREATED TASK_REQUEUED WORKFLOW_STEP_READY SCHEDULED_TASK_RELEASED"
    text status "PENDING DELIVERED FAILED"
    int attempts
    text claimed_by "publisher holding it"
    timestamptz claimed_at "free again after 30 s"
    jsonb payload "everything the job needs"
  }
  TASK_DEPENDENCIES {
    text task_id PK,FK "child"
    text depends_on PK,FK "parent"
  }
  WORKFLOWS {
    text id PK
    text status "RUNNING COMPLETED FAILED CANCELLED"
    text idempotency_key "unique per tenant"
  }
  TENANTS {
    text id PK
    text status "ACTIVE SUSPENDED"
    text plan_id FK
    text api_key_hash "sha256 only, never the key"
  }
  PLANS {
    text id PK
    int rate_limit
    int max_concurrent_executions
    int max_jobs_per_period
    int billing_period_days
    int weight "fair publishing share"
  }
  TENANT_OVERRIDES {
    text tenant_id FK "unique"
    int rate_limit "null = use the plan"
    int max_concurrent_executions
    int max_jobs_per_period
    int weight
  }
  TENANT_USAGE {
    text tenant_id FK
    timestamptz billing_period_start "unique with tenant_id"
    int accepted_jobs "charged in the accepting transaction"
  }
  TENANT_CONCURRENCY_LEASES {
    text tenant_id FK
    text task_id FK
    text worker_id
    text lease_token
    timestamptz expires_at
  }
  RECURRING_SCHEDULES {
    text id PK
    text cron_expression
    text timezone
    timestamptz next_run_at
    text status "ACTIVE PAUSED DISABLED"
    text misfire_policy "SKIP_MISSED RUN_ONCE CATCH_UP_ALL"
    text overlap_policy "ALLOW_OVERLAP FORBID_OVERLAP"
    text execution_lease_token "FORBID_OVERLAP: one run at a time"
    timestamptz execution_lease_expires_at
    int version
  }
```

## Indexes and triggers that carry a guarantee

| Object | Guarantee |
|---|---|
| `idx_tasks_idempotency (tenant_id, idempotency_key)` | one task per tenant key |
| `idx_tasks_operator_idempotency (idempotency_key) WHERE tenant_id IS NULL` | one task per operator key (migration 21) |
| `idx_tasks_workflow_step (workflow_id, step_key)` | one task per workflow step |
| `idx_unique_occurrence` | one occurrence per schedule and time |
| `idx_workflows_idempotency` | one workflow per key |
| `unique_tenant_billing_period` | one usage row per tenant and period |
| trigger `outbox_event_inserted_wakeup` | a new event wakes the publisher (NOTIFY) |
| trigger `task_slot_freed_wakeup` | a finished task wakes the publisher to refill a tenant slot |

Redis holds no record of truth: the BullMQ queue (rebuilt from PostgreSQL by the
reconciler if lost), the rate-limit windows and worker heartbeats.
