# Sequence diagrams

The important flows, in order. Each one is also a trace on the interactive
trace map, which adds the stored values at every step.

## 1. One task, client to client

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant A as API
  participant P as PostgreSQL
  participant Pub as Publisher
  participant R as Redis (BullMQ)
  participant W as Worker

  C->>A: POST /tasks {name, payload, idempotencyKey?}
  A->>P: BEGIN · key lookup · INSERT task (QUEUED, v1) · INSERT outbox (PENDING) · COMMIT
  Note over P: trigger: NOTIFY outbox_wakeup
  A-->>C: 201 {id} (200 if the key was seen before)
  P-->>Pub: NOTIFY (or the next poll)
  Pub->>P: claim event (FOR UPDATE SKIP LOCKED, attempts + 1)
  Pub->>R: add(jobId = taskId)
  Pub->>P: event DELIVERED
  R->>W: job
  W->>P: read task (skip if COMPLETED / FAILED / CANCELLED)
  W->>P: QUEUED → PROCESSING (v2) · claim_token · lease = DB now + 30 s
  W->>W: run handler (renew lease every 10 s)
  W->>P: PROCESSING → COMPLETED (v3) + result · checks version and token
  W->>R: ack: job completed
  C->>A: GET /tasks/:id
  A->>P: SELECT task
  A-->>C: 200 {status: COMPLETED, result}
```

## 2. Worker killed mid-task: lease expiry and reclaim

```mermaid
sequenceDiagram
  participant WA as Worker A
  participant P as PostgreSQL
  participant R as Redis (BullMQ)
  participant WB as Worker B

  WA->>P: claim (v2, token A, lease 30 s)
  WA->>WA: running…
  Note over WA: SIGKILL: no write, no ack
  Note over R: lock expires (30 s) → job back to waiting
  R->>WB: same job
  WB->>P: SELECT … FOR UPDATE: PROCESSING, owner A
  alt lease still valid
    WB->>R: delay until the lease runs out
  else lease expired
    WB->>P: reclaim: owner B, token B, retries + 1, v3
    WB->>WB: run handler again (at-least-once)
    WB->>P: COMPLETED (v4), token B checked
  end
  opt worker A was only frozen
    WA->>P: renew or complete with token A
    P-->>WA: refused: token mismatch
  end
```

## 3. Redis down: the outbox holds the work

```mermaid
sequenceDiagram
  participant C as Client
  participant A as API
  participant P as PostgreSQL
  participant Pub as Publisher
  participant R as Redis

  Note over R: down
  C->>A: POST /tasks
  A->>P: task + outbox event (PENDING)
  A-->>C: 201 (PostgreSQL is enough to accept work)
  Pub->>Pub: Redis not ready: claim nothing, wait
  Note over A: /health 200 "degraded" · rate limiter fails open
  Note over R: back up
  Pub->>P: claim PENDING events
  Pub->>R: add jobs
  Pub->>P: DELIVERED
```

## 4. A deploy: drain, then hand back uncharged

```mermaid
sequenceDiagram
  participant K as Orchestrator
  participant WA as Worker A
  participant P as PostgreSQL
  participant R as Redis (BullMQ)
  participant WB as Worker B

  K->>WA: SIGTERM
  WA->>WA: stop taking jobs, running tasks continue
  alt finishes within the 20 s drain
    WA->>P: COMPLETED as usual
  else still running at 20 s
    WA->>WA: abort the handler
    WA->>P: PROCESSING → QUEUED · retries unchanged · token checked
    WA->>R: move job to delayed (now): no attempt used
    R->>WB: job
    WB->>P: claim, run from the start, COMPLETED
  end
  WA->>K: exits before the 30 s guard (k8s allows 40 s)
```

## 5. Tenant submission: key, quota, rate limit, admission

```mermaid
sequenceDiagram
  participant C as Tenant client
  participant A as API
  participant R as Redis
  participant P as PostgreSQL
  participant Pub as Publisher
  participant W as Worker

  C->>A: POST /tenant/tasks (Bearer key, idempotencyKey)
  A->>P: tenant by sha256(key) · SUSPENDED → 403
  A->>R: rate limit script (only allowed requests recorded) · fails open
  A->>P: BEGIN · key lookup · charge quota · task · outbox · COMMIT
  A-->>C: 201 (resend: 200 same task · other body: 409)
  Pub->>P: free slots = limit − in flight · weighted fair pick
  Pub->>R: add job
  R->>W: job
  W->>P: tenant concurrency lease (tenant row locked) → claim → run
  W->>P: COMPLETED · lease released · NOTIFY refills the slot
```

## 6. Workflow (diamond) and its failure cascade

```mermaid
sequenceDiagram
  participant A as API
  participant P as PostgreSQL
  participant W as Workers

  A->>P: one transaction: workflow + 4 steps + dependencies, outbox only for "start"
  W->>P: start COMPLETED
  Note over P: same transaction: left, right open parents → 0 → QUEUED + outbox
  par
    W->>P: left running
  and
    W->>P: right COMPLETED (join: 2 → 1 open parents)
  end
  W->>P: left COMPLETED (join → QUEUED + outbox)
  W->>P: join runs with {left, right} outputs → COMPLETED
  Note over P: no step open → workflow COMPLETED
  Note over P: if a step FAILS instead: every BLOCKED step below it → CANCELLED, and the workflow ends FAILED once nothing is running
```

## 7. Cron with FORBID_OVERLAP

```mermaid
sequenceDiagram
  participant S as Scheduler
  participant P as PostgreSQL
  participant W1 as Worker (occurrence 1)
  participant W2 as Worker (occurrence 2)

  S->>P: schedule due (FOR UPDATE SKIP LOCKED) → occurrence 1 (unique per time)
  W1->>P: take execution lease (renewed to now + 300 s while running)
  S->>P: next minute → occurrence 2
  W2->>P: lease held → delay 5 s, ask again
  W1->>P: COMPLETED · release lease
  W2->>P: within 5 s: lease free → take it → run
```
