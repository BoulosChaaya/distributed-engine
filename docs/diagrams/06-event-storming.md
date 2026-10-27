# Event storming

Event storming lays a domain out as sticky notes: who acts, what they ask for,
what happened as a result, and the rules that react automatically. Usually it is
run before building; here it is done afterwards, from the code, so every note
below is something the engine actually does.

| Colour | Note | Meaning |
|---|---|---|
| yellow | actor | who starts it |
| blue | command | a request; it can be refused |
| orange | event | a fact, recorded in PostgreSQL; it can't be refused |
| lilac | policy | "whenever X happens, do Y", run by the engine itself |
| green | read model | what someone looks at |

## Accepting work

```mermaid
flowchart LR
  classDef actor fill:#fff3b0,stroke:#b8a230,color:#3a3300
  classDef cmd fill:#cfe3ff,stroke:#4a78b5,color:#0d2747
  classDef evt fill:#ffd2a8,stroke:#c46f22,color:#4a2400
  classDef pol fill:#e6d6ff,stroke:#7a5bb5,color:#2c1a4d
  classDef read fill:#d4f2dc,stroke:#3e8f57,color:#103a1d

  tenant([Tenant client]):::actor --> submit[Submit task]:::cmd
  operator([Operator]):::actor --> submit
  submit --> refused{{"refused: 401 · 403 suspended ·<br/>429 rate · 429 quota · 409 key conflict"}}
  submit --> accepted[Task accepted]:::evt
  submit --> replay[Same task returned<br/>idempotent]:::evt
  accepted --> charged[Quota charged]:::evt
  accepted --> pol1[/"Whenever an outbox event is written:<br/>wake the publisher"/]:::pol
  pol1 --> pol2[/"Whenever a tenant has a free slot:<br/>publish its next event, fairly by weight"/]:::pol
  pol2 --> published[Job published]:::evt
  accepted --> status[(Task status · tenant usage)]:::read
```

## Running work

```mermaid
flowchart LR
  classDef cmd fill:#cfe3ff,stroke:#4a78b5,color:#0d2747
  classDef evt fill:#ffd2a8,stroke:#c46f22,color:#4a2400
  classDef pol fill:#e6d6ff,stroke:#7a5bb5,color:#2c1a4d
  classDef read fill:#d4f2dc,stroke:#3e8f57,color:#103a1d

  published[Job published]:::evt --> claimed[Task claimed<br/>lease + token]:::evt
  claimed --> completed[Task completed]:::evt
  claimed --> failedonce[Attempt failed]:::evt
  claimed --> handedback[Task handed back<br/>worker shutting down]:::evt
  failedonce --> polr[/"Whenever an attempt fails and budget remains:<br/>requeue with backoff"/]:::pol
  polr --> claimed
  failedonce --> failed[Task failed]:::evt
  handedback --> claimed
  claimed --> lost[Lease expired]:::evt
  lost --> pollease[/"Whenever a lease expires:<br/>reclaim, charge one retry"/]:::pol
  pollease --> claimed
  completed --> polslot[/"Whenever a task finishes:<br/>refill the tenant slot"/]:::pol
  completed --> metrics[(Dashboard · Grafana)]:::read
  failed --> retrycmd[Retry failed task]:::cmd
  retrycmd --> requeued[Task requeued<br/>fresh budget]:::evt
  requeued --> published
```

## Workflows, schedules and recovery

```mermaid
flowchart LR
  classDef actor fill:#fff3b0,stroke:#b8a230,color:#3a3300
  classDef cmd fill:#cfe3ff,stroke:#4a78b5,color:#0d2747
  classDef evt fill:#ffd2a8,stroke:#c46f22,color:#4a2400
  classDef pol fill:#e6d6ff,stroke:#7a5bb5,color:#2c1a4d

  client([Client]):::actor --> startwf[Start workflow]:::cmd
  startwf --> wfstarted[Workflow started<br/>steps BLOCKED or QUEUED]:::evt
  stepdone[Step completed]:::evt --> polrel[/"Whenever a step completes:<br/>release children whose parents are all done"/]:::pol
  polrel --> stepready[Step ready]:::evt
  stepfailed[Step failed]:::evt --> polcas[/"Whenever a step fails:<br/>cancel every blocked step below it"/]:::pol
  polcas --> stepcancelled[Steps cancelled]:::evt
  stepdone --> polfin[/"Whenever nothing is open:<br/>close the workflow"/]:::pol
  stepcancelled --> polfin
  polfin --> wfdone[Workflow completed / failed]:::evt

  clock([Clock]):::actor --> due[Schedule due]:::evt
  due --> polmis[/"Misfire policy:<br/>skip, run once, or catch up"/]:::pol
  polmis --> occ[Occurrence created]:::evt
  occ --> polover[/"FORBID_OVERLAP:<br/>one run at a time, oldest first,<br/>recheck every 5 s"/]:::pol

  clock --> scan[Reconciler cycle]:::evt
  scan --> polorph[/"Whenever a task has no job in Redis<br/>and nothing pending: re-emit it"/]:::pol
  polorph --> requeued2[Task requeued]:::evt
```

## What the notes say about the design

- **Every event is a row** (or a change to one) in PostgreSQL, written in the same
  transaction as the state it describes. That is why no policy can miss an event:
  the outbox and the NOTIFY triggers are policies reading those rows.
- **Policies are idempotent.** Each one can run twice (a publisher crash, two
  replicas) and the second run changes nothing: jobId = taskId, version checks,
  unique indexes, SKIP LOCKED.
- **Only commands are refused.** Rate, quota, keys and tenant status are checked
  at the command; once an event is recorded, the engine finishes what it started
  or records why it couldn't.
