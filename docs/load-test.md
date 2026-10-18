# Load and endurance test

60 minutes of steady multi-tenant load against the full docker compose stack, with a
worker, Redis, PostgreSQL and the API each failing once along the way. Run by the
[Load workflow](../.github/workflows/load.yml) (`scripts/load-test.sh`) in GitHub Actions,
run [39160107458](https://github.com/BoulosChaaya/distributed-engine/actions/runs/39160107458), commit `8b87a54`, Oct 18, 2026.

## What ran

- **Stack:** API (with the outbox publisher, scheduler and reconciler), 3 workers, PostgreSQL 16, Redis 7, Prometheus, and a small HTTP sink that records every side effect of the `http` tasks.
- **Load:** k6 against the tenant API. Three tenants, heavy / medium / light at 10/4/2 submissions per second, with weights 10 / 3 / 1 and concurrency limits 10 / 6 / 4.
- **Mix:** sleep 25,773, echo 11,635, http 10,369, flaky 4,509, scheduled 2,896, poison 1,156, workflow 1,157.
  - `flaky` fails once, then succeeds.
  - `poison` always fails and must end FAILED.
  - `scheduled` is due 20–60 s later.
  - A workflow is a four-step diamond.
  - 5% of submissions were sent twice with the same idempotency key.
- **Faults, at fixed fractions of the run:**

| At | Fault |
|---|---|
| +540 s | SIGKILL worker holding 2 running tasks |
| +1260 s | Redis stopped |
| +1980 s | PostgreSQL frozen |
| +2520 s | SIGKILL API |

## Result: every check passed

After the load, every submission the API accepted was joined, in PostgreSQL, against `tasks`, `workflows` and the sink:

| Check | Result |
|---|---|
| no accepted task lost | pass |
| every accepted task finished | pass |
| every task ended in its expected state | pass |
| every workflow completed | pass |
| every completed http task reached the sink | pass |
| idempotent replays returned the same task | pass |
| outbox has no PENDING events | pass |
| outbox has no FAILED events | pass |
| PostgreSQL connections stayed under max_connections | pass |
| k6 completed | pass |

## Throughput and latency

| | |
|---|---|
| Accepted | 57,495 (15.96/s) of 60,338 requests |
| Drained after the load stopped | 43 s |
| Submit latency, median / p95 / p99 / max | 5 ms / 9 ms / 15 ms / 15.2 s |
| Peak outbox lag (oldest pending event) | 26.2 s |
| PostgreSQL connections, peak | 51 of 100 |

Submit latency includes the requests held while PostgreSQL was frozen; the median and p95 are the steady state.

Queue wait is the time from the task's creation to its start, on the database clock, for tasks that run at once (sleep, echo, http):

| Tenant | Tasks | Queue wait p50 / p95 / p99 | Created → completed p50 / p95 |
|---|---|---|---|
| heavy | 29,875 | 14 ms / 36 ms / 4286 ms | 178 ms / 776 ms |
| light | 5,934 | 18 ms / 42 ms / 1028 ms | 183 ms / 774 ms |
| medium | 11,968 | 19 ms / 39 ms / 1900 ms | 188 ms / 772 ms |

The tails come from the faults. Split by when the task was created:

| Created | Tasks | p50 / p95 / p99 / max |
|---|---|---|
| outside any fault window | 43,767 | 17 ms / 35 ms / 44 ms / 575 ms |
| during SIGKILL worker holding 2 running tasks (until 60 s after) | 1,202 | 17 ms / 37 ms / 75 ms / 30.7 s |
| during Redis stopped (until 60 s after) | 1,061 | 85 ms / 20.9 s / 23.3 s / 24.0 s |
| during PostgreSQL frozen (until 60 s after) | 932 | 18 ms / 2187 ms / 2881 ms / 3095 ms |
| during SIGKILL API (until 60 s after) | 815 | 17 ms / 36 ms / 46 ms / 30.0 s |

The longest single waits (about 30 s) in the two kill windows are the recovery timers at their compose defaults, not lost work:
- **Worker kill:** it matches a job the killed worker had just taken from Redis but not yet started. BullMQ hands such a job out again only when its 30 s job lock expires (15 s with the Kubernetes settings). It is not one of the 2 reclaimed tasks: a reclaim does not change `started_at`.
- **API kill:** it matches the 30 s stale-claim timeout. Events the killed publisher had claimed but not yet published are taken over by the restarted publisher after 30 s.

## What each fault cost

| Fault | Requests refused | Recovery |
|---|---|---|
| SIGKILL worker holding 2 running tasks | 0 | 2 lease reclaims (each charged one retry) |
| Redis stopped | 0 | submissions kept being accepted; the outbox held them and published on reconnect |
| PostgreSQL frozen | 0 | requests waited for the database and completed after it resumed |
| SIGKILL API | 11 (no response: 11) | restarted; in-flight requests got no response; the publisher, scheduler and reconciler resumed |
| outside any fault | 0 | |

- **Retries:** 2 were used by tasks that should succeed first time (sleep / echo / http), across 47,777 tasks.
  - By kind: echo 0 (0 tasks), flaky 4509 (4509 tasks), http 0 (0 tasks), poison 2312 (1156 tasks), scheduled 0 (0 tasks), sleep 2 (2 tasks).
  - Flaky tasks use one retry each and poison tasks two, by design.
- **Re-executed side effects:** 0 of 10,369 http side effects ran more than once.
  - Execution is at least once: a task running when its worker dies runs again.
  - This counts how often that reached the outside world.

## Memory

Resident memory of the largest process, as a 1-minute average. A worker restarts at about 15% and the API at 70%.

| Process | 10% | 30% | 50% | 65% | 85% | 100% |
|---|---|---|---|---|---|---|
| api | 225 MB | 229 MB | 231 MB | 233 MB | 225 MB | 226 MB |
| worker | 228 MB | 241 MB | 248 MB | 248 MB | 248 MB | 249 MB |

## Reproduce

```bash
DURATION_MIN=60 bash scripts/load-test.sh   # needs Docker; writes results.md and results.json
```

Or run the **Load** workflow from the Actions tab with the duration you want.

Limits of what this shows:
- One machine with 3 workers, at about 16 submissions per second.
- One hour of load.
- Synthetic handlers.
- It does not measure the ceiling. See [KNOWN-LIMITS.md](KNOWN-LIMITS.md).

