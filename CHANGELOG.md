# Changelog

## 1.0.1 (2026-10-27)

Bug fixes and security updates found by tracing the system map: every
checkpoint of 17 traces was checked against three questions (what if this
message is **lost**, arrives **twice**, or two arrive **together**). Each fix
came with a test or a live run that failed before it.

### Security
- Next.js 16.4.0 → 16.4.1: two critical unauthenticated remote code
  execution advisories in the dashboard (GHSA-2464-fhr4-w9xw,
  GHSA-673q-mjm6-r5xr). (#81)
- body-parser 2.3.0 → 2.4.0 under express: DoS through a compressed request
  body (GHSA-27r3-jxmh-p828). (#81)

### Fixed
- **Operator `POST /tasks` accepts `idempotencyKey`.** A 201 lost after the
  commit left the client one option, to resend, and every resend created and
  ran another task. The route now uses the tenant route's path; migration 21
  adds the index for keys without a tenant. (#82)
- **FORBID_OVERLAP no longer waits for the lease to expire.** A blocked
  occurrence started up to 5 minutes after the previous one finished,
  because the holder keeps renewing its lease 5 minutes ahead. It now
  rechecks every 5 s. (#83)
- **The rate limiter counts only allowed requests.** Rejected requests filled
  the window too, so a client retrying over its limit got almost nothing
  through instead of its limit. One atomic Lua script now does the check. (#84)
- **A deploy no longer fails tasks.** On SIGTERM, tasks that outlived the
  30 s shutdown guard were killed, then reclaimed with a retry charged; a
  task with `maxRetries: 0` ended FAILED. Workers now drain for 20 s, then
  hand the rest back as QUEUED without charging a retry. (#85)

### Checked and unchanged
Cancel racing completion, operator retry of a FAILED task, a workflow step
failing mid-diamond, the tenant same-key race (already handled), the tenant
concurrency lease race (locked on the tenant row), scheduled release, and an
outbox event reaching its attempt limit (the reconciler re-emits the task).

## 1.0.0 (2026-10-18)

First frozen release. See the release notes on GitHub.
