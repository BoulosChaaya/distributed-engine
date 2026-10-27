// Turns the files scripts/load-test.sh leaves in $OUT into results.md and
// results.json, prints one ok/FAIL line per hard check, and exits 1 if any
// hard check failed. Usage: node load-report.mjs <out dir> key=value...
import { readFileSync, writeFileSync, existsSync } from 'node:fs';

const out = process.argv[2];
const arg = Object.fromEntries(process.argv.slice(3).map((kv) => kv.split('=')));
const num = (k) => Number(arg[k]);
const read = (f) => (existsSync(`${out}/${f}`) ? readFileSync(`${out}/${f}`, 'utf8') : '');
const rows = (f) => read(f).trim().split('\n').filter(Boolean).map((l) => l.split(/[|\t]/));

const k6 = JSON.parse(read('k6-summary.json'));
const metric = (name) => k6.metrics[name]?.values ?? {};
const accepted = metric('engine_accepted').count ?? 0;
const httpReqs = metric('http_reqs').count ?? 0;
const dur = metric('http_req_duration');
const seconds = num('end') - num('start');

// Rejections, attributed to the fault window they fell in (fault start to
// 60 s after recovery), so outage cost and steady-state errors are separate.
const faults = rows('faults.log').map((r) => r[0].split(' ')).map(([t, ...rest]) => ({ t: Number(t), label: rest.join(' ') }));
const windows = [];
for (let i = 0; i < faults.length; i++) {
  if (!faults[i].label.startsWith('fault:')) continue;
  const rec = faults.slice(i + 1).find((f) => f.label.startsWith('recover:'));
  windows.push({ label: faults[i].label.replace('fault: ', ''), from: faults[i].t, to: (rec ? rec.t : faults[i].t) + 60, n: 0, byStatus: {} });
}
const outside = { n: 0, byStatus: {} };
for (const [status, , t] of rows('rejected.tsv')) {
  // Windows can overlap; the most recent fault gets the blame.
  const w = windows.filter((x) => Number(t) >= x.from && Number(t) <= x.to).at(-1) ?? outside;
  w.n++;
  w.byStatus[status] = (w.byStatus[status] ?? 0) + 1;
}

const latency = rows('latency.txt').map(([tenant, n, waitP50, waitP95, waitP99, e2eP50, e2eP95]) => ({ tenant, n: +n, waitP50: +waitP50, waitP95: +waitP95, waitP99: +waitP99, e2eP50: +e2eP50, e2eP95: +e2eP95 }));
const kinds = Object.fromEntries(rows('kinds.txt').map(([k, n]) => [k, +n]));
kinds.workflow = rows('workflows.tsv').length;
const [plainTasks, plainRetries] = (rows('retries.txt')[0] ?? [0, 0]).map(Number);
const memory = rows('memory.txt').map((r) => r[0].split(' ')).map(([job, ...points]) => ({
  job: job.replace(/^engine_|_process_resident_memory_bytes$/g, ''),
  points: points.map((p) => p.split(':')).map(([pct, v]) => ({ pct: Number(pct), bytes: Number(v) })),
}));
const retriesByKind = rows('retries-by-kind.txt').map(([kind, tasks, retries]) => ({ kind, tasksRetried: +tasks, retries: +retries }));

// Queue wait for tasks created inside a fault window vs outside any.
const pct = (xs, p) => (xs.length ? xs[Math.min(xs.length - 1, Math.floor(p * xs.length))] : null);
const waitStats = (xs) => { xs.sort((a, b) => a - b); return { n: xs.length, p50: pct(xs, 0.5), p95: pct(xs, 0.95), p99: pct(xs, 0.99), max: xs.at(-1) ?? null }; };
const waitIn = new Map(windows.map((w) => [w.label, []]));
const waitOut = [];
for (const [created, ms] of rows('waits.txt').map((r) => r.map(Number))) {
  const w = windows.filter((x) => created >= x.from && created <= x.to).at(-1);
  (w ? waitIn.get(w.label) : waitOut).push(ms);
}
const waitByWindow = [...waitIn].map(([label, xs]) => ({ window: label, ...waitStats(xs) }));
const waitOutside = waitStats(waitOut);
const conn = rows('pg-connections.txt').map((r) => Number(r[0].split(' ')[1]));

const checks = [
  ['no accepted task lost', num('lost') === 0, `${arg.lost} missing`],
  ['every accepted task finished', num('open') === 0, `${arg.open} not terminal after drain`],
  ['every task ended in its expected state', num('wrong') === 0, `${arg.wrong} unexpected (see unexpected-states.txt)`],
  ['every workflow completed', num('wf_bad') === 0, `${arg.wf_bad} not COMPLETED`],
  ['every completed http task reached the sink', num('no_effect') === 0, `${arg.no_effect} without a side effect`],
  ['every repeated http call carried the same Idempotency-Key', num('reexec_nokey') === 0, `${arg.reexec_nokey} repeats with a missing or changed key`],
  ['idempotent replays returned the same task', num('mismatch') === 0, `${arg.mismatch} mismatches`],
  ['outbox has no PENDING events', num('outbox_pending') === 0, `${arg.outbox_pending} pending`],
  ['outbox has no FAILED events', num('outbox_failed') === 0, `${arg.outbox_failed} failed`],
  ['PostgreSQL connections stayed under max_connections', num('peak_conn') < num('max_conn'), `peak ${arg.peak_conn} of ${arg.max_conn}`],
  ['k6 completed', num('k6_status') === 0, `exit ${arg.k6_status}`],
];

const result = {
  config: { durationMin: num('duration_min'), workers: num('workers'), ratesPerSecond: arg.rates },
  window: { start: num('start'), end: num('end'), seconds, drainSeconds: num('drained') - num('end') },
  submissions: { httpRequests: httpReqs, accepted, acceptedPerSecond: +(accepted / seconds).toFixed(2), byKind: kinds },
  submitLatencyMs: { med: dur.med, p95: dur['p(95)'], p99: dur['p(99)'], max: dur.max },
  rejectedByFaultWindow: windows.map(({ label, n, byStatus }) => ({ fault: label, n, byStatus })),
  rejectedOutsideFaults: outside,
  perTenantMs: latency,
  retries: { tasks: plainTasks, retriesUsed: plainRetries },
  sideEffects: { httpCompleted: num('http_done'), executedMoreThanOnce: num('reexec'), repeatsWithoutOneKey: num('reexec_nokey') },
  recovery: { leaseReclaims: num('reclaims'), requeuedByReconciler: num('requeued'), retriesByKind },
  queueWaitMs: { outsideFaults: waitOutside, byFaultWindow: waitByWindow },
  peaks: { queueDepth: Number(read('peak-queue.txt')) || null, outboxLagSeconds: Number(read('peak-lag.txt')) || 0, pgConnections: num('peak_conn'), maxConnections: num('max_conn') },
  memoryBytes: memory,
  faults,
  checks: checks.map(([name, ok, detail]) => ({ name, ok, detail })),
};
writeFileSync(`${out}/results.json`, JSON.stringify(result, null, 2));

const mb = (b) => (Number.isFinite(b) ? `${(b / 1048576).toFixed(0)} MB` : 'n/a');
const t0 = num('start');
const md = [
  `# Load test result`,
  ``,
  `${num('duration_min')} min, ${num('workers')} workers, tenants heavy/medium/light at ${arg.rates} submissions/s.`,
  ``,
  `## Checks`,
  ``,
  `| Check | Result |`,
  `|---|---|`,
  ...checks.map(([name, ok, detail]) => `| ${name} | ${ok ? 'pass' : `**FAIL**: ${detail}`} |`),
  ``,
  `## Throughput and latency`,
  ``,
  `- Accepted: ${accepted} submissions (${result.submissions.acceptedPerSecond}/s) out of ${httpReqs} HTTP requests; drained ${result.window.drainSeconds} s after the load stopped.`,
  `- Mix: ${Object.entries(kinds).map(([k, n]) => `${k} ${n}`).join(', ')}.`,
  `- Submit latency (all requests, including during faults): median ${dur.med?.toFixed(1)} ms, p95 ${dur['p(95)']?.toFixed(1)} ms, p99 ${dur['p(99)']?.toFixed(1)} ms, max ${(dur.max / 1000).toFixed(1)} s.`,
  ``,
  `| Tenant | Tasks | Queue wait p50 / p95 / p99 | Created → completed p50 / p95 |`,
  `|---|---|---|---|`,
  ...latency.map((l) => `| ${l.tenant} | ${l.n} | ${l.waitP50} / ${l.waitP95} / ${l.waitP99} ms | ${l.e2eP50} / ${l.e2eP95} ms |`),
  ``,
  `Immediate tasks only (sleep, echo, http), measured on the database clock.`,
  ``,
  `Queue wait by when the task was created:`,
  ``,
  `| Created | Tasks | p50 / p95 / p99 / max |`,
  `|---|---|---|---|`,
  `| outside any fault window | ${waitOutside.n} | ${waitOutside.p50} / ${waitOutside.p95} / ${waitOutside.p99} / ${waitOutside.max} ms |`,
  ...waitByWindow.map((w) => `| during ${w.window} (until 60 s after) | ${w.n} | ${w.p50} / ${w.p95} / ${w.p99} / ${w.max} ms |`),
  ``,
  `## Faults`,
  ``,
  `| At | Event |`,
  `|---|---|`,
  ...faults.map((f) => `| +${f.t - t0} s | ${f.label} |`),
  ``,
  `| Fault window (until 60 s after recovery) | Rejected submissions |`,
  `|---|---|`,
  ...windows.map((w) => `| ${w.label} | ${w.n}${w.n ? ` (${Object.entries(w.byStatus).map(([s, n]) => `${s === '0' ? 'no response' : s}: ${n}`).join(', ')})` : ''} |`),
  `| outside any fault window | ${outside.n}${outside.n ? ` (${Object.entries(outside.byStatus).map(([s, n]) => `${s === '0' ? 'no response' : s}: ${n}`).join(', ')})` : ''} |`,
  ``,
  `## Recovery cost`,
  ``,
  `- Lease reclaims (a task taken over from a dead or frozen worker): ${num('reclaims')}. Tasks re-published by the reconciler: ${num('requeued')}.`,
  `- Retries used by tasks that should succeed first time (sleep/echo/http): ${plainRetries} across ${plainTasks} tasks (each reclaim charges one).`,
  `- Retries by kind: ${retriesByKind.map((r) => `${r.kind} ${r.retries} (${r.tasksRetried} tasks)`).join(', ')}.`,
  `- http side effects executed more than once: ${num('reexec')} of ${num('http_done')} (at-least-once execution: a task running when its worker died runs again). Repeats without one stable Idempotency-Key: ${num('reexec_nokey')}; the rest a receiver honouring the key would drop.`,
  `- Peak queue depth (waiting + prioritized): ${result.peaks.queueDepth ?? 'n/a'}. Peak outbox lag: ${result.peaks.outboxLagSeconds.toFixed(1)} s.`,
  `- PostgreSQL connections: peak ${num('peak_conn')} of ${num('max_conn')} (sampled every 5 s; ${conn.length} samples).`,
  ``,
  `Resident memory of the largest process, 1-minute average (a worker restarts at ~15%, the API at 70%):`,
  ``,
  `| Process | ${memory[0]?.points.map((p) => `${p.pct}%`).join(' | ') ?? ''} |`,
  `|---|${memory[0]?.points.map(() => '---').join('|') ?? ''}|`,
  ...memory.map((m) => `| ${m.job} | ${m.points.map((p) => mb(p.bytes)).join(' | ')} |`),
  ``,
].join('\n');
writeFileSync(`${out}/results.md`, md);

for (const [name, ok, detail] of checks) console.log(`   ${ok ? 'ok' : 'FAIL'}: ${name}${ok ? '' : ` (${detail})`}`);
console.log(`   RESULT: ${accepted} accepted, ${result.submissions.acceptedPerSecond}/s; submit p95 ${dur['p(95)']?.toFixed(1)} ms; drained in ${result.window.drainSeconds} s`);
const failed = checks.filter(([, ok]) => !ok).length;
console.log(failed ? `== LOAD TEST FAILED (${failed} checks)` : '== LOAD TEST PASSED');
process.exit(failed ? 1 : 0);
