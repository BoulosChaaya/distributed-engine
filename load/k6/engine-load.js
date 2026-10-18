// Steady multi-tenant load against the tenant API, for scripts/load-test.sh.
//
// Three tenants at different rates submit a mix of task kinds. Every accepted
// submission is written to the console log ("A <kind> <tenant> <taskId>
// <token>") so the driver can check afterwards, in PostgreSQL, that each one
// exists and ended in the state its kind must end in. The script itself only
// checks what it can see: response codes and idempotent replays.
//
// Environment:
//   API            base URL (default http://api:3000)
//   SINK           base URL the http tasks call (default http://sink:8080)
//   DURATION       e.g. 60m
//   RATE_HEAVY / RATE_MEDIUM / RATE_LIGHT   submissions per second
//   KEY_HEAVY / KEY_MEDIUM / KEY_LIGHT      tenant API keys
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const API = __ENV.API || 'http://api:3000';
const SINK = __ENV.SINK || 'http://sink:8080';
const DURATION = __ENV.DURATION || '10m';

const accepted = new Counter('engine_accepted');
const rejected = new Counter('engine_rejected');
const idempotencyMismatch = new Counter('engine_idempotency_mismatch');

function tenantScenario(name, rate) {
  return {
    executor: 'constant-arrival-rate',
    exec: 'submit',
    rate: Number(rate),
    timeUnit: '1s',
    duration: DURATION,
    preAllocatedVUs: Math.max(10, Number(rate) * 3),
    maxVUs: Math.max(50, Number(rate) * 40),
    env: { TENANT: name },
    tags: { tenant: name },
  };
}

export const options = {
  scenarios: {
    heavy: tenantScenario('heavy', __ENV.RATE_HEAVY || 10),
    medium: tenantScenario('medium', __ENV.RATE_MEDIUM || 4),
    light: tenantScenario('light', __ENV.RATE_LIGHT || 2),
  },
  // A request still waiting on a dependency outage is not abandoned early:
  // the point is to see how long it takes, not to time it out.
  thresholds: {},
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const KEYS = { heavy: __ENV.KEY_HEAVY, medium: __ENV.KEY_MEDIUM, light: __ENV.KEY_LIGHT };

function token() {
  return `${exec.scenario.name}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
}

// Weighted mix. Expected final state is decided by the kind, see the driver.
const MIX = [
  [45, 'sleep'],
  [20, 'echo'],
  [18, 'http'],
  [8, 'flaky'],
  [5, 'scheduled'],
  [2, 'poison'],
  [2, 'workflow'],
];
const TOTAL = MIX.reduce((s, [w]) => s + w, 0);

function pickKind() {
  let r = Math.random() * TOTAL;
  for (const [w, kind] of MIX) {
    if ((r -= w) < 0) return kind;
  }
  return 'echo';
}

function body(kind, tok) {
  switch (kind) {
    case 'sleep': return { name: 'sleep', payload: { ms: 100 + Math.floor(Math.random() * 700) } };
    case 'echo': return { name: 'echo', payload: { token: tok } };
    case 'http': return { name: 'http', payload: { url: `${SINK}/hit/${tok}`, method: 'POST', body: { token: tok } } };
    // Fails once, then succeeds: exercises the retry path under load.
    case 'flaky': return { name: 'flaky', payload: { failTimes: 1 } };
    // Released by the scheduler 20-60 s later.
    case 'scheduled': return { name: 'echo', payload: { token: tok }, scheduledFor: new Date(Date.now() + 20000 + Math.random() * 40000).toISOString() };
    // Never succeeds within its budget: must end FAILED, nothing else.
    case 'poison': return { name: 'flaky', payload: { failTimes: 10 }, maxRetries: 1 };
    default: return null;
  }
}

const WORKFLOW = (tok) => ({
  name: `load-diamond-${tok}`,
  idempotencyKey: `wf-${tok}`,
  steps: [
    { key: 'start', name: 'echo', payload: { token: tok } },
    { key: 'left', name: 'sleep', payload: { ms: 200 }, dependsOn: ['start'] },
    { key: 'right', name: 'echo', payload: { side: 'right' }, dependsOn: ['start'] },
    { key: 'join', name: 'echo', dependsOn: ['left', 'right'] },
  ],
});

function post(path, payload, tenant) {
  return http.post(`${API}${path}`, JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEYS[tenant]}` },
    tags: { route: path },
    timeout: '90s',
  });
}

export function submit() {
  const tenant = __ENV.TENANT;
  const kind = pickKind();
  const tok = token();

  if (kind === 'workflow') {
    const res = post('/tenant/workflows', WORKFLOW(tok), tenant);
    if (res.status === 201 || res.status === 200) {
      accepted.add(1, { kind });
      console.log(`W ${tenant} ${res.json('data.workflow.id')}`);
    } else {
      rejected.add(1, { status: String(res.status), kind });
      console.log(`R ${res.status} ${kind} ${Math.floor(Date.now() / 1000)}`);
    }
    return;
  }

  const payload = body(kind, tok);
  payload.idempotencyKey = `t-${tok}`;
  const res = post('/tenant/tasks', payload, tenant);
  const ok = check(res, { 'task accepted': (r) => r.status === 201 });
  if (!ok) {
    rejected.add(1, { status: String(res.status), kind });
    console.log(`R ${res.status} ${kind} ${Math.floor(Date.now() / 1000)}`);
    return;
  }
  const id = res.json('data.id');
  accepted.add(1, { kind });
  console.log(`A ${kind} ${tenant} ${id} ${tok}`);

  // 5%: send the same request again. It must return the same task (200),
  // never create a second one.
  if (Math.random() < 0.05) {
    const again = post('/tenant/tasks', payload, tenant);
    const same = again.status === 200 && again.json('data.id') === id;
    check(again, { 'idempotent replay returns the same task': () => same });
    if (!same && again.status !== 0 && again.status < 500) {
      idempotencyMismatch.add(1);
      console.log(`M ${tenant} ${id} status=${again.status}`);
    }
  }
}

export function handleSummary(data) {
  return { [`${__ENV.OUT_DIR || '/out'}/k6-summary.json`]: JSON.stringify(data, null, 2) };
}
