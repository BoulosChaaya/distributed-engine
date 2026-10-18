#!/usr/bin/env node
// Fails when the pods can open more PostgreSQL connections than PostgreSQL
// accepts. Worst case = sum over services of HPA maxReplicas x pool size,
// plus a fixed headroom for superuser-reserved slots, psql and backups.
//
// Reads the manifests in k8s/ directly (no cluster, no YAML library): each
// file is split into documents and the few fields needed are read with
// anchored patterns. Run from the repository root:
//   node scripts/check-db-connection-budget.mjs
import { readFileSync } from 'node:fs';

const HEADROOM = 20;
// Pool sizes the code uses when POSTGRES_MAX_CONNECTIONS is not set.
const CODE_DEFAULT_POOL = { api: 20, worker: 10 };
// Connections a pod holds outside its pool: the API's outbox publisher keeps
// one open for LISTEN outbox_wakeup (OUTBOX_LISTEN, on by default).
const DEDICATED = { api: 1, worker: 0 };

const docs = (file) => readFileSync(file, 'utf8').split(/^---\s*$/m);
const kindOf = (d) => d.match(/^kind:\s*(\S+)/m)?.[1];

function service(name, file) {
  const ds = docs(file);
  const hpa = ds.find((d) => kindOf(d) === 'HorizontalPodAutoscaler');
  const deploy = ds.find((d) => kindOf(d) === 'Deployment');
  const maxReplicas = Number(hpa?.match(/^\s*maxReplicas:\s*(\d+)/m)?.[1] ?? deploy.match(/^\s*replicas:\s*(\d+)/m)[1]);
  const pool = deploy.match(/- name: POSTGRES_MAX_CONNECTIONS\s*\n\s*value:\s*"?(\d+)"?/);
  const listenOff = /- name: OUTBOX_LISTEN\s*\n\s*value:\s*"?false"?/.test(deploy);
  return { name, maxReplicas, pool: pool ? Number(pool[1]) : CODE_DEFAULT_POOL[name], fromCode: !pool, dedicated: listenOff ? 0 : DEDICATED[name] };
}

const services = [service('api', 'k8s/api.yaml'), service('worker', 'k8s/worker.yaml')];
const maxConnections = Number(readFileSync('k8s/postgres.yaml', 'utf8').match(/max_connections=(\d+)/)[1]);

let total = HEADROOM;
for (const s of services) {
  const n = s.maxReplicas * (s.pool + s.dedicated);
  total += n;
  const per = s.dedicated ? `(${s.pool} pooled + ${s.dedicated} dedicated)` : `${s.pool}`;
  console.log(`${s.name.padEnd(7)} ${String(s.maxReplicas).padStart(3)} pods x ${per.padStart(3)} connections${s.fromCode ? ' (code default)' : ''} = ${n}`);
}
console.log(`headroom                         = ${HEADROOM}`);
console.log(`worst case ${total} / max_connections ${maxConnections}`);

if (total > maxConnections) {
  console.error(`FAIL: at full scale the pods can open ${total} connections but PostgreSQL accepts ${maxConnections}.`);
  console.error('Lower POSTGRES_MAX_CONNECTIONS or HPA maxReplicas, or raise max_connections (and PostgreSQL memory).');
  process.exit(1);
}
console.log('ok: connection budget fits');
