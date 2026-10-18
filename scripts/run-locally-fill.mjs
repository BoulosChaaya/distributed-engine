// Puts the output of each step, from a log of scripts/check-run-locally.sh,
// into the <!-- output:start --> ... <!-- output:end --> slot after the
// matching ```bash block of docs/RUN-LOCALLY.md.
//   node scripts/run-locally-fill.mjs <log>
import { readFileSync, writeFileSync } from 'node:fs';

const doc = 'docs/RUN-LOCALLY.md';
// Strip terminal colour codes (ESC [ ... m).
const log = readFileSync(process.argv[2], 'utf8').replace(new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, 'g'), '');
const outputs = new Map();
for (const part of log.split(/^@@@ /m).slice(1)) {
  const [head, ...rest] = part.split('\n');
  const m = head.match(/^step (\d+)/);
  if (m) outputs.set(Number(m[1]), rest.join('\n').trim());
}
let n = 0;
const md = readFileSync(doc, 'utf8').replace(
  /(^```bash\n[\s\S]*?^```$\n(?:\n)?)(?:<!-- output:start -->[\s\S]*?<!-- output:end -->)?/gm,
  (all, block) => {
    n++;
    const text = outputs.get(n);
    if (text === undefined) return all;
    return `${block}<!-- output:start -->\n${text ? '```text\n' + text + '\n```\n' : ''}<!-- output:end -->`;
  },
);
writeFileSync(doc, md);
console.log(`filled ${outputs.size} step outputs into ${doc}`);
