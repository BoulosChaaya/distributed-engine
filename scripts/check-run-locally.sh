#!/usr/bin/env bash
# Runs docs/RUN-LOCALLY.md top to bottom: every ```bash block, in order, in
# one shell, the way a person following the guide would. Blocks marked
# "(in the browser)" are prose, not code, so nothing is skipped silently.
# Fails on the first command that fails. Each block's output is printed
# after a "@@@ step N" line, which `scripts/run-locally-fill.mjs <log>` uses
# to refresh the outputs shown in the guide.
set -euo pipefail
cd "$(dirname "$0")/.."

doc=docs/RUN-LOCALLY.md
steps="${RUNNER_TEMP:-/tmp}/run-locally-steps.sh"
node -e '
const md = require("fs").readFileSync(process.argv[1], "utf8");
const blocks = [...md.matchAll(/^```bash\n([\s\S]*?)^```$/gm)].map((m) => m[1]);
let out = "set -euo pipefail\n";
blocks.forEach((b, i) => { out += `printf "\\n@@@ step %d\\n" ${i + 1}\n${b}`; });
out += "printf \"\\n@@@ end\\n\"\n";
require("fs").writeFileSync(process.argv[2], out);
console.error(`${blocks.length} blocks`);
' "$doc" "$steps"

trap 'docker compose --profile metrics down -v --remove-orphans >/dev/null 2>&1 || true; rm -f .env' EXIT
[ ! -f .env ] || { echo "refusing to run: .env exists here (the guide would reuse it)"; exit 1; }
bash "$steps"
echo "== RUN-LOCALLY GUIDE PASSED"
