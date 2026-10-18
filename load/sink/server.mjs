// The outside world for the load test's `http` tasks. Counts every call per
// token, so the driver can tell a task whose side effect never happened (a
// COMPLETED task with 0 calls: a bug) from one that ran more than once
// (at-least-once execution after a crash: expected, and counted).
import http from 'node:http';

const hits = new Map();
const port = Number(process.env.PORT || 8080);

http.createServer((req, res) => {
  const m = req.url.match(/^\/hit\/([\w.-]+)$/);
  if (m) {
    hits.set(m[1], (hits.get(m[1]) || 0) + 1);
    req.resume();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end('{"ok":true}');
    return;
  }
  if (req.url === '/dump') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    for (const [token, n] of hits) res.write(`${token}\t${n}\n`);
    res.end();
    return;
  }
  if (req.url === '/healthz') {
    res.writeHead(200);
    res.end('ok');
    return;
  }
  res.writeHead(404);
  res.end();
}).listen(port, () => console.log(`sink listening on ${port}`));
