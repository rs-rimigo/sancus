// Benchmark upstream: a static JSON echo on :8000 across 4 processes so it never becomes the
// bottleneck, plus a token-verification stub on :8001 that answers "Bearer good" -> {id:1}.
const cluster = require('cluster');
const http = require('http');

if (cluster.isPrimary) {
  for (let i = 0; i < 4; i++) cluster.fork();
  http.createServer((req, res) => {
    let b = '';
    req.on('data', (c) => (b += c));
    req.on('end', () => {
      const ok = /Bearer good/.test(b) || req.headers.authorization === 'Bearer good';
      res.writeHead(ok ? 200 : 401, { 'Content-Type': 'application/json' });
      res.end(ok ? '{"id":1,"role":"bench"}' : '{"message":"Invalid Token"}');
    });
  }).listen(8001);
} else {
  const body = Buffer.from('{"message":"hello world"}');
  http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': body.length });
    res.end(body);
  }).listen(8000);
}
