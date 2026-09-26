// Mock upstreams and auth service for the e2e suite. Node built-ins only.
const http = require('http');
const zlib = require('zlib');

const state = { count: 0, swr: 0, authCalls: 0 };

function upstream(label) {
  return (req, res) => {
    const u = new URL(req.url, 'http://x');
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const body = Buffer.concat(chunks);
      const p = u.pathname;
      const json = (obj, headers = {}) => {
        res.writeHead(200, { 'Content-Type': 'application/json', ...headers });
        res.end(JSON.stringify(obj));
      };
      if (p === '/slow') return setTimeout(() => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('slow-done'); }, Number(u.searchParams.get('ms') || 3000));
      if (p === '/sse') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        let i = 0;
        const t = setInterval(() => { res.write(`data: ${++i}\n\n`); if (i === 3) { clearInterval(t); res.end(); } }, 200);
        return;
      }
      if (p === '/nostore') return json({ x: 1 }, { 'Cache-Control': 'no-store' });
      if (p === '/cookie') return json({ x: 1 }, { 'Set-Cookie': 's=1' });
      if (p === '/fail') { res.writeHead(500); return res.end('boom'); }
      if (p === '/gz') {
        const gz = zlib.gzipSync(JSON.stringify({ gz: true, pad: 'x'.repeat(2000) }));
        res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Encoding': 'gzip' });
        return res.end(gz);
      }
      if (p === '/count') return json({ count: ++state.count });
      if (p === '/swr') return json({ swr: ++state.swr });
      if (p === '/multipart') {
        const files = (body.toString('latin1').match(/filename="/g) || []).length;
        return json({ files, bytes: body.length, ct: String(req.headers['content-type'] || '').split(';')[0] });
      }
      if (p === '/big') return json({ pad: 'y'.repeat(5000) });
      return json({ node: label, path: req.url, method: req.method, headers: req.headers, body: body.toString() });
    });
  };
}

function auth(req, res) {
  if (req.url === '/__stats') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ calls: state.authCalls })); }
  let b = '';
  req.on('data', (c) => (b += c));
  req.on('end', () => {
    state.authCalls++;
    const t = (JSON.parse(b || '{}').token || '');
    const users = { 'Bearer good': { id: 7, role: 'admin' }, 'Bearer other': { id: 8, role: 'user' } };
    if (users[t]) { res.writeHead(200, { 'Content-Type': 'application/json', 'x-user-role': users[t].role }); return res.end(JSON.stringify(users[t])); }
    res.writeHead(401, { 'Content-Type': 'application/json', 'www-authenticate': 'Bearer' });
    res.end(JSON.stringify({ message: 'Invalid Token' }));
  });
}

const servers = [
  http.createServer(upstream('A')).listen(8000),
  http.createServer(upstream('B')).listen(8002),
  http.createServer(auth).listen(8001),
];
process.on('SIGTERM', () => { servers.forEach((s) => s.close()); process.exit(0); });
console.log('mocks ready');
