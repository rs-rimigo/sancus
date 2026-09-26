// Demo "catalog" service. Run two copies (REPLICA=a/b) to see round-robin and failover.
const http = require('http');
const replica = process.env.REPLICA || 'a';
const products = [{ id: 1, name: 'Gateway', price: 0 }, { id: 2, name: 'Rate limit', price: 0 }, { id: 3, name: 'Cache', price: 0 }];
let hits = 0;
http.createServer((req, res) => {
  const [path, query] = req.url.split('?');
  const json = (code, body, headers = {}) => { res.writeHead(code, { 'Content-Type': 'application/json', ...headers }); res.end(JSON.stringify(body, null, 2)); };
  hits++;
  console.log(`catalog-${replica} ${req.method} ${req.url}  (hit #${hits})`);
  if (path === '/products') return json(200, { servedBy: replica, generatedAt: new Date().toISOString(), hit: hits, products });
  if (/^\/products\/\d+$/.test(path)) { const p = products.find((x) => x.id === Number(path.split('/')[2])); return p ? json(200, { servedBy: replica, ...p }) : json(404, { message: 'no such product' }); }
  if (path === '/inventory') return json(200, { servedBy: replica, stock: { 1: 12, 2: 0, 3: 7 } });
  if (/^\/inventory\/\d+$/.test(path)) return json(200, { servedBy: replica, id: Number(path.split('/')[2]), stock: 12 });
  if (path === '/search') return json(200, { servedBy: replica, q: new URLSearchParams(query).get('q'), lang: req.headers['accept-language'] || 'any', generatedAt: new Date().toISOString() });
  if (path === '/slow') return setTimeout(() => json(200, { servedBy: replica, tookMs: 2500 }), 2500);
  if (path === '/flaky') return json(500, { message: 'catalog is having a bad day' });
  if (path === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
    let i = 0; const t = setInterval(() => { res.write(`data: {"tick":${++i},"replica":"${replica}"}\n\n`); if (i === 5) { clearInterval(t); res.end(); } }, 400);
    return;
  }
  if (path === '/orders' && req.method === 'POST') { let b = ''; req.on('data', (c) => (b += c)).on('end', () => json(201, { servedBy: replica, accepted: JSON.parse(b || '{}'), idempotencyKey: req.headers['x-idempotency-key'] })); return; }
  if (path === '/admin/stats') return json(200, { servedBy: replica, hits });
  if (path === '/private-data') return json(200, { secret: 'never cached' }, { 'Cache-Control': 'no-store' });
  json(404, { message: 'not found in catalog service' });
}).listen(Number(process.env.PORT) || 8020, () => console.log(`catalog-${replica} on`, process.env.PORT || 8020));
