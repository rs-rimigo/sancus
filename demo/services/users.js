// Demo "users" service: shows what an upstream receives from the gateway.
const http = require('http');
const orders = [{ id: 1, item: 'Sancus mug', total: 12.5 }];
http.createServer((req, res) => {
  const [path] = req.url.split('?');
  const json = (code, body) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body, null, 2)); };
  console.log(`users ${req.method} ${req.url}  user=${req.headers['x-authorized-for-id'] || '-'} role=${req.headers['x-user-role'] || '-'} request-id=${req.headers['x-request-id']}`);
  if (path === '/profile') return json(200, { user: req.headers['x-authorized-for-id'], role: req.headers['x-user-role'], requestId: req.headers['x-request-id'], seenHeaders: Object.keys(req.headers) });
  if (path === '/orders' && req.method === 'GET') return json(200, { user: req.headers['x-authorized-for-id'], orders });
  if (path === '/orders' && req.method === 'POST') { let b = ''; req.on('data', (c) => (b += c)).on('end', () => { const o = { id: orders.length + 1, ...JSON.parse(b || '{}') }; orders.push(o); json(201, o); }); return; }
  json(404, { message: 'not found in users service' });
}).listen(Number(process.env.PORT) || 8010, () => console.log('users service on', process.env.PORT || 8010));
