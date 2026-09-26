// Demo token-verification service. Sancus calls POST /v1/verify/token with { token }.
// "Bearer alice" is an admin, "Bearer bob" a user, anything else is rejected.
const http = require('http');
const users = { 'Bearer alice': { id: 'alice', role: 'admin' }, 'Bearer bob': { id: 'bob', role: 'user' } };
http.createServer((req, res) => {
  let b = '';
  req.on('data', (c) => (b += c)).on('end', () => {
    const token = (() => { try { return JSON.parse(b).token; } catch { return ''; } })();
    const u = users[token];
    console.log(`auth  ${token || '(no token)'} -> ${u ? u.id : 401}   forwarded: ${req.headers['x-forwarded-method']} ${req.headers['x-forwarded-uri']}`);
    if (!u) { res.writeHead(401, { 'Content-Type': 'application/json', 'WWW-Authenticate': 'Bearer' }); return res.end('{"message":"Invalid Token"}'); }
    res.writeHead(200, { 'Content-Type': 'application/json', 'X-User-Role': u.role }); // X-User-Role is copied upstream via AUTH_UPSTREAM_HEADERS
    res.end(JSON.stringify(u));
  });
}).listen(Number(process.env.PORT) || 8001, () => console.log('auth service on', process.env.PORT || 8001));
