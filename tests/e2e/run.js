/* End-to-end suite. Requires Redis on localhost:6379. Run: npm run test:e2e
 * GATEWAY_ENTRY=/path/to/build/index.js to test a packed/installed build. */
const { spawn, execFileSync } = require('child_process');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const zlib = require('zlib');

const ROOT = path.resolve(__dirname, '..', '..');
const ENTRY = process.env.GATEWAY_ENTRY || path.join(ROOT, 'build', 'index.js');
const CHECK = path.join(path.dirname(ENTRY), 'commands', 'check.js');
const GW = 'http://127.0.0.1:3100';
const GW2 = 'http://127.0.0.1:3101';

const results = [];
const check = (name, ok, detail = '') => { results.push({ name, ok, detail }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${ok ? '' : `  -- ${detail}`}`); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function req(method, url, { headers = {}, body, raw = false } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const t0 = Date.now();
    let firstByteAt = 0;
    const r = http.request({ host: u.hostname, port: u.port, path: u.pathname + u.search, method, headers }, (res) => {
      const chunks = [];
      res.on('data', (c) => { if (!firstByteAt) firstByteAt = Date.now(); chunks.push(c); });
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        let json;
        if (!raw) { try { json = JSON.parse(buf.toString()); } catch { /* not json */ } }
        resolve({ status: res.statusCode, headers: res.headers, body: buf, text: buf.toString(), json, ttfb: firstByteAt - t0, total: Date.now() - t0 });
      });
    });
    r.on('error', reject);
    if (body) r.write(body);
    r.end();
  });
}
const get = (url, headers) => req('GET', url, { headers });

async function waitFor(url, ms = 8000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { try { const r = await get(url); if (r.status < 500) return; } catch { /* retry */ } await sleep(100); }
  throw new Error(`timeout waiting for ${url}`);
}

function start(cmd, args, env, tag) {
  const p = spawn(cmd, args, { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  p.stdout.on('data', (d) => process.env.E2E_VERBOSE && process.stdout.write(`[${tag}] ${d}`));
  p.stderr.on('data', (d) => process.env.E2E_VERBOSE && process.stderr.write(`[${tag}] ${d}`));
  return p;
}

const yml = {
  demo: `
service:
  name: demo
  nodes: [http://127.0.0.1:8000]
  rewrite: { from: "^/v1", to: "" }
  timeout: 1500
  headers: { add: { X-Gateway: sancus }, remove: [x-secret] }
apis:
  - name: demo
    routes:
      - { path: /v1/echo, methods: [GET, POST], bypass: [AUTH, GEO_FENCE] }
      - { path: "/v1/echo/{int:id}", methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: "/v1/items/{str:slug}", methods: [GET], bypass: [AUTH, GEO_FENCE], headers: { add: { X-Route: param } } }
      - { path: /v1/items/special, methods: [GET], bypass: [AUTH, GEO_FENCE], headers: { add: { X-Route: exact } } }
      - { path: /v1/private, methods: [GET, POST], bypass: [GEO_FENCE] }
      - { path: /v1/optional, methods: [GET], bypass: [AUTH, GEO_FENCE], resolveUser: true }
      - { path: /v1/geo, methods: [GET], bypass: [AUTH] }
      - { path: /v1/rl, methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perMinute: 2 } }
      - { path: /v1/rlday, methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perDay: 1 } }
      - { path: /v1/g1, methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perMinute: 2, group: g } }
      - { path: /v1/g2, methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perMinute: 2, group: g } }
      - { path: /v1/hidden, methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perMinute: 5, hideHeaders: true } }
      - { path: /v1/count, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LRU, ttl: 60, key: PATH, browserTtl: 30 } }
      - { path: /v1/nostore, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LRU, ttl: 60 } }
      - { path: /v1/cookie, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LRU, ttl: 60 } }
      - { path: /v1/gz, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LFU, ttl: 60 } }
      - { path: /v1/vary, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LRU, ttl: 60, varyHeaders: [Accept-Language] } }
      - { path: /v1/user, methods: [GET], bypass: [GEO_FENCE], cache: { strategy: LRU, ttl: 60, key: PATH_QUERY_USER, browserTtl: 30 } }
      - { path: /v1/swr, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: SWR, ttl: 4, key: PATH } }
      - { path: /v1/slow, methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: /v1/sse, methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: /v1/multipart, methods: [POST], bypass: [AUTH, GEO_FENCE] }
      - { path: /v1/big, methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: /v1/fail, methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: /v1/blocked, methods: [GET], bypass: [AUTH, GEO_FENCE], policies: { ip-restriction: { deny: [203.0.113.99] } } }
      - { path: /v1/needs-header, methods: [GET], bypass: [AUTH, GEO_FENCE], policies: { require-header: { header: X-Idempotency-Key } } }
`,
  multi: `
service: { name: multi, nodes: [http://127.0.0.1:8000, http://127.0.0.1:8002] }
apis: [{ name: m, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
  failover: `
service: { name: failover, nodes: [http://127.0.0.1:8003, http://127.0.0.1:8000], retries: 1 }
apis: [{ name: f, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
  breaker: `
service: { name: breaker, nodes: [http://127.0.0.1:8003], circuitBreaker: { volumeThreshold: 3, errorThresholdPercentage: 50, resetTimeout: 60000 } }
apis: [{ name: b, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
  hostbound: `
service: { name: hostbound, nodes: [http://127.0.0.1:8000], hosts: [api.example.test] }
apis: [{ name: h, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
  legacy: `
service: { name: legacy, host: LEGACY_URL, port: 8000 }
apis: [{ name: l, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
  drain: `
service: { name: drain, nodes: [http://127.0.0.1:8000], timeout: 10000 }
apis: [{ name: d, routes: [{ path: /slow, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]
`,
};

const policyModule = `
module.exports = {
  name: 'require-header',
  priority: 10,
  schema: { type: 'object', required: ['header'], properties: { header: { type: 'string' } } },
  create: ({ header }) => (req, res, next) =>
    req.headers[header.toLowerCase()] ? next() : res.status(400).json({ message: 'missing ' + header }),
};
`;

async function flushRedis() {
  const Redis = require(path.join(ROOT, 'node_modules', 'ioredis'));
  const r = new Redis('redis://127.0.0.1:6379');
  for (const pattern of ['sancus:*', 'ratelimit:*', 'ipratelimit:*', 'blocked:*']) {
    let cursor = '0';
    do { const [n, keys] = await r.scan(cursor, 'MATCH', pattern, 'COUNT', 500); cursor = n; if (keys.length) await r.del(...keys); } while (cursor !== '0');
  }
  await r.quit();
}

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sancus-e2e-'));
  const cfg = path.join(tmp, 'api_configs');
  const pol = path.join(tmp, 'policies');
  fs.mkdirSync(cfg); fs.mkdirSync(pol);
  for (const [name, content] of Object.entries(yml)) fs.writeFileSync(path.join(cfg, `${name}.yml`), content);
  fs.writeFileSync(path.join(pol, 'require-header.js'), policyModule);
  fs.copyFileSync(path.join(ROOT, 'examples', 'geofence', 'india-states.json'), path.join(tmp, 'in.json'));
  fs.mkdirSync(path.join(tmp, 'bad'));
  fs.writeFileSync(path.join(tmp, 'bad', 'x.yml'), 'service: { name: bad }\napis: []\n'); // no host/nodes

  await flushRedis();
  const procs = [];
  const mocks = start('node', [path.join(__dirname, 'mocks.js')], {}, 'mocks'); procs.push(mocks);

  const baseEnv = {
    CONFIG_DIR: cfg, POLICIES_DIR: pol, GEOFENCE_FILE: path.join(tmp, 'in.json'), LEGACY_URL: 'http://127.0.0.1',
    AUTH_URL: 'http://127.0.0.1:8001', AUTH_UPSTREAM_HEADERS: 'x-user-role', AUTH_CLIENT_HEADERS: 'www-authenticate', ALLOWED_ORIGINS: 'https://app.example.test',
    TRUST_PROXY: '1', TRUSTED_IPS: '127.0.0.1', LOG_LEVEL: 'warn', SHUTDOWN_DELAY_MS: '300',
    IP_RATE_LIMIT_CAPACITY: '5', IP_RATE_LIMIT_REFILL_RATE: '1', IP_BLOCK_THRESHOLD: '3', IP_BLOCK_WINDOW_MS: '60000', IP_BLOCK_DURATION_MS: '60000',
  };
  const gw = start('node', [ENTRY], { ...baseEnv, PORT: '3100', ADMIN_TOKEN: 'secret', CONFIG_WATCH: 'true' }, 'gw1'); procs.push(gw);
  const gw2 = start('node', [ENTRY], { ...baseEnv, PORT: '3101', CONFIG_WATCH: 'false', GEOFENCE_FILE: path.join(ROOT, 'in.json'), REDIS_URL: 'redis://127.0.0.1:1', AUTH_URL: 'http://127.0.0.1:8009', AUTH_FAIL_OPEN: 'true' }, 'gw2'); procs.push(gw2);
  const cleanup = () => procs.forEach((p) => { try { p.kill('SIGKILL'); } catch { /* gone */ } });
  process.on('exit', cleanup);

  try {
    await waitFor(`${GW}/health`);
    await waitFor(`${GW2}/health`, 15000);

    // ---- config check command ----
    let out = execFileSync('node', [CHECK, cfg]).toString();
    check('check: valid dir exits 0 and lists services', /OK\s+demo\.yml/.test(out));
    let bad = null; try { execFileSync('node', [CHECK, path.join(tmp, 'bad')], { stdio: 'pipe' }); } catch (e) { bad = e; }
    check('check: invalid config exits 1 with an error', bad && bad.status === 1 && /ERROR/.test(String(bad.stderr)), bad && String(bad.stderr));

    // ---- admin ----
    let r = await get(`${GW}/health/ready`);
    check('ready: 200 with config + redis', r.status === 200 && r.json.status === 'UP', r.text);
    r = await get(`${GW}/metrics`);
    check('metrics: 401 without ADMIN_TOKEN', r.status === 401);
    r = await get(`${GW}/metrics`, { Authorization: 'Bearer secret' });
    check('metrics: Prometheus text with sancus_ metrics', r.status === 200 && /sancus_http_requests_total/.test(r.text) && /text\/plain/.test(r.headers['content-type']));
    r = await get(`${GW}/routes`, { Authorization: 'Bearer secret' });
    check('routes: lists services and policies', r.status === 200 && r.json.services.some((s) => s.name === 'demo') && r.json.policies.includes('require-header') && r.json.policies.includes('ip-restriction'));
    r = await get(`${GW2}/health/ready`);
    check('ready (degraded): 200 DEGRADED with redis down, pod stays in service', r.status === 200 && r.json.status === 'DEGRADED' && r.json.redis === 'down', r.text);

    // ---- request id, CORS, security headers ----
    r = await get(`${GW}/api/demo/v1/echo`, { 'X-Request-Id': 'trace-abc' });
    check('request id: incoming honoured, echoed, forwarded', r.headers['x-request-id'] === 'trace-abc' && r.json.headers['x-request-id'] === 'trace-abc' && r.json.headers['correlation-id'] === 'trace-abc');
    r = await get(`${GW}/api/demo/v1/echo`);
    check('request id: generated uuid when absent', /^[0-9a-f-]{36}$/.test(r.headers['x-request-id'] || ''));
    check('no x-powered-by', r.headers['x-powered-by'] === undefined);
    r = await req('OPTIONS', `${GW}/api/demo/v1/echo`, { headers: { Origin: 'https://app.example.test', 'Access-Control-Request-Method': 'POST' } });
    check('cors: preflight for allowed origin', r.status === 200 && r.headers['access-control-allow-origin'] === 'https://app.example.test' && /PATCH/.test(r.headers['access-control-allow-methods']));
    r = await get(`${GW}/api/demo/v1/echo`, { Origin: 'https://evil.example' });
    check('cors: no ACAO for disallowed origin', r.headers['access-control-allow-origin'] === undefined);

    // ---- routing ----
    r = await get(`${GW}/api/demo/v1/echo?a=1&b=2`);
    check('proxy: rewrite strips /v1 and keeps query', r.status === 200 && r.json.path === '/echo?a=1&b=2' && r.json.node === 'A');
    check('headers: service add/remove applied', r.json.headers['x-gateway'] === 'sancus');
    r = await get(`${GW}/api/demo/v1/echo`, { 'x-secret': 'hide-me' });
    check('headers: remove strips x-secret', r.json.headers['x-secret'] === undefined);
    check('headers: X-Forwarded-For/Host set upstream', !!r.json.headers['x-forwarded-for'] && !!r.json.headers['x-forwarded-host']);
    r = await get(`${GW}/api/demo/v1/echo/42`);
    check('routing: typed int param matches', r.status === 200);
    r = await get(`${GW}/api/demo/v1/echo/abc`);
    check('routing: int param rejects non-digits -> 404', r.status === 404);
    r = await get(`${GW}/api/demo/v1/items/special`);
    check('routing: exact route beats parameterized', r.json.headers['x-route'] === 'exact');
    r = await get(`${GW}/api/demo/v1/items/other`);
    check('routing: parameterized still matches', r.json.headers['x-route'] === 'param');
    r = await req('DELETE', `${GW}/api/demo/v1/echo`);
    check('routing: 405 with Allow', r.status === 405 && r.headers.allow === 'GET, POST, HEAD', r.headers.allow);
    r = await get(`${GW}/api/nope/x`);
    check('routing: unknown service 404', r.status === 404);
    r = await get(`${GW}/api/hostbound/echo`);
    check('hosts: wrong Host -> 404', r.status === 404);
    r = await get(`${GW}/api/hostbound/echo`, { Host: 'api.example.test' });
    check('hosts: matching Host -> 200', r.status === 200);
    r = await get(`${GW}/api/legacy/echo`);
    check('upstream: legacy host env + port works', r.status === 200 && r.json.node === 'A');

    // ---- auth ----
    r = await get(`${GW}/api/demo/v1/private`);
    check('auth: missing token -> 401', r.status === 401);
    r = await get(`${GW}/api/demo/v1/private`, { Authorization: 'Bearer bad' });
    check('auth: rejection returned verbatim (401 body + WWW-Authenticate)', r.status === 401 && r.json.message === 'Invalid Token' && r.headers['www-authenticate'] === 'Bearer');
    const before = (await get('http://127.0.0.1:8001/__stats')).json.calls;
    r = await get(`${GW}/api/demo/v1/private`, { Authorization: 'Bearer good' });
    check('auth: valid token proxied with identity + upstream headers', r.status === 200 && r.json.headers['x-authorized-for-id'] === '7' && r.json.headers['x-user-role'] === 'admin');
    check('auth: X-Forwarded-* sent to auth service is not leaked to upstream as identity', r.json.headers['x-forwarded-uri'] === undefined);
    await get(`${GW}/api/demo/v1/private`, { Authorization: 'Bearer good' });
    await get(`${GW}/api/demo/v1/private`, { Authorization: 'Bearer good' });
    const after = (await get('http://127.0.0.1:8001/__stats')).json.calls;
    check('auth: token cache — 3 requests, 1 auth call', after - before === 1, `calls=${after - before}`);
    r = await get(`${GW}/api/demo/v1/optional`);
    check('resolveUser: anonymous passes', r.status === 200 && r.json.headers['x-authorized-for-id'] === undefined);
    r = await get(`${GW}/api/demo/v1/optional`, { Authorization: 'Bearer other' });
    check('resolveUser: identity resolved when token present', r.status === 200 && r.json.headers['x-authorized-for-id'] === '8');
    r = await get(`${GW2}/api/demo/v1/private`, { Authorization: 'Bearer good' });
    check('auth fail-open (degraded): auth down + AUTH_FAIL_OPEN -> anonymous 200', r.status === 200 && r.json.headers['x-authorized-for-id'] === undefined, `${r.status}`);

    // ---- geo-fence ----
    r = await get(`${GW}/api/demo/v1/geo`);
    check('geo: missing coordinates -> 400 SE0406', r.status === 400 && r.json.response_code === 'SE0406');
    r = await get(`${GW}/api/demo/v1/geo`, { 'X-COORDINATES': 'x,y' });
    check('geo: invalid coordinates -> 400 SE0407', r.status === 400 && r.json.response_code === 'SE0407');
    r = await get(`${GW}/api/demo/v1/geo`, { 'X-COORDINATES': '17.385,78.4867' });
    check('geo: Hyderabad inside banned polygon -> SE0405', r.status === 400 && r.json.response_code === 'SE0405');
    r = await get(`${GW}/api/demo/v1/geo`, { 'X-COORDINATES': '51.5,-0.1' });
    check('geo: London outside -> proxied', r.status === 200 && r.json.node === 'A');
    r = await get(`${GW}/api/geo/check`, { 'X-COORDINATES': '51.5,-0.1' });
    check('geo: /api/geo/check answers directly', r.status === 200 && r.json.response_code === 'SS0200');
    r = await get(`${GW2}/api/demo/v1/geo`);
    check('geo: empty polygon set disables the check (no header needed)', r.status === 200 && r.json.node === 'A', `${r.status}`);
    r = await get(`${GW2}/api/geo/check`);
    check('geo: /api/geo/check reports allowed when disabled', r.status === 200 && r.json.response_code === 'SS0200');

    // ---- rate limiting (per-route; distinct XFF IPs, TRUST_PROXY=1) ----
    const ip = (n) => ({ 'X-Forwarded-For': `203.0.113.${n}` });
    r = await get(`${GW}/api/demo/v1/rl`, ip(1));
    check('ratelimit: headers on allow (both families)', r.headers['x-ratelimit-limit'] === '2' && r.headers['x-ratelimit-remaining'] === '1' && r.headers['ratelimit-limit'] === '2, 2;w=60' && r.headers['ratelimit-reset'] !== undefined);
    await get(`${GW}/api/demo/v1/rl`, ip(1));
    r = await get(`${GW}/api/demo/v1/rl`, ip(1));
    check('ratelimit: 3rd/min -> 429 + Retry-After', r.status === 429 && Number(r.headers['retry-after']) > 0 && r.headers['x-ratelimit-remaining'] === '0');
    r = await get(`${GW}/api/demo/v1/rl`, ip(2));
    check('ratelimit: other client IP unaffected (trust proxy)', r.status === 200);
    await get(`${GW}/api/demo/v1/rlday`, ip(3));
    r = await get(`${GW}/api/demo/v1/rlday`, ip(3));
    check('ratelimit: daily window', r.status === 429 && /Daily/.test(r.json.message));
    await get(`${GW}/api/demo/v1/g1`, ip(4)); await get(`${GW}/api/demo/v1/g1`, ip(4));
    r = await get(`${GW}/api/demo/v1/g2`, ip(4));
    check('ratelimit: group shares quota across routes', r.status === 429);
    r = await get(`${GW}/api/demo/v1/hidden`, ip(5));
    check('ratelimit: hideHeaders suppresses headers', r.status === 200 && r.headers['x-ratelimit-limit'] === undefined);
    r = await get(`${GW2}/api/demo/v1/rl`, ip(1));
    check('ratelimit fail-open (degraded): redis down -> 200, no headers', r.status === 200 && r.headers['x-ratelimit-limit'] === undefined);

    // ---- IP limiter: capacity 5, then block after 3 bursts ----
    const burst = ip(50);
    const codes = [];
    for (let i = 0; i < 9; i++) codes.push((await get(`${GW}/api/demo/v1/echo`, burst)).status);
    check('ip limiter: 5 allowed, then 429s, then 403 block', codes.slice(0, 5).every((c) => c === 200) && codes.slice(5, 8).every((c) => c === 429) && codes[8] === 403, codes.join(','));
    r = await get(`${GW}/api/demo/v1/echo`, burst);
    check('ip limiter: blocked response has blockedUntil + Retry-After', r.status === 403 && !!r.json.blockedUntil && !!r.headers['retry-after']);
    r = await get(`${GW}/api/demo/v1/echo`);
    check('ip limiter: trusted IP bypasses', r.status === 200);

    // ---- cache ----
    r = await get(`${GW}/api/demo/v1/count`);
    const c1 = r.json.count;
    check('cache: MISS with X-Cache-Key', r.headers['x-cache-status'] === 'MISS' && /^[0-9a-f]{16}$/.test(r.headers['x-cache-key']));
    await sleep(150);
    r = await get(`${GW}/api/demo/v1/count`);
    check('cache: HIT serves stored body + Age + ETag + Cache-Control', r.headers['x-cache-status'] === 'HIT' && r.json.count === c1 && r.headers.age !== undefined && /^W\//.test(r.headers.etag) && /public, max-age=30/.test(r.headers['cache-control']));
    const etag = r.headers.etag;
    r = await get(`${GW}/api/demo/v1/count`, { 'If-None-Match': etag });
    check('cache: If-None-Match -> 304', r.status === 304 && r.body.length === 0);
    r = await req('HEAD', `${GW}/api/demo/v1/count`);
    check('cache: HEAD served from cache without body', r.status === 200 && r.headers['x-cache-status'] === 'HIT' && r.body.length === 0);
    r = await get(`${GW}/api/demo/v1/count`, { 'Cache-Control': 'no-cache' });
    check('cache: request no-cache bypasses lookup (fresh upstream)', r.json.count === c1 + 1 && r.headers['x-cache-status'] === undefined);
    await get(`${GW}/api/demo/v1/nostore`); await sleep(100);
    r = await get(`${GW}/api/demo/v1/nostore`);
    check('cache: upstream no-store -> BYPASS', r.headers['x-cache-status'] === 'BYPASS');
    await get(`${GW}/api/demo/v1/cookie`); await sleep(100);
    r = await get(`${GW}/api/demo/v1/cookie`);
    check('cache: Set-Cookie -> BYPASS', r.headers['x-cache-status'] === 'BYPASS');
    r = await get(`${GW}/api/demo/v1/gz`); await sleep(150);
    r = await get(`${GW}/api/demo/v1/gz`);
    let gzOk = false; try { gzOk = JSON.parse(zlib.gunzipSync(r.body).toString()).gz === true; } catch { /* corrupt */ }
    check('cache: gzip body survives a HIT byte-for-byte', r.headers['x-cache-status'] === 'HIT' && r.headers['content-encoding'] === 'gzip' && gzOk);
    await get(`${GW}/api/demo/v1/vary`, { 'Accept-Language': 'en' }); await sleep(100);
    r = await get(`${GW}/api/demo/v1/vary`, { 'Accept-Language': 'fr' });
    const fr = r.headers['x-cache-status'];
    r = await get(`${GW}/api/demo/v1/vary`, { 'Accept-Language': 'en' });
    check('cache: varyHeaders separate entries', fr === 'MISS' && r.headers['x-cache-status'] === 'HIT' && /Accept-Language/.test(r.headers.vary));
    r = await get(`${GW}/api/demo/v1/user`, { Authorization: 'Bearer good' }); await sleep(100);
    const u1 = r.headers['x-cache-status'];
    r = await get(`${GW}/api/demo/v1/user`, { Authorization: 'Bearer other' });
    const u2 = r.headers['x-cache-status'];
    r = await get(`${GW}/api/demo/v1/user`, { Authorization: 'Bearer good' });
    check('cache: PATH_QUERY_USER isolates users, private + Vary Authorization', u1 === 'MISS' && u2 === 'MISS' && r.headers['x-cache-status'] === 'HIT' && /private/.test(r.headers['cache-control']) && /Authorization/.test(r.headers.vary));
    r = await get(`${GW}/api/demo/v1/swr`);
    const s1 = r.json.swr;
    await sleep(3200);
    r = await get(`${GW}/api/demo/v1/swr`);
    check('cache SWR: stale served immediately with STALE', r.headers['x-cache-status'] === 'STALE' && r.json.swr === s1);
    await sleep(400);
    r = await get(`${GW}/api/demo/v1/swr`);
    check('cache SWR: background revalidation refreshed the entry', r.headers['x-cache-status'] === 'HIT' && r.json.swr === s1 + 1, `${r.headers['x-cache-status']} ${r.json.swr} vs ${s1 + 1}`);
    r = await req('DELETE', `${GW}/cache/demo`, { headers: { Authorization: 'Bearer secret' } });
    check('cache: purge by service', r.status === 200 && r.json.purged >= 5, r.text);
    r = await get(`${GW}/api/demo/v1/count`);
    check('cache: MISS after purge', r.headers['x-cache-status'] === 'MISS');
    r = await get(`${GW2}/api/demo/v1/count`);
    check('cache (degraded): redis down -> proxied, no cache headers', r.status === 200 && r.headers['x-cache-status'] === 'MISS');

    // ---- streaming, compression, multipart, errors ----
    r = await get(`${GW}/api/demo/v1/sse`);
    check('sse: streamed (first byte well before end), no compression', /event-stream/.test(r.headers['content-type']) && r.total - r.ttfb >= 300 && r.headers['content-encoding'] === undefined && (r.text.match(/data:/g) || []).length === 3, `ttfb=${r.ttfb} total=${r.total}`);
    r = await get(`${GW}/api/demo/v1/big`, { 'Accept-Encoding': 'gzip' });
    check('compression: large JSON gzipped', r.headers['content-encoding'] === 'gzip' && JSON.parse(zlib.gunzipSync(r.body).toString()).pad.length === 5000);
    const boundary = '----e2e' + Date.now();
    const png = Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]), Buffer.alloc(300, 7)]);
    const mp = Buffer.concat([
      Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="title"\r\n\r\nhello\r\n`),
      Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="a.png"\r\nContent-Type: image/png\r\n\r\n`), png, Buffer.from('\r\n'),
      Buffer.from(`--${boundary}--\r\n`),
    ]);
    r = await req('POST', `${GW}/api/demo/v1/multipart`, { headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': mp.length }, body: mp });
    check('multipart: file forwarded upstream', r.status === 200 && r.json.files === 1 && r.json.ct === 'multipart/form-data' && r.json.bytes > 300, r.text);
    const bad2 = Buffer.concat([Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="x.exe"\r\nContent-Type: application/x-msdownload\r\n\r\nMZ\r\n--${boundary}--\r\n`)]);
    r = await req('POST', `${GW}/api/demo/v1/multipart`, { headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': bad2.length }, body: bad2 });
    check('multipart: disallowed type -> 400', r.status === 400);
    r = await get(`${GW}/api/demo/v1/fail`);
    check('proxy: upstream 500 passed through', r.status === 500 && r.text === 'boom');
    r = await get(`${GW}/api/demo/v1/slow`);
    check('proxy: upstream timeout -> 504', r.status === 504 && r.json.response_code === 'SE0504');
    r = await req('POST', `${GW}/api/demo/v1/echo`, { headers: { 'Content-Type': 'application/json' }, body: '{"k":"v"}' });
    check('proxy: POST body streamed intact', r.json.body === '{"k":"v"}');

    // ---- upstream nodes: round-robin, failover, breaker ----
    const a = (await get(`${GW}/api/multi/echo`)).json.node, b = (await get(`${GW}/api/multi/echo`)).json.node;
    check('nodes: round-robin alternates', a !== b && ['A', 'B'].includes(a) && ['A', 'B'].includes(b), `${a},${b}`);
    r = await get(`${GW}/api/failover/echo`);
    check('nodes: dead node retried onto healthy node', r.status === 200 && r.json.node === 'A');
    const t = Date.now(); r = await get(`${GW}/api/failover/echo`);
    check('nodes: dead node skipped while marked unhealthy', r.status === 200 && Date.now() - t < 200);
    const bcodes = [];
    for (let i = 0; i < 4; i++) bcodes.push((await get(`${GW}/api/breaker/echo`)).status);
    r = await get(`${GW}/api/breaker/echo`);
    check('breaker: 502s then opens -> 503 + Retry-After', bcodes.slice(0, 3).every((c) => c === 502) && r.status === 503 && r.headers['retry-after'] === '60', bcodes.join(','));

    // ---- policies ----
    r = await get(`${GW}/api/demo/v1/blocked`, { 'X-Forwarded-For': '203.0.113.99' });
    check('policy: ip-restriction deny -> 403', r.status === 403);
    r = await get(`${GW}/api/demo/v1/blocked`, { 'X-Forwarded-For': '203.0.113.98' });
    check('policy: ip-restriction other IP passes', r.status === 200);
    r = await get(`${GW}/api/demo/v1/needs-header`);
    check('policy: custom module from POLICIES_DIR rejects', r.status === 400 && /missing/.test(r.json.message));
    r = await get(`${GW}/api/demo/v1/needs-header`, { 'X-Idempotency-Key': 'k' });
    check('policy: custom module passes', r.status === 200);

    // ---- hot reload ----
    fs.writeFileSync(path.join(cfg, 'newsvc.yml'), `service: { name: newsvc, nodes: [http://127.0.0.1:8002] }\napis: [{ name: n, routes: [{ path: /echo, methods: [GET], bypass: [AUTH, GEO_FENCE] }] }]\n`);
    await sleep(1200);
    r = await get(`${GW}/api/newsvc/echo`);
    check('hot reload: new service file served without restart', r.status === 200 && r.json.node === 'B');
    fs.writeFileSync(path.join(cfg, 'broken.yml'), 'service: { name: broken }\napis: [\n');
    await sleep(1200);
    r = await get(`${GW}/api/newsvc/echo`);
    const m = (await get(`${GW}/metrics`, { Authorization: 'Bearer secret' })).text;
    check('hot reload: invalid file rejected, previous config kept, metric incremented', r.status === 200 && /sancus_config_reloads_total\{status="error"\} 1/.test(m));
    fs.unlinkSync(path.join(cfg, 'broken.yml'));
    await sleep(1200);

    // ---- metrics content ----
    const metrics = (await get(`${GW}/metrics`, { Authorization: 'Bearer secret' })).text;
    check('metrics: per-route counters, histograms, cache + ratelimit + upstream gauges', /sancus_http_requests_total\{service="demo",route="\/v1\/rl",method="GET",code="429"\}/.test(metrics) && /sancus_http_request_duration_seconds_bucket/.test(metrics) && /sancus_upstream_duration_seconds_bucket/.test(metrics) && /sancus_cache_events_total\{service="demo",status="HIT"\}/.test(metrics) && /sancus_rate_limited_total\{service="-",route="-",scope="ip"\}/.test(metrics) && /sancus_upstream_up\{service="failover",node="http:\/\/127.0.0.1:8003"\} 0/.test(metrics));

    // ---- graceful shutdown: in-flight request completes, exit 0 ----
    const inflight = get(`${GW}/api/drain/slow?ms=1500`);
    await sleep(300);
    const exited = new Promise((res) => gw.on('exit', (code) => res(code)));
    gw.kill('SIGTERM');
    await sleep(100);
    const draining = await get(`${GW}/health/ready`);
    check('shutdown: readiness 503 while still serving during the drain delay', draining.status === 503 && draining.headers.connection === 'close', `${draining.status} ${draining.headers.connection}`);
    await sleep(500);
    let refused = false; try { await get(`${GW}/health/ready`); } catch { refused = true; }
    r = await inflight;
    const code = await exited;
    check('shutdown: in-flight request completes during drain', r.status === 200 && r.text === 'slow-done');
    check('shutdown: new connections refused, exit code 0', refused && code === 0, `refused=${refused} code=${code}`);
  } catch (e) {
    check('suite crashed', false, e.stack || String(e));
  } finally {
    procs.forEach((p) => { try { p.kill('SIGTERM'); } catch { /* gone */ } });
    fs.rmSync(tmp, { recursive: true, force: true });
  }

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
  process.exit(failed.length ? 1 : 0);
})();
