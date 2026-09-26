/* Sancus benchmark harness. Requires wrk, oha, and Redis on localhost.
 *   node tests/bench/run.js            (env: DURATION=20 WARMUP=5 CONNS=100 THREADS=4 RUNS=2 RATE=2000)
 * Method: wrk --latency for throughput + percentiles (median of RUNS), oha at a fixed rate for
 * coordinated-omission-free latency, ps sampling for gateway CPU and RSS. Results -> tests/bench/results/. */
const { spawn, execSync, spawnSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const ROOT = path.resolve(__dirname, '..', '..');
const ENTRY = process.env.GATEWAY_ENTRY || path.join(ROOT, 'build', 'index.js');
const DURATION = Number(process.env.DURATION) || 20;
const WARMUP = Number(process.env.WARMUP) || 5;
const CONNS = Number(process.env.CONNS) || 100;
const THREADS = Number(process.env.THREADS) || 4;
const RUNS = Number(process.env.RUNS) || 2;
const RATE = Number(process.env.RATE) || 2000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const procs = [];
const start = (args, env = {}) => {
  const p = spawn('node', args, { env: { ...process.env, ...env }, stdio: 'ignore' });
  procs.push(p);
  return p;
};
process.on('exit', () => procs.forEach((p) => { try { p.kill('SIGKILL'); } catch { /* gone */ } }));

async function waitFor(url) {
  for (let i = 0; i < 100; i++) {
    try { const r = await fetch(url); if (r.status < 500) return; } catch { /* retry */ }
    await sleep(100);
  }
  throw new Error(`timeout ${url}`);
}

function pidsOf(p) {
  const kids = spawnSync('pgrep', ['-P', String(p.pid)]).stdout.toString().trim().split('\n').filter(Boolean);
  return [p.pid, ...kids.map(Number)];
}

function wrk(url, headers) {
  const args = ['-t', String(THREADS), '-c', String(CONNS), '-d', `${DURATION}s`, '--latency', ...Object.entries(headers).flatMap(([k, v]) => ['-H', `${k}: ${v}`]), url];
  const out = execSync(`wrk ${args.map((a) => `'${a}'`).join(' ')}`).toString();
  const num = (re) => { const m = out.match(re); return m ? Number(m[1]) : NaN; };
  const lat = (p) => { const m = out.match(new RegExp(`\\n\\s*${p}%\\s+([\\d.]+)(us|ms|s)`)); if (!m) return NaN; const v = Number(m[1]); return m[2] === 'us' ? v / 1000 : m[2] === 's' ? v * 1000 : v; };
  return { rps: num(/Requests\/sec:\s+([\d.]+)/), p50: lat(50), p90: lat(90), p99: lat(99), errors: num(/Non-2xx or 3xx responses:\s+(\d+)/) || 0, sockErr: /Socket errors/.test(out) ? out.match(/Socket errors: (.*)/)[1] : '' };
}

function oha(url, headers) {
  const args = ['-z', `${DURATION}s`, '-c', String(CONNS), '-q', String(RATE), '--no-tui', '--output-format', 'json', ...Object.entries(headers).flatMap(([k, v]) => ['-H', `${k}: ${v}`]), url];
  const j = JSON.parse(execSync(`oha ${args.map((a) => `'${a}'`).join(' ')}`, { stdio: ['ignore', 'pipe', 'ignore'] }).toString());
  const p = j.latencyPercentiles;
  const ms = (s) => Math.round(s * 1000 * 1000) / 1000;
  return { rate: RATE, achievedRps: Math.round(j.summary.requestsPerSec), p50: ms(p.p50), p90: ms(p.p90), p99: ms(p.p99), p999: ms(p['p99.9']), successRate: j.summary.successRate };
}

async function sample(gw, ms) {
  const pids = pidsOf(gw).join(',');
  const cpu = []; let rss = 0;
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const rows = spawnSync('ps', ['-o', '%cpu=,rss=', '-p', pids]).stdout.toString().trim().split('\n').filter(Boolean);
    let c = 0, r = 0;
    for (const row of rows) { const [a, b] = row.trim().split(/\s+/).map(Number); c += a; r += b; }
    cpu.push(c); rss = Math.max(rss, r);
    await sleep(1000);
  }
  return { cpuAvg: Math.round(cpu.reduce((a, b) => a + b, 0) / Math.max(cpu.length, 1)), rssMb: Math.round(rss / 1024) };
}

const median = (arr) => { const s = [...arr].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };

(async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'sancus-bench-'));
  const cfg = path.join(tmp, 'api_configs'); fs.mkdirSync(cfg);
  fs.writeFileSync(path.join(cfg, 'bench.yml'), `
service: { name: bench, nodes: [http://127.0.0.1:8000] }
apis:
  - name: bench
    routes:
      - { path: /echo,   methods: [GET], bypass: [AUTH, GEO_FENCE] }
      - { path: /rl,     methods: [GET], bypass: [AUTH, GEO_FENCE], rateLimit: { perMinute: 100000000 } }
      - { path: /auth,   methods: [GET], bypass: [GEO_FENCE] }
      - { path: /cached, methods: [GET], bypass: [AUTH, GEO_FENCE], cache: { strategy: LRU, ttl: 300, key: PATH } }
      - { path: /all,    methods: [GET], bypass: [GEO_FENCE], rateLimit: { perMinute: 100000000, key: USER }, cache: { strategy: LRU, ttl: 300, key: PATH } }
`);
  fs.writeFileSync(path.join(tmp, 'in.json'), '{"type":"FeatureCollection","features":[]}');

  execSync(`redis-cli --scan --pattern 'sancus:*' | xargs -r redis-cli del >/dev/null; redis-cli --scan --pattern 'ratelimit:*' | xargs -r redis-cli del >/dev/null; redis-cli --scan --pattern 'ipratelimit:*' | xargs -r redis-cli del >/dev/null`, { shell: '/bin/bash' });
  start([path.join(__dirname, 'upstream.js')]);
  await waitFor('http://127.0.0.1:8000/');

  const base = { NODE_ENV: 'production', CONFIG_DIR: cfg, CONFIG_WATCH: 'false', GEOFENCE_FILE: path.join(tmp, 'in.json'), AUTH_URL: 'http://127.0.0.1:8001', IP_RATE_LIMIT_CAPACITY: '100000000', IP_RATE_LIMIT_REFILL_RATE: '10000000' };
  const instances = {
    shipped: { PORT: '3100', ...base },                                                              // defaults: info access log, IP limiter on, 1 worker
    tuned:   { PORT: '3101', ...base, LOG_LEVEL: 'warn', IP_RATE_LIMIT_ENABLED: 'false' },           // typical prod tuning
    tuned4:  { PORT: '3102', ...base, LOG_LEVEL: 'warn', IP_RATE_LIMIT_ENABLED: 'false', WORKERS: '4' },
  };
  const gws = {};
  for (const [name, env] of Object.entries(instances)) { gws[name] = start([ENTRY], env); await waitFor(`http://127.0.0.1:${env.PORT}/health`); }

  const H = { Authorization: 'Bearer good' };
  const scenarios = [
    { name: 'direct upstream (baseline)', url: 'http://127.0.0.1:8000/', headers: {} },
    { name: 'proxy, as shipped (info log + IP limiter)', gw: 'shipped', path: '/api/bench/echo', headers: {} },
    { name: 'proxy, tuned', gw: 'tuned', path: '/api/bench/echo', headers: {} },
    { name: 'proxy + route rate limit', gw: 'tuned', path: '/api/bench/rl', headers: {} },
    { name: 'proxy + auth (cached token)', gw: 'tuned', path: '/api/bench/auth', headers: H },
    { name: 'cache HIT', gw: 'tuned', path: '/api/bench/cached', headers: {} },
    { name: 'auth + rate limit + cache HIT', gw: 'tuned', path: '/api/bench/all', headers: H },
    { name: 'proxy, tuned, 4 workers', gw: 'tuned4', path: '/api/bench/echo', headers: {} },
    { name: 'auth + rate limit + cache HIT, 4 workers', gw: 'tuned4', path: '/api/bench/all', headers: H },
  ];

  const results = [];
  for (const s of scenarios) {
    const url = s.url || `http://127.0.0.1:${instances[s.gw].PORT}${s.path}`;
    const probe = await fetch(url, { headers: s.headers });
    if (probe.status !== 200) throw new Error(`${s.name}: probe returned ${probe.status}`);
    process.stdout.write(`${s.name.padEnd(48)} warmup...`);
    execSync(`wrk -t${THREADS} -c${CONNS} -d${WARMUP}s ${Object.entries(s.headers).map(([k, v]) => `-H '${k}: ${v}'`).join(' ')} '${url}' >/dev/null`);
    const runs = [];
    let res = { cpuAvg: 0, rssMb: 0 };
    for (let i = 0; i < RUNS; i++) {
      const sampler = s.gw ? sample(gws[s.gw], DURATION * 1000) : Promise.resolve(res);
      runs.push(wrk(url, s.headers));
      res = await sampler;
    }
    const r = { name: s.name, rps: Math.round(median(runs.map((x) => x.rps))), p50: median(runs.map((x) => x.p50)), p90: median(runs.map((x) => x.p90)), p99: median(runs.map((x) => x.p99)), errors: runs.reduce((a, x) => a + x.errors, 0), sockErr: runs.map((x) => x.sockErr).filter(Boolean)[0] || '', ...res };
    results.push(r);
    console.log(` ${String(r.rps).padStart(7)} req/s  p50 ${r.p50}ms  p99 ${r.p99}ms  cpu ${r.cpuAvg}%  rss ${r.rssMb}MB${r.errors ? `  NON-2XX ${r.errors}` : ''}${r.sockErr ? `  sock: ${r.sockErr}` : ''}`);
  }

  console.log(`\nFixed-rate latency (oha, ${RATE} req/s, ${CONNS} conns, ${DURATION}s):`);
  const fixed = [];
  for (const s of scenarios.filter((x) => ['direct upstream (baseline)', 'proxy, tuned', 'auth + rate limit + cache HIT', 'proxy, tuned, 4 workers'].includes(x.name))) {
    const url = s.url || `http://127.0.0.1:${instances[s.gw].PORT}${s.path}`;
    const r = { name: s.name, ...oha(url, s.headers) };
    fixed.push(r);
    console.log(`${s.name.padEnd(48)} p50 ${r.p50}ms  p90 ${r.p90}ms  p99 ${r.p99}ms  p99.9 ${r.p999}ms  ok ${(r.successRate * 100).toFixed(2)}%`);
  }

  const meta = { date: new Date().toISOString(), node: process.version, cpu: os.cpus()[0].model, cores: os.cpus().length, memGb: Math.round(os.totalmem() / 1073741824), os: `${os.type()} ${os.release()}`, wrk: `-t${THREADS} -c${CONNS} -d${DURATION}s --latency, median of ${RUNS}`, redis: execSync('redis-cli info server | grep redis_version').toString().trim() };
  const outDir = path.join(__dirname, 'results'); fs.mkdirSync(outDir, { recursive: true });
  const file = path.join(outDir, `${meta.date.slice(0, 19).replace(/[:T]/g, '-')}.json`);
  fs.writeFileSync(file, JSON.stringify({ meta, results, fixed }, null, 2));
  console.log(`\nsaved ${path.relative(ROOT, file)}`);
  fs.rmSync(tmp, { recursive: true, force: true });
  process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
