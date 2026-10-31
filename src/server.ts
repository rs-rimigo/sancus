import compression from 'compression';
import express, { Express, NextFunction, Request, Response } from 'express';
import http from 'http';
import path from 'path';
import { lifecycle } from './configs/lifecycle';
import getLogger from './configs/logger';
import IPRateLimiter from './middlewares/ipRateLimiter';
import { metricsMiddleware } from './middlewares/metrics';
import { accessLogger, requestIdMiddleware, stripOwnedHeaders } from './middlewares/requestContext';
import AdminRoute from './routes/admin.route';
import CommonRequestController from './controllers/commonRequest.controller';
import CommonRequestRoute from './routes/commonRequest.route';
import GeoFenceRoute from './routes/geoFenceRequest.route';
import PolicyRegistry from './services/policyRegistry';
import RedisService from './services/redis.service';
import RouteRegistry from './services/routeRegistry';
import ResponseEnum from './types/responseEnums';
import { parseTrustProxy } from './utils/clientIp';
import { COMPRESSION_ENABLED, COMPRESSION_THRESHOLD } from './utils/compression';
import CorsHandler from './utils/corsUtil';
import SancusResponse from './utils/responseUtil';

const logger = getLogger();

// Custom policies, then validate + compile every YAML config (throws on invalid config),
// then hot-reload on changes unless CONFIG_WATCH=false.
const configDir = process.env.CONFIG_DIR || path.join(__dirname, '..', 'api_configs');
PolicyRegistry.getInstance().loadDir(process.env.POLICIES_DIR || path.join(__dirname, '..', 'policies'));
const routeRegistry = RouteRegistry.getInstance();
routeRegistry.initialize(configDir);
if (process.env.CONFIG_WATCH !== 'false') routeRegistry.watch(configDir);

if (!process.env.ADMIN_TOKEN) logger.warn('ADMIN_TOKEN is not set: /metrics, /routes and DELETE /cache are disabled');
const app: Express = express();
const port = Number(process.env.PORT) || 3000;
const redisService = RedisService.getInstance();

app.disable('x-powered-by');
app.set('etag', false); // proxied responses keep the upstream ETag; gateway-generated bodies get none
// Which proxies to trust for X-Forwarded-For; req.ip is derived from this.
const trustProxy = parseTrustProxy(process.env.TRUST_PROXY);
app.set('trust proxy', trustProxy);
const ipLimiterEnabled = process.env.IP_RATE_LIMIT_ENABLED !== 'false';
if (ipLimiterEnabled && trustProxy === false) {
  // Behind any load balancer every request would share the balancer's IP and one bucket: a site-wide 429.
  logger.fatal('IP rate limiter is enabled but TRUST_PROXY=false. Set TRUST_PROXY to your proxy (e.g. 1, or a CIDR list) or set IP_RATE_LIMIT_ENABLED=false.');
  process.exit(1);
}
if (trustProxy === true) logger.warn('TRUST_PROXY=true trusts every X-Forwarded-For hop: clients can forge their IP and dodge per-IP limits. Use a hop count or CIDR list unless every hop is yours.');

// The request chain every proxied request runs, in order. Express runs it for admin, geo and
// anything the fast path below does not take; the fast path runs the same functions directly.
const drainHeader = (_req: Request, res: Response, next: NextFunction) => {
  // While draining, ask keep-alive clients to reconnect, so they move to a pod that is staying.
  if (lifecycle.shuttingDown) res.setHeader('Connection', 'close');
  next();
};
const corsHeaders = (req: Request, res: Response, next: NextFunction) => {
  CorsHandler.setHeaders(req, res);
  next();
};
const ipLimiter = ipLimiterEnabled ? new IPRateLimiter().middleware() : undefined;
// corsHeaders runs before the IP limiter so a browser sees its 429/403 instead of a CORS error.
const chainHead = [requestIdMiddleware, stripOwnedHeaders, drainHeader, accessLogger, metricsMiddleware, corsHeaders, ...(ipLimiter ? [ipLimiter] : [])];

chainHead.forEach((mw) => app.use(mw));
// Proxied responses are compressed in ProxyService (native zlib, decided once from the upstream's
// headers). The middleware stays only on the admin endpoints whose bodies can be large.
if (COMPRESSION_ENABLED) app.use(['/metrics', '/routes'], compression({ threshold: COMPRESSION_THRESHOLD }));

app.use(AdminRoute);
app.use('/api/geo', GeoFenceRoute);
app.use('/api/*', CommonRequestRoute);

function onError(err: Error, req: Request, res: Response): void {
  logger.error({ err: err.message, stack: err.stack, url: req.originalUrl }, 'unhandled pipeline error');
  if (!res.headersSent) new SancusResponse(ResponseEnum.INTERNAL_SERVER_ERROR, {}, res);
  else res.end();
}
// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((err: Error, req: Request, res: Response, _next: NextFunction) => onError(err, req, res));

/**
 * Fast path for proxied requests (/api/<service>/..., not /api/geo). Express's router walks a
 * stack of layers per request: path regexes, the query parser, expressInit, one closure per hop.
 * Here the request gets what expressInit gives it (the Express req/res prototypes, so req.ip with
 * trust proxy, req.hostname and res.json behave exactly the same) and then runs the same chain as
 * plain function calls.
 */
const FAST_PATH = /^\/api\/(?!geo(?:[/?]|$))[^/?]/i; // case-insensitive, like Express's own matching
const proxyChain = [...chainHead, new CommonRequestController().pipeline];
const appRequest = (app as any).request;
const appResponse = (app as any).response;
const queryParser = app.get('query parser fn') as (qs: string) => unknown;

function fastPath(req: Request, res: Response): void {
  Object.setPrototypeOf(req, appRequest);
  Object.setPrototypeOf(res, appResponse);
  const r = req as any;
  r.res = res;
  (res as any).req = req;
  res.locals = Object.create(null);
  r.originalUrl = req.url;
  r.baseUrl = '';
  r.params = {};
  // Express parses the query string on every request; here only if something reads req.query.
  Object.defineProperty(req, 'query', {
    configurable: true,
    enumerable: true,
    get() {
      const i = r.originalUrl.indexOf('?');
      const value = queryParser(i === -1 ? '' : r.originalUrl.slice(i + 1));
      Object.defineProperty(req, 'query', { value, writable: true, configurable: true, enumerable: true });
      return value;
    },
  });

  let i = 0;
  const next = (err?: unknown): void => {
    if (err) return onError(err as Error, req, res);
    const mw = proxyChain[i++] as (req: Request, res: Response, next: NextFunction) => unknown;
    try {
      const out = mw(req, res, next as NextFunction);
      if (out && typeof (out as Promise<unknown>).catch === 'function') (out as Promise<unknown>).catch(next);
    } catch (e) {
      next(e);
    }
  };
  next();
}

const handler = (req: http.IncomingMessage, res: http.ServerResponse) => {
  if (FAST_PATH.test(req.url ?? '')) fastPath(req as Request, res as Response);
  else app(req, res);
};

async function startServer() {
  try {
    await redisService.initializeAndWait();
    logger.info('Redis connection ready');
  } catch (error) {
    logger.warn({ err: (error as Error).message }, 'Redis unavailable at startup; limits and cache fail open until it returns');
  }

  const server = http.createServer(handler).listen(port, process.env.HOST || '0.0.0.0', () => {
    logger.info({ port }, 'Sancus gateway listening');
  });

  // Keep-alive must exceed the idle timeout of any load balancer in front of the gateway
  // (e.g. AWS ALB defaults to 60s) so the LB never reuses a connection Node already closed.
  const keepAliveTimeout = Number(process.env.KEEP_ALIVE_TIMEOUT_MS) || 125_000;
  server.keepAliveTimeout = keepAliveTimeout;
  server.headersTimeout = keepAliveTimeout + 1000;

  // Graceful shutdown in two phases:
  //  1. readiness turns 503 and responses carry `Connection: close`, but the server keeps
  //     serving for SHUTDOWN_DELAY_MS so load balancers and Kubernetes endpoints stop routing here;
  //  2. stop accepting, drain in-flight requests (up to SHUTDOWN_TIMEOUT_MS), exit.
  const delayMs = Number(process.env.SHUTDOWN_DELAY_MS ?? 5000);
  const shutdown = (signal: string) => {
    if (lifecycle.shuttingDown) return;
    lifecycle.shuttingDown = true;
    logger.info({ signal, delayMs }, 'shutting down: draining traffic');
    routeRegistry.close();
    setTimeout(() => {
      const force = setTimeout(() => {
        logger.warn('shutdown timeout reached, exiting with open connections');
        process.exit(1);
      }, Number(process.env.SHUTDOWN_TIMEOUT_MS) || 10_000).unref();
      server.close(async () => {
        clearTimeout(force);
        await redisService.close();
        process.exit(0);
      });
      server.closeIdleConnections?.();
    }, delayMs);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

startServer().catch((error) => {
  logger.error({ err: (error as Error).message }, 'failed to start');
  process.exit(1);
});
