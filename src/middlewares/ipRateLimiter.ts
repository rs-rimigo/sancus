import { NextFunction, Request, Response } from 'express';
import getLogger from '../configs/logger';
import { rateLimited } from '../configs/metrics';
import RedisService from '../services/redis.service';
import AlertService from '../utils/alerts';
import { getClientIp } from '../utils/clientIp';
import { BlockCache } from '../utils/blockCache';
import { rateLimitHeaders } from '../utils/rateLimitHeaders';
import { defineScript, runScript } from '../utils/redisScript';

/**
 * Global per-IP token bucket in Redis, plus temporary blocking of IPs that keep bursting.
 *   IP_RATE_LIMIT_CAPACITY     bucket size / burst (default 200)
 *   IP_RATE_LIMIT_REFILL_RATE  tokens per second (default 5)
 *   IP_BLOCK_THRESHOLD         429s within IP_BLOCK_WINDOW_MS before the IP is blocked (default 20)
 *   IP_BLOCK_WINDOW_MS         (default 60000)
 *   IP_BLOCK_DURATION_MS       (default 900000 = 15 min)
 *   TRUSTED_IPS                comma-separated IPs that skip this limiter
 * Fails open when Redis is unavailable.
 */
interface IPRateLimiterOptions {
  capacity?: number;
  refillRatePerSecond?: number;
  trustedIps?: string[];
  blockThreshold?: number;
  blockWindowMs?: number;
  blockDurationMs?: number;
}

const logger = getLogger();

// One round trip per request: first the block check, then refill by elapsed time and consume one
// token if available. A blocked IP returns {-1, 0, msLeftOnBlock} without touching its bucket;
// otherwise {allowed, tokensLeft, retryAfterSeconds}.
const TOKEN_BUCKET = defineScript('sancusIpBucket', 2, `
  local key = KEYS[1]
  local blockedKey = KEYS[2]
  local now = tonumber(ARGV[1])
  local capacity = tonumber(ARGV[2])
  local refillRate = tonumber(ARGV[3])

  local blockedMs = redis.call('PTTL', blockedKey)
  if blockedMs > 0 then
    return {-1, 0, blockedMs}
  end

  local bucket = redis.call('HMGET', key, 'tokens', 'lastRefill')
  local tokens = tonumber(bucket[1]) or capacity
  local lastRefill = tonumber(bucket[2]) or now
  -- A pod whose clock is behind must not refill backwards (or let the next pod over-refill).
  tokens = math.min(capacity, tokens + (math.max(0, now - lastRefill) / 1000) * refillRate)
  now = math.max(now, lastRefill)

  local allowed = 0
  local retryAfter = 0
  if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
  else
    retryAfter = math.ceil((1 - tokens) / refillRate)
  end

  redis.call('HSET', key, 'tokens', tokens, 'lastRefill', now)
  redis.call('EXPIRE', key, 600)
  return {allowed, math.floor(tokens), retryAfter}
`);

class IPRateLimiter {
  private readonly redisService = RedisService.getInstance();
  private readonly alertService = AlertService.getInstance();
  private readonly capacity: number;
  private readonly refillRatePerSecond: number;
  private readonly blockThreshold: number;
  private readonly blockWindowMs: number;
  private readonly blockDurationMs: number;
  private readonly trustedIpSet: Set<string>;
  // Blocked IPs this worker already knows about: answered with 403 without a Redis round trip.
  // ponytail: kept at most 5 s per entry, so deleting `blocked:ip:<ip>` in Redis unblocks within 5 s.
  private readonly blocked = new BlockCache<{ until: number; retryAt: number }>();
  private rememberBlock(ip: string, retryAt: number): void {
    this.blocked.set(ip, { until: Math.min(retryAt, Date.now() + 5_000), retryAt });
  }

  constructor(options: IPRateLimiterOptions = {}) {
    this.capacity = options.capacity ?? Number(process.env.IP_RATE_LIMIT_CAPACITY ?? 200);
    this.refillRatePerSecond = options.refillRatePerSecond ?? Number(process.env.IP_RATE_LIMIT_REFILL_RATE ?? 5);
    this.blockThreshold = options.blockThreshold ?? Number(process.env.IP_BLOCK_THRESHOLD ?? 20);
    this.blockWindowMs = options.blockWindowMs ?? Number(process.env.IP_BLOCK_WINDOW_MS ?? 60_000);
    this.blockDurationMs = options.blockDurationMs ?? Number(process.env.IP_BLOCK_DURATION_MS ?? 900_000);
    this.trustedIpSet = new Set([
      ...(options.trustedIps ?? []),
      ...(process.env.TRUSTED_IPS || '').split(',').map((ip) => ip.trim()).filter(Boolean),
    ]);
  }

  /** Record a 429 for this IP; block it once it has bursted `blockThreshold` times in the window. */
  private async trackBurst(ip: string): Promise<void> {
    const redis = this.redisService.getClient();
    if (!redis) return;
    const now = Date.now();
    const key = `ipratelimit:burst:${ip}`;
    await redis.zadd(key, now, `${now}-${Math.random()}`);
    await redis.zremrangebyscore(key, 0, now - this.blockWindowMs);
    await redis.pexpire(key, this.blockWindowMs);
    const violations = await redis.zcard(key);

    const shouldBlock = violations >= this.blockThreshold;
    if (shouldBlock) {
      await redis.set(`blocked:ip:${ip}`, String(now), 'PX', this.blockDurationMs);
      this.rememberBlock(ip, now + this.blockDurationMs);
      await redis.del(key, `ipratelimit:bucket:${ip}`);
      logger.warn({ ip, violations }, 'IP blocked for repeated rate-limit bursts');
    }
    this.alertService.alert(
      `ip-burst:${ip}`,
      shouldBlock ? '🚨 IP Blocked' : '⚠️ IP Rate Limit Burst',
      `**IP:** ${ip}\n**Violations:** ${violations} in ${Math.round(this.blockWindowMs / 1000)}s\n**Limit:** ${this.capacity} burst, ${this.refillRatePerSecond}/s${shouldBlock ? `\n**Blocked for:** ${Math.round(this.blockDurationMs / 60000)} min` : ''}`
    );
  }

  private forbid(res: Response, blockedMs: number) {
    res.setHeader('Retry-After', String(Math.max(1, Math.ceil(blockedMs / 1000))));
    return res.status(403).json({
      error: 'Forbidden',
      message: 'Your IP address has been temporarily blocked due to suspicious activity.',
      blockedUntil: new Date(Date.now() + blockedMs).toISOString(),
    });
  }

  public middleware() {
    return async (req: Request, res: Response, next: NextFunction) => {
      const ip = getClientIp(req);
      if (this.trustedIpSet.has(ip)) return next();

      const known = this.blocked.get(ip);
      if (known) return this.forbid(res, known.retryAt - Date.now());

      const redis = this.redisService.getClient();
      if (!redis) return next(); // fail open

      try {
        const [allowed, tokens, retryAfter] = await runScript<[number, number, number]>(
          redis, TOKEN_BUCKET, `ipratelimit:bucket:${ip}`, `blocked:ip:${ip}`, Date.now(), this.capacity, this.refillRatePerSecond
        );

        if (allowed === 1) return next();
        if (allowed === -1) {
          this.rememberBlock(ip, Date.now() + retryAfter); // retryAfter is the block's remaining ms here
          return this.forbid(res, retryAfter);
        }

        rateLimited.inc({ service: '-', route: '-', scope: 'ip' });
        await this.trackBurst(ip).catch((e) => logger.warn({ err: (e as Error).message }, 'burst tracking failed'));
        const windowSeconds = Math.ceil(this.capacity / this.refillRatePerSecond);
        Object.entries(rateLimitHeaders(this.capacity, tokens, retryAfter, windowSeconds)).forEach(([k, v]) => res.setHeader(k, v));
        res.setHeader('Retry-After', String(retryAfter));
        return res.status(429).json({ error: 'Too Many Requests', message: 'IP rate limit exceeded.', retryAfter });
      } catch (e) {
        logger.warn({ err: (e as Error).message, ip }, 'IP rate limit check failed, allowing request');
        return next();
      }
    };
  }
}

export default IPRateLimiter;
