import { createHash } from 'crypto';
import type { Request } from 'express';
import getLogger from '../configs/logger';
import { rateLimited } from '../configs/metrics';
import type { APIRoute, HttpMethod, RateLimitConfig } from '../types/api';
import AlertService from '../utils/alerts';
import { getClientIp } from '../utils/clientIp';
import { BlockCache } from '../utils/blockCache';
import { rateLimitHeaders } from '../utils/rateLimitHeaders';
import { defineScript, runScript } from '../utils/redisScript';
import RedisService from './redis.service';

interface EnforceParams {
  serviceName: string;
  route: APIRoute;
  method: HttpMethod;
  req: Request;
  userId?: string;
  apiKey?: string;
}

interface EnforceResult {
  allowed: boolean;
  headers: Record<string, string>;
  retryAfterSeconds?: number;
  message?: string;
}

const logger = getLogger();
// ponytail: remember a block locally for at most 5 s, so deleting it in Redis (an operator unblock)
// takes effect within 5 s; a still-blocked caller costs one Redis check per 5 s per worker.
const LOCAL_BLOCK_MS = 5_000;

interface LocalBlock {
  until: number;    // epoch ms this worker answers from memory (at most LOCAL_BLOCK_MS ahead)
  retryAt: number;  // epoch ms the block really ends, for Retry-After
  limit: number;    // for the rate-limit headers on a cached rejection
  windowS: number;
  message: string;
}

/**
 * Sliding-window counter (the Cloudflare / Redis-docs approximation of a sliding log). Counts are
 * kept for the current fixed window and the one before it; the estimate is
 *   previous * (share of the previous window still inside the sliding window) + current.
 * One hash of three fields per key, O(1) time and memory, however many requests a user makes.
 *
 * Both windows of a route (KEYS[1] = day, KEYS[2] = minute; a limit of 0 means "not set") are
 * checked in one atomic call, and a request is counted in every window only if every window
 * allows it: a request the minute limit rejects never uses up day quota.
 *
 * Clock skew between pods: if another pod already moved a key into a later window than this
 * pod's clock says, that window is treated as current instead of wiping the counts.
 *
 * Returns {allowed, dayRemaining, dayReset, minuteRemaining, minuteReset, retryAfter, rejectedBy}
 * (seconds; rejectedBy 1 = day, 2 = minute, 0 = allowed). Rejected requests are not counted.
 */
const SLIDING_WINDOWS = defineScript('sancusSlidingWindows', 2, `
  local now = tonumber(ARGV[1])

  local function load(key, windowMs, limit)
    local start = now - (now % windowMs)
    local h = redis.call('HMGET', key, 'w', 'c', 'p')
    local w = tonumber(h[1])
    local cur = tonumber(h[2]) or 0
    local prev = tonumber(h[3]) or 0
    if w and w > start then start = w end
    local rolled = w ~= start
    if rolled then
      if w == start - windowMs then prev = cur else prev = 0 end
      cur = 0
    end
    local elapsed = math.max(0, now - start)
    local weight = (windowMs - elapsed) / windowMs
    local s = { key = key, windowMs = windowMs, limit = limit, start = start, cur = cur, prev = prev,
                weight = weight, rolled = rolled,
                reset = math.max(1, math.ceil((windowMs - elapsed) / 1000)) }
    s.ok = prev * weight + cur + 1 <= limit
    if not s.ok then
      local waitMs
      if cur + 1 <= limit and prev > 0 then
        waitMs = windowMs * (1 - (limit - cur - 1) / prev) - elapsed      -- later in this window
      else
        local t = 0                                                         -- in the next window, prev = cur
        if cur > limit - 1 then t = windowMs * (1 - (limit - 1) / cur) end
        waitMs = (windowMs - elapsed) + t
      end
      s.retryAfter = math.max(1, math.ceil(waitMs / 1000))
    end
    return s
  end

  local function save(s)
    redis.call('HSET', s.key, 'w', s.start, 'c', s.cur, 'p', s.prev)
    redis.call('PEXPIRE', s.key, s.windowMs * 2)
  end

  local wins = {}
  for i = 1, 2 do
    local limit = tonumber(ARGV[2 * i + 1])
    if limit > 0 then
      local s = load(KEYS[i], tonumber(ARGV[2 * i]), limit)
      s.slot = i
      wins[#wins + 1] = s
    end
  end

  for _, s in ipairs(wins) do
    if not s.ok then
      for _, r in ipairs(wins) do if r.rolled then save(r) end end
      return {0, 0, 0, 0, 0, s.retryAfter, s.slot}
    end
  end

  local out = {1, 0, 0, 0, 0, 0, 0}
  for _, s in ipairs(wins) do
    s.cur = s.cur + 1
    save(s)
    out[2 * s.slot] = math.max(0, math.floor(s.limit - (s.prev * s.weight + s.cur)))
    out[2 * s.slot + 1] = s.reset
  end
  return out
`);

class RateLimitService {
  private static instance: RateLimitService;
  private readonly redisService = RedisService.getInstance();
  private readonly alertService = AlertService.getInstance();
  private readonly trustedIpSet: Set<string>;
  private readonly blocks = new BlockCache<LocalBlock>();

  private constructor() {
    this.trustedIpSet = new Set(
      (process.env.TRUSTED_IPS || '').split(',').map((ip) => ip.trim()).filter(Boolean)
    );
  }

  public static getInstance(): RateLimitService {
    if (!RateLimitService.instance) RateLimitService.instance = new RateLimitService();
    return RateLimitService.instance;
  }

  private resolveIdentifier(strategy: RateLimitConfig['key'], req: Request, userId?: string, apiKey?: string): string {
    const ip = `ip:${getClientIp(req)}`;
    switch (strategy) {
      // The key itself never lands in Redis or alerts: a short hash identifies it.
      case 'API_KEY': return apiKey ? `apikey:${createHash('sha256').update(apiKey).digest('hex').slice(0, 16)}` : ip;
      case 'USER': return userId ? `user:${userId}` : ip;
      case 'USER_OR_IP': return userId ? `user:${userId}` : ip;
      case 'IP_USER': return userId ? `${ip}|user:${userId}` : ip;
      case 'IP':
      default: return ip;
    }
  }

  private async consumeWindows(baseKey: string, perDay: number, perMinute: number): Promise<number[]> {
    const redis = this.redisService.getClient();
    if (!redis) throw new Error('Redis client unavailable');
    // `:sw` = sliding-window counter hash. New names on purpose: the sorted-set keys of the old
    // log would answer WRONGTYPE (and the limit fail open) until they expired, up to a day.
    return runScript<number[]>(redis, SLIDING_WINDOWS, `${baseKey}:d:sw`, `${baseKey}:m:sw`, Date.now(), 86_400_000, perDay, 60_000, perMinute);
  }


  /** Forget every per-worker block (tests, and a config reload that relaxes limits). */
  public clearBlocks(): void {
    this.blocks.clear();
  }

  private rejected(rl: RateLimitConfig, block: LocalBlock, now: number): EnforceResult {
    const retryAfter = Math.max(1, Math.ceil((block.retryAt - now) / 1000));
    return {
      allowed: false,
      headers: rl.hideHeaders ? {} : rateLimitHeaders(block.limit, 0, retryAfter, block.windowS),
      retryAfterSeconds: retryAfter,
      message: block.message,
    };
  }

  /**
   * Enforce the route's rateLimit block. Day window is checked before minute so a day
   * rejection never burns a minute token. Fails open (allows) when Redis is unreachable.
   *
   * A caller who was rejected is remembered in this worker until their Retry-After (or the
   * route's blockFor) runs out, and answered from memory without touching Redis. With
   * `blockFor`, the block is also written to Redis so every pod enforces it.
   */
  public async enforce({ serviceName, route, method, req, userId, apiKey }: EnforceParams): Promise<EnforceResult> {
    const rl = route.rateLimit;
    if (!rl || (!rl.perMinute && !rl.perDay)) return { allowed: true, headers: {} };
    if (this.trustedIpSet.has(getClientIp(req))) return { allowed: true, headers: {} };

    const identifier = this.resolveIdentifier(rl.key, req, userId, apiKey);
    const baseKey = rl.group
      ? `ratelimit:group:${rl.group}:${identifier}`
      : `ratelimit:${serviceName}:${method}:${route.path}:${identifier}`;

    const windows = [
      { slot: 1, name: 'day', ms: 86_400_000, limit: rl.perDay ?? 0 },
      { slot: 2, name: 'minute', ms: 60_000, limit: rl.perMinute ?? 0 },
    ].filter((w) => w.limit > 0);
    const tightest = windows[windows.length - 1];

    const now = Date.now();
    const known = this.blocks.get(baseKey, now);
    if (known) {
      rateLimited.inc({ service: serviceName, route: route.path, scope: 'route' });
      return this.rejected(rl, known, now);
    }
    const remember = (retryAt: number, limit: number, windowS: number, message: string): LocalBlock => {
      const block = { until: Math.min(retryAt, now + LOCAL_BLOCK_MS), retryAt, limit, windowS, message };
      this.blocks.set(baseKey, block);
      return block;
    };

    const blockKey = `${baseKey}:blocked`;
    const blockMs = (rl.blockFor ?? 0) * 1000;
    try {
      if (blockMs) {
        // Blocked by another pod? Only routes with blockFor pay for this extra round trip.
        const redis = this.redisService.getClient();
        if (!redis) throw new Error('Redis client unavailable');
        const pttl = await redis.pttl(blockKey);
        if (pttl > 0) {
          const block = remember(now + pttl, tightest.limit, tightest.ms / 1000, 'Temporarily blocked for exceeding the rate limit');
          rateLimited.inc({ service: serviceName, route: route.path, scope: 'route' });
          return this.rejected(rl, block, now);
        }
      }

      const r = await this.consumeWindows(baseKey, rl.perDay ?? 0, rl.perMinute ?? 0);
      if (r[0] !== 1) {
        const w = windows.find((x) => x.slot === r[6]) ?? tightest;
        rateLimited.inc({ service: serviceName, route: route.path, scope: 'route' });
        let retryAt = now + r[5] * 1000;
        if (blockMs) {
          // NX: retries during the block never extend it.
          await this.redisService.getClient()?.set(blockKey, '1', 'PX', blockMs, 'NX');
          retryAt = Math.max(retryAt, now + blockMs);
        }
        const message = w.name === 'day' ? 'Daily rate limit exceeded' : 'Per-minute rate limit exceeded';
        const block = remember(retryAt, w.limit, w.ms / 1000, message);
        const result = this.rejected(rl, block, now);
        this.alertService.alert(
          `ratelimit:${identifier}`,
          '⚠️ Rate Limit Exceeded',
          `**Identifier:** ${identifier}\n**Endpoint:** ${method} ${route.path}\n**Service:** ${serviceName}\n**Window:** ${w.name} (limit ${w.limit})\n**Retry After:** ${result.retryAfterSeconds}s${blockMs ? ` (blocked for ${rl.blockFor}s)` : ''}`
        );
        return result;
      }

      // Headers describe the window with the fewest requests left.
      let best: { w: (typeof windows)[number]; remaining: number; reset: number } | undefined;
      for (const w of windows) {
        const remaining = r[2 * w.slot - 1], reset = r[2 * w.slot];
        if (!best || remaining < best.remaining) best = { w, remaining, reset };
      }
      return { allowed: true, headers: rl.hideHeaders || !best ? {} : rateLimitHeaders(best.w.limit, best.remaining, best.reset, best.w.ms / 1000) };
    } catch (e) {
      // ponytail: fail open — a Redis outage must not take the API down with it
      logger.warn({ err: (e as Error).message, key: baseKey }, 'rate limit check failed, allowing request');
      return { allowed: true, headers: {} };
    }
  }
}

export type { EnforceResult };
export default RateLimitService;
