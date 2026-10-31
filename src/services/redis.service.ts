/**
 * Redis Singleton Service
 * 
 * Provides a shared Redis connection with connection pooling for use across the project.
 * Uses singleton pattern to ensure only one connection pool is created.
 * 
 * Usage:
 *   const redis = RedisService.getInstance();
 *   await redis.set('key', 'value');
 *   const value = await redis.get('key');
 */

import Redis from 'ioredis';
import getLogger from '../configs/logger';
import AlertService from '../utils/alerts';
import { installScripts } from '../utils/redisScript';

const logger = getLogger();

class RedisService {
  private static instance: RedisService;
  private redis?: Redis;
  private isConnecting: boolean = false;
  // A Redis that is connected but not answering must not stall requests. Every command has a
  // short timeout; one timeout trips a breaker that skips Redis entirely for REDIS_BACKOFF_MS,
  // so caching, limits and the token cache fail open at full speed until Redis answers again.
  private readonly commandTimeoutMs = Number(process.env.REDIS_COMMAND_TIMEOUT_MS) || 250;
  private readonly backoffMs = Number(process.env.REDIS_BACKOFF_MS) || 5000;
  private skipUntil = 0;
  private degraded = false; // an outage alert was sent and recovery has not been announced yet
  // Alerts follow traffic, not time: one on the 1st request served without Redis, then one
  // every REDIS_ALERT_EVERY requests after it (1st, 1001st, 2001st, ...), and one on recovery.
  private readonly alertEvery = Math.max(1, Number(process.env.REDIS_ALERT_EVERY) || 1000);
  private failedRequests = 0;
  private outage = 0; // numbers each outage so its alerts never collide with the last one's

  private constructor() {
    // Private constructor to enforce singleton
  }

  /**
   * Get the singleton instance of RedisService
   */
  public static getInstance(): RedisService {
    if (!RedisService.instance) {
      RedisService.instance = new RedisService();
    }
    return RedisService.instance;
  }

  /**
   * Get the Redis client instance
   * Creates connection if not already connected
   */
  public getClient(): Redis | undefined {
    if (this.skipping) return undefined; // breaker open: callers fail open without waiting
    if (!this.redis && !this.isConnecting) {
      this.initialize();
    }
    return this.redis;
  }

  /**
   * Initialize Redis connection and wait for it to be ready
   * Call this at server startup to ensure connection pool is ready
   */
  public async initializeAndWait(): Promise<void> {
    if (this.redis) {
      // Already initialized, check if connected
      try {
        await this.redis.ping();
        return;
      } catch (error) {
        // Connection exists but not ready, wait for it
      }
    }

    if (!this.redis && !this.isConnecting) {
      this.initialize();
    }

    // Wait for connection to be ready (with timeout)
    return new Promise((resolve, reject) => {
      if (!this.redis) {
        reject(new Error('Redis client not initialized'));
        return;
      }

      const timeout = setTimeout(() => {
        reject(new Error('Redis connection timeout'));
      }, 10000); // 10 second timeout

      const onReady = () => {
        clearTimeout(timeout);
        this.redis?.removeListener('error', onError);
        resolve();
      };

      const onError = (err: Error) => {
        clearTimeout(timeout);
        this.redis?.removeListener('ready', onReady);
        reject(err);
      };

      if (this.redis.status === 'ready') {
        clearTimeout(timeout);
        resolve();
      } else {
        this.redis.once('ready', onReady);
        this.redis.once('error', onError);
      }
    });
  }

  /**
   * Initialize Redis connection with connection pooling
   */
  private initialize(): void {
    if (this.redis || this.isConnecting) {
      return;
    }

    this.isConnecting = true;
    const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

    if (!process.env.REDIS_URL) {
      logger.info(`REDIS_URL not set. Falling back to default ${redisUrl}`);
    }

    // Create Redis connection with connection pooling
    // ioredis automatically handles connection pooling
    this.redis = new Redis(redisUrl, {
      enableOfflineQueue: false,
      maxRetriesPerRequest: 3,
      // Never give up: Redis restarts and failovers must heal without restarting the gateway.
      retryStrategy: (attempt) => Math.min(attempt * 200, 2000),
      // Failover that keeps the TCP connection (the old primary demoted to a replica): writes answer
      // READONLY. Reconnect so DNS/endpoint resolution finds the new primary, and resend the command.
      reconnectOnError: (err) => (/READONLY/.test(err.message) ? 2 : false),
      lazyConnect: true,
      // Connection pool settings
      keepAlive: 30000, // Keep connections alive
      connectTimeout: 10000, // Connection timeout
      commandTimeout: this.commandTimeoutMs,
      // Commands issued in the same event-loop tick go out as one write. Under load a request's
      // limiter, token and cache commands share writes with other requests', which cuts syscalls
      // and Redis CPU. REDIS_AUTO_PIPELINING=false turns it off.
      enableAutoPipelining: process.env.REDIS_AUTO_PIPELINING !== 'false',
    });

    // Lua scripts go on the client before its first command (see utils/redisScript).
    installScripts(this.redis);

    // Every command funnels through sendCommand: watch for timeouts there.
    const client = this.redis as any;
    const send = client.sendCommand.bind(client);
    client.sendCommand = (command: any, stream?: unknown) => {
      const result = send(command, stream);
      command?.promise?.catch((err: Error) => this.noteFailure(err));
      return result;
    };

    this.redis.on('error', (err) => {
      logger.error(`Redis client error: ${err.message}`);
      this.lastError = err.message;
    });

    this.redis.on('connect', () => {
      logger.info('Redis client connected');
      this.isConnecting = false;
    });

    this.redis.on('ready', () => {
      logger.info('Redis client ready');
      this.isConnecting = false;
      this.skipUntil = 0;
    });

    this.redis.on('close', () => {
      logger.warn('Redis client connection closed');
      this.isConnecting = false;
    });

    // Attempt to connect
    this.redis.connect().catch((err) => {
      logger.error(`Failed to connect to Redis: ${err.message}`);
      this.isConnecting = false;
    });
  }

  /**
   * Trip the breaker when Redis cannot do its job: a timeout, or a server that answers but cannot
   * serve writes (READONLY replica, LOADING, failover in progress). Tripping marks Redis unhealthy,
   * so the outage alert fires instead of limits silently failing open. Closed connections already
   * fail fast and are seen by `healthy`.
   */
  public noteFailure(err: Error): void {
    const msg = err?.message ?? '';
    if (!/timed out|READONLY|LOADING|MASTERDOWN|CLUSTERDOWN|TRYAGAIN/i.test(msg)) return;
    this.lastError = msg.slice(0, 120);
    if (Date.now() >= this.skipUntil) {
      logger.warn({ backoffMs: this.backoffMs }, 'Redis is not answering: serving without cache, limits and token cache');
    }
    this.skipUntil = Date.now() + this.backoffMs;
  }

  private lastError = '';

  /** Connected, ready, and not inside a skip window. */
  public get healthy(): boolean {
    return !!this.redis && (this.redis as any).status === 'ready' && !this.skipping;
  }

  /** Called once per proxied request: counts requests served without Redis and alerts on the schedule. */
  public noteRequest(): void {
    if (this.healthy) {
      this.alertUp();
      return;
    }
    this.failedRequests += 1;
    if ((this.failedRequests - 1) % this.alertEvery === 0) this.alertDown(this.failedRequests);
  }

  private alertDown(count: number): void {
    if (!this.degraded) this.outage += 1;
    this.degraded = true;
    const why = this.skipping ? `Redis is not answering within ${this.commandTimeoutMs} ms` : `the gateway cannot reach Redis${this.lastError ? ` (${this.lastError})` : ''}`;
    AlertService.getInstance().alert(
      `redis-down:${this.outage}:${count}`, // unique per outage and count: this schedule replaces the time-based cooldown
      '🚨 Redis unavailable, gateway degraded',
      `Request #${count.toLocaleString('en-US')} served without Redis: ${why}. Requests are going through without the response cache, rate limits and the token cache. Next alert at #${(count + this.alertEvery).toLocaleString('en-US')}.\n**Redis:** ${this.target()}`
    );
  }

  private alertUp(): void {
    if (!this.degraded) return;
    const total = this.failedRequests;
    this.degraded = false;
    this.failedRequests = 0;
    AlertService.getInstance().alert(
      `redis-up:${this.outage}`,
      '✅ Redis recovered',
      `Cache, rate limits and the token cache are back on. ${total.toLocaleString('en-US')} requests were served without Redis.\n**Redis:** ${this.target()}`
    );
  }

  /** host:port only, never credentials from REDIS_URL */
  private target(): string {
    try { const u = new URL(process.env.REDIS_URL || 'redis://127.0.0.1:6379'); return `${u.hostname}:${u.port || 6379}`; } catch { return 'unknown'; }
  }

  /** True while the breaker is open. */
  public get skipping(): boolean {
    return Date.now() < this.skipUntil;
  }

  /**
   * Check if Redis is available
   */
  public async isAvailable(): Promise<boolean> {
    if (!this.redis || this.skipping) {
      return false;
    }

    try {
      await this.redis.ping();
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Close Redis connection
   */
  public async close(): Promise<void> {
    if (this.redis) {
      await this.redis.quit();
      this.redis = undefined;
      this.isConnecting = false;
      logger.info('Redis connection closed');
    }
  }

  /**
   * Get Redis client directly (for advanced usage)
   * Use getClient() for most cases
   */
  public getRedisClient(): Redis | undefined {
    return this.getClient();
  }
}

export default RedisService;

