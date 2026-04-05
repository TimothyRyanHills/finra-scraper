/**
 * Server-side rate limiting with per-user, per-plan, and per-endpoint controls.
 *
 * Defends against: automated bots replaying RPC calls at high volume
 * to scrape data from exposed endpoints.
 */

class RateLimiter {
  constructor(options = {}) {
    // In-memory store — swap for Redis in production
    this.store = new Map();
    this.cleanupInterval = options.cleanupInterval || 60000;

    this.planLimits = options.planLimits || {
      free:       { windowMs: 60000, maxRequests: 30 },
      starter:    { windowMs: 60000, maxRequests: 100 },
      pro:        { windowMs: 60000, maxRequests: 500 },
      enterprise: { windowMs: 60000, maxRequests: 2000 },
    };

    // Per-endpoint overrides: { "GET:/api/search": { windowMs: 60000, maxRequests: 10 } }
    this.endpointLimits = options.endpointLimits || {};

    this._startCleanup();
  }

  /**
   * Express-compatible rate limiting middleware.
   */
  middleware() {
    return (req, res, next) => {
      const userId = req.user?.id || req.ip;
      const userPlan = req.user?.plan || 'free';
      const endpoint = `${req.method}:${req.path}`;

      // Use endpoint-specific limit if available, otherwise fall back to plan limit
      const limit = this.endpointLimits[endpoint] || this.planLimits[userPlan];
      if (!limit) {
        return next();
      }

      const key = `${userId}:${endpoint}`;
      const now = Date.now();
      const record = this.store.get(key);

      if (!record || now - record.windowStart > limit.windowMs) {
        // New window
        this.store.set(key, { windowStart: now, count: 1 });
        this._setRateLimitHeaders(res, limit, 1);
        return next();
      }

      record.count++;

      if (record.count > limit.maxRequests) {
        const retryAfter = Math.ceil((record.windowStart + limit.windowMs - now) / 1000);
        this._setRateLimitHeaders(res, limit, record.count);
        res.set('Retry-After', String(retryAfter));

        return res.status(429).json({
          error: 'Rate limit exceeded',
          retryAfterSeconds: retryAfter,
          limit: limit.maxRequests,
          window: `${limit.windowMs / 1000}s`,
        });
      }

      this._setRateLimitHeaders(res, limit, record.count);
      next();
    };
  }

  _setRateLimitHeaders(res, limit, count) {
    res.set('X-RateLimit-Limit', String(limit.maxRequests));
    res.set('X-RateLimit-Remaining', String(Math.max(0, limit.maxRequests - count)));
  }

  _startCleanup() {
    this._cleanupTimer = setInterval(() => {
      const now = Date.now();
      for (const [key, record] of this.store) {
        // Remove entries whose window has fully expired
        if (now - record.windowStart > 300000) {
          this.store.delete(key);
        }
      }
    }, this.cleanupInterval);

    // Allow process to exit without waiting for the timer
    if (this._cleanupTimer.unref) {
      this._cleanupTimer.unref();
    }
  }

  destroy() {
    clearInterval(this._cleanupTimer);
  }
}

module.exports = { RateLimiter };
