/**
 * Request anomaly detection and alerting.
 *
 * Monitors for patterns consistent with automated RPC scraping:
 * - Unnaturally consistent request timing (bots loop at fixed intervals)
 * - High volume from a single user/IP
 * - Access to many distinct endpoints in a short window (crawling)
 * - Requests to endpoints the user's frontend should never call
 */

class AnomalyDetector {
  constructor(options = {}) {
    this.userActivity = new Map();
    this.alerts = [];
    this.alertCallback = options.onAlert || ((alert) => console.warn('[ANOMALY]', alert));

    this.thresholds = {
      // Max unique endpoints hit in a 60s window before flagging
      endpointCrawlLimit: options.endpointCrawlLimit || 20,
      // Min time between requests (ms) — humans rarely go below 200ms
      minRequestInterval: options.minRequestInterval || 100,
      // Number of suspiciously regular intervals before flagging
      regularIntervalCount: options.regularIntervalCount || 10,
      // Tolerance for "regular" intervals (ms)
      regularIntervalTolerance: options.regularIntervalTolerance || 50,
      // Window size for tracking activity (ms)
      windowMs: options.windowMs || 60000,
    };

    this._startCleanup();
  }

  /**
   * Express-compatible middleware.
   */
  middleware() {
    return (req, res, next) => {
      const userId = req.user?.id || req.ip;
      const now = Date.now();
      const endpoint = `${req.method}:${req.path}`;

      let activity = this.userActivity.get(userId);
      if (!activity) {
        activity = { requests: [], endpoints: new Set(), windowStart: now };
        this.userActivity.set(userId, activity);
      }

      // Reset window if expired
      if (now - activity.windowStart > this.thresholds.windowMs) {
        activity.requests = [];
        activity.endpoints = new Set();
        activity.windowStart = now;
      }

      activity.requests.push(now);
      activity.endpoints.add(endpoint);

      // Check: endpoint crawling
      if (activity.endpoints.size > this.thresholds.endpointCrawlLimit) {
        this._alert({
          type: 'endpoint_crawl',
          userId,
          uniqueEndpoints: activity.endpoints.size,
          ip: req.ip,
          userAgent: req.headers['user-agent'],
        });
      }

      // Check: unnaturally fast requests
      if (activity.requests.length >= 2) {
        const lastTwo = activity.requests.slice(-2);
        const interval = lastTwo[1] - lastTwo[0];
        if (interval < this.thresholds.minRequestInterval) {
          this._alert({
            type: 'superhuman_speed',
            userId,
            intervalMs: interval,
            ip: req.ip,
          });
        }
      }

      // Check: suspiciously regular timing (bot fingerprint)
      if (activity.requests.length >= this.thresholds.regularIntervalCount) {
        const recent = activity.requests.slice(-this.thresholds.regularIntervalCount);
        if (this._hasRegularInterval(recent)) {
          this._alert({
            type: 'regular_interval_pattern',
            userId,
            requestCount: recent.length,
            ip: req.ip,
            userAgent: req.headers['user-agent'],
          });
        }
      }

      // Attach anomaly info for downstream handlers
      req.anomalyFlags = activity.endpoints.size > this.thresholds.endpointCrawlLimit
        ? ['endpoint_crawl'] : [];

      // Include bot detection flags from request-validator if present
      if (req.suspiciousRequest) {
        req.anomalyFlags.push(...(req.botDetectionFlags || []));
      }

      next();
    };
  }

  /**
   * Detect if request timestamps show a suspiciously regular pattern.
   * Humans have variable timing; bots often use fixed sleep() intervals.
   */
  _hasRegularInterval(timestamps) {
    const intervals = [];
    for (let i = 1; i < timestamps.length; i++) {
      intervals.push(timestamps[i] - timestamps[i - 1]);
    }

    const avgInterval = intervals.reduce((a, b) => a + b, 0) / intervals.length;
    const allClose = intervals.every(
      iv => Math.abs(iv - avgInterval) < this.thresholds.regularIntervalTolerance
    );

    return allClose;
  }

  _alert(details) {
    const alert = { ...details, timestamp: new Date().toISOString() };
    this.alerts.push(alert);
    this.alertCallback(alert);
  }

  getAlerts() {
    return [...this.alerts];
  }

  clearAlerts() {
    this.alerts = [];
  }

  _startCleanup() {
    this._cleanupTimer = setInterval(() => {
      const now = Date.now();
      for (const [userId, activity] of this.userActivity) {
        if (now - activity.windowStart > this.thresholds.windowMs * 5) {
          this.userActivity.delete(userId);
        }
      }
    }, 60000);

    if (this._cleanupTimer.unref) {
      this._cleanupTimer.unref();
    }
  }

  destroy() {
    clearInterval(this._cleanupTimer);
  }
}

module.exports = { AnomalyDetector };
