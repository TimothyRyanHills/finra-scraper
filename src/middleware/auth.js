/**
 * Server-side authentication and plan-tier enforcement middleware.
 *
 * Defends against: free-tier users replaying authenticated requests
 * to access paid endpoints discovered via frontend RPC inspection.
 */

class PlanTierEnforcer {
  constructor(options = {}) {
    this.planHierarchy = options.planHierarchy || ['free', 'starter', 'pro', 'enterprise'];
    this.endpointPermissions = new Map();
    this.auditLog = options.auditLog || console;
  }

  /**
   * Register which plan tier is required for a given endpoint/method combo.
   * @param {string} method - HTTP method (GET, POST, etc.)
   * @param {string} path - URL pattern (supports :param placeholders)
   * @param {string} requiredPlan - Minimum plan tier required
   */
  registerEndpoint(method, path, requiredPlan) {
    const key = `${method.toUpperCase()}:${path}`;
    this.endpointPermissions.set(key, requiredPlan);
  }

  /**
   * Bulk register endpoint permissions from a config object.
   * @param {Object} config - { "GET:/api/data": "pro", ... }
   */
  registerFromConfig(config) {
    for (const [key, plan] of Object.entries(config)) {
      this.endpointPermissions.set(key, plan);
    }
  }

  /**
   * Check if a user's plan meets the minimum required tier.
   */
  meetsRequirement(userPlan, requiredPlan) {
    const userLevel = this.planHierarchy.indexOf(userPlan);
    const requiredLevel = this.planHierarchy.indexOf(requiredPlan);
    if (userLevel === -1 || requiredLevel === -1) return false;
    return userLevel >= requiredLevel;
  }

  /**
   * Express-compatible middleware that enforces plan-based access.
   * Blocks requests where the user's plan doesn't meet the endpoint requirement.
   */
  middleware() {
    return (req, res, next) => {
      const user = req.user;

      if (!user || !user.plan) {
        this.auditLog.warn('[AUTH] Request with no user/plan info', {
          ip: req.ip,
          path: req.path,
          method: req.method,
        });
        return res.status(401).json({ error: 'Authentication required' });
      }

      const key = `${req.method}:${req.path}`;
      const requiredPlan = this._matchEndpoint(req.method, req.path);

      if (!requiredPlan) {
        // No restriction registered — default deny for unregistered endpoints
        this.auditLog.warn('[AUTH] Access to unregistered endpoint', {
          user: user.id,
          plan: user.plan,
          endpoint: key,
        });
        return res.status(403).json({ error: 'Endpoint not available' });
      }

      if (!this.meetsRequirement(user.plan, requiredPlan)) {
        this.auditLog.warn('[AUTH] Plan tier violation', {
          user: user.id,
          userPlan: user.plan,
          requiredPlan,
          endpoint: key,
          ip: req.ip,
          userAgent: req.headers['user-agent'],
        });
        return res.status(403).json({
          error: 'Upgrade required',
          requiredPlan,
          currentPlan: user.plan,
        });
      }

      next();
    };
  }

  /**
   * Match a request against registered endpoints, supporting path params.
   */
  _matchEndpoint(method, path) {
    const exactKey = `${method}:${path}`;
    if (this.endpointPermissions.has(exactKey)) {
      return this.endpointPermissions.get(exactKey);
    }

    // Check parameterized routes
    for (const [pattern, plan] of this.endpointPermissions) {
      const [patternMethod, patternPath] = pattern.split(':');
      if (patternMethod !== method) continue;

      const patternParts = patternPath.split('/');
      const pathParts = path.split('/');
      if (patternParts.length !== pathParts.length) continue;

      const matches = patternParts.every((part, i) =>
        part.startsWith(':') || part === pathParts[i]
      );
      if (matches) return plan;
    }

    return null;
  }
}

/**
 * Middleware to strip sensitive fields from responses based on plan tier.
 * Prevents data leakage even if an endpoint is accessible to lower tiers.
 */
function responseFieldFilter(fieldRules) {
  return (req, res, next) => {
    const originalJson = res.json.bind(res);

    res.json = (data) => {
      const userPlan = req.user?.plan || 'free';
      const filtered = stripFieldsByPlan(data, userPlan, fieldRules);
      return originalJson(filtered);
    };

    next();
  };
}

function stripFieldsByPlan(data, userPlan, fieldRules) {
  if (Array.isArray(data)) {
    return data.map(item => stripFieldsByPlan(item, userPlan, fieldRules));
  }

  if (data && typeof data === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(data)) {
      const rule = fieldRules[key];
      if (rule) {
        const planHierarchy = ['free', 'starter', 'pro', 'enterprise'];
        const userLevel = planHierarchy.indexOf(userPlan);
        const requiredLevel = planHierarchy.indexOf(rule);
        if (userLevel < requiredLevel) continue; // Strip this field
      }
      result[key] = stripFieldsByPlan(value, userPlan, fieldRules);
    }
    return result;
  }

  return data;
}

module.exports = { PlanTierEnforcer, responseFieldFilter, stripFieldsByPlan };
