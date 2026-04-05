/**
 * Request validation and bot detection middleware.
 *
 * Defends against: automated scripts replaying browser RPC calls
 * by detecting non-browser request patterns and enforcing request integrity.
 */

/**
 * Validates that requests contain expected browser fingerprints.
 * Bots that replay raw RPC calls often miss subtle browser headers.
 */
function browserFingerprint(options = {}) {
  const requiredHeaders = options.requiredHeaders || [
    'accept',
    'accept-language',
    'accept-encoding',
  ];

  const suspiciousUserAgents = options.suspiciousUserAgents || [
    /python-requests/i,
    /axios/i,
    /node-fetch/i,
    /go-http-client/i,
    /curl/i,
    /wget/i,
    /httpie/i,
    /scrapy/i,
    /postman/i,
  ];

  return (req, res, next) => {
    const flags = [];

    // Check for missing standard browser headers
    for (const header of requiredHeaders) {
      if (!req.headers[header]) {
        flags.push(`missing_header:${header}`);
      }
    }

    // Check user-agent against known automation tools
    const ua = req.headers['user-agent'] || '';
    if (!ua) {
      flags.push('missing_user_agent');
    } else {
      for (const pattern of suspiciousUserAgents) {
        if (pattern.test(ua)) {
          flags.push(`suspicious_ua:${ua}`);
          break;
        }
      }
    }

    // Attach flags for downstream monitoring — don't block outright
    // since sophisticated bots will spoof these headers.
    // Use this as a signal combined with other heuristics.
    req.botDetectionFlags = flags;
    req.suspiciousRequest = flags.length > 0;

    next();
  };
}

/**
 * Validates request origin to prevent cross-origin RPC replay.
 * Ensures requests come from your own frontend, not external scripts.
 */
function originValidator(allowedOrigins) {
  return (req, res, next) => {
    // Skip for non-mutating requests if desired, but for RPC defense
    // we validate all requests
    const origin = req.headers['origin'];
    const referer = req.headers['referer'];

    if (!origin && !referer) {
      // No origin headers — likely not from a browser
      req.botDetectionFlags = req.botDetectionFlags || [];
      req.botDetectionFlags.push('no_origin_or_referer');
      req.suspiciousRequest = true;
    } else if (origin && !allowedOrigins.includes(origin)) {
      return res.status(403).json({ error: 'Invalid origin' });
    }

    next();
  };
}

/**
 * Per-request HMAC signature validation.
 * The frontend signs each request with a short-lived token; the server verifies it.
 * This raises the bar significantly — attackers must reverse engineer your signing logic.
 */
const crypto = require('crypto');

function requestSignatureValidator(options = {}) {
  const { secret, maxAgeMs = 30000, headerName = 'x-request-signature' } = options;

  return (req, res, next) => {
    const signature = req.headers[headerName];
    const timestamp = req.headers['x-request-timestamp'];

    if (!signature || !timestamp) {
      return res.status(401).json({ error: 'Missing request signature' });
    }

    // Reject stale requests (replay protection)
    const requestTime = parseInt(timestamp, 10);
    if (isNaN(requestTime) || Date.now() - requestTime > maxAgeMs) {
      return res.status(401).json({ error: 'Request expired' });
    }

    // Reconstruct and verify signature
    const payload = `${req.method}:${req.path}:${timestamp}`;
    const expectedSignature = crypto
      .createHmac('sha256', secret)
      .update(payload)
      .digest('hex');

    if (!crypto.timingSafeEqual(
      Buffer.from(signature, 'hex'),
      Buffer.from(expectedSignature, 'hex')
    )) {
      return res.status(401).json({ error: 'Invalid request signature' });
    }

    next();
  };
}

module.exports = { browserFingerprint, originValidator, requestSignatureValidator };
