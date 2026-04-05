/**
 * Example: Wiring up all defensive middleware on an Express app.
 *
 * This shows how to combine plan enforcement, rate limiting,
 * request validation, response filtering, and anomaly detection.
 */

const express = require('express');
const { PlanTierEnforcer, responseFieldFilter } = require('./middleware/auth');
const { RateLimiter } = require('./middleware/rate-limiter');
const { browserFingerprint, originValidator, requestSignatureValidator } = require('./middleware/request-validator');
const { AnomalyDetector } = require('./monitoring/anomaly-detector');

const app = express();

// --- 1. Request validation (bot detection, origin check) ---

app.use(browserFingerprint());
app.use(originValidator(['https://yourapp.com', 'https://app.yourapp.com']));

// Optional: HMAC request signing for high-value endpoints
// app.use('/api/premium', requestSignatureValidator({ secret: process.env.REQUEST_SIGNING_SECRET }));

// --- 2. Anomaly detection (runs on every request, flags suspicious patterns) ---

const anomalyDetector = new AnomalyDetector({
  onAlert: (alert) => {
    console.error('[SECURITY ALERT]', JSON.stringify(alert));
    // In production: send to your SIEM, Slack, PagerDuty, etc.
  },
});
app.use(anomalyDetector.middleware());

// --- 3. Rate limiting (per-user, per-plan) ---

const rateLimiter = new RateLimiter({
  planLimits: {
    free:       { windowMs: 60000, maxRequests: 30 },
    starter:    { windowMs: 60000, maxRequests: 100 },
    pro:        { windowMs: 60000, maxRequests: 500 },
    enterprise: { windowMs: 60000, maxRequests: 2000 },
  },
  endpointLimits: {
    'GET:/api/search': { windowMs: 60000, maxRequests: 10 }, // Search is expensive
    'GET:/api/export':  { windowMs: 60000, maxRequests: 5 },  // Export is very expensive
  },
});
app.use(rateLimiter.middleware());

// --- 4. Plan tier enforcement (the critical defense) ---

const enforcer = new PlanTierEnforcer();

// Register what plan each endpoint requires
enforcer.registerFromConfig({
  // Free-tier endpoints
  'GET:/api/profile':       'free',
  'GET:/api/basic-data':    'free',

  // Paid endpoints — THIS is where most apps fail.
  // The frontend hides these behind a paywall UI, but the backend
  // serves them to any authenticated user. Fix: enforce here.
  'GET:/api/contacts':      'starter',
  'GET:/api/contacts/:id':  'starter',
  'GET:/api/analytics':     'pro',
  'GET:/api/export':        'pro',
  'GET:/api/bulk-search':   'pro',
  'GET:/api/intent-data':   'enterprise',
  'GET:/api/company-graph': 'enterprise',
});

app.use('/api', enforcer.middleware());

// --- 5. Response field filtering (defense in depth) ---
// Even if a free user somehow reaches a shared endpoint,
// strip premium fields from the response.

app.use(responseFieldFilter({
  email:        'starter',   // Free users don't get emails
  phone:        'pro',       // Only pro+ get phone numbers
  revenue:      'pro',       // Financial data is premium
  intentScore:  'enterprise', // Intent signals are top tier
  techStack:    'enterprise',
}));

// --- Routes ---

app.get('/api/profile', (req, res) => {
  res.json({ id: req.user.id, name: req.user.name, plan: req.user.plan });
});

app.get('/api/contacts', (req, res) => {
  // This data is safe to return — the plan enforcer already verified
  // the user's tier, and the response filter will strip premium fields.
  res.json({
    contacts: [
      { name: 'Jane Doe', company: 'Acme', email: 'jane@acme.com', phone: '+1234567890', intentScore: 87 },
      { name: 'John Smith', company: 'Globex', email: 'john@globex.com', phone: '+0987654321', intentScore: 42 },
    ],
  });
});

app.get('/api/analytics', (req, res) => {
  res.json({ visitors: 15000, conversion: 3.2, revenue: 450000 });
});

// --- Start ---

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log('Defensive middleware active: auth, rate-limit, anomaly detection, request validation');
});
