# Defensive Security Guide: Protecting Against RPC Reverse Engineering

## The Attack

Attackers use browser DevTools or proxy tools (Burp Suite, mitmproxy) to:

1. **Discover endpoints** — Inspect network traffic to find every RPC/API call the frontend makes
2. **Reverse engineer the protocol** — Map out request formats, headers, auth tokens, payload structure
3. **Replay requests** — Use scripts (Python, Node, curl) to call those endpoints directly
4. **Bypass the paywall** — Access paid-tier data using a free-tier session token, because the backend doesn't check plan permissions

This works because most B2B SaaS apps enforce access control in the **frontend** (hiding UI elements) rather than the **backend** (checking permissions on every request).

## Why Frontend-Only Access Control Fails

```
Browser → "Show premium button?" → Check plan in JS → Hide/show UI
                                                         ↓
Attacker → Skip the UI entirely → Call API directly → Get all data
```

The frontend is the attacker's machine. They control it completely. Any check that runs client-side can be bypassed.

## Defenses (Implemented in This Repo)

### 1. Server-Side Plan Enforcement (`src/middleware/auth.js`)

**The single most important defense.** Every API endpoint must verify the user's plan tier before returning data.

- `PlanTierEnforcer` — Middleware that maps endpoints to required plan tiers and blocks unauthorized access
- `responseFieldFilter` — Strips premium fields from responses based on plan, even on shared endpoints
- Default-deny for unregistered endpoints

### 2. Server-Side Rate Limiting (`src/middleware/rate-limiter.js`)

Per-user, per-plan, per-endpoint rate limits enforced at the server.

- Free-tier users get strict limits (30 req/min)
- Expensive endpoints (search, export) get their own lower limits
- Proper `429` responses with `Retry-After` headers

### 3. Request Validation & Bot Detection (`src/middleware/request-validator.js`)

Multiple layers of request validation:

- **Browser fingerprinting** — Flag requests missing standard browser headers
- **Origin validation** — Reject requests from unauthorized origins
- **Request signing (HMAC)** — Each request carries a short-lived signature; replay-resistant

### 4. Anomaly Detection (`src/monitoring/anomaly-detector.js`)

Real-time monitoring for scraping patterns:

- **Endpoint crawling** — User hitting too many unique endpoints (discovery phase)
- **Superhuman speed** — Requests faster than any human could click
- **Regular interval detection** — Bots using `sleep(1000)` loops have unnaturally consistent timing
- Alerts pipe to your monitoring system (SIEM, Slack, PagerDuty)

## Quick Start

See `src/example-setup.js` for a complete Express app with all defenses wired up.

## Checklist

- [ ] **Every endpoint checks plan tier server-side** (not just in the UI)
- [ ] **Rate limits are server-side**, per-user, and plan-aware
- [ ] **Response filtering** strips premium fields regardless of how the request arrived
- [ ] **Internal endpoints are not exposed** to the frontend at all
- [ ] **Request signing** is enabled for high-value endpoints
- [ ] **Monitoring** alerts on scraping patterns (crawling, speed, regularity)
- [ ] **Separate APIs** for frontend (BFF) vs. paid API consumers
- [ ] **Session tokens are scoped** to specific permissions, not blanket access
- [ ] **Audit logging** captures all access attempts with user, IP, and user-agent
- [ ] **Regular security review** of what endpoints the frontend actually calls vs. what's exposed
