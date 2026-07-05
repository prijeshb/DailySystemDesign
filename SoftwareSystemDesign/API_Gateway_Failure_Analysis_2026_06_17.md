# API Gateway — Failure Analysis
**Date:** 2026-06-17  
**Approach:** Failure-first. For each component: What if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?

---

## Failure Map (Components + Dependencies)

```
Client → [Global LB] → [Gateway Node] → [Redis] → [Service Registry] → [Backend Service]
                             │
                          [Config Store]
                          [Observability]
```

---

## 1. Gateway Node Failure

### What fails
A gateway node crashes (OOM, hardware fault, deploy gone wrong).

### Impact without mitigation
All in-flight requests on that node: connection reset. New requests: Global LB still routes to it → 10-30s of partial failures until health check removes it.

### Detection
Global LB (AWS ALB) health checks every 5s. After 2 consecutive failures → removes node from pool. Worst case: ~10s exposure.

### Prevention
```
Gateway nodes are stateless → no data loss on crash.
Global LB health check:
  GET /healthz → 200 OK {"status": "up", "redis": "up", "config": "loaded"}
  Failure threshold: 2 consecutive → deregister
  
Auto-scaling group: replace dead node within 2 min (AMI pre-baked with all deps)
```

### Recovery
```
1. LB removes dead node → 0 traffic to it
2. ASG spins new node → registers with LB → receives traffic
3. Zero state to restore (stateless)
4. Time to recovery: 2-3 min
```

### Residual risk
If ALL gateway nodes die simultaneously → full outage.
Mitigation: multi-AZ deployment. 3 AZs × N nodes. AZ failure only kills N nodes. No single AZ kills entire fleet.

---

## 2. Redis Cluster Failure

Redis is used for: rate limiting, API key cache, JWT jti revocation blocklist.

### Scenario A: Redis is slow (high latency, not down)

**Impact:** Rate limit check adds 50-100ms instead of 1ms → gateway P99 latency explodes.

**Detection:** Gateway tracks Redis operation latency. Alert at P99 > 10ms.

**Mitigation:**
```
Per-request Redis timeout: 5ms
If Redis GET/SET exceeds 5ms timeout → treat as failure → circuit breaker
```

### Scenario B: Redis primary fails (Sentinel election in progress, ~30s)

**Rate limiting:** No Redis → can't read/write counters.

```
Decision: Fail-open (allow all traffic through)
Why: Fail-closed (block all) = 100% revenue blocked for 30s. Worse than brief rate-limit bypass.

Fail-open implementation:
  On Redis error → log "rate_limit_skipped=true" → proceed to upstream
  
After Redis recovers:
  Counters resume. Users who slipped through during outage: already served.
  Not worth retrospective blocking.
  
Abuse detection: monitor for traffic spikes during outage. If IP × 100 normal rate → post-hoc block.
```

**API key validation:** Can't lookup key in Redis.

```
Options:
  a) Fail-open: assume key valid (risk: revoked keys accepted for 30s)
  b) Fail-closed: reject all API key requests (safe but breaks integrations)
  c) Local memory fallback: gateway caches last 10K API key lookups in LRU (60s TTL)
     → serves from local cache during Redis outage
     
Choose (c): local LRU covers ~80% of active API keys (Pareto distribution of usage)
Remaining 20% (first-time or rare keys): fail-open during outage, log for audit
```

**JWT jti revocation:** Can't check blocklist.

```
Decision: Skip blocklist check. Serve the JWT as valid.
Risk: a revoked token (e.g., logout) may be accepted for ~30s.
Acceptable: JWT TTL is 15 min. 30s gap on revocation is manageable.
If concerned: shorten JWT TTL to 5 min. Still serves well.
```

### Scenario C: Redis cluster fully dead (multi-node failure, rare)

```
Escalate all three fail-open behaviors:
  Rate limiting: skip
  API key: local LRU + fail-open
  JWT revocation: skip blocklist

Outcome: gateway still works, slightly degraded security
         vs alternative: full gateway outage
         
Recovery: Redis cluster auto-heals from persistence (AOF/RDB). Or failover to replica cluster.
```

---

## 3. Service Registry (Consul) Failure

Gateway uses service registry to discover upstream instance IPs.

### What fails
Consul is down or partitioned.

### Impact without mitigation
Gateway can't resolve upstreams → all requests fail with "no healthy instances."

### Mitigation: Local Instance Cache

```
Gateway caches last-known instance list per service (in-memory, 30s TTL):

On registry lookup:
  1. Try Consul watch (streaming, immediate)
  2. If Consul unavailable → serve from local cache
  3. Cache staleness: at most 30s behind reality

Risk: stale cache contains crashed instances → gateway sends requests to dead IPs.
Mitigation: active health check (gateway pings /health every 10s independently of Consul)
  → dead instance removed from local cache even if Consul is down
```

### Recovery
```
Consul recovers → gateway receives updated instance list → overwrites stale cache
Consul has leader election (Raft): if leader dies, new leader elected in ~3s
  → brief inconsistency window, local cache covers it
```

---

## 4. Config Store (etcd) Failure

Gateway route table and policies loaded from etcd.

### Impact at startup vs runtime

**At startup (etcd down):** Gateway can't load config → fails to start.
```
Mitigation: bundle last-known-good config in container image (baked in at deploy).
  → Gateway starts with bundled config
  → Config watch fails → log warning → operate in degraded mode (no hot-reload)
  → Alert ops team: "Config store unreachable"
```

**At runtime (etcd goes down):** Gateway already has config in memory → continues serving normally.
```
Impact: can't apply new config changes until etcd recovers.
Operations can't:
  - Add new routes
  - Change rate limits
  - Update auth policies
  
Not a user-facing failure. Operational inconvenience only.
```

---

## 5. Individual Backend Service Failure

### Scenario A: Backend crashes mid-deployment

```
Gateway: sends request → connection refused / connection timeout
Circuit breaker: failure count +1
After 5 failures in 10s → OPEN

OPEN behavior:
  All new requests to that upstream → immediate 503
  No connection attempts (saves connection pool slots for other upstreams)
  After 30s → HALF_OPEN → probe with 1 request
  
Client receives:
  HTTP 503 Service Unavailable
  Retry-After: 30
  Body: {"error": "service_unavailable", "upstream": "order-svc"}
```

### Scenario B: Backend slow (timeout, not crash)

```
Per-route timeout (e.g., 5s for order-svc):
  Gateway waits 5s → timeout → count as failure → circuit breaker tracks

Client experience: 5s wait then 504 Gateway Timeout
Trade-off: short timeout = fast failure but may abort valid slow requests
           long timeout = stale connections pile up, thread pool exhaustion

Configure timeouts based on P99 latency of each service:
  /health: 100ms
  product read: 500ms
  order create: 5000ms (DB write, payment check)
  report generation: 30000ms (async, client polls)
```

### Scenario C: One backend instance is bad (not all)

```
Gateway sees: 3/10 instances of order-svc returning 500s
Circuit breaker: counts per-upstream (not per-instance) → 5 failures → OPEN
Better: per-instance circuit breaker → only remove the 3 bad instances

Implementation:
  Per-instance failure tracking in LB selector:
    instance_health[10.0.1.45] = {fail_count: 8, last_fail: T}
    On select: filter instances where fail_count < 5
    If all instances unhealthy → open circuit, 503
```

### Scenario D: Backend slow only for specific user

```
Example: order-svc has a DB hotspot for user_id=123 (popular merchant)
Gateway can't distinguish — all requests to order-svc look same

Detection: gateway logs latency per user_id per upstream
  Alert: user 123's order-svc P99 = 10s vs global P99 = 200ms
  
Fix: order-svc tier, not gateway's problem. Gateway just surfaces it.
Gateway can: route user 123 to isolated tenant shard (if multi-tenant routing configured)
```

---

## 6. TLS Certificate Expiry

### What fails
HTTPS connections rejected by clients → all traffic fails.

### Mitigation
```
Certificate management: AWS ACM / Let's Encrypt (auto-renew)
Monitoring: alert at 30 days, 7 days, 1 day before expiry
Auto-renewal: ACM renews 60 days before expiry, hot-swaps (no downtime)

Let's Encrypt:
  ACME protocol: gateway proves domain ownership → cert issued
  Certbot: cron job 2x/day checks renewal → renews at 30-day mark
  Nginx/Envoy: cert reload without restart (SIGHUP or hot cert path)
```

### If cert expires anyway (operational failure)
```
All HTTPS clients → SSL handshake error
Mitigation: have wildcard cert backup (.env.example stored in secrets manager)
Manual recovery:
  1. Download new cert from ACM
  2. Replace cert file
  3. SIGHUP gateway → reload TLS cert without restart
  4. Time to recover: 5-10 min
```

---

## 7. Gateway Overload (Traffic Spike)

### What fails
100K RPS → 200K RPS suddenly (viral event, DDoS). Gateway CPU maxed → P99 latency spikes → connection queue fills → clients time out.

### Layer 1: Rate Limiting (user/IP level)
```
Per-user: 100 req/min standard, 10 req/min burst protection
Per-IP (unauthenticated): 20 req/min
Global rate: no — allow legitimate traffic to scale
```

### Layer 2: Connection Limit
```
Gateway: accept queue = 10K pending connections
Beyond that: TCP RST (instant client error, no gateway resource consumed)
```

### Layer 3: Auto-scaling
```
CloudWatch metric: gateway CPU > 70% for 60s → scale out
  New instance: ready in 3 min (pre-baked AMI)
  ASG max: 100 nodes (handles 1M RPS)

Problem: scale-out takes 3 min. Traffic spike is instant.
Solution: over-provision 2× normal capacity always (cost decision)
         OR: global rate limit at CloudFront/WAF (1M req/min globally) before gateway
```

### Layer 4: DDoS
```
Cloudflare / AWS WAF in front of gateway:
  IP reputation blocks
  L7 rate limiting (before traffic reaches gateway)
  Challenge page for suspicious patterns
  
Gateway-level: API key auth rejects unauthenticated bot traffic quickly
```

---

## 8. Config Update Gone Wrong (Bad Deploy)

### What fails
Ops deploys new route config with a bug: typo in upstream name, wrong timeout (1ms instead of 1000ms), regex catastrophe.

### Mitigation

```
Config validation (pre-deploy):
  CI pipeline: validate config schema before merging
  Dry-run: load config in shadow mode → check all upstreams exist in service registry
  
Gradual rollout:
  Push to 1 gateway node → watch error rate for 60s
  If error rate < 0.1% → push to all
  If error rate > 1% → auto-rollback to previous etcd key version
  
Rollback:
  etcd stores version history → revert in 1 etcd command
  Gateway watch → receives change → reloads old config
  Time to rollback: <30s
```

---

## 9. Clock Skew (JWT Expiry)

### What fails
Gateway verifies JWT exp claim. If gateway clock is 5 min ahead, valid JWTs appear expired.

### Impact
```
Users get 401 Unauthorized even with valid tokens → login loops.
```

### Mitigation
```
Leeway: accept JWTs expired within 30s of exp (clock skew tolerance)
NTP: gateway nodes sync every minute, max drift ~1s
Monitor: alert if any gateway's clock drifts > 5s from NTP

JWT verify code:
  now = current_time()
  if token.exp < (now - LEEWAY_SECONDS):    # not: token.exp < now
      return 401 Unauthorized
```

---

## 10. Upstream Component Failure (Cascading)

### The scenario: Payment service takes Auth service down

```
Payment Service → Auth Service (to validate internal tokens)
Auth Service overloaded by Payment Service's retries

Gateway sees: Auth Service returning 500s
Circuit breaker opens for Auth Service
ALL requests requiring auth: 401 / 503

Cascade: one service failure → auth down → all authenticated routes fail
```

### Prevention
```
Bulkhead isolation: Auth Service has separate thread pool for:
  - External (gateway) calls: 200 threads
  - Internal (payment-svc) calls: 50 threads

Payment Service's retries consume internal pool → never touch external pool
Gateway's auth checks always have capacity

Implementation: Auth Service API gateway key differentiates caller
  X-Caller-Service: payment-svc → routed to internal pool
  X-Caller-Service: gateway → routed to external pool (isolated)
```

### If Auth Service is still overloaded
```
Gateway: JWT local verify doesn't call Auth Service at all
  → Immune to Auth Service outage for JWT validation
  
API key: requires Redis lookup → if Auth Service is down but Redis is up → still works
Only opaque token introspection (POST /introspect) needs Auth Service
  → Don't use introspection for high-frequency routes
```

---

## Failure Priority Matrix

| Failure | P(occur) | Impact | Mitigation quality |
|---------|----------|--------|-------------------|
| Gateway node crash | Medium | Low (LB routes around) | High (30s recovery) |
| Redis primary failover | Low | Medium (rate limit skip) | High (fail-open, local cache) |
| Backend service crash | High | Medium (1 upstream) | High (circuit breaker) |
| Config store (etcd) down | Low | Low (runtime: ops only) | High (in-memory fallback) |
| TLS cert expiry | Very Low | Critical (all traffic) | High (ACM auto-renew) |
| Traffic spike / DDoS | Medium | High | Medium (WAF + autoscale) |
| Bad config deploy | Medium | High | High (validation + rollback) |
| Clock skew | Very Low | Medium | High (NTP + leeway) |
| Cascade (one svc → all) | Low | Critical | Medium (bulkhead) |
