# API Gateway — System Design
**Date:** 2026-06-17  
**Difficulty:** Hard  
**Real-world:** AWS API Gateway, Kong, Netflix Zuul, Nginx, Envoy, Cloudflare Workers

---

## First Principles — Do We Even Need a Gateway?

Without a gateway:
```
Mobile App → User Service (port 8001)
Mobile App → Order Service (port 8002)
Mobile App → Payment Service (port 8003)
```

Problems:
- Every client knows every service's address → tight coupling
- Auth logic duplicated in every service
- Rate limiting logic duplicated in every service
- No single place to enforce HTTPS, logging, tracing
- Adding a new service = client update required

**Gateway solves:** single entry point, cross-cutting concerns (auth, rate-limit, logging) centralized, client decoupled from backend topology.

---

## Scope & Scale Assumptions

- 50M DAU, peak 100K requests/sec
- 50 backend microservices
- Mix of mobile, browser, third-party API clients
- SLA: P99 gateway-added latency < 5ms (gateway overhead, not backend latency)
- 99.99% availability (4 nines = <52 min/year downtime)

---

## Entities

```
Client
  id, type (mobile|browser|third_party), api_key?, auth_token?

Route
  id, path_pattern, method, upstream_service, strip_prefix, timeout_ms, retry_policy

Service (upstream)
  id, name, instances[], health_check_url, circuit_breaker_state

RateLimit Policy
  id, key_by (user_id|api_key|ip), requests_per_window, window_seconds

JWT / Token
  sub (user_id), roles[], exp, iat, jti (token ID for revocation)

Request Log
  request_id, timestamp, client_ip, method, path, status_code, latency_ms, upstream_service, user_id?

Circuit Breaker State
  service_id, state (CLOSED|OPEN|HALF_OPEN), failure_count, last_failure_at, next_probe_at
```

---

## Actions

| Actor | Action |
|-------|--------|
| Client | Send HTTP request |
| Gateway | Terminate TLS |
| Gateway | Parse + validate request |
| Gateway | Authenticate (JWT verify / API key lookup) |
| Gateway | Authorize (role-based route access) |
| Gateway | Rate limit check |
| Gateway | Resolve upstream (service discovery) |
| Gateway | Check circuit breaker |
| Gateway | Forward request to upstream |
| Gateway | Receive + transform response |
| Gateway | Emit request log + metrics + trace |
| Gateway | Return response to client |
| Gateway | Health-check upstreams (background) |
| Ops | Add/update route config |
| Ops | Update rate limit policies |

---

## Data Flow

```
Client
  │
  │ HTTPS (TLS 1.3 termination at gateway)
  ▼
┌──────────────────────────────────────────────────────────┐
│                    API Gateway Node                       │
│                                                          │
│  1. Parse request                                        │
│     - Extract path, method, headers, body                │
│     - Generate request_id (UUID / Snowflake)             │
│     - X-Request-ID: propagate to upstream                │
│                                                          │
│  2. Route resolution                                     │
│     - Match path against route table (in-memory trie)    │
│     - 404 if no match                                    │
│                                                          │
│  3. Auth (per route policy)                              │
│     - JWT: local RS256 verify (no network call, ~0.5ms)  │
│     - API key: Redis lookup (1 cache call)               │
│     - No auth routes: skip                               │
│                                                          │
│  4. Rate limit                                           │
│     - Redis sliding window check                         │
│     - Key = user_id + route (or ip for unauth)           │
│     - 429 if exceeded                                    │
│                                                          │
│  5. Circuit breaker check                                │
│     - In-memory state per upstream                       │
│     - OPEN → 503 immediately (no network call)           │
│                                                          │
│  6. Load balance + forward                               │
│     - Pick healthy upstream instance (round-robin or CH) │
│     - Set request timeout (per route config)             │
│     - Propagate headers (X-Request-ID, X-User-ID)        │
│                                                          │
│  7. Receive upstream response                            │
│     - Update circuit breaker (success/failure)           │
│     - Apply response transforms (strip internal headers) │
│                                                          │
│  8. Log + metrics + trace                                │
│     - Async write to log buffer                          │
│     - Emit Prometheus counter/histogram                  │
│     - Jaeger span end                                    │
│                                                          │
│  9. Return response to client                            │
└──────────────────────────────────────────────────────────┘
  │
  ▼
Backend Services (User, Order, Payment, Notification, ...)
```

---

## High-Level Design

```
                        ┌─────────────────────────┐
                        │  Global Load Balancer    │
                        │  (AWS ALB / Cloudflare)  │
                        │  Anycast routing → POP   │
                        └────────────┬────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                       │
    ┌─────────▼─────────┐  ┌─────────▼─────────┐  ┌────────▼──────────┐
    │  Gateway Node 1   │  │  Gateway Node 2   │  │  Gateway Node 3   │
    │  (Stateless)      │  │  (Stateless)      │  │  (Stateless)      │
    └─────────┬─────────┘  └─────────┬─────────┘  └─────────┬─────────┘
              └──────────────────────┼──────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                       │
    ┌─────────▼──────┐    ┌──────────▼──────┐    ┌──────────▼──────┐
    │  Redis Cluster │    │  Service Reg.   │    │  Config Store   │
    │  Rate Limits   │    │  (Consul/K8s)   │    │  (etcd/Consul)  │
    │  Token Cache   │    │  Health + IPs   │    │  Routes, Policies│
    │  API Keys      │    └─────────────────┘    └─────────────────┘
    └────────────────┘
              │
              ▼
    ┌─────────────────────────────────────────────────────────┐
    │                   Backend Services                       │
    │  User (×5)  Order (×8)  Payment (×3)  Notification (×4) │
    └─────────────────────────────────────────────────────────┘
              │
              ▼
    ┌─────────────────────────────────────────────────────────┐
    │                  Observability                           │
    │   Prometheus (metrics)  Jaeger (traces)  Loki (logs)    │
    └─────────────────────────────────────────────────────────┘
```

**Why stateless gateway nodes?** No session state held locally → any node can handle any request → simple horizontal scaling + zero-downtime deploys.

---

## Low-Level Design

### 1. Route Table (In-Memory Trie)

Route matching must be O(path_length), not O(N_routes).

```
Routes loaded from config at startup, refreshed via config store watch:

  /api/v1/users/*       → User Service
  /api/v1/orders/*      → Order Service
  /api/v1/payments/*    → Payment Service
  /api/v1/products/*    → Product Service  (no auth required)

Trie structure:
  root
   └─ api
       └─ v1
           ├─ users → { upstream: user-svc, auth: JWT, ratelimit: user_standard }
           ├─ orders → { upstream: order-svc, auth: JWT, ratelimit: user_standard }
           ├─ payments → { upstream: payment-svc, auth: JWT, ratelimit: user_strict }
           └─ products → { upstream: product-svc, auth: none, ratelimit: ip_standard }

Wildcard matching: * captures remaining path segments
Parameterized: /users/:id → extract id from path
```

### 2. Auth — JWT (Self-Contained) vs API Key

**JWT (for human users — mobile/browser):**
```
Token: Header.Payload.Signature (RS256)

Gateway:
  1. Extract Bearer token from Authorization header
  2. Check exp (expiry) — client-side, no network call
  3. Verify signature with public key (cached in memory, refreshed hourly)
  4. Extract user_id, roles from payload
  5. Attach X-User-ID: <id> header before forwarding

Why RS256 (asymmetric):
  - Gateway holds PUBLIC key only
  - Private key stays with Auth Service (signs tokens)
  - Even if gateway is compromised, attacker can't mint tokens

Revocation problem:
  JWT is stateless → can't revoke mid-life
  Solutions:
    a) Short TTL (15 min) + refresh token (simple, industry standard)
    b) jti blocklist in Redis: CHECK redis GET jti:{token_id} → exists = revoked
       Cost: 1 Redis GET per request (~0.5ms)
    c) Opaque token introspection: POST /introspect → Auth Service (100ms, coupling)
  
  Use (a) for most cases. Add (b) for high-security (admin tokens, password reset).
```

**API Key (for third-party/server clients):**
```
Request: Authorization: ApiKey <key>

Gateway:
  key_hash = SHA256(raw_key)          ← never store raw key
  api_key_info = Redis.GET(api_keys:{key_hash})
  → { user_id, plan, rate_limit_tier, scopes[] }
  
  Cache TTL: 60s (tolerable staleness — revoked keys valid for up to 60s)
  
  If not in Redis → DB lookup → populate cache
  If not in DB → 401 Unauthorized
```

**Trade-off: Local JWT verify vs Remote introspection**

| | Local JWT verify | Opaque token introspection |
|--|---|---|
| Latency | ~0.5ms (CPU only) | ~10-50ms (network to Auth Service) |
| Freshness | Stale up to TTL | Always current |
| Auth Service load | None | N * RPS (gateway traffic) |
| Revocation | Only via jti blocklist or expiry | Immediate |
| Use | Default (mobile/browser) | Admin APIs, instant revoke required |

### 3. Rate Limiting — Sliding Window Counter

```
Key: ratelimit:{user_id}:{route_id}:{window_bucket}
Window: 60 seconds, 5 buckets of 12s each

Algorithm:
  now = current_time()
  current_bucket = floor(now / 12)
  
  # Atomic Lua script (runs in Redis, no TOCTOU):
  MULTI:
    INCR  ratelimit:{user_id}:{route_id}:{current_bucket}
    EXPIRE ratelimit:{user_id}:{route_id}:{current_bucket} 120
  EXEC
  
  # Read last 5 buckets:
  total = sum(GET ratelimit:{user_id}:{route_id}:{current_bucket - i} for i in 0..4)
  
  if total > limit:
    return 429, Retry-After: 12 - (now % 12)
  else:
    proceed

Trade-off vs Token Bucket:
  Token bucket: smoother burst handling, but harder to inspect current state
  Sliding window: easy to explain, slightly more Redis calls
  
At 100K RPS: 100K Redis rate-limit operations/sec
  → Redis cluster can handle 1M+ ops/sec → fine
  
Fallback if Redis down: fail-open (allow all) — see Failure Analysis
```

### 4. Service Discovery + Load Balancing

```
Consul / Kubernetes Service Registry:
  Every backend instance registers on startup:
    { service: "order-svc", address: "10.0.1.45", port: 8080, health: "/health" }
  
  Deregisters on graceful shutdown.
  Health check: gateway polls /health every 10s.
  Unhealthy → removed from pool within 30s.

Gateway load balancing (per upstream):
  Default: Round-robin across healthy instances
  Sticky: Consistent hash on X-User-ID (for session-affinity scenarios)
  Weighted: canary deployment (90% v1, 10% v2)

Instance selection:
  available = filter(instances, state=HEALTHY)
  if len(available) == 0: circuit → 503
  selected = round_robin.next(available)
  
Cache TTL: gateway refreshes service registry every 5s (DNS TTL or Consul watch)
```

### 5. Circuit Breaker (per upstream service)

```
State machine (in-memory per gateway node, NOT shared via Redis):

  CLOSED → OPEN: 5 failures in 10s
  OPEN → HALF_OPEN: after 30s timeout
  HALF_OPEN → CLOSED: next request succeeds
  HALF_OPEN → OPEN: next request fails

Why not share state via Redis?
  Race condition: 10 gateway nodes all try to OPEN circuit simultaneously
  Redis write overhead on every request
  In-memory per-node: each node independently trips
  Effect: all nodes trip within a few seconds anyway (same upstream failing)
  Simpler + faster

Failure counting: sliding window (5 failures in last 10s, not 5 total)
Timeout errors count as failures (default 5s per-route, configurable)

On OPEN:
  Immediate 503 → no upstream call made
  Response header: X-Circuit-Open: true (internal only, stripped before client)
  
Fallback response options (configured per route):
  a) 503 with Retry-After
  b) Cached last-known-good response (stale-while-revalidate)
  c) Static fallback JSON (e.g., empty product list)
```

### 6. Request/Response Transformation

```
Before forwarding to upstream:
  Strip client headers: Authorization (replaced with X-User-ID)
  Add: X-Request-ID, X-User-ID, X-User-Roles, X-Forwarded-For
  Rewrite path: /api/v1/users/123 → /users/123 (strip prefix)
  
Before returning to client:
  Strip internal headers: X-Internal-Service-Name, X-Pod-ID
  Add: X-Request-ID (for client-side correlation)
  CORS headers: Access-Control-Allow-Origin (configured per route)
  
Protocol translation (advanced):
  REST → gRPC: gateway transcodes JSON ↔ Protobuf
  Why: internal services use gRPC (efficient), external clients use REST
  Cost: gateway does JSON→proto encoding → adds ~1ms
```

### 7. Observability

```
Every request generates:

Metrics (Prometheus):
  gateway_requests_total{method, path_template, status, upstream} Counter
  gateway_request_duration_ms{method, path_template, upstream} Histogram (P50/P95/P99)
  gateway_circuit_breaker_state{upstream} Gauge
  gateway_rate_limit_exceeded_total{route, key_type} Counter

Trace (Jaeger / OpenTelemetry):
  Span: gateway_request
    → child span: auth_check (if JWT)
    → child span: rate_limit_check
    → child span: upstream_forward
    → child span: upstream_response
  
  Propagate W3C traceparent header to upstream → distributed trace

Logs (structured JSON → Loki):
  {
    "request_id": "req-abc123",
    "ts": "2026-06-17T10:23:45.123Z",
    "method": "POST",
    "path": "/api/v1/orders",
    "status": 201,
    "upstream": "order-svc",
    "upstream_instance": "10.0.1.45:8080",
    "latency_ms": 42,
    "user_id": "usr-789",
    "rate_limited": false,
    "circuit_open": false
  }
  
Async log drain:
  Write to in-process ring buffer → background goroutine batch-sends to Loki
  Never block request path on log write
  Buffer size: 100K entries. On overflow: drop oldest (log loss acceptable)
```

### 8. Config Hot-Reload (Zero-Downtime Route Updates)

```
Config stored in etcd/Consul.
Gateway watches for changes (long-poll or gRPC stream).

On change:
  1. Download new config
  2. Validate (dry-run: no undefined upstreams, no malformed regexes)
  3. Atomic swap: replace in-memory route table pointer (single pointer update, thread-safe)
  4. Old config: garbage collected
  5. Log: "Config reloaded: added 2 routes, modified 1 rate-limit policy"

No restart. No request dropped.

Rollback:
  etcd stores history → revert to previous version in 1 command
  Gateway receives change → reloads old config
```

---

## Key Trade-offs

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Auth mechanism | JWT local verify (default) | Opaque token introspection | Introspection adds 10-50ms + Auth Service load at 100K RPS |
| Rate limit storage | Redis | In-memory per node | In-memory: inconsistent limits across 10 gateway nodes |
| Circuit breaker state | In-memory per node | Redis shared state | Redis: write overhead on every request + race on state transition |
| Route storage | In-memory trie | DB lookup per request | DB lookup: ~5ms per request × 100K RPS = unsustainable |
| Log drain | Async ring buffer | Synchronous write | Synchronous: ~5ms per log write would dominate P99 |
| Service discovery | Consul watch (5s refresh) | DNS TTL | DNS: 30-300s TTL → stale IPs too long during failover |
| Gateway state | Stateless | Stateful sessions | Stateful: can't horizontally scale or hot-reload |

---

## Capacity Estimates

```
Traffic: 100K RPS peak

Gateway cluster:
  Each node: 10K RPS capacity (conservative, single-core hot path ~100µs/req)
  Min nodes: 10 (for 100K RPS)
  With 2× redundancy: 20 nodes
  
Redis cluster (rate limiting + token cache):
  100K RPS × 2 Redis ops/req = 200K Redis ops/sec
  Redis single node: 100K ops/sec → cluster of 3 shards (each handling 70K ops/sec with headroom)
  
Memory per gateway node:
  Route trie: ~5MB (50 services × 100 routes × ~1KB metadata)
  Circuit breaker state: ~1KB (50 upstreams × 20 bytes each)
  In-process log buffer: 100MB (100K entries × 1KB each)
  Total: ~200MB per node — trivial
  
Network:
  Average request: 1KB request + 5KB response = 6KB
  100K RPS × 6KB = 600MB/sec → 5Gbps NIC adequate
```

---

## Config-Driven Route Example

```yaml
routes:
  - id: create-order
    path: /api/v1/orders
    method: POST
    upstream: order-svc
    strip_prefix: /api/v1
    timeout_ms: 5000
    retry:
      attempts: 2
      on_status: [502, 503, 504]
      backoff_ms: 200
    auth:
      type: jwt
      required_roles: [user]
    rate_limit:
      policy: user_standard        # 100 req/min per user
    circuit_breaker:
      failure_threshold: 5
      window_seconds: 10
      open_duration_seconds: 30
    fallback:
      type: static
      status: 503
      body: '{"error": "service_unavailable", "retry_after": 30}'
```

---

## Real-World Reference

| System | Gateway Approach | Notes |
|--------|-----------------|-------|
| **Netflix Zuul** | JVM-based, filter chain | Each filter = one step (auth, rate-limit, route) |
| **Kong** | Nginx + Lua plugins | Plugin-per-concern, same filter chain model |
| **AWS API Gateway** | Managed, Lambda integrations | Adds ~10ms for Lambda cold start avoidance |
| **Envoy** | C++ sidecar proxy | Used in service mesh (Istio), microsecond overhead |
| **Cloudflare Workers** | V8 isolates at edge | Code runs at CDN PoP, sub-1ms auth |

Engineering blogs:
- Netflix: "Zuul 2: The Netflix Journey to Asynchronous, Non-Blocking Systems" (netflixtechblog.com)
- Cloudflare: "How Cloudflare analyzes 1M DNS queries per second" (blog.cloudflare.com)
- Uber: "Integrating API Gateways with Traffic Management" (eng.uber.com)
