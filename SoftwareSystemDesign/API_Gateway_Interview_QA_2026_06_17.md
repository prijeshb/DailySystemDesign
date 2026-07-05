# API Gateway — Interview Q&A
**Date:** 2026-06-17

---

## Opening Questions

**Q: Design an API Gateway.**

**Starter answer (2 min):**
> "Before designing, let me ask: are we building a gateway for internal microservices, a public API product, or both? And what's the scale — requests per second, number of backend services?
>
> Assuming a mid-large company: 50 backend microservices, 100K RPS public-facing. The gateway needs to: route requests to the right service, authenticate clients, enforce rate limits, and handle failures gracefully. I'll start with entities and work through the request lifecycle..."

---

## Q1: Why do we need an API Gateway? Couldn't each service handle its own auth?

**Answer:**
> Cross-cutting concerns — auth, rate limiting, logging, TLS termination — applied in every service means code duplication, inconsistency, and harder upgrades. If I want to change from HS256 to RS256 JWT, I'd update 50 services. With a gateway, I update one.
>
> It also decouples clients from backend topology. Clients don't know if order-svc has 3 instances or 30 — the gateway handles service discovery and load balancing transparently.

**Interviewer follow-up:** What's the downside?
> The gateway becomes a single point of failure and a potential bottleneck. You mitigate both: redundant gateway nodes behind a load balancer, and keeping the gateway thin (fast path only — no business logic).

---

## Q2: How does authentication work in your design? JWT vs API key vs session?

**Answer:**
> I'd support two mechanisms:
>
> **JWT (RS256) for user-facing clients** (mobile, browser). The gateway verifies the signature locally using the Auth Service's public key — no network call, ~0.5ms. Extract user_id and roles from the payload, forward as X-User-ID header to upstream.
>
> **API keys for server-to-server** (third-party integrations). Hash the raw key (SHA256), look up in Redis. Returns the associated user_id, plan, and rate limit tier.
>
> I'd avoid opaque token introspection for high-frequency routes — it adds 10-50ms of latency and creates a hard dependency on the Auth Service.

**Follow-up:** How do you handle JWT revocation (e.g., user logs out)?

> JWT is stateless — you can't "unsign" it. Three approaches:
> 1. **Short TTL** (15 min) + refresh tokens — industry standard. After logout, refresh token invalidated; JWT dies naturally in 15 min.
> 2. **jti blocklist in Redis** — gateway checks SET jti:{token_id} on every request (1 Redis call). On logout, Auth Service adds jti to Redis with same TTL as JWT. Effective but adds 1 Redis lookup.
> 3. **Opaque introspection** for admin tokens where instant revocation is critical.
>
> Default to (1). Add (2) for sensitive tokens.

---

## Q3: How do you rate limit? What happens when Redis goes down?

**Answer:**
> Sliding window counter per (user_id, route) stored in Redis. I use 5 buckets of 12 seconds each to approximate a 60-second sliding window. The math:
>
> ```
> total = sum of last 5 bucket counters
> if total > limit: 429
> ```
>
> The buckets auto-expire after 120s, so no cleanup needed.
>
> When Redis goes down: I fail-open — skip rate limit and allow the request through. The alternative, fail-closed (block everything), is worse: 100% of users are blocked because of a rate-limiting infrastructure failure, not because they did anything wrong. The brief bypass window (30s during Redis Sentinel election) is acceptable.

**Follow-up:** What if you have 10 gateway nodes and Redis is up — is the rate limit accurate?

> Yes, because all 10 nodes share the same Redis — the counters are global, not per-node. This is the key reason to use Redis over in-memory counters per node (in-memory: user could get 10× the limit by hitting all 10 nodes).

**Follow-up:** Token bucket vs sliding window — which is better?

> Token bucket is better for bursty traffic — a user with 100 req/min limit can send 100 at once (burst) then nothing for the rest of the minute. Sliding window is smoother — it prevents bursts by spreading the window across time. I'd use sliding window for most APIs to prevent a sudden burst from hammering the upstream.

---

## Q4: How does the circuit breaker work? When does it open?

**Answer:**
> It's a state machine with 3 states: CLOSED (normal), OPEN (failing fast), HALF-OPEN (probing recovery).
>
> Trigger: 5 failures (timeouts or 5xx errors) in a 10-second sliding window → trips to OPEN. In OPEN state, all requests to that upstream fail immediately with 503 — no actual connection attempt. After 30 seconds, one probe request goes through. Success → CLOSED. Failure → OPEN again.
>
> The key insight: without it, a slow upstream causes thread pool exhaustion because threads wait for the timeout (e.g., 5 seconds each). With 500 concurrent users, that's 2,500 thread-seconds held. Circuit breaker converts "slow failure" to "instant failure" once the pattern is detected.

**Follow-up:** Do you share circuit breaker state across all gateway nodes via Redis?

> No — I keep it in-memory per node. Sharing via Redis adds overhead on every request and introduces a race condition (10 nodes trying to transition state simultaneously). In practice, all nodes see the same upstream failures within a few seconds and trip independently — the effect is the same.

---

## Q5: How do you do service discovery? What if the service registry goes down?

**Answer:**
> I use Consul (or Kubernetes service endpoints if on K8s). Each backend service instance registers on startup with its IP, port, and a health check URL. The gateway subscribes to changes via Consul watch — it gets notified within seconds when instances appear or disappear.
>
> If Consul goes down: I serve from a local in-memory cache of the last-known instance list (30-second TTL). Additionally, the gateway independently health-checks each instance every 10 seconds — so even with Consul down, we can detect crashed instances and remove them from the local pool.

**Follow-up:** How do you handle a backend deployment (rolling update, half instances on old version)?

> During rolling deploy, both old and new instances are in the pool. The gateway routes to both. If the new instances have a bug (higher error rate), the circuit breaker trips for the whole upstream. Better: deploy with a canary weight. Configure service registry to mark new instances with weight=5% — gateway sends 5% of traffic to new version. Monitor error rates. Promote or rollback based on metrics.

---

## Q6: How do you minimize the latency overhead of the gateway?

**Answer:**
> The gateway should add < 5ms P99 overhead. Key optimizations:
>
> - **Route table as in-memory trie** — O(path length) matching, not O(N routes)
> - **JWT: local verify, no network call** — ~0.5ms CPU
> - **Redis operations with 5ms timeout** — if Redis is slow, we fail-open and don't wait
> - **Async logging** — write to in-memory ring buffer, batch-drain in background. Never block the request path for I/O
> - **Connection pool to upstreams** — persistent HTTP/2 connections, no TCP handshake per request
> - **Metrics emitted async** — Prometheus scrapes our metrics endpoint, we don't push synchronously

**Follow-up:** What's your expected P99 latency breakdown?

> TLS handshake (cached session resumption): ~0.5ms  
> Route lookup: ~0.1ms  
> JWT verify: ~0.5ms  
> Redis rate limit: ~1ms  
> Circuit breaker check (in-memory): ~0.01ms  
> Forward to upstream: network + backend time  
> Log write (async, non-blocking): 0ms on request path  
> Total gateway overhead: **~3ms** at P99

---

## Q7: How do you handle a new service being added? Do you need to redeploy the gateway?

**Answer:**
> No. Config hot-reload: routes are stored in etcd/Consul. The gateway watches for changes. When a new route is added:
> 1. Config validator verifies the new route (upstream exists in service registry, no schema errors)
> 2. Gateway gets notified via watch event
> 3. Atomic swap of in-memory route table pointer — thread-safe, zero dropped requests
> 4. Done, no restart
>
> This also enables A/B testing of routes and gradual rollout of config changes.

---

## Q8: How do you handle timeouts and retries?

**Answer:**
> Per-route configuration:
> ```
> timeout_ms: 5000
> retry:
>   attempts: 2
>   on_status: [502, 503, 504]   # server errors only
>   backoff_ms: 200
> ```
>
> Critical: only retry on idempotent operations (GET, PUT) or on transient server errors (502/503/504) — never retry on 4xx (client error, retrying is useless) or on POST without idempotency keys (risk of duplicate writes).
>
> If POST has an idempotency key (X-Idempotency-Key header): safe to retry — upstream will deduplicate.

**Follow-up:** What's the risk of retries under load?

> Retry amplification: if upstream is at capacity and you retry twice, you just tripled its load. Fix: exponential backoff with jitter. Also: don't retry if the circuit breaker is open. And: set a global retry budget (max 10% of gateway's requests are retries at any given time).

---

## Q9: How does the gateway integrate with observability?

**Answer:**
> Three pillars:
>
> **Metrics (Prometheus):** Counters (requests_total, rate_limit_exceeded_total) and histograms (request_duration_ms broken down by method, path template, status, upstream). Alert on: error rate > 1%, P99 latency > 500ms, circuit breaker open.
>
> **Traces (Jaeger / OpenTelemetry):** Every request gets a trace. Gateway creates a root span, adds child spans for auth check, rate limit check, upstream call. Propagates traceparent header to backend — distributed trace across gateway + all services. When an order takes 3 seconds, you can see: gateway overhead 3ms, User Service 200ms, DB query 2.7s.
>
> **Logs (structured JSON → Loki):** request_id, user_id, method, path, status, latency_ms, upstream. Written async to avoid request path I/O. Log sampling: 100% error logs, 1% success logs at high traffic.

---

## Q10: How is an API Gateway different from a Service Mesh (Istio)?

**Answer:**
> Both handle routing, load balancing, and circuit breaking — but at different scopes:
>
> | | API Gateway | Service Mesh (Istio/Envoy) |
> |--|---|---|
> | **Position** | North-south (client → cluster) | East-west (service → service) |
> | **Auth** | External client auth (JWT, API key) | mTLS between services (identity, not user auth) |
> | **Rate limiting** | Per-user/client | Per-service-to-service |
> | **Observability** | Client-visible metrics | Internal service metrics |
> | **Who deploys** | Platform/infra team | Injected sidecar (transparent to dev) |
>
> In practice: you use both. Gateway handles external traffic entering the cluster. Service mesh handles traffic between microservices inside the cluster. They complement, not replace each other.

---

## Q11: Hard Q — How would you design the gateway to handle 1M RPS?

**Answer:**
> Key bottlenecks at 1M RPS:
>
> **1. Gateway nodes:** 10K RPS per node → 100 nodes minimum. Horizontal scaling, stateless — straightforward.
>
> **2. Redis:** 1M RPS × 2 ops = 2M Redis ops/sec. Single Redis cluster tops out at ~1M ops/sec → need Redis Cluster with 4+ shards. Rate limit keys shard by user_id hash naturally.
>
> **3. JWT verify:** RS256 verify is CPU-bound (~0.5ms). 1M RPS × 0.5ms = 500K CPU-ms/sec = 500 CPU cores just for JWT. Mitigation: cache the verification result by jti (SHA256) in Redis — if same token seen again within 60s, skip re-verify. Typical user makes multiple API calls per session, so hit rate is high.
>
> **4. Network:** 1M RPS × 6KB avg = 6GB/sec = 48Gbps. Need 10Gbps NICs + bonding, or distribute across 10+ PoPs with Anycast routing (each PoP handles 100K RPS).
>
> **5. Service discovery:** At 1M RPS, refreshing service registry every 5s across 100 nodes = 20 registry reads/sec. Fine.
>
> The real constraint at 1M RPS is usually network bandwidth and Redis. Solution: geo-distribute with Anycast, Redis Cluster, JWT result caching.

---

## Common Follow-up Questions

**"What if two rate limit windows race?"**
> Redis Lua script makes increment + read atomic. No TOCTOU.

**"How do you test the gateway in staging?"**
> Shadow mode: production traffic duplicated (mirrored) to staging gateway. Staging forwards to staging backends. Responses discarded. Compare error rates and latency between prod and staging gateway versions.

**"What's the difference between a gateway and a reverse proxy (Nginx)?"**
> Nginx is a primitive reverse proxy: route by URL, load balance, cache. A gateway adds: auth, rate limiting, service discovery integration, circuit breaking, request transformation, API key management. Kong = Nginx + gateway logic as plugins.

**"How do you handle WebSocket connections?"**
> WebSocket starts as HTTP upgrade. Gateway proxies the upgrade handshake, then creates a persistent bidirectional TCP tunnel between client and upstream. Rate limiting: per-connection (not per-message, as messages are too frequent). Circuit breaker: on WS disconnect count, not per-message. Sticky routing: once WS is established, must stay on same upstream instance (stateful connection).

**"What about GraphQL? Same gateway?"**
> GraphQL is a single endpoint (POST /graphql). Gateway handles auth and rate limiting the same way. Complexity: one endpoint, many possible operations. Rate limit by operation name (extracted from request body) not by URL. Also: query complexity scoring — limit by estimated backend work, not just request count.
