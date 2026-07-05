# Rate Limiter — Interview Q&A
**Date:** 2026-05-17

---

## Round 1: Requirements (First 5 Minutes)

**Interviewer:** "Design a rate limiter."

**Your move — ask these:**
> - Global rate limit or per-user/per-endpoint?
> - What scale? (RPS, # of users)
> - Hard limit (strict 429) or soft (best-effort)?
> - Client-side or server-side?
> - Single datacenter or distributed?

**Good answer framing:**
> "I'll design a distributed server-side rate limiter at 10K RPS, supporting per-user per-endpoint limits, with hard enforcement. I'll start with first principles — do we really need centralized state, or can we do this locally?"

---

## Core Questions & Strong Answers

---

**Q: Why not just do rate limiting locally on each API node?**

A: Local rate limiting is O(1) and zero latency — it's a valid first step. But with N nodes, each node only sees 1/N of traffic. A user can hit N×limit total requests by spreading across nodes. This is fine for rough abuse prevention, but fails for strict per-user limits. For strict enforcement, we need shared state — Redis becomes the single counter store.

**Follow-up:** "What if we accept some over-admission — say 10%?"
→ Then local with periodic sync is fine. Sync every 100ms, accept up to 10% window of over-admission. Reduces Redis load by 100x.

---

**Q: Why Redis over a SQL database for counters?**

A: Three reasons: (1) Redis INCR is atomic — no need for transactions or locking. (2) In-memory — sub-millisecond reads/writes vs 5-20ms for SQL. (3) TTL is a first-class feature — counters self-expire without a cleanup job. SQL would need a scheduler to delete old rows and can't handle 10K writes/sec without connection pool exhaustion.

**Follow-up:** "What about consistency in Redis?"
→ Redis is single-threaded for command execution — INCR is atomic. For sliding window with ZADD/ZCARD, wrap in Lua script for atomicity. Redis Cluster has eventual consistency between replicas (async replication), so we accept ~100ms of counter lag in failover.

---

**Q: Walk me through the sliding window counter algorithm.**

A:
```
Two values stored:
  prev_count: requests in previous complete window
  curr_count: requests in current partial window
  curr_window_start: timestamp

On each request:
  elapsed = (now - curr_window_start) / window_size
  approx = prev_count × (1 - elapsed) + curr_count
  if approx < limit: ALLOW, curr_count++
  else: DENY
```
This gives ~99.97% accuracy vs perfect sliding log, using O(1) memory. The approximation error is bounded by the ratio of burst to window size — acceptable for most use cases.

---

**Q: What if two requests arrive simultaneously and both read count=99 (limit=100)?**

A: Classic race condition. Both increment to 100, both get ALLOWED — but we've actually admitted 2 requests over the 99th slot. Solution: Lua script combines the read-check-write into an atomic operation. Redis executes it single-threaded, so no interleaving. Alternative: Redis Transaction with WATCH/MULTI/EXEC, but Lua is simpler and faster.

---

**Q: Your rate limiter adds latency to every request. How do you minimize it?**

A:
1. **Co-locate Redis** — same AZ as API gateway, <1ms RTT
2. **Pipeline Redis commands** — batch INCR + EXPIRE in one round trip
3. **Local rule cache** — rules rarely change, cache in-process for 60s (no Redis call)
4. **Async logging** — don't wait for audit log write in hot path
5. **Timeout + fail open** — if Redis > 2ms, fail open (don't block on limiter)
6. **Result:** Rate limiter adds ~0.5ms P50, ~2ms P99

---

**Q: How do you handle burst traffic — a user sends 1000 requests in 1 second but their limit is 100/minute?**

A: Depends on algorithm chosen:
- **Fixed window:** First 100 in that minute's window pass, rest rejected. Bursty but bounded.
- **Token bucket:** Allows burst up to bucket capacity, then throttles to refill rate. Best for bursty-but-fair use cases (Stripe uses this).
- **Leaky bucket:** Queues requests, processes at fixed rate — smooths bursts but adds latency.

For public APIs, token bucket is usually the answer — allows clients to "save up" quota and burst, which matches real usage patterns.

---

**Q: What happens when Redis goes down?**

A: Two choices — fail open (allow all requests) or fail closed (deny all). I'd fail open with alerting for most APIs. Rationale: Rate limiter failure is unlikely to coincide with an actual attack. Failing closed would cause a service outage — worse than a brief period without rate limiting. For security-critical endpoints (login, password reset), consider fail closed. Implementation: wrap Redis call in try-catch with 2ms timeout; on exception, log + allow.

**Follow-up:** "How do you prevent abuse during the Redis outage window?"
→ Fall back to local in-process counter — not globally accurate but provides per-node protection. Better than no protection.

---

**Q: How do you rate limit distributed DDoS — 10,000 IPs each sending 10 req/s?**

A: This is a different problem — IP-level flood vs user-level fairness. Solutions:
1. **IP rate limiting** — 100 req/min per IP at edge (Cloudflare, AWS WAF) — before traffic reaches our system
2. **Geographic rate limiting** — if 90% traffic from one region is anomalous, apply stricter limits
3. **Progressive penalties** — first violation: 429, second: 1hr block, third: 24hr block
4. **CAPTCHA challenge** — for browser clients, redirect to CAPTCHA instead of hard 429
5. Key insight: True DDoS is an infrastructure problem (CDN, Anycast), not an application rate limiter problem.

---

**Q: How do you support different limits for different tiers? (Free vs Pro vs Enterprise)**

A:
```
Rule lookup priority:
  1. user_id + endpoint (most specific)
  2. user_id + tier (tier default)
  3. endpoint (global endpoint limit)
  4. global (last resort)

Rules table:
  (client_id, endpoint, limit, window_sec, tier)
```
Admin API updates rules → invalidates cache via Redis Pub/Sub → nodes refresh within 1s. Config stored in PostgreSQL, cached in Redis + local LRU.

---

**Q: Design the data model. What exactly is stored in Redis?**

A:
```
# Fixed window counter
Key:   rl:{client_id}:{endpoint_hash}:{window_ts}
Type:  String (integer)
TTL:   window_size + 5s buffer
Value: 47 (counter)

# Sliding window log
Key:   rl:log:{client_id}:{endpoint_hash}
Type:  Sorted Set
Score: Unix timestamp (float)
Member: request_id

# Token bucket
Key:   rl:tb:{client_id}:{endpoint_hash}
Type:  Hash
Fields: tokens (float), last_refill (unix_ts_ms)
TTL:   idle_timeout (e.g., 1 hour)

# Deduplication (idempotency)
Key:   rl:dedup:{request_id}
Type:  String
Value: "ALLOW" or "DENY"
TTL:   10s
```

---

**Q: How would you test this system?**

A:
1. **Unit tests:** Each algorithm independently — boundary conditions, race conditions (via goroutine stress test)
2. **Integration tests:** Redis commands — verify Lua atomicity, TTL behavior
3. **Load tests:** 10K RPS with k6 or Locust — verify P99 latency < 5ms
4. **Failure tests:** Kill Redis mid-test — verify fail-open behavior and counter recovery
5. **Accuracy tests:** Send exactly 1000 requests, verify exactly 100 allowed (±1 for sliding window approximation)
6. **Chaos tests:** Network partition between API nodes and Redis — verify no crash, just degraded mode

---

## Rapid Fire

| Question | Answer |
|---|---|
| What HTTP status code for rate limit? | 429 Too Many Requests |
| What header tells client when to retry? | `Retry-After: <seconds>` |
| How to rate limit by IP behind load balancer? | Use `X-Forwarded-For` header (trust only first hop) |
| What Redis command is atomic for counter? | `INCR` (or Lua script for check-and-increment) |
| Fixed window vs sliding window — which more accurate? | Sliding window (but more memory) |
| Token bucket vs leaky bucket — which allows bursting? | Token bucket |
| How many Redis nodes for HA? | Min 3 (1 primary + 2 replicas) with Sentinel |
| What is the error rate of sliding window counter? | < 0.003% |

---

## Red Flags to Avoid

- ❌ "I'd use a database for counters" — too slow
- ❌ "Just use the app server memory" — doesn't work distributed
- ❌ Not mentioning atomicity — shows lack of distributed systems understanding
- ❌ Not discussing fail-open vs fail-closed — shows lack of production thinking
- ❌ Jumping to solution without requirements — always clarify scale + strictness first

---

## Real-World References

- **Cloudflare:** Uses sliding window counter in their edge network (blog: "How we built rate limiting")
- **Stripe:** Token bucket per API key, 100 req/s default (documented in API reference)
- **AWS API Gateway:** Token bucket with burst capacity (separate from steady-state rate)
- **GitHub API:** Fixed window, 5000 req/hr, returns X-RateLimit-* headers
- **Redis Blog:** "Rate Limiting with Redis" — INCR + EXPIRE pattern

