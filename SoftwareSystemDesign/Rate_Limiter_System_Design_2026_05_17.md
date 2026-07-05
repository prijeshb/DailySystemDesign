# Design: Distributed Rate Limiter
**Date:** 2026-05-17 | **Difficulty:** Medium-Hard | **Companies:** Meta, Google, Stripe, Cloudflare

---

## First Principles: Do We Even Need It?

> "Do we really need a rate limiter?"

**Why YES:**
- Prevent DoS / brute-force attacks
- Fair resource allocation across clients
- Control cost (API calls to paid services)
- Protect downstream services from cascading overload

**Why it's HARD:**
- Distributed: multiple API gateway nodes must share state
- Low latency: must not add >1-2ms to every request
- High availability: rate limiter itself can't be a SPOF
- Correctness: must not allow 2x traffic due to race conditions

---

## Entities

| Entity | Description |
|---|---|
| **Client** | Identified by user_id, IP, or API key |
| **Rule** | (client_id, endpoint, limit, window_seconds) |
| **Counter** | Tracks requests in current window for a client |
| **Request** | Incoming API call with metadata |
| **Decision** | ALLOW or DENY with quota headers |

---

## Actions

1. `checkAndIncrement(client_id, endpoint)` → ALLOW / DENY
2. `getRemainingQuota(client_id, endpoint)` → int
3. `updateRule(client_id, limit, window)` → admin op
4. `resetCounter(client_id)` → admin/expiry op
5. `syncCounters()` → cross-node reconciliation

---

## Data Flow

```
Client Request
     │
     ▼
API Gateway (Load Balanced)
     │  Extract: user_id / IP / API key
     ▼
Rate Limiter Middleware
     │  1. Fetch Rule from Config Cache (local TTL=60s)
     │  2. Build key: "rl:{client_id}:{endpoint}:{window}"
     │  3. INCR key in Redis (atomic)
     │  4. If first request → SET TTL = window_seconds
     │
     ├── counter <= limit  →  Forward to Backend
     │                         + headers: X-RateLimit-Remaining
     │
     └── counter > limit   →  Return 429
                               + headers: X-RateLimit-Reset
```

---

## Algorithms — Trade-off Matrix

### 1. Fixed Window Counter
```
|--window--|--window--|--window--|
0         60        120        180  (seconds)

Key: "rl:user123:60"  → counter
```
- **Pro:** O(1) space, simple Redis INCR
- **Con:** Burst at boundary — user can send 2x limit at window edge
- **Used by:** Simple internal APIs

### 2. Sliding Window Log
```
Store: sorted set of request timestamps
On each request:
  ZREMRANGEBYSCORE key 0 (now - window)  ← remove old
  ZCARD key                               ← current count
  ZADD key now request_id                ← add new
```
- **Pro:** Perfect accuracy
- **Con:** O(n) memory per user per window
- **Used by:** Low-volume admin APIs

### 3. Sliding Window Counter (Recommended)
```
count = prev_window_count × (remaining fraction) + curr_window_count

e.g., window=60s, now=75s (25s into new window)
fraction_elapsed = 25/60 = 0.42
approx_count = old_count × (1 - 0.42) + curr_count
             = 100 × 0.58 + 40 = 98
```
- **Pro:** O(1) memory, ~accurate (within 0.003% error)
- **Con:** Approximation, not exact
- **Used by:** Cloudflare, most production systems

### 4. Token Bucket
```
Bucket: {tokens: 100, last_refill: timestamp}
On request:
  refill = (now - last_refill) × rate
  tokens = min(capacity, tokens + refill)
  if tokens >= 1: tokens -= 1, ALLOW
  else: DENY
```
- **Pro:** Allows controlled bursting (bursty traffic patterns)
- **Con:** Two reads + write per request, clock dependency
- **Used by:** AWS API Gateway, Stripe

### 5. Leaky Bucket
```
Queue-based: requests enter bucket, drain at fixed rate
```
- **Pro:** Smooth output rate
- **Con:** Increased latency, queue overflow = drop
- **Used by:** Network traffic shaping

---

## High-Level Design

```
                    ┌─────────────────┐
                    │  Config Service  │  (Rules DB + Cache)
                    │  PostgreSQL +    │
                    │  Local LRU Cache │
                    └────────┬────────┘
                             │ rule fetch (TTL=60s)
                             │
Client ──► API Gateway ──► Rate Limiter ──► Backend Services
  (N)         (N nodes)      Middleware      (protected)
                             │
                    ┌────────▼────────┐
                    │  Redis Cluster  │
                    │  (3 shards,     │
                    │   3 replicas)   │
                    └─────────────────┘
```

**Component Sizing (10K RPS):**
- Redis: ~1M ops/sec capacity, counter ops take <0.5ms
- Each counter key: ~50 bytes × 10M users = ~500MB RAM
- Config cache: per-node LRU, avoids DB hit per request

---

## Low-Level Design

### Redis Key Schema
```
Counter:   rl:{client_id}:{endpoint}:{window_start}
           rl:user123:POST/api/send:1716000060
           TTL = window_size + buffer (e.g., 65s for 60s window)

Token Bucket:  rl:tb:{client_id}
               Hash: {tokens, last_refill_ts}

Rule:      rule:{client_id}:{endpoint}
           Hash: {limit, window_sec, burst_limit}
```

### Atomic Counter (Lua Script — prevents race condition)
```lua
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local count = redis.call('INCR', key)
if count == 1 then
  redis.call('EXPIRE', key, window)
end
if count > limit then
  return {0, count}   -- DENY
else
  return {1, count}   -- ALLOW
end
```
**Why Lua?** Atomic execution — no race between INCR and EXPIRE.

### Response Headers (RFC 6585 + Draft)
```http
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1716000120   (Unix timestamp)
Retry-After: 47
```

### Distributed Counter — Multi-node Problem
```
Problem: 3 API gateway nodes each have local state
         Node1 sees 40 req, Node2 sees 35 req, Node3 sees 30 req
         Total = 105, but each would ALLOW (< 100 limit)

Solution A: Centralized Redis (current design)
  - All nodes write to same Redis → consistent
  - Adds ~1ms network hop per request

Solution B: Local + Periodic Sync
  - Each node tracks locally, syncs every 100ms
  - Lower latency, ~1% over-admission window
  - Good for non-critical limits

Solution C: Gossip Protocol
  - Nodes share counts via gossip
  - Eventual consistency, complex implementation
```

---

## Sharding Strategy

**Problem:** Single Redis can't handle 10M+ users at scale.

```
Shard by: hash(client_id) % num_shards

Benefits:
  - Horizontal scale
  - Failure isolation (1 shard down ≠ all users blocked)

Challenge: Hot shards (power users)
Solution:  Consistent hashing + virtual nodes
           client_id → hash ring → shard
```

---

## Idempotency Consideration

Rate limiter itself is idempotent by design:
- INCR is safe to retry — but only do it once per request
- Network retries: use request_id deduplication window (5s)
- If same request_id seen twice → return cached decision, don't re-increment

```
Key: dedup:{request_id}  TTL=10s
Value: {decision, timestamp}
```

---

## Scalability Numbers

| Metric | Value |
|---|---|
| Target RPS | 10,000 |
| Users | 10M |
| Avg key size | 50 bytes |
| Keys in memory | ~10M × 2 windows = 1GB RAM |
| Redis ops/request | 2 (EVAL + optional GET) |
| Redis ops/sec needed | 20,000 |
| Redis capacity | 1M ops/sec (single node) |
| Headroom | 50x — safe |

**Read-heavy optimization:** Rules are read 10K/sec but change rarely → cache in process memory with 60s TTL. Eliminates Redis rule lookups entirely.

