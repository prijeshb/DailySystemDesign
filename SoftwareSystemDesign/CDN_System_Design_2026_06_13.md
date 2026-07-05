# CDN (Content Delivery Network) — System Design
**Date:** 2026-06-13

---

## First Principles: Do We Even Need a CDN?

**The fundamental problem:** Speed of light.

Mumbai user hitting US-East origin:
- Round-trip distance: ~14,000 km
- Speed of light in fiber (66% of c): ~200,000 km/s
- Minimum RTT: 14,000 / 200,000 × 1000 = **70ms** — and that's just propagation, no processing
- Real RTT with routing hops: **150–200ms per round trip**
- TLS handshake (2 RTTs) + HTTP request: **400–600ms** before first byte arrives

You cannot solve this with faster code. It's physics.

**When NOT to use a CDN:**
- Content is 100% dynamic and personalized (cache ratio ~0%)
- All users are in one region (within 30ms of origin)
- Traffic is tiny — CDN adds operational cost

**When YES:**
- Global users, or even multi-city same country
- Significant static content (images, JS, CSS, video)
- Origin can't absorb full traffic (protection)
- Need DDoS mitigation at the edge

**Why it works:** CDN physically moves content to within 10ms of the user. Physics solved.

---

## Entities

| Entity | Description |
|--------|-------------|
| **Origin Server** | Source of truth for all content |
| **Edge PoP** | Point of Presence — a data center edge location close to users |
| **Edge Server** | Individual server inside a PoP |
| **Origin Shield** | Intermediate cache layer between PoPs and origin (collapse of misses) |
| **Cache Object** | Stored response: `{key: URL+headers, value: content+metadata+TTL}` |
| **Customer** | Business using the CDN (uploads/configures content) |
| **End User** | Browser/app requesting content |
| **Control Plane** | CDN management system (config, purge, analytics) |

---

## Actions

1. User requests URL → routing layer directs to nearest PoP
2. PoP checks local cache → HIT: serve immediately
3. PoP cache MISS → check origin shield → HIT: serve, cache at PoP
4. Shield MISS → fetch from origin → cache at shield + PoP → serve
5. Customer purges content → invalidation propagated to all PoPs
6. Customer pushes content → pre-warmed to PoPs (push CDN)
7. TLS terminates at edge (local handshake)
8. PoP health checks → routing layer removes unhealthy PoPs
9. Edge applies transformations: compression, image resize, minification

---

## Data Flow

```
End User (Mumbai)
    │
    ├── DNS query: "cdn.example.com"
    │   → Anycast: same IP from all PoPs, BGP routes to nearest
    │     OR DNS geo: nameserver returns Mumbai PoP IP
    │
    ▼
Mumbai PoP  (cache lookup: O(1) in-memory)
    │
    ├── CACHE HIT (95%+ for static content)
    │   → Serve response in <5ms
    │
    └── CACHE MISS
            │
            ▼
        Origin Shield (Singapore)
            │
            ├── SHIELD HIT (covers ~80% of PoP misses)
            │   → Serve to PoP, PoP caches, serve to user
            │
            └── SHIELD MISS
                    │
                    ▼
                Origin (US-East)
                    → Serve to shield, shield caches
                    → Shield serves to PoP, PoP caches, serves user
```

**End result:** Origin sees only ~1% of user requests (5% miss at PoP × 20% miss at shield = 1%).

---

## High Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                     CDN CONTROL PLANE                       │
│  Config API (TTL, rules, origins)  │  Purge API (invalidate)│
│  Analytics Pipeline (logs → OLAP)  │  Customer Dashboard    │
└──────────────────────┬──────────────────────────────────────┘
                       │ config distribution (pull, ~30s)
     ┌─────────────────┼──────────────────────┐
     │                 │                      │
PoP (US-West)    PoP (EU-West)         PoP (Mumbai)   ... 200+ PoPs
  [edge servers]   [edge servers]       [edge servers]
     │                 │                      │
     └─────────────────┼──────────────────────┘
                       │ all miss → origin shield
               Origin Shield (US-East)
                       │
               Origin Server (US-East)   ← only sees ~1% of requests
```

---

## Low Level Design

### 1. Routing: Getting Users to the Nearest PoP

**Option A — Anycast (BGP-based)**
```
All PoPs announce the SAME IP prefix (e.g., 104.16.0.0/12)
BGP routes packets to the topologically nearest AS (PoP)

User in Mumbai → ISP BGP → shortest AS path → Mumbai PoP
User in London → ISP BGP → shortest AS path → Amsterdam PoP

No DNS lookup per user. Routing done by internet infrastructure.
Used by: Cloudflare, Google (1.1.1.1 from 300+ PoPs)
```

**Option B — DNS Geo-routing**
```
cdn.example.com → Authoritative NS checks client IP
  Client from 103.x.x.x (Mumbai) → return Mumbai PoP IP
  Client from 185.x.x.x (London) → return Amsterdam PoP IP
  TTL = 30s (low TTL to react to PoP failures quickly)
Used by: Akamai, AWS CloudFront
```

**Trade-off:**

| | Anycast | DNS Geo |
|--|--|--|
| Routing precision | BGP topology (very good) | IP geolocation DB (good, occasionally wrong) |
| Failover speed | ~30s BGP convergence | DNS TTL (30-60s) |
| Complexity | Requires BGP peering globally | Standard DNS infrastructure |
| Multi-CDN | Hard (IP conflicts) | Easy (CNAME chains) |

---

### 2. Cache: Keys, TTLs, and Control

**Cache Key Construction:**
```
Base: method + host + path + query_string
key = "GET:cdn.example.com:/image.jpg:v=3"

With Vary header:
  Vary: Accept-Encoding → also include encoding
  key = "GET:cdn.example.com:/image.jpg:v=3:gzip"

NEVER include: Cookie, Authorization (would make cache per-user = no caching)
Normalize: sort query params (a=1&b=2 == b=2&a=1), lowercase headers
```

**Cache-Control directives (what customers set on responses):**
```http
# Static assets with fingerprinting (best practice)
Cache-Control: public, max-age=31536000, immutable
# → Cache 1 year. Browser/CDN never revalidate. Zero requests for this file.

# HTML pages — fresh but tolerate brief stale
Cache-Control: public, max-age=60, stale-while-revalidate=30
# → Serve for 60s fresh. Next 30s: serve stale while async revalidate in background.

# API responses
Cache-Control: public, max-age=10
# → Cache 10s. Good for leaderboards, trending items, public feeds.

# Personalized — never cache at CDN
Cache-Control: private, no-store
# → CDN skips. Only browser-local cache.

# Conditional GET (bandwidth-saving)
ETag: "abc123"
# → CDN sends: If-None-Match: "abc123" → Origin: 304 Not Modified (headers only)
```

**TTL Guidelines:**
```
JS/CSS with fingerprint (app.a3b4c5.js):  1 year (immutable)
Images with version param (?v=42):         1 year
Homepage HTML:                             60s + stale-while-revalidate
Product page (public, changes hourly):     300s
API: public trending data:                 10-30s
API: user-specific:                        no-store (don't cache)
Video segments (.ts chunks):              1 year (never change once created)
```

---

### 3. Multi-Tier Cache at Edge

Each PoP runs a two-tier cache:

```
Request arrives at edge server
    │
    ├── L1 (RAM, 128GB): hot objects, <1MB each
    │   HIT → response in <1ms
    │
    └── L2 (NVMe SSD, 16TB per server): warm objects, up to 1GB each
        HIT → response in 2-5ms
        │
        └── MISS → Origin Shield fetch (50-100ms)

Within a PoP: consistent hashing across server pool
  key → hash → assigned to specific server within PoP
  → prevents duplicating same object across all servers
  → maximizes effective cache capacity
```

**Eviction policy:**
- L1: LFU (Least Frequently Used) — protects hot items from being evicted by large one-time downloads
- L2: LRU with size weighting — large objects penalized (cost = size × recency_score)

---

### 4. Origin Shield

**Why needed:**
```
Without shield:
  100 PoPs × 1000 req/s × 5% miss rate = 5,000 req/s to origin

With shield:
  100 PoPs miss → all go to shield → shield has 95% hit rate
  → only 250 req/s reach origin (20x reduction)
```

**Shield placement:** Single region, closest to origin.
- If origin is US-East → shield in US-East or same DC
- Not globally distributed (that would just be another PoP tier)

**Shield failure:** All PoPs bypass shield → hit origin directly. Origin may be overwhelmed.
- Fix: two shield regions in active-passive, failover via health check
- Fix: increase PoP TTLs temporarily (reduce miss rate) during shield outage

---

### 5. Cache Invalidation

**Method 1: URL Versioning (Best — zero propagation delay)**
```
Old: /static/app.js  (cached everywhere for 1 year)
New: /static/app.a3b4c5.js  (new fingerprint → cache miss → fetched fresh)

HTML updated to reference new URL.
Old file expires naturally. No purge needed.
Globally effective instantly.
```

**Method 2: TTL-based natural expiry**
```
Content expires after max-age seconds.
PoP revalidates on next request.
No infrastructure needed.
Stale window = up to max-age seconds.
```

**Method 3: Hard Purge (immediate, propagated)**
```
Customer → Purge API: DELETE /cache?url=https://cdn.example.com/image.jpg

Control Plane:
  1. Create purge_job (DB, status=PENDING)
  2. Publish to Kafka topic: cache.invalidations
     key = hash(url) for ordered processing

Each PoP subscribes to Kafka:
  → receives purge event
  → DELETE matching cache keys (exact or wildcard pattern)
  → ACK back to control plane

Job marked COMPLETE when all PoPs ACK (or timeout 30s)
SLA: 95% of PoPs purged within 2s, 99.9% within 10s

Customer gets: purge_id for status polling
Cloudflare claims: <150ms average purge propagation
```

**Wildcard purge:**
```
DELETE /cache?url_prefix=https://cdn.example.com/user/123/*
→ purges all cached assets for user 123 (e.g., profile image, banner)
More expensive: PoP must scan cache key index, not just one key
Rate limited: max 1,000 wildcard purges/min per customer
```

---

### 6. TLS Termination at Edge

Without CDN:
```
User (Mumbai) → TLS handshake → Origin (US-East)
  2 RTT × 150ms = 300ms just for TLS
```

With CDN:
```
User (Mumbai) → TLS handshake → Mumbai PoP (10ms away)
  2 RTT × 10ms = 20ms for TLS

Mumbai PoP → single long-lived TLS connection → Origin Shield
  Origin sees PoP's IP, not user's (for privacy/simplicity)
  Connection reused for thousands of requests
```

**TLS 1.3 0-RTT Session Resumption:**
```
First visit:   TLS 1.3 = 1 RTT handshake (down from 2 RTT in 1.2)
Returning:     Session ticket → 0-RTT → data in very first packet

Risk: 0-RTT is replay-vulnerable. Safe for: GET requests only.
CDN enforces: 0-RTT allowed only for safe methods (GET, HEAD, OPTIONS).
```

---

### 7. Request Coalescing (Thundering Herd Fix)

```
Popular URL's cache expires. 10,000 concurrent users request it.
Without coalescing: 10,000 requests to origin simultaneously → origin overwhelmed.

With coalescing:
  Request 1: cache MISS → LOCK SET coalesce:{url} NX EX 5
    → got lock → fetch from origin
  Requests 2-9,999: cache MISS → lock already held → WAIT (subscribe to completion)
    → when request 1 completes → broadcasts to all waiters → all served from cache

Origin sees: 1 request.
Result: 10,000 users served, origin protected.
```

Implemented at: edge server level (in-process queue per URL key).

---

### 8. Image Optimization at Edge

Modern CDNs run server-side transforms on the fly:

```
Request: /image.jpg?w=400&format=webp&quality=80

Edge server:
  1. Check cache for key: /image.jpg:w=400:webp:80
  2. Miss → fetch /image.jpg from origin shield
  3. Transform: decode → resize to 400px → encode as WebP @ 80%
  4. Store transformed version in L2 cache
  5. Serve

Result: user gets 400px WebP, origin stores only original.
Saves: 60-80% bandwidth vs original JPEG at full size.
```

Used by: Cloudflare Images, Akamai Image Manager, AWS CloudFront Functions.

---

### Capacity Estimates (Global CDN)

```
Assumptions:
  300 PoPs globally
  Each PoP: 100 edge servers
  Each server: 128GB RAM L1 + 16TB NVMe L2

Storage per PoP:
  L1: 100 × 128GB = 12.8TB
  L2: 100 × 16TB = 1.6PB

Cache HIT ratio target: 95%+
Traffic: handle 100M req/s globally (Cloudflare handles 45M+ as of 2024)
Bandwidth: provision 100Tbps egress globally
```

---

## Technology Choices

| Component | Choice | Why |
|-----------|--------|-----|
| Routing | Anycast (BGP) | No DNS lookup, sub-30s failover |
| Edge cache storage | RAM + NVMe | L1 hot in memory, L2 warm on disk |
| Cache key index | In-process hash map | O(1) lookup, no network call |
| Config distribution | Gossip + Kafka | Eventually consistent, low overhead |
| Purge propagation | Kafka (per-PoP consumer group) | Durable, ordered, at-least-once |
| Analytics | Kafka → ClickHouse | High ingest, fast aggregation queries |
| Shield-to-Origin | Long-lived HTTP/2 connection pool | Multiplexed, connection reuse |
| TLS | TLS 1.3 + 0-RTT for GET | Minimal handshake overhead |
| Eviction | LFU (L1) + LRU (L2) | Protect hot items from large one-offs |

---

## Real-World Reference

- **Cloudflare**: anycast from 300+ cities, claims <150ms global cache purge, ~45M req/s
- **Akamai**: DNS-based geo, 4,000+ PoPs, strong enterprise SLAs
- **AWS CloudFront**: DNS geo, tight AWS integration (S3 origins, Lambda@Edge for computation)
- **Fastly**: Varnish-based, instant purge (50ms), popular with media companies
- **Fastly Engineering Blog**: "How Fastly's CDN handles instant purge" — Kafka + consistent hashing per PoP
