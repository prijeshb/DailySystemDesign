# System Design: URL Shortener (bit.ly / TinyURL)
**Date:** 2026-05-15  
**Difficulty:** Medium-Hard  
**Companies:** Almost every FAANG / startup interview pool

---

## 0. First Principles Thinking

> **Do we even need this?**

Before jumping in, ask *why* URL shorteners exist:
- Long URLs are ugly in tweets, SMS, print media (character limits)
- Short URLs are trackable (clicks, geolocation, device type)
- Long URLs expose internal structure / query params you may want to hide

**Why not just use query params on your own domain?**  
You could (`mysite.com/redirect?url=...`), but:
- Still long, not memorable
- Doesn't provide analytics as a service
- Doesn't decouple the destination from the link you share

So yes — a dedicated URL shortener service is justified. Now let's design it.

---

## 1. Clarifying Questions (ask the interviewer first)

| Question | Why It Matters |
|---|---|
| Read-heavy or write-heavy? | Shapes caching strategy |
| Do we need custom aliases? (`bit.ly/my-brand`) | Adds complexity to key generation |
| Do we need analytics? (click counts, geo, device) | Adds async write path |
| Should links expire? | Affects storage & cleanup jobs |
| What's the expected scale? | Determines sharding strategy |
| Do we need user accounts? | Affects authorization layer |
| Should we detect malicious URLs? | Adds a scanning pipeline |

**Assumed scale for this design:**
- 500M new URLs/day written
- 50B redirects/day read (~100:1 read:write ratio)
- Links live for 5 years by default
- Analytics required (click count, country, device)
- Custom aliases supported

---

## 2. Entities

```
User
├── user_id (UUID)
├── email
├── created_at

ShortLink
├── short_code     (e.g., "aB3xYz")  ← primary lookup key
├── original_url
├── user_id        (nullable — anonymous links)
├── created_at
├── expires_at
├── is_custom      (boolean)
├── is_active      (boolean)

ClickEvent  (analytics)
├── event_id
├── short_code
├── timestamp
├── country_code
├── device_type    (mobile/desktop/bot)
├── referrer
```

---

## 3. Actions / APIs

### Write Path
```
POST /api/v1/shorten
Body: { original_url, custom_alias?, expires_in_days? }
Auth: Bearer token (optional for anonymous)

Response: { short_url: "https://sho.rt/aB3xYz", expires_at }
```

### Read Path (the hot path — must be < 10ms)
```
GET /{short_code}
→ HTTP 301 (permanent) or 302 (temporary) redirect to original_url
```

> **Trade-off: 301 vs 302**
> - **301 (Permanent):** Browser caches the redirect → zero future load on your servers. But you lose analytics on repeat visits from the same browser.
> - **302 (Temporary):** Every request hits your servers → full analytics, but higher load.
> - **Decision:** Use 302 if analytics matter. Use 301 for CDN-centric, analytics-light deployments.

### Analytics
```
GET /api/v1/links/{short_code}/stats
Response: { total_clicks, clicks_by_country[], clicks_by_device[], clicks_over_time[] }
```

### Admin
```
DELETE /api/v1/links/{short_code}   → soft delete (is_active = false)
GET    /api/v1/links                → list user's links
```

---

## 4. Data Flow

### Write Flow
```
Client
  → API Gateway (rate limiting, auth)
    → URL Shortener Service
        1. Validate URL (malformed? malicious?)
        2. Check if URL already shortened by this user (dedup?)
        3. Generate short_code (see key generation section)
        4. Write to Primary DB (ShortLink record)
        5. Publish event to Kafka → Cache Warmer subscribes
        6. Return short URL to client
```

### Read Flow (the 99% case)
```
Client → CDN (if cached, return immediately ~5ms)
       → Load Balancer
         → Redirect Service (stateless, many replicas)
             1. Check local in-memory cache (L1, ~1ms)
             2. Check Redis (L2, ~3ms)
             3. Check DB read replica (L3, ~20ms)
             4. Return 302 redirect
             5. Async: publish ClickEvent to Kafka
```

### Analytics Flow (async, decoupled)
```
Kafka Topic: click-events
  → Analytics Consumer (Flink / Spark Streaming)
      → Aggregate by short_code, country, device
      → Write to OLAP store (ClickHouse / Druid)
  → Counter Consumer
      → Increment click counter in Redis (INCR command, atomic)
```

---

## 5. Key Generation — The Core Problem

This is what interviewers *really* want to discuss.

### Option A: Hash-Based (MD5/SHA-256 → take first 6 chars)

```
MD5(original_url) → "5d41402abc4b2a76b9719d91..."
Take first 7 chars → "5d41402"
```

**Pros:** Deterministic — same URL always maps to same code (natural dedup)  
**Cons:**
- **Collision risk:** 7 base62 chars = 62^7 = 3.5T combinations. With 500M/day writes, birthday paradox gives you collisions sooner than you think.
- On collision, retry with `MD5(url + salt)` → non-deterministic now
- Can't guarantee uniqueness without DB lookup

### Option B: Auto-Increment ID → Base62 Encode ✅ (Recommended)

```
DB sequence: 1, 2, 3... → 100,000,001
Base62 encode: "VR7x2k"
```

**Base62 alphabet:** `[0-9][A-Z][a-z]` = 62 chars  
`62^6` = ~56 billion unique codes with 6 chars — enough for decades

**Pros:** Guaranteed unique, no collision, simple  
**Cons:**
- Sequential IDs are guessable (enumeration attack — `aaaaaa`, `aaaaab`...)
- Single DB sequence = bottleneck at scale

**Fix for guessability:** Shuffle the base62 alphabet deterministically, or XOR the ID with a secret before encoding.

### Option C: Pre-Generated Key Pool (KGS — Key Generation Service)

```
KGS pre-generates millions of unique 6-char codes
Stores them in "unused_keys" table
URL Shortener Service grabs a batch (e.g., 1000 keys) on startup
Uses from local pool → replenishes when low
```

**Pros:** Key generation is O(1) at request time, no DB call needed for key  
**Cons:** KGS is a single point of failure (needs HA setup). Keys may be wasted if service crashes.

> **Trade-off Summary:**  
> For a startup → Option B (auto-increment + base62).  
> For internet-scale with enumeration concerns → Option C (KGS with HA).

---

## 6. High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Clients                           │
│           (Browser, Mobile, API consumers)              │
└────────────────────┬────────────────────────────────────┘
                     │
              ┌──────▼──────┐
              │     CDN      │  ← Caches redirect responses
              │  (CloudFront)│    (for popular links)
              └──────┬──────┘
                     │ (miss)
              ┌──────▼──────┐
              │ API Gateway  │  ← Rate limiting, Auth, SSL termination
              └──┬───────┬──┘
                 │       │
        ┌────────▼──┐  ┌─▼──────────┐
        │ URL Write │  │ Redirect   │  ← Stateless, scale horizontally
        │  Service  │  │  Service   │    Thousands of replicas
        └─────┬─────┘  └─────┬──────┘
              │               │
       ┌──────▼──────┐  ┌─────▼──────┐
       │   Primary   │  │   Redis    │  ← L2 cache: short_code → URL
       │  DB (Write) │  │  Cluster   │    TTL matches link expiry
       └──────┬──────┘  └────────────┘
              │ replication
       ┌──────▼──────┐        ┌──────────────┐
       │  Read       │        │    Kafka     │  ← Click events stream
       │  Replicas   │        └──────┬───────┘
       └─────────────┘               │
                              ┌──────▼───────┐
                              │  Analytics   │
                              │  Consumer    │
                              └──────┬───────┘
                                     │
                              ┌──────▼───────┐
                              │  ClickHouse  │  ← OLAP for analytics queries
                              └──────────────┘
```

---

## 7. Low-Level Design

### Database Schema

**ShortLinks Table** (Primary store)
```sql
CREATE TABLE short_links (
    short_code    VARCHAR(10)  PRIMARY KEY,
    original_url  TEXT         NOT NULL,
    user_id       UUID         REFERENCES users(id),
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMP,
    is_custom     BOOLEAN      DEFAULT FALSE,
    is_active     BOOLEAN      DEFAULT TRUE,
    click_count   BIGINT       DEFAULT 0   -- approximate, updated async
);

CREATE INDEX idx_user_links ON short_links(user_id, created_at DESC);
CREATE INDEX idx_expires    ON short_links(expires_at) WHERE expires_at IS NOT NULL;
```

**ClickEvents Table** (in ClickHouse, columnar)
```sql
CREATE TABLE click_events (
    short_code    String,
    clicked_at    DateTime,
    country_code  LowCardinality(String),
    device_type   LowCardinality(String),
    referrer      String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(clicked_at)
ORDER BY (short_code, clicked_at);
```

### Redis Cache Design
```
Key:   shortlink:{short_code}
Value: { "url": "https://original.com/...", "expires_at": 1789000000 }
TTL:   min(link.expires_at - now(), 24h)   ← don't cache beyond expiry

Key:   clickcount:{short_code}
Value: integer (INCR for atomic increment)
TTL:   24h, flush to DB nightly
```

### Sharding Strategy

With 500M URLs/day at 500 bytes/row ≈ 250GB/day, we need sharding after ~year 1.

**Shard by:** `short_code` (first 2 chars → 62^2 = 3844 possible shards, group into 16 logical shards)  
**Why not user_id?** The read path doesn't know user_id — it only has `short_code`. Shard key must match the primary lookup key.

```
shard_id = hash(short_code) % NUM_SHARDS
```

### URL Validation & Safety
```
On write:
1. Parse URL (is it valid format?)
2. Check against Google Safe Browsing API (async if latency-sensitive)
3. Check internal blocklist (domain blocklist in Redis Set)
4. If suspicious: flag for manual review, don't short yet

On read (extra safety):
- Check is_active flag
- Check expires_at
- Optionally show interstitial warning for flagged URLs
```

---

## 8. Caching Strategy

| Layer | Technology | Hit Rate Target | TTL |
|---|---|---|---|
| L0: Browser | HTTP Cache-Control | N/A (301 mode) | Permanent |
| L1: CDN | CloudFront / Fastly | 60-70% | 1 hour |
| L2: In-process | LRU map in service | 90%+ for hot URLs | 5 minutes |
| L3: Redis | Redis Cluster | 95%+ | 24 hours |
| L4: DB | Read Replica | Fallback | — |

**Cache invalidation:** On link deletion/deactivation, send cache invalidation message via Kafka → all redirect service instances evict the key.

> **Trade-off:** Aggressive caching means a deleted link might still redirect for up to 1 hour (CDN TTL). Acceptable for most cases; for abuse/DMCA takedowns, use CDN purge API immediately.

---

## 9. Rate Limiting

```
Anonymous users:  5 shortens/hour per IP
Free users:       100 shortens/day per account
Pro users:        10,000 shortens/day per account
Redirect:         No limit per se, but 1000 req/min per IP (DDoS protection)
```

**Implementation:** Token bucket per user_id/IP in Redis.  
`INCR rate:{user_id}:{window}` with `EXPIRE` set once on first call.

---

## 10. Back-of-Envelope Estimates

| Metric | Calculation | Result |
|---|---|---|
| Writes/sec | 500M / 86400 | ~5,800 writes/sec |
| Reads/sec | 50B / 86400 | ~580,000 reads/sec |
| Storage/year | 500M × 365 × 500B | ~91 TB |
| Redis RAM (hot URLs, 20% of links = 80% of traffic) | 100M entries × 600B | ~60 GB |
| Redirect service replicas needed | 580K req/s ÷ 10K req/s per instance | ~60 instances |

---

## 11. Key Design Decisions & Trade-offs

| Decision | Option A | Option B | Choice & Reason |
|---|---|---|---|
| Key generation | Hash-based | Auto-increment + base62 | B — no collision risk |
| Redirect type | 301 Permanent | 302 Temporary | 302 — analytics need every hit |
| Analytics write | Synchronous | Async via Kafka | Async — don't slow the hot path |
| DB type | Relational (Postgres) | NoSQL (DynamoDB) | Relational — ACID for key uniqueness, joins for user links |
| Cache eviction | LRU | LFU | LFU — URL popularity is Zipfian (few URLs get most traffic) |
| Sharding key | user_id | short_code | short_code — matches the read path's lookup key |
