# URL Shortener — System Design
**Date:** 2026-05-25 | **Difficulty:** Medium | **Real-world:** TinyURL, Bitly, t.co

---

## 0. First Principles — Do We Even Need This?

**Problem:** Long URLs are unwieldy in tweets, SMS, print, QR codes.

| Long URL | Short URL |
|---|---|
| Unreadable, breaks in emails | Clean, shareable |
| Exposes internal URL structure | Hides internals |
| No analytics | Click tracking, geo, device |
| Can't be edited after share | Can redirect to new destination |

**Decision:** Build when you need **shareable links, analytics, or URL management**. Don't build if you just need URL masking — a simple redirect rule works. The scale justification: billions of redirects/day means we need a dedicated, horizontally scalable system.

---

## 1. Entities

| Entity | Description |
|---|---|
| **URL Mapping** | short_code → long_url + metadata |
| **User** | Optional owner of a short URL |
| **Click Event** | timestamp, ip, user_agent, referrer, short_code |
| **Short Code** | 6-8 char Base62 identifier (a-z A-Z 0-9) |
| **Alias** | Custom vanity short code (e.g., `/sale2025`) |
| **Expiry** | TTL on a mapping, after which redirect returns 410 Gone |

---

## 2. Actions

```
User    → shorten(long_url, custom_alias?, ttl?)  → short_code
User    → redirect(short_code)                    → 301/302 + long_url
User    → delete(short_code)                      → 204
User    → analytics(short_code)                   → click stats
System  → expire(short_code)                      → soft delete
System  → cleanup_expired()                       → background job
```

**301 vs 302:**
| 301 Permanent | 302 Temporary |
|---|---|
| Browser caches redirect | No caching — every hit reaches server |
| Reduces server load | Enables accurate click analytics |
| Can't update destination | Can change destination anytime |

**Choose 302** if analytics matter (most production cases).

---

## 3. Data Flow

### 3.1 Shorten Flow
```
Client
  │
  ▼
API Gateway (rate limit, auth)
  │
  ▼
URL Service
  ├─ Validate long_url (reachable? malware check?)
  ├─ Idempotency check: SELECT short_code WHERE long_url = ?  ← hash index
  │     hit  → return existing short_code
  │     miss → generate new short_code
  │               ├─ Option A: hash(long_url) → Base62 → take 6 chars
  │               └─ Option B: get next ID from ID Generator → Base62 encode
  ├─ INSERT into URL_Store (short_code, long_url, user_id, created_at, ttl)
  └─ Return short_code
```

### 3.2 Redirect Flow (Hot Path — must be <10ms)
```
Client
  │  GET /{short_code}
  ▼
CDN / Edge Cache
  │  hit  → immediate 302 redirect (no origin hit)
  │  miss ↓
Load Balancer
  │
  ▼
Redirect Service (stateless, many replicas)
  │
  ├─ Read-through Cache (Redis)
  │     hit  → return long_url → 302
  │     miss ↓
  ├─ URL DB (read replica)
  │     found  → populate cache → 302
  │     not found → 404
  │     expired    → 410 Gone
  │
  └─ Async: publish ClickEvent → Kafka → Analytics Service
```

### 3.3 Analytics Flow
```
Redirect Service
  └─► Kafka topic: click-events
           └─► Analytics Consumer
                    ├─ Aggregate in memory (tumbling window)
                    └─► ClickHouse / TimescaleDB (time-series)
                              └─► Dashboard API
```

---

## 4. High-Level Design

```
                    ┌──────────────┐
                    │   CDN/Edge   │  ← cache hot short codes at edge
                    └──────┬───────┘
                           │ miss
              ┌────────────▼───────────┐
              │      Load Balancer      │
              └──┬──────────┬──────────┘
                 │          │
        ┌────────▼──┐  ┌────▼────────┐
        │ URL        │  │ Redirect    │
        │ Service    │  │ Service     │ (read-only, stateless, N replicas)
        └────┬───────┘  └──────┬──────┘
             │                 │
     ┌───────▼──────┐   ┌──────▼──────┐
     │  ID Generator │   │  Redis      │  ← read cache
     │  (Snowflake   │   │  Cluster    │
     │   or counter) │   └──────┬──────┘
     └───────────────┘          │ miss
                                │
                    ┌───────────▼──────────┐
                    │   URL Database        │
                    │  (Cassandra / MySQL   │
                    │   with sharding)      │
                    └──────────────────────┘
                                │
                    ┌───────────▼──────────┐
                    │   Kafka              │  ← click events
                    └───────────┬──────────┘
                    ┌───────────▼──────────┐
                    │  Analytics Service   │
                    │  (ClickHouse)        │
                    └──────────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Short Code Generation — Two Approaches

**Approach A: Hash-based**
```
md5(long_url) → 128-bit → take first 43 bits → Base62 → 7 chars
```
- Pro: deterministic, idempotent by nature
- Con: collisions possible; need retry with salt: `md5(url + salt)`

**Approach B: ID Generator (Preferred)**
```
Auto-increment counter (distributed) → Base62 encode
  62^6 = ~56 billion unique codes (6 chars)
  62^7 = ~3.5 trillion unique codes (7 chars)
```
Use **Snowflake ID** or a dedicated counter service (ticket server).
- Pro: no collisions, predictable length
- Con: sequential IDs slightly guessable — add random component or shuffle Base62 alphabet

### 5.2 Database Schema

```sql
-- URL Store (sharded by short_code)
CREATE TABLE url_mappings (
  short_code   CHAR(8)       PRIMARY KEY,
  long_url     TEXT          NOT NULL,
  user_id      BIGINT,
  created_at   TIMESTAMP     DEFAULT NOW(),
  expires_at   TIMESTAMP,
  is_active    BOOLEAN       DEFAULT TRUE,
  click_count  BIGINT        DEFAULT 0   -- eventually consistent counter
);

CREATE INDEX idx_long_url_hash ON url_mappings (MD5(long_url));
-- Idempotency: look up existing short code for a given long URL
```

```sql
-- Click Events (append-only, partitioned by day)
CREATE TABLE click_events (
  event_id     BIGINT,
  short_code   CHAR(8),
  clicked_at   TIMESTAMP,
  ip_hash      CHAR(64),     -- hashed for privacy
  country      CHAR(2),
  device_type  VARCHAR(20),
  referrer     TEXT
) PARTITION BY RANGE (clicked_at);
```

### 5.3 Sharding Strategy

**Shard by:** `hash(short_code) % N`

| Option | Pros | Cons |
|---|---|---|
| Hash on short_code | Even distribution | Can't range scan |
| Hash on user_id | User's URLs co-located | Hot users → hot shards |
| Consistent hashing | Easy rebalancing | More complex setup |

**Use consistent hashing** for the URL store — adds/removes nodes only move ~1/N of data.

### 5.4 Caching Strategy

```
Cache key:   short_code
Cache value: {long_url, expires_at, is_active}
TTL:         min(24h, remaining_url_ttl)
Eviction:    LRU

Cache size estimate:
  Daily active codes = 100M
  Entry size ≈ 512 bytes
  Cache top 20% = 20M entries × 512B ≈ 10 GB  ← fits in Redis
```

**Trade-off:** Caching speeds up redirects but may serve stale `long_url` if owner updates destination. Mitigate with:
- Short cache TTL (minutes) for mutable mappings
- Active cache invalidation on update (`DEL short_code` on write)

### 5.5 Idempotency

Idempotency requirement: calling `shorten(url)` twice returns the same short code.

```
Implementation:
1. Hash long_url → look up in DB index
2. If found and not expired → return existing short_code
3. If not found → generate new, INSERT with IF NOT EXISTS (CAS)
4. On conflict (race) → re-read and return winner
```

**Why it matters:** Without idempotency, same URL accumulates duplicate codes → wastes namespace, breaks user expectations.

### 5.6 Expiry / Cleanup

```
Soft delete: set is_active = false at expires_at
Hard delete: background job runs nightly
  DELETE FROM url_mappings WHERE expires_at < NOW() - 30 days

On redirect: if expires_at < NOW() → return 410 Gone (not 404)
CDN: send Cache-Control: no-store for expired/deleted codes
```

---

## 6. Scale Estimates

```
Assumptions:
  Writes (shorten):   10M/day  → ~116/sec
  Reads (redirect): 1B/day    → ~11,600/sec  (100:1 read/write ratio)
  Avg long_url size: 200 bytes
  Short code size:   8 bytes + metadata ≈ 500 bytes/row

Storage:
  10M URLs/day × 500B = 5 GB/day → ~1.8 TB/year (manageable)

Cache hit rate target: 99%+ (Zipf distribution — top 1% URLs get 80% traffic)
```

---

## 7. Key Trade-offs

| Decision | Choice | Trade-off |
|---|---|---|
| 301 vs 302 | 302 | Analytics accuracy vs. server load |
| Hash vs ID generation | ID (Snowflake) | Determinism vs. no-collision guarantee |
| SQL vs NoSQL | Cassandra or MySQL+sharding | Flexibility vs. write throughput |
| Cache TTL | Short (5 min) | Freshness vs. cache efficiency |
| Sync vs Async analytics | Async (Kafka) | Throughput vs. slight delay in stats |
| Idempotency scope | Per-user or global | Namespace efficiency vs. privacy |
