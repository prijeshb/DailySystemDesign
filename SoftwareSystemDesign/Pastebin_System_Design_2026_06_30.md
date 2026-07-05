# Pastebin — System Design
*Date: 2026-06-30*

---

## First Principles — Do We Even Need This?

**Problem:** Developers need to share code snippets, logs, and config with others — without emailing files, without spinning up a repo, without copying walls of text into chat.

**Why not just use a file upload service?** File uploads require download, can't be read inline, no syntax highlighting, no anonymous sharing, no TTL.

**Why not just use GitHub Gist?** Valid alternative. Pastebin's differentiator: no account required, immediate sharing, simpler UX for one-off shares, and a custom-domain share link.

**Core insight:** The system is fundamentally a key-value store — short random key → text content — with optional TTL, optional auth, and read-heavy access patterns.

---

## Entities

```
Paste
  id          VARCHAR(8)   PK   -- base62 short code e.g. "aB3xK9mZ"
  user_id     BIGINT       FK   -- NULL for anonymous
  title       VARCHAR(255)      -- optional
  language    VARCHAR(32)       -- "python", "javascript", "plain"
  content_ref VARCHAR(512)      -- S3 key if >10KB, else NULL
  content     TEXT              -- inline if ≤10KB
  visibility  ENUM('public','private','unlisted')
  expires_at  TIMESTAMP         -- NULL = never
  view_count  BIGINT DEFAULT 0
  created_at  TIMESTAMP

User
  id          BIGINT       PK
  email       VARCHAR(255)
  created_at  TIMESTAMP
```

---

## Actions & Data Flow

```
CREATE  POST /pastes       → generate ID → store → return short URL
READ    GET  /p/{id}       → check cache → check DB → return content
DELETE  DELETE /p/{id}     → auth check → mark deleted → purge cache
LIST    GET  /user/pastes  → DB query by user_id
```

**Create flow:**
```
Client → API → ID Generator → Redis: SET id NX EX ttl → Postgres INSERT
      ← short URL ← API
```

**Read flow (hot path):**
```
Client → CDN (cache-control: max-age=300) → hit? return
       → API Server → Redis GET {id} → hit? return
       → Postgres → populate Redis → return
```

**Expiry flow:**
```
expires_at column in Postgres → Background job (every 5min):
  DELETE FROM pastes WHERE expires_at < now() LIMIT 1000
Also: Redis key TTL auto-evicts cached copy
```

---

## Scale Estimates

| Metric | Assumption | Value |
|--------|-----------|-------|
| Users | Registered | 10M |
| DAU | 5% | 500K/day |
| Creates | 5 pastes/DAU | 2.5M/day → 29 writes/sec |
| Reads | 10:1 read:write | 25M/day → 290 reads/sec |
| Avg paste size | code snippets | 5KB |
| Storage growth | 2.5M × 5KB/day | 12.5GB/day |
| Retention | 6 months hot | ~2.2TB |

**ID space:**
```
Base62 (A-Za-z0-9) with 8 chars: 62^8 = 218 trillion unique IDs
At 2.5M creates/day: 218T / 2.5M = 240,000 years to exhaust
→ 6-char base62 (56B IDs) is sufficient for any realistic scale
```

---

## Back-of-Envelope AWS Cost

### Scenario A: Startup ($200/month budget)

| Service | Config | Cost/month |
|---------|--------|-----------|
| EC2 t3.medium (API) ×2 | 2 vCPU / 4GB | $62 |
| RDS Postgres db.t3.medium | 2 vCPU / 4GB, 100GB | $57 |
| ElastiCache cache.t3.micro | 0.5GB Redis | $14 |
| S3 (2.2TB + 25M reads) | $0.023/GB + GETs | $52 |
| CloudFront (1TB egress) | $0.085/GB | $85 |
| **Total** | | **~$270/month** |

**Budget constraint ($200/month) — limitations:**
- Drop CloudFront → all reads hit API servers → max ~500 reads/sec without CDN
- Single RDS (no Multi-AZ) → DB failure = 5-10 min downtime
- Single Redis → cache miss storm if Redis restarts
- No autoscaling → viral paste can take down the service
- No read replicas → all 290 reads/sec hit primary DB

### Scenario B: Production ($3,000/month)

| Service | Config | Cost/month |
|---------|--------|-----------|
| ECS Fargate (API) 8 tasks | 1vCPU/2GB each | $120 |
| RDS Postgres Multi-AZ | db.r6g.large | $340 |
| ElastiCache cluster | 3-node r6g.large | $420 |
| S3 + Intelligent Tiering | 2.2TB + 250M GETs | $85 |
| CloudFront | 10TB egress | $850 |
| ALB + WAF | | $200 |
| **Total** | | **~$2,015/month** |

Handles: 5K reads/sec, 300 writes/sec, 99.9% SLA.

---

## High-Level Design

```
              ┌──────────────────────────────────────────────┐
              │              CLIENTS (Web / API / CLI)        │
              └──────────────────┬───────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │    CloudFront CDN        │
                    │  (cache GET /p/{id})     │
                    └────────────┬────────────┘
                                 │ cache miss
                    ┌────────────▼────────────┐
                    │      API Gateway         │
                    │  + WAF + Rate Limiter    │
                    └────────────┬────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │              Paste Service            │
              │  (Node.js / Go, stateless, 4-8 pods) │
              └────┬────────┬───────────┬────────────┘
                   │        │           │
          ┌────────▼──┐ ┌───▼───┐ ┌────▼──────┐
          │  Postgres  │ │ Redis │ │    S3     │
          │ (metadata) │ │(cache)│ │(large txt)│
          └────────────┘ └───────┘ └───────────┘
                   │
          ┌────────▼──────────┐
          │   Kafka + Flink   │
          │ (view analytics)  │
          └───────────────────┘
```

---

## Low-Level Design

### ID Generation

**Option 1: Pre-generated key pool (chosen for simplicity)**
```
Key Generator Service:
  1. Pre-generates 1M base62 keys (6-char) using random(62^6)
  2. Stores in: keys_available(key VARCHAR PK)
  3. On paste create: SELECT key FROM keys_available LIMIT 1 FOR UPDATE SKIP LOCKED
                      DELETE FROM keys_available WHERE key = $1
                      (moves to keys_used table on paste insert)

Pros: No collision, no coordination needed per request, keys pre-validated unique
Cons: Extra service, pre-generation job must run periodically
```

**Option 2: Snowflake ID → base62 encode**
```
Snowflake 64-bit → base62 → ~10-11 chars (longer than Option 1)
Shorter: take lower 40 bits → base62 → 7 chars
Risk: birthday collision if we truncate bits (very low but non-zero)
```

**Use Option 1** for clarity. Pre-generate weekly batch of 10M keys.

### Content Storage Decision

```
If content_size ≤ 10KB → store inline in Postgres TEXT column
  Pros: single read, no S3 roundtrip
  Cons: row size bloat on large pastes

If content_size > 10KB → store in S3, save key in content_ref
  S3 key: pastes/{year}/{month}/{paste_id}.txt
  Pros: Postgres stays lean for metadata queries
  Cons: extra S3 GET on read path (mitigated by Redis caching full content)
```

**90th percentile paste: <5KB → most pastes in Postgres inline.**

### Redis Caching

```
Key: paste:{id}
Value: JSON { title, language, content, visibility, expires_at }
TTL: min(paste.expires_at - now(), 24h)

On CREATE: SET paste:{id} {json} EX {ttl}
On READ:   GET paste:{id} → hit = return, miss = DB + SET
On DELETE: DEL paste:{id}
```

**Cache stampede prevention (viral paste):**
```
Cache miss → SET rebuild_lock:{id} NX EX 3 → only 1 request rebuilds
            → Others: BLPOP result_queue:{id} TIMEOUT 2
Rebuilder: Postgres → SET paste:{id} → LPUSH result_queue:{id}
```

### Expiry

```
Active expiry (Redis TTL): cache entry auto-deleted, returns 404 → DB fallback check
Passive DB cleanup (cron every 5min):
  DELETE FROM pastes WHERE expires_at < now() LIMIT 5000
  Also: DELETE presigned URLs from S3 or trigger S3 lifecycle rule

S3 lifecycle rule:
  prefix: pastes/ → transition to Glacier after 30d → delete after 180d
```

### Anonymous vs Authenticated Pastes

```
Anonymous:
  No user_id, visibility='public' or 'unlisted' only
  Rate limit: 10 creates/hour per IP (Redis INCR + EX 3600)
  No delete capability (paste expires via TTL only)

Authenticated:
  50 creates/hour, max 500 active pastes
  Can delete own pastes
  Can set visibility='private' (only owner can read)
```

### Syntax Highlighting

```
Server-side (at create time):
  language field → Pygments/highlight.js → pre-generate HTML → store highlighted_html
  Pros: CDN can cache rendered HTML, no client JS needed
  Cons: doubles storage per paste

Client-side (recommended):
  Return raw content + language field
  Client-side highlight.js renders highlighting in browser
  Pros: lean storage, CDN caches raw content efficiently
  Cons: requires JS (non-issue for modern clients)
```

---

## Trade-offs

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Pre-generated keys vs Snowflake | Pre-generated pool | No collision + deterministic length. Cost: key gen service complexity vs simpler Snowflake. |
| Inline content vs S3 always | Inline if ≤10KB | Faster read for small pastes (no S3). Cost: Postgres rows larger. S3 for large keeps DB lean. |
| CDN caching raw content | Yes, max-age=300 | 5-min stale reads acceptable. Cost: delete doesn't propagate instantly to CDN. |
| Client-side syntax highlight | Yes | Zero storage overhead, no server compute. Cost: JS dependency. |
| Async analytics (Kafka) | Yes | Write path stays fast. Cost: view counts delayed 1-2s. |
| Redis cache with TTL | Yes | Reduces DB load 10:1. Cost: stale reads possible during TTL window if paste deleted early. |

---

## Real-World Reference

**GitHub Gist architecture blog (2013):** Gist stores snippets in Git repos — each gist IS a Git repo. Overkill for read-only snippets but enables versioning. Pastebin's simpler model is correct for most use cases.

**Hastebin (open-source pastebin):** Stores all content in Redis → Redis RDB for persistence. Works at small scale. At 2TB+ data: Redis becomes expensive ($0.20+/GB vs S3 $0.023/GB) → hybrid model wins.

**SourceHut paste service:** Stores content as flat files on disk. Simple, but horizontal scaling requires distributed FS (NFS/EFS) — adds latency and cost.

**Key lesson from production:** The ID collision problem is often overengineered. At 2.5M creates/day with 62^6 = 56B IDs, even if you use random-only generation, birthday collision probability after 1 year is `1 - e^(-n²/2N) ≈ n²/2N = (900M)² / (2 × 56B) ≈ 0.7%`. Check-and-retry on collision is sufficient — no need for pre-generation at this scale. Pre-generation is an interview answer that shows you understand operational concerns.
