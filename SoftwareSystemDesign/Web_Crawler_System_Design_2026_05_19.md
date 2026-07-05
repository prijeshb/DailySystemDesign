# Web Crawler — System Design
**Date:** 2026-05-19 | **Difficulty:** Hard

---

## Why Build a Web Crawler?

Before designing anything, ask: **do we actually need this?**

A web crawler is needed when you must discover and index content at internet scale without knowing URLs upfront. Use cases: search engines (Google, Bing), SEO auditors, data pipelines, archival (Wayback Machine), price trackers. Without it, you'd need every site to push content to you — impractical at scale.

**Constraint:** The web has ~50B+ pages. Crawl top 1B pages in 1 month → ~400 URLs/second sustained.

---

## Entities

| Entity | Key Attributes |
|---|---|
| **URL** | url (PK), domain, depth, priority_score, last_crawled_at, status |
| **Page** | content_hash, url, crawled_at, status_code, size_bytes, html |
| **Domain** | domain (PK), robots_txt, crawl_delay_ms, last_fetched_at |
| **Link** | source_url, target_url, anchor_text |
| **CrawlJob** | job_id, seed_urls[], scope, depth_limit, politeness |

---

## Actions

1. **Seed** — load initial URLs into frontier
2. **Schedule** — pick next URL respecting politeness per domain
3. **Fetch** — HTTP GET the page
4. **Parse** — extract links + content from HTML
5. **Deduplicate** — check if URL or content already seen
6. **Store** — persist raw HTML + metadata
7. **Enqueue** — add new unseen URLs to frontier

---

## Data Flow

```
Seed URLs
   │
   ▼
URL Frontier (priority queue, sharded by domain)
   │
   ├──► Politeness Enforcer (per-domain rate limiter)
   │
   ▼
Fetcher Workers (horizontal pool)
   │
   ├──► DNS Cache (local TTL cache → global Redis cache)
   ├──► Robots.txt Cache (per domain, recheck daily)
   │
   ▼
HTML Parser
   │
   ├──► Link Extractor ──► URL Dedup (Bloom Filter) ──► URL Frontier
   └──► Content Dedup (SHA-256 hash) ──► Object Store (S3)
                                     └──► URL DB (Cassandra)
```

---

## High-Level Design

```
┌──────────────────────────────────────────────────────────┐
│                     CONTROL PLANE                         │
│  Scheduler / Prioritizer  ◄──  Crawl Config / Seeds      │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────┐
│    URL FRONTIER SERVICE     │  ← Kafka topics (sharded by domain hash)
│  per-domain priority queues │    high-priority, normal, revisit
└──────────────┬──────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
  Fetcher Pod      Fetcher Pod   ... (100s of pods, autoscaled)
  - DNS resolve    - Robots check
  - HTTP client    - Rate limiter
       │
       ▼
  Parser Service
  - Link extraction
  - Content extraction
  - Lang detection
       │
  ┌────┴──────────────┐
  ▼                   ▼
URL Dedup          Content Store
(Redis Bloom)      (S3 + metadata in Cassandra)
  │
  ▼
Back to Frontier (new URLs)
```

---

## Low-Level Design

### URL Frontier

**Why not a simple queue?** Must respect per-domain politeness (robots.txt crawl-delay, usually 1–10s). Naive queue would hammer one domain.

```
Partition frontier by domain hash:
  domain_hash % N_partitions → Kafka partition

Each fetcher worker "owns" a set of partitions.
Per-domain token bucket in Redis:
  - key: "crawl:rate:{domain}"
  - allow 1 fetch per crawl_delay_ms
  - if bucket empty, re-queue URL with delay
```

**Priority scoring:**
```
priority = α * pagerank_estimate
         + β * (1 / depth)
         + γ * recency_score
         + δ * domain_authority
```

### URL Deduplication — Bloom Filter

**Why Bloom Filter over hash set?**
- 1B URLs × 64 bytes (SHA hash) = 64 GB hash set → too large in memory
- Bloom Filter: 1B URLs, 1% false positive → ~1.2 GB
- False positive = skip a valid URL (acceptable). False negative = impossible.

```python
bloom = BloomFilter(capacity=1_000_000_000, error_rate=0.01)

def should_crawl(url: str) -> bool:
    normalized = normalize_url(url)  # lowercase, strip fragment, canonical params
    if normalized in bloom:
        return False  # probably seen (1% chance wrong — acceptable)
    bloom.add(normalized)
    return True
```

**Distributed Bloom Filter:** Use Redis with `BF.ADD` / `BF.EXISTS` (RedisBloom module).

### Content Deduplication

Same content, different URLs (mirrors, www vs non-www):
```
content_hash = SHA256(response_body)
Check Cassandra: SELECT url FROM pages WHERE content_hash = ?
If exists → mark as duplicate, skip storage, still index URL→existing content
```

### DNS Caching

Raw DNS lookup: 50–200ms. At 400 fetches/sec across thousands of domains this is a bottleneck.
```
L1: In-process TTL cache (per fetcher, 60s TTL)  → ~microseconds
L2: Redis cluster DNS cache (5 min TTL)          → ~1ms
L3: Actual DNS resolution                         → ~100ms
```

### Robots.txt Handling

```
Cache robots.txt per domain (Redis, 24h TTL)
On fetch:
  1. Check if domain in robots cache
  2. If not → fetch /robots.txt (max 500KB, timeout 5s)
  3. Parse: disallow rules, crawl-delay, sitemap hints
  4. Store parsed result in cache
  5. Respect disallow rules — drop URL silently if blocked
```

---

## Trade-offs

| Decision | Option A | Option B | Choice + Reason |
|---|---|---|---|
| URL queue | Central Redis sorted set | Kafka per-domain partitions | **Kafka** — Redis sorted set bottlenecks at scale; Kafka gives durable, partitioned, high-throughput queuing |
| Dedup | In-memory hash set | Bloom Filter (Redis) | **Bloom Filter** — 50× memory savings; 1% false positive tolerable for crawling |
| Content store | RDBMS | Object store (S3) + Cassandra metadata | **S3 + Cassandra** — HTML blobs need cheap bulk storage; metadata needs fast lookup by URL/hash |
| Fetcher scaling | Thread-per-URL | Async I/O (aiohttp) | **Async I/O** — HTTP is I/O-bound; async handles 1000s of concurrent fetches per pod |
| Crawl order | BFS | Priority-based | **Priority** — BFS wastes budget on low-value deep pages; prioritization maximizes index quality |
| DNS | System resolver | Local + Redis cache | **Layered cache** — system resolver adds 100ms per new domain; cached DNS cuts this to <1ms |

---

## Politeness & Ethics

- **robots.txt** — mandatory. Violating it is hostile and legally risky.
- **crawl-delay** — honor it. Default: 1 req/domain/second if unspecified.
- **User-Agent** — identify your crawler (`MyBot/1.0 (+https://example.com/bot`).
- **Noindex meta tag** — honor `<meta name="robots" content="noindex">`.
- **Bandwidth** — avoid crawling during peak hours for small sites.

---

## Scaling Numbers

| Metric | Value | Reasoning |
|---|---|---|
| Target URLs/day | 35M | 400/sec × 86400 |
| Fetcher pods | ~200 | 2 req/sec/pod average (mixed latency) |
| Storage/day | ~700 GB | avg 20KB/page × 35M |
| Bloom filter size | ~1.2 GB | 1B URLs, 1% FP rate |
| DNS cache hit rate | ~95% | most pages cluster on popular domains |
| Kafka partitions | 1000 | ~35K msgs/sec; 35 msg/sec/partition |

---

*See also: Web_Crawler_Failure_Analysis_2026_05_19.md | Web_Crawler_Interview_QA_2026_05_19.md*
