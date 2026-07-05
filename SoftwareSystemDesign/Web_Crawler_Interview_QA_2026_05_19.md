# Web Crawler — Interview Q&A
**Date:** 2026-05-19

---

## Opening Questions (Scoping)

**Q: Design a web crawler.**
> Start here — always clarify before diving in.

Good clarifying questions:
- "What's the scale? Crawl the entire web, or a subset?"
- "Is this for a search engine index, or a specific use case like price tracking?"
- "Should it handle JavaScript-rendered content (SPAs)?"
- "What freshness requirements? Recrawl frequency?"
- "Any politeness requirements (robots.txt, rate limits)?"

Model answer opening:
> "I'll design a distributed crawler targeting 1B pages/month (~400 URLs/sec). It'll respect robots.txt, deduplicate URLs and content, prioritize high-value pages, and store raw HTML + metadata for downstream indexing. I'll focus on the core crawl pipeline before discussing scaling."

---

## Architecture Questions

**Q: How do you prioritize which URLs to crawl first?**

> Don't say "just BFS." Show you understand quality vs breadth.

- Priority score = PageRank estimate + link depth + domain authority + recency
- High-priority: news sites, high-domain-authority, frequently updated pages
- Low-priority: deep pagination, low-authority domains, rarely updated
- Use a priority queue (Kafka with weighted message priorities or Redis sorted set)

**Follow-up: How do you estimate PageRank before you've crawled the page?**
> Use domain-level authority (Moz DA, Ahrefs DR as proxies), in-link count from already-crawled pages, and historical crawl data. It's iterative — rank improves as more pages are discovered.

---

**Q: How do you avoid re-crawling the same URL?**

Two-level answer:
1. **URL dedup**: Bloom filter in Redis (`BF.EXISTS url`) before enqueuing
2. **Content dedup**: SHA-256 of response body → check Cassandra before storing

**Follow-up: What if the Bloom filter has a false positive?**
> A valid URL is skipped — we think we've seen it but haven't. For a crawler this is acceptable: we might miss 1% of new URLs but save enormous memory. The alternative (hash set) would require 50× more memory for the same URL set. In practice, we also keep a Cassandra `urls_seen` table as ground truth; the Bloom filter is just the fast-path cache.

---

**Q: How do you handle politeness / not overwhelming target servers?**

- Parse and cache `robots.txt` per domain (24h TTL)
- Respect `Crawl-delay` directive (default 1 req/sec/domain if unspecified)
- Per-domain token bucket rate limiter in Redis
- Maximum 2 concurrent connections per domain (bulkhead)
- Identify bot via User-Agent header

**Follow-up: What if the site has no robots.txt?**
> Default to conservative behavior: 1 request/second, standard User-Agent. No crawl-delay specified = we choose a reasonable default, not "crawl as fast as possible."

---

**Q: How do you handle JavaScript-heavy sites (SPAs)?**

- Basic HTML fetcher misses content rendered by JS
- Solution: headless browser pool (Playwright/Puppeteer)
- Cost: 10–100× slower than raw HTTP; resource-heavy
- Approach: detect SPAs (no content in raw HTML but `<script>` heavy, `<div id="root"/>`)
  → route to headless browser tier
- Two-tier architecture: fast HTTP fetcher (95% of pages) + headless tier (5% JS sites)

---

## Deep-Dive Questions

**Q: Walk me through what happens when a fetcher crashes mid-crawl.**

> This tests your failure-first thinking.

1. Fetcher acquires a URL from Kafka, sets `url.status = IN_PROGRESS` + `lease_expires_at = now+60s` in Cassandra
2. Fetcher crashes after step 1 but before completing
3. Kafka: since message wasn't ACKed, it's redelivered to another consumer in the group (within ~10s)
4. New fetcher attempts to process URL — checks if lease expired; if not, skips (another pod may be doing it)
5. Reaper cron job runs every 5min: finds URLs where `status=IN_PROGRESS AND lease_expires_at < now()` → resets to `PENDING`, re-enqueues
6. After 4 failures → `status=FAILED`, alert, move to DLQ

**Follow-up: What if the reaper itself fails?**
> Reaper is a cron job — if it misses a run, stuck URLs accumulate but no data is lost. Next reaper run cleans them. Reaper is idempotent (updating IN_PROGRESS to PENDING is safe to repeat). Run reaper on multiple pods with distributed lock (Redis `SET NX`) so only one runs at a time.

---

**Q: Your crawler is hitting one domain with 10,000 pending URLs but the domain is slow. How do you prevent it from starving other domains?**

> Bulkhead pattern.

- Max 2 concurrent connections per domain (enforced by per-domain semaphore in Redis)
- If domain has 10K pending URLs but semaphore full → those URLs wait in a per-domain delayed queue
- Fetcher workers pick from *multiple* domains round-robin, not just next URL in queue
- This way, slow domains don't monopolize fetcher threads

---

**Q: How does your URL normalization prevent duplicate crawls of the same logical page?**

Normalization steps:
1. Lowercase scheme and host: `HTTP://Example.COM` → `http://example.com`
2. Remove default ports: `:80` for http, `:443` for https
3. Decode percent-encoding where safe: `%2F` stays `/`, but `%20` → space then re-encode
4. Remove fragment: `#section` doesn't change server-side content
5. Canonical query params: sort params alphabetically, remove tracking params (`utm_*`, `fbclid`, etc.)
6. Remove trailing slashes (or canonicalize consistently)

**Follow-up: What if two URLs return the same content despite different paths?**
> Content-level dedup handles this: SHA-256 hash of response body. If hash exists in DB, mark new URL as `duplicate`, don't re-store HTML, but do record URL → content_hash mapping for index purposes.

---

**Q: How would you scale the crawler to 10× the current load?**

Current: 400 URLs/sec → Scale to 4,000 URLs/sec

- **Fetchers**: horizontal scale. Add pods. Kafka auto-rebalances partitions.
- **Kafka**: add brokers + partitions (1000 → 5000 partitions)
- **Redis Bloom Filter**: Redis Cluster already horizontally scalable; increase shard count
- **DNS**: bloom filter likely the bottleneck — evaluate sharding by domain-hash
- **Cassandra**: add nodes (linear scale); re-balance token ring
- **S3**: already massively scalable; only concern is prefix hotspots → ensure key uses hash prefix
- **Parser**: stateless, horizontal scale

Bottleneck to identify first: measure latency per stage. Usually DNS or fetcher I/O is the first constraint.

---

**Q: How do you handle a "spider trap" — a site that generates infinite unique URLs?**

Detection:
- Depth limit from seed (max 10 hops)
- Per-domain URL count ceiling (max 1M URLs/domain)
- Pattern detection: if >10K URLs match `domain/path?id=[0-9]+` → flag as potential trap, throttle aggressively
- URL similarity hash: SimHash of URL structure; if too many URLs are near-identical in structure, deprioritize

Response:
- Add domain to "limited crawl" list → drop queue after N URLs
- Alert human reviewer for manual inspection

---

## Behavioral / Design Philosophy

**Q: Would you use a database or a message queue for the URL frontier?**

> Show you understand trade-offs, not just facts.

Both have been used in production:
- **Database** (Redis sorted set or SQL): simpler, easy to inspect, supports complex priority queries. Bottleneck at scale — single write path.
- **Message queue** (Kafka): higher throughput, durable, partitionable by domain, built-in consumer groups for scaling. Less flexible for dynamic re-prioritization.

My choice: **Kafka** as primary frontier for throughput + durability, with **Redis sorted set** as overflow/priority buffer for re-queuing and priority updates. Real systems (Google, CommonCrawl) use Kafka-like distributed log for frontier.

---

**Q: How does CommonCrawl or Google actually do this?**

Real-world references:
- **CommonCrawl**: open dataset; uses Apache Nutch (Hadoop-based crawler), stores to S3. Publicly documented at commoncrawl.org/the-data/get-started/
- **Google**: Published "Large-Scale Incremental Processing Using Distributed Transactions" (Percolator paper) — triggers re-crawl when content changes detected
- **Mercator (AltaVista)**: Classic academic paper — introduced partitioned URL frontier, politeness queues. Still the conceptual basis for most crawlers.
- **Focused Crawling** (Chakrabarti et al.): Prioritize topically relevant pages — good reference for priority scoring

---

## Quick-Fire

| Q | A |
|---|---|
| Why Bloom filter over hash set? | 50× memory savings; 1% false positive tolerable |
| What consistency level for Cassandra? | QUORUM writes, LOCAL_QUORUM reads |
| How often recrawl a page? | Based on change frequency; news = hours, docs = weeks, static = months |
| Max HTML size to parse? | Cap at 5MB; most pages <100KB |
| How handle redirects? | Follow up to 5 hops; store final URL; record redirect chain |
| What if site returns 429? | Back off exponentially; respect Retry-After header |
| Difference: focused vs broad crawler? | Focused: only crawl topically relevant pages (better for vertical search). Broad: everything |
| One metric to monitor crawler health? | Pages crawled per second per fetcher pod (throughput proxy) |
