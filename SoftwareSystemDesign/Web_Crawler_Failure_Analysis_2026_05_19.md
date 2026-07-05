# Web Crawler — Failure Analysis
**Date:** 2026-05-19

Failure-first thinking: every component can fail. What happens when it does?

---

## Component Map (for failure tracing)

```
Seeds → Frontier (Kafka) → Fetcher → Parser → Dedup (Redis) → Store (S3/Cassandra)
                                ↑               ↓
                            DNS Cache       URL Frontier (new URLs)
```

---

## 1. URL Frontier (Kafka) Fails

### What fails
- Kafka broker crash / partition leader election
- Disk full on broker
- Consumer lag: fetchers fall behind producers

### Impact
- Fetchers starve (no URLs to process) — crawl stops
- In-flight messages may be lost if broker crashes before ack

### Prevention
- Replication factor = 3 (survive 2 broker failures)
- `min.insync.replicas = 2` — producer must ack 2 replicas before success
- `acks=all` on producer side
- Monitor consumer lag; alert if lag > 1M messages

### Recovery
- Kafka self-heals via leader election (30–60s)
- Fetchers pause → resume automatically when partition available
- Idempotent consumers: `enable.idempotence=true` prevents duplicate processing

### What if Kafka is permanently down?
- Fallback: secondary Redis sorted set for URL frontier (limited capacity)
- Replay from Cassandra URL table: `SELECT url WHERE status='pending'`

---

## 2. Fetcher Pod Fails (mid-crawl)

### What fails
- Pod OOM-killed, crash loop, node failure
- Network timeout mid-fetch (partial response)
- TLS handshake failure

### Impact
- In-flight URLs lost (not re-queued)
- Domain may be left in "being crawled" state → blocked

### Prevention
- Kafka consumer group: on pod death, partitions rebalanced to live pods in ~10s
- Lease-based URL locking: fetcher sets `url.status = IN_PROGRESS, lease_expires_at = now+60s`
  - If lease expires without completion → URL returned to frontier
- Fetcher is **stateless** — all state in Kafka/Cassandra

### Recovery
```
Reaper job (cron every 5min):
  SELECT url WHERE status='IN_PROGRESS' AND lease_expires_at < now()
  → SET status='PENDING', enqueue back to Kafka
```

### Retry policy
```
Attempt 1: immediate
Attempt 2: +30s (exponential backoff)
Attempt 3: +5min
Attempt 4: +1hr
After 4 failures: mark url.status = FAILED, alert
```

---

## 3. DNS Resolution Fails

### What fails
- L1 cache miss + L2 cache miss → upstream DNS unreachable
- DNS server slow (>2s timeout)
- NXDOMAIN (domain deleted/expired)

### Impact
- Every new-domain URL stalls waiting for DNS
- At 400 fetches/sec, DNS bottleneck cascades into fetcher slowdown

### Prevention
- L1 in-process cache (60s TTL) absorbs repeated same-domain bursts
- L2 Redis cache (5min TTL) across all fetcher pods
- DNS prefetch: when parser extracts a new domain link, proactively resolve DNS asynchronously
- Multiple DNS resolvers (8.8.8.8, 1.1.1.1, internal) with fallback

### Recovery
- NXDOMAIN → mark domain as `INACTIVE`, skip all pending URLs for that domain
- DNS timeout → retry with fallback resolver, then exponential backoff
- Circuit breaker: if >50% DNS lookups fail in 30s window → pause fetching, alert

---

## 4. Redis (Bloom Filter / DNS Cache) Fails

### What fails
- Redis primary crash, all replicas behind
- Redis memory pressure evicts bloom filter keys
- Network partition between fetcher pods and Redis

### Impact
- **Bloom filter down** → dedup fails → fetchers re-crawl already-seen URLs
  - Wastes bandwidth but **not data-corrupting** (idempotent writes to S3/Cassandra)
- **DNS cache down** → L1 cache still works; only new domains degrade

### Prevention
- Redis Cluster with 3 shards × 2 replicas (M+S per shard)
- Bloom filter persisted to RDB snapshot every 5min
- Separate Redis instances for DNS cache vs Bloom filter (different failure blast radius)

### Recovery
- Bloom filter rebuild: scan Cassandra `url` table, re-populate bloom filter
  - 1B records at 100K/sec → ~3 hours. During rebuild: allow duplicate crawls (idempotent)
- Use **shadow write**: also write URLs to Cassandra `url_seen` table as permanent record
  - Bloom filter is optimization layer, not source of truth

---

## 5. Parser Service Fails

### What fails
- Malformed HTML causes parser crash
- Memory spike on huge page (10MB+ HTML)
- CPU spike from malicious deeply-nested HTML (ReDoS)

### Impact
- Fetched content sits unprocessed
- Link extraction stops → frontier starves over time

### Prevention
- Parse in sandboxed subprocess with memory limit (e.g., 256MB per parse job)
- Hard timeout per parse: 5 seconds
- Input validation: cap HTML size at 5MB before parsing
- Regex with timeout guards (no catastrophic backtracking)

### Recovery
- Parser crashes → Kafka message not acked → redelivered to another parser pod
- If same URL fails 3x parsing → store raw HTML, mark `parse_status=FAILED`, skip link extraction
- Dead letter queue for persistently failing pages

---

## 6. Object Store (S3) Fails / Slow

### What fails
- S3 region outage
- Throttling (S3 rate limits per prefix)
- Eventual consistency: PUT succeeded but GET returns 404 for ~100ms

### Impact
- Parsed content lost if write fails
- Metadata in Cassandra may point to non-existent S3 object

### Prevention
- S3 key structure to avoid hot partitions: `{date}/{content_hash[:2]}/{content_hash}.html`
  - Hash prefix distributes across S3 partitions
- Write with retry (3x, exponential backoff)
- Store `s3_key` in Cassandra only after successful S3 write (write-through ordering)

### Recovery
- Cross-region replication (S3 CRR) → failover to secondary region
- If S3 fully down → buffer raw HTML in Kafka (temporary), replay writes on recovery

---

## 7. Cassandra (URL Metadata DB) Fails

### What fails
- Node crash (Cassandra is designed for this — gossip protocol)
- Quorum not met (too many nodes down)
- Write timeout under heavy load

### Impact
- URL status updates lost → duplicate crawling possible
- Content hash index unavailable → content dedup fails

### Prevention
- Cassandra replication factor = 3, `QUORUM` consistency for writes
- Separate keyspaces for URL metadata vs content hashes (different access patterns)
- Lightweight transactions (`IF NOT EXISTS`) for URL insertion → idempotent

### Recovery
- Cassandra auto-repairs via hinted handoff and anti-entropy repair
- Missed URL status updates → reaper job catches stale `IN_PROGRESS` URLs (see §2)

---

## 8. Cascading Failure: Slow Target Sites

### Scenario
Target site responds slowly (5s per request). 100 fetcher threads all blocked on that domain.
Fetchers can't process other domains.

### Prevention (Bulkhead Pattern)
```
Per-domain thread/connection limit: max 2 concurrent connections to any domain
If domain >2 in-flight → delay URL scheduling for that domain
```

### Impact
- Slow domain only consumes 2 connections → other domains unaffected
- Slow domain falls behind in crawl schedule (acceptable)

---

## 9. Poison URL (Infinite Loop / Trap)

### Scenario
A site generates infinite URLs: `/page?offset=0`, `/page?offset=1`, ... `/page?offset=999999999`

### Prevention
- URL normalization: canonicalize query params, remove tracking params
- Depth limit: max depth from seed (e.g., 10 hops)
- Per-domain URL count limit: max 1M URLs per domain
- Detect path patterns: if >10K URLs match `domain/path?param=N` → mark as trap, skip

---

## Summary: Failure → Response Matrix

| Component | Failure Mode | Response |
|---|---|---|
| Kafka frontier | Broker crash | Replica failover, reaper re-queues in-flight |
| Fetcher pod | Crash/OOM | Lease expiry → URL re-queued; partition rebalance |
| DNS | Timeout/down | Layered cache absorbs; circuit breaker pauses |
| Redis Bloom | Crash | Idempotent re-crawl tolerated; rebuild from Cassandra |
| Parser | Crash/malformed | DLQ after 3 retries; sandbox protects other jobs |
| S3 | Outage | Kafka buffer; CRR failover; retry with backoff |
| Cassandra | Node loss | RF=3, quorum writes, reaper for stuck URLs |
| Target site | Slow | Bulkhead (max 2 conns/domain) |
| Trap URLs | Infinite URLs | Depth limit, per-domain cap, pattern detection |
