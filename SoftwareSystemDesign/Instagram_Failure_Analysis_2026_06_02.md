# Instagram — Failure Analysis Deep Dive
**Date:** 2026-06-02

---

## Component Failure Map

```
Client
  └── API Gateway          → if down: total outage; mitigation: multi-region, health checks
       ├── Upload Service   → if down: no new posts; media upload retryable
       ├── Feed Service     → if down: no feed reads; serve stale from CDN
       ├── User Service     → if down: no auth/profile; cached sessions still work
       └── Social Service   → if down: follow/unfollow queued; feed generation degrades

Storage
  ├── S3 (media)           → 99.999999999% durability; region failure = CDN serves stale cache
  ├── Redis (feeds/likes)  → cluster failover ~30s; cold rebuild on full loss
  ├── MySQL (posts/users)  → primary-replica; read from replica on primary failure
  ├── Cassandra (comments) → tunable consistency; quorum reads survive node failure
  └── Elasticsearch        → replica shards; degraded search on partial failure

Queue/Async
  ├── SQS (fanout)         → at-least-once; idempotent consumers required
  ├── SQS (transcoding)    → DLQ after 3 retries; raw image shown as fallback
  └── SNS (S3 events)      → retry on failure; orphan reconciler as safety net
```

---

## Failure Mode 1: Upload Service Dies Mid-Request

**Upstream failure:** Client receives timeout  
**Downstream failure:** S3 has raw media, DB has no Post record  

**Prevention:**
- Write idempotency token to DB as "PENDING" *before* issuing pre-signed URL
- On S3 upload complete event → reconciler validates PENDING records and marks COMPLETE
- TTL on PENDING records: auto-expire after 1 hour if no S3 confirmation

**Recovery:**
- Client retries with same idempotency token → same pre-signed URL returned → safe
- Nightly S3 audit: objects with no corresponding DB record → move to quarantine bucket

---

## Failure Mode 2: Feed Fanout Lag (Celebrity Post)

**The math:** Selena Gomez (400M followers) posts → 400M Redis ZADD operations  
At 100K writes/sec → 4000 seconds (>1 hour) to complete fanout

**Prevention:**
- Celebrity flag (follower_count > 1M): skip push fanout entirely
- Read-time merge: feed = precomputed_feed + latest_celebrity_posts
- Rate limit fanout workers to protect Redis: max 50K writes/sec per post

**Recovery:**
- If queue depth > 1M: shed non-priority fanout, prioritize recently active users
- Monitoring alert: P99 fanout completion time > 60s → page on-call

---

## Failure Mode 3: Redis Full / OOM

**What happens:** Redis evicts feed entries (LRU). Users get empty feeds.

**Prevention:**
- Max memory policy: `allkeys-lru` — evict least recently used feeds first
- Only keep feeds for users active in last 30 days
- Memory threshold alert at 80% → trigger cold user eviction job

**Recovery:**
- Cache miss → rebuild feed from DB (fan-in from followed users' posts)
- Rebuild throttled: max 1000 rebuilds/sec to prevent DB overload
- Pre-compute: rebuild on user login (lazy, not all-at-once)

---

## Failure Mode 4: Transcoding Worker Backlog

**What happens:** Viral event (new product launch) → 10x normal upload rate → transcoding queue depth 1M+

**Prevention:**
- Auto-scaling workers based on SQS queue depth (target: queue depth < 1000)
- Show original unprocessed media immediately; transcode async
- Per-format priority: thumbnail first (needed for feed), full resolution last

**Recovery:**
- DLQ alert: any message in DLQ → notify on-call
- Manual replay from DLQ once workers recovered
- Max message retries = 3; beyond that, post marked `transcoding_failed` → retry nightly job

---

## Failure Mode 5: Database Hot Shard

**What happens:** Users in same shard all become very active (e.g., shard holds Kylie Jenner, MrBeast) → CPU/IO spike

**Prevention:**
- Shard by `hash(user_id) % N` — even distribution, but celebrities still co-locate
- Virtual shards: map logical → physical, enables rebalancing without rehashing
- Read replicas per physical shard (2x replicas) — route reads to replicas

**Recovery:**
- Shard split: copy shard → split users across 2 new shards → update routing table
- Zero-downtime: dual-write during migration, verify checksums, cut over

---

## Idempotency Patterns Used

| Operation | Idempotency Key | Why Safe |
|-----------|----------------|----------|
| Upload post | client_generated UUID | S3 and DB checked before write |
| Like post | (user_id, post_id) UNIQUE constraint | Duplicate insert → ignore |
| Follow user | (follower_id, followee_id) UNIQUE | Duplicate insert → ignore |
| Fanout write | (post_id, follower_id) | Redis ZADD NX flag |
| Transcoding | Check output S3 key exists | Skip if already processed |

---

## Cascading Failure Prevention

**Circuit Breakers:**
- Feed Service → Redis: open if error rate > 5% in 10s window → fallback to DB pull
- Feed Service → Post DB: open if latency > 1s → serve from CDN metadata cache
- Upload Service → Transcoding queue: open if queue depth > 100K → reject uploads with 429

**Bulkheads:**
- Celebrity fanout workers: separate thread pool from normal fanout
- Story reads: separate service with separate DB connection pool from Post reads
- Video vs photo uploads: separate queues and workers (videos 10x slower to transcode)

**Graceful Degradation Ladder:**
1. Full experience (all services healthy)
2. Stale feed (Redis down → serve last cached feed)
3. Profile-only (Feed Service down → user can view profiles, no home feed)
4. Read-only mode (DB primary down → reads from replica, writes queued)
5. CDN-only (full backend down → serve last cached pages with "we'll be back" banner)

