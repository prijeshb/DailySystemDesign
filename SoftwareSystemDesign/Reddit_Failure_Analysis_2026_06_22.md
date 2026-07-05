# Reddit — Failure Analysis
**Date:** 2026-06-22

---

## Failure Map

```
Client → [CDN] → [API Gateway] → [Feed/Post/Vote Service] → [Redis] → [Postgres]
                                                           → [Kafka] → [Consumers]
                                                                     → [Elasticsearch]
```

---

## Failure 1: Redis Crashes (Vote Buffer Lost)

**What fails:** Vote Service writes `INCR vote_buffer:post:{id}` to Redis. Redis OOM/crash before flush worker runs.

**Impact:** Up to 30 seconds of votes (potentially millions on viral posts) never reach Postgres. Vote counts permanently off.

**Chain:**
```
Redis OOM (AOF disabled)
  → vote buffer evicted
  → flush worker reads empty keys → writes 0 delta
  → Postgres counts frozen at pre-crash value
  → Hot scores stale → feed ranking degrades
```

**Prevention:**
1. Redis AOF with `appendfsync everysec` — max 1 second of votes lost vs. flush interval
2. Vote flush worker reads from Redis AND checks Postgres last flush timestamp; if gap > 60s without data, alerts
3. Vote events also written to Kafka (dual-write): `vote_buffer:post:{id}` in Redis AND `vote_events` Kafka topic. On Redis crash, replay Kafka topic to rebuild buffer.

**Recovery:**
```
Redis restart → restore from AOF
If AOF missing: replay Kafka vote_events topic from last flush timestamp
  → re-INCR counters → flush worker resumes normally
Max data loss: 1 second (AOF) or 0 (Kafka replay)
```

---

## Failure 2: Postgres Primary Fails (Post/Comment Writes Down)

**What fails:** DB primary crashes. Can't write posts, comments, or flush vote counts.

**Impact timeline:**
```
0s:   Primary fails. In-flight writes return error.
0-30s: App retries → connection pool exhausts → 500s to users
30s:  RDS Multi-AZ automatic failover starts
60-90s: New primary elected. DNS updated.
90-120s: Connection pools reconnect. Writes resume.
```

**During failover:**
- Read replicas still serve feed reads (tolerable — small lag)
- New posts fail with 503 → client retries with idempotency key
- Vote buffer continues accumulating in Redis (flush will succeed after failover)
- Comments fail → client shows "comment failed, retry"

**Prevention:**
- RDS Multi-AZ enabled (automatic failover in <2 min)
- Idempotency keys on all post/comment creation → safe to retry
- Circuit breaker in Post Service: after 5 write failures → open circuit → queue writes locally (max 500, 2-min buffer) → replay on reconnect
- Vote flush worker: detects DB failure → holds flushes → retries with exponential backoff

**Read replica handling:**
```
Reads during primary failure:
  Feed reads → replica (stale by up to replica lag, acceptable)
  Post detail → replica (may miss last few comments — OK)
  Vote display → Redis cache (may be stale — OK, fuzzed anyway)
  
After failover, promote replica to primary:
  replica lag at failover time = max data loss (usually <1s for RDS)
```

---

## Failure 3: Vote Flush Worker Dies

**What fails:** Background worker that reads Redis vote buffers and writes to Postgres stops.

**Impact:** Vote counts in Postgres never update. Feed scores freeze. Hot feed becomes "post creation order" effectively.

**Detection:** Worker publishes heartbeat to Redis `SET vote_worker:heartbeat now() EX 60`. Watchdog checks every 30s.

**Cascade risk:**
```
Worker dies → Redis vote buffers grow unboundedly
30 min later → Redis memory fills → eviction → vote data lost permanently
```

**Prevention:**
- Multiple worker instances (leader election via Redis SETNX `vote_worker:lock NX EX 90`)
- Redis `maxmemory-policy = noeviction` for vote buffer keys (Redis returns error on OOM instead of silently evicting)
- OOM alert at 70% memory → manual intervention before eviction risk
- Vote flush also triggered by Kafka consumer (backup path): every minute, consume `vote_events` topic and ensure Postgres is updated

**Recovery:**
```
Restart worker → it reads all vote_buffer:* keys → batch flushes to Postgres
Backlog clears in seconds (Redis pipeline batch writes, not one at a time)
```

---

## Failure 4: Elasticsearch Fails (Search Down)

**What fails:** Search cluster OOM or network partition. Search requests 500.

**Impact:** Search broken. No effect on feed, voting, or post creation.

**Degraded mode:**
```python
try:
    results = elasticsearch.search(query)
except ElasticsearchException:
    # Fallback to Postgres full-text search
    results = postgres.query(
        "SELECT id, title FROM posts WHERE to_tsvector(title) @@ plainto_tsquery($1) LIMIT 20",
        query
    )
    # Mark results as degraded in response header: X-Search-Degraded: true
```

Postgres FTS is slower (300ms vs 50ms) and less relevant (no score boost) but functional.

**Circuit breaker:** After 10 ES failures → open → all search routes to Postgres fallback → reduce ES call volume while it recovers.

**Kafka indexing lag:** If ES was down, posts created during outage aren't indexed. On ES recovery, Kafka consumer replays from last committed offset → all missed posts reindexed automatically.

---

## Failure 5: CDN Fails / Cache Poisoning

**What fails:** Fastly/CloudFront edge nodes unreachable or serving wrong content.

**Impact on media:** Images/videos fail to load. Text posts still work (served via API).

**Impact on static assets:** JS/CSS fails → app broken for users without cache.

**Origin shield:**
```
Client → CDN edge (Miss) → Origin Shield (regional mid-tier cache) → S3
If CDN edge down: client can talk directly to Origin Shield as fallback
If Origin Shield down: client talks to S3 directly (slower, expensive)
```

**Cache poisoning prevention:**
- Media URLs include content hash: `cdn.example.com/media/{sha256_hash}.jpg`
- Hash in URL = immutable content → wrong cache entry has wrong URL → naturally self-invalidates
- Post URLs are parameterized with `?v=` version to bust on edit

**CDN failover:**
```
DNS health check on CDN edge → if down → route to backup CDN provider (multi-CDN)
Failover time: 60s (DNS TTL)
During 60s: some users get failed media → show placeholder
```

---

## Failure 6: Kafka Consumer Falls Behind (Notification Lag)

**What fails:** Notification consumer or feed fan-out consumer can't keep up with vote/post events.

**Impact:** Online users don't see new posts in feed for minutes. Notifications delayed.

**Detection:** Monitor consumer lag per topic partition: `kafka.consumer_lag > 100K` → alert.

**Cascade:**
```
Kafka lag grows → event processing delayed → feed fan-out delayed
→ users see stale hot feeds → more DB queries (pull path hits DB instead of cache)
→ DB load spikes
```

**Prevention:**
- Fan-out consumer is not blocking: on lag detected, skip fan-out for cold subreddits (< 1K online subscribers). Only fan-out to large/active subreddits.
- Consumer parallelism: partition key = subreddit_id → N consumers, one per partition, scale horizontally
- Backpressure: if consumer lag > 1M events → pause new event production (rate limit post creation temporarily)

**Recovery:**
```
Scale out consumer group → more consumers → faster processing
If events are time-sensitive (notifications): skip events older than 10 min
  consumer.seek(partition, offset_at_now - 10min) → only process recent events
Older events dropped for notifications (no point notifying 20min later)
Older events NOT dropped for Postgres updates (feed scores still need updating)
```

---

## Failure 7: Hot Subreddit Thundering Herd

**What fails:** r/worldnews posts a major story. 500K users hit feed simultaneously. Redis sorted set hot feed key expires at same moment.

**Impact:**
```
500K simultaneous requests → cache miss → 500K Postgres queries
→ DB CPU: 100% → query queue depth grows → timeout → 500s
→ retry storm → amplifies
```

**Prevention:**
```python
# On cache miss: only one worker rebuilds
rebuild_lock = redis.SET(f"rebuild:{key}", 1, nx=True, ex=5)
if rebuild_lock:
    # This request rebuilds
    data = postgres.query("SELECT ...")
    redis.zadd(f"subreddit:{id}:hot", data, ex=300)
else:
    # Others wait then re-check
    time.sleep(0.1)
    data = redis.zrange(...)  # retry cache
    if not data:
        data = postgres.query(...)  # last resort
```

- Stagger TTLs: `TTL = 300 + random.randint(0, 60)` — no synchronized expiry
- Warm cache proactively: Score Updater pre-writes feed on every hot_score change, not relying on TTL expiry
- Shadow traffic: route 1% of feed traffic to origin at all times → cache always warm

---

## Summary: Failure First Checklist

| Component | Single Point? | Mitigation | Data Loss Risk |
|-----------|--------------|------------|----------------|
| Redis (vote buffer) | Yes | AOF + Kafka dual-write | Max 1s (AOF) |
| Postgres primary | Yes | Multi-AZ automatic failover | <1s (replica lag) |
| Vote flush worker | Yes | Multiple instances + Redis watchdog | None (Redis durable) |
| Elasticsearch | Yes | Postgres FTS fallback + circuit breaker | None (reindexed from Kafka) |
| CDN | No (multi-CDN) | DNS failover in 60s | None |
| Kafka | Replicated (3x) | Partition replication | None |
| Score updater | Yes | Redis sorted set stale until restart | Feed ranks stale, not lost |
