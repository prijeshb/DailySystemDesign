# Twitter Feed System — Failure Analysis & Resilience Playbook
**Date:** 2026-05-14 | **System:** Twitter/X Social Feed

> Rule: For every component ask three questions — what if IT fails, what if the component BEFORE it fails, what if the component AFTER it fails? Then: detection, prevention, recovery, and real-world precedent.

---

## Failure Analysis Map

```
User → [API Gateway] → [Tweet Service] → [TweetDB (Cassandra)]
                              ↓
                          [Kafka] → [Fan-Out Service] → [Redis Feed Cache]
                              ↓
                      [Notification Service] → [APNs / FCM]
                              ↓
                      [Search Indexer] → [Elasticsearch]
                              ↓
                      [Trending Service] → [Redis Counters]

User → [API Gateway] → [Timeline Service] → [Redis Feed Cache]
                                          → [TweetDB (Cassandra)]
                                          → [User Cache (Redis)]
```

---

## 1. API Gateway Failure

### 1a. API Gateway itself crashes

**What breaks:**
- ALL traffic (reads and writes) are blocked. No tweets can be posted. No feeds load.
- This is the single most catastrophic failure — affects 100% of users.

**Detection:**
- Load balancer health checks fail within 5 seconds.
- Synthetic monitoring: probes hit the gateway from 5 global locations every 10 seconds. Any failure triggers PagerDuty.
- Client side: connection refused / timeout.

**Prevention:**
- Run 50+ API Gateway instances behind a Layer 4 load balancer (AWS NLB / Anycast DNS).
- Multi-region deployment: primary region (us-east-1) + hot standby (eu-west-1). DNS failover (Route53 health checks) switches in < 60 seconds.
- No single instance holds state — all state is in downstream services.

**Recovery:**
- Auto-scaling group replaces dead instances within 2 minutes.
- If entire AZ fails: load balancer routes to remaining AZs instantly (pre-warmed).
- Global failover: DNS TTL set to 30 seconds for the API endpoint to allow fast failover.

**Real-world:** Twitter's 2022 outage during mass layoffs included gateway-level instability from under-staffed incident response.

---

### 1b. Component BEFORE API Gateway fails (DNS / CDN Edge)

**What breaks:**
- DNS resolution fails → clients cannot resolve api.twitter.com.
- CDN edge nodes go down → media (images, videos) fail to load, API requests that route through CDN fail.

**Solution:**
- DNS: Use multiple DNS providers (Route53 + Cloudflare) with failover. TTL = 30s so clients pick up changes fast.
- CDN: Origin fallback — if CDN edge is unreachable, clients retry direct to origin. CDN is a cache, not a single point of failure for the API.
- For media: serve from multiple CDN PoPs. Client retries with exponential backoff on media load failure (media failure is lower priority than API failure).

---

### 1c. Component AFTER API Gateway fails (Tweet Service or Timeline Service crash)

**What breaks:**
- Gateway receives request, routes to Tweet Service → connection refused → gateway returns 503.
- From client's perspective: the same as if the gateway failed.

**Solution:**
- Circuit breaker at the gateway level (e.g., Hystrix / Envoy): if Tweet Service returns 503 for 50% of requests in 10 seconds, open the circuit, return a cached / graceful-degraded response without hitting the service.
- Graceful degradation: if Timeline Service is down, show cached feed (even if stale by minutes) rather than empty screen.
- Health checks: gateway continuously pings downstream services. Unhealthy instances removed from rotation.

---

## 2. Tweet Service Failure

### 2a. Tweet Service itself crashes (during tweet creation)

**What breaks:**
- User clicks "Post" → no acknowledgment → tweet may or may not have been written to Cassandra.
- If tweet was written to DB but Kafka event was never enqueued: tweet exists in DB but never fans out → user sees their tweet on their own profile, but followers never see it.

**Detection:**
- Monitoring: tweets written to TweetDB vs. Kafka events published should match within 5 minutes. Alert if gap > 1%.
- Client: shows spinner indefinitely. After 5s timeout, client shows "Tweet failed. Retry?"

**Prevention:**
- **Transactional outbox pattern:** Tweet Service writes to two places atomically:
  - `tweets` table in Cassandra (the tweet itself).
  - `outbox` table in Cassandra (the pending Kafka event).
  - A separate outbox-reader process continuously polls outbox table and publishes to Kafka, then deletes the row.
  - Even if Tweet Service crashes after writing to Cassandra, the outbox row survives and will be published when the service restarts.

**Recovery:**
- Kubernetes restarts the pod within 30 seconds.
- Client retries with idempotency key → if tweet was already created, returns existing tweet_id (no duplicate).
- Outbox reader catches up pending events on restart.

---

### 2b. Component BEFORE Tweet Service fails (API Gateway overloads Tweet Service)

**What breaks:**
- Traffic spike (viral event, Super Bowl): API Gateway is healthy but Tweet Service is overwhelmed.
- Tweet Service starts returning 429 or 503 under load.
- Queue depth builds up, latency spikes.

**Solution:**
- Rate limiting at API Gateway: each user limited to 300 tweets/day, 5 tweets/minute. This is not just a product rule — it's a load protection rule.
- Request queuing with back-pressure: Tweet Service has an internal queue. If queue depth > threshold, return 503 immediately (fail fast) rather than accepting work it can't do.
- Auto-scaling: Tweet Service pods scale horizontally based on CPU/queue depth metrics. Scale-up triggered at 70% CPU, scale-down at 30%.
- Load shedding: non-critical writes (view count increments, analytics events) are deprioritized. Tweet creation is P0 — gets its own isolated worker pool.

---

### 2c. Component AFTER Tweet Service fails (Cassandra TweetDB is down)

**What breaks:**
- Tweet Service accepts the request but cannot persist the tweet.
- Tweet must not be ack'd to the user as "posted" if it wasn't durably saved.

**Solution:**
- If Cassandra quorum write fails (requires acknowledgment from majority of replicas):
  - Return 503 to client. Do NOT ack the tweet.
  - Client shows "Failed to post. Please try again."
  - Client retries with same idempotency_key. Once Cassandra recovers, tweet is created exactly once.
- Write to a local in-memory queue as emergency buffer? **No** — if the pod restarts, buffered tweets are lost. Better to reject and let the client retry.
- Cassandra self-heals: coordinator retries against healthy replicas. At replication factor 3, can tolerate 1 node down with no data loss.

---

## 3. Kafka Failure

### 3a. Kafka broker crashes

**What breaks:**
- `tweet.created` events cannot be published.
- Fan-out Service stops receiving events → feeds stop updating.
- Notification Service stops receiving events → notifications delayed.
- Search Indexer stops → new tweets missing from search.
- **Tweets themselves are safe** (already persisted in Cassandra before Kafka is touched).

**Detection:**
- Kafka broker health metrics: under-replicated partitions > 0 → alert.
- Consumer lag metrics: if fan-out consumer lag > 10K messages, alert (fans are falling behind).

**Prevention:**
- Kafka runs with replication factor 3, min.insync.replicas = 2. Can lose 1 broker with no message loss.
- Multiple brokers across AZs (3 AZs × N brokers). AZ failure only affects partitions on that AZ's leaders — leaders auto-rebalance to replicas in other AZs within 30 seconds.

**Recovery:**
- Kafka controller auto-elects new partition leaders within 30 seconds of broker failure.
- Fan-out consumers resume from their last committed offset — no events are lost, just delayed.
- Outbox pattern (from Tweet Service) ensures events are republished if they were lost before Kafka wrote them.
- Worst case: consumer processes events slightly out of order. Feed may have a few tweets in wrong order. Acceptable — tweets are re-sorted by tweet_id at read time.

---

### 3b. Fan-Out Service is a slow consumer (Kafka lag)

**What breaks:**
- Kafka queue fills up. Tweets are posted but feeds aren't updated for minutes or hours.
- Users won't see recent tweets in their feeds.

**Detection:**
- Consumer lag alert: if fan-out lag > 100K messages (roughly 10 seconds of normal traffic at 10K/s), page on-call.

**Solution:**
- Scale out Fan-Out Service horizontally: add more consumer instances. Kafka partitions are the unit of parallelism — run as many consumers as partitions.
- For lag spikes during viral events: pre-provision extra capacity. Auto-scaling consumer group based on lag metric.
- Emergency: fan-out rate limiting — for celebrities (>1M followers), fan-out-on-read handles their tweets, so the queue is only processing normal users. Prioritize normal users.
- Dead letter queue: if fan-out for a specific user_id repeatedly fails (e.g., that user's Redis shard is down), move event to DLQ, continue with others, retry DLQ separately.

---

## 4. Redis Feed Cache Failure

### 4a. Redis node crashes (shard goes down)

**What breaks:**
- All users whose feeds are on that shard get cache misses.
- Timeline Service falls back to computing feeds from DB — extremely expensive at scale.
- If DB fallback is not implemented: users see empty feeds.

**Detection:**
- Redis node health check fails. Cluster marks it as failed.
- Cache hit rate drops sharply for affected user_ids → alert if hit rate < 95%.

**Prevention:**
- Redis Cluster with at least 1 replica per shard (Redis Cluster replication).
- On primary failure, replica is auto-promoted within 10-15 seconds (Redis Sentinel / Cluster failover).
- Keep last-known-good feeds in DB or re-compute from TweetDB as emergency fallback (expensive but possible).

**Recovery:**
- Replica takes over: affected users may see slightly stale feeds (replica may lag 1-2 seconds behind primary), but feeds load.
- New replica promoted. Old primary's data may need rebuilding.
- Feed rebuild: for users who lost their feed cache, re-run fan-out backfill from TweetDB. Prioritize active users (those who have opened the app recently).

---

### 4b. Redis OOM (out of memory)

**What breaks:**
- Redis starts evicting feed cache entries (eviction policy: allkeys-lru).
- Evicted feeds → cache miss → fallback computation for that user.

**Prevention:**
- Feed TTL: `EXPIRE feed:{user_id} 86400` — feeds for users who don't log in for 24h are automatically evicted.
- Cold user optimization: users inactive for 7+ days don't get pre-built feeds at all. Save Redis memory for active users.
- Monitor Redis memory usage: alert at 75% capacity, scale cluster (add shards) before hitting OOM.

**Trade-off:** Evicting inactive user feeds saves memory but means those users get a slow first-load experience (feed computed from scratch). For 200M DAU / 400M MAU, roughly 50% of accounts are inactive on any given day — not building feeds for them halves Redis memory requirements.

---

## 5. TweetDB (Cassandra) Failure

### 5a. Single Cassandra node failure

**What breaks:**
- Nothing immediately — at RF=3, one node failure is handled transparently.
- Writes and reads route to remaining replicas.
- Consistency: with QUORUM reads/writes, need 2 of 3 replicas → still achievable with 2 nodes.

**Detection:**
- Cassandra nodetool: `nodetool status` shows DOWN node. Alert fires.

**Recovery:**
- The down node is replaced (new node bootstraps, streams data from peers).
- During repair period: read repair ensures data consistency as clients read from available nodes.

---

### 5b. Cassandra cluster-wide failure / split brain

**What breaks:**
- With a network partition, the cluster may split into two groups (neither has quorum).
- QUORUM writes fail → Tweet Service returns 503 → users cannot post.
- QUORUM reads fail → Timeline Service cannot fetch tweet content.

**Prevention:**
- Deploy Cassandra across 3 AZs: even if one AZ loses network, the other 2 still have quorum (2 of 3).
- Never deploy a Cassandra cluster that spans across regions without understanding quorum cost (cross-region latency explodes with QUORUM).
- Tunable consistency: for read-heavy paths (fetching tweet content), can drop to LOCAL_ONE (read from nearest node) — accept potential stale data for non-critical reads.

**Recovery:**
- Once partition heals, Cassandra's "last write wins" reconciliation handles conflicts.
- Hinted handoff: writes made during partition are replayed to the recovered node.

---

## 6. Social Graph DB (Follow table) Failure

### 6a. Social Graph DB crashes

**What breaks:**
- Fan-Out Service cannot fetch follower lists → cannot push tweets to feeds.
- Follow / unfollow operations fail → user cannot follow new accounts.
- Timeline Service cannot check celebrity-follow relationships (for fan-out-on-read merging).

**Detection:**
- DB health check fails. Fan-out consumer error rate spikes.

**Prevention:**
- PostgreSQL with streaming replication to hot standby. Failover via Patroni or AWS RDS Multi-AZ.
- Cache: follower lists for active users are cached in Redis (`followers:{user_id}` → Set of follower_ids, TTL 1 hour). Fan-Out Service reads from cache first.
- Follower list cache invalidated on follow/unfollow events (but eventual consistency is acceptable for fan-out).

**Recovery:**
- RDS Multi-AZ auto-failover: standby promoted within 60-90 seconds (DNS update).
- Fan-out pauses (Kafka events queue up during failover), resumes after recovery.
- Users can continue reading their cached feeds during the 60-90 second outage.

**Failure cascade to avoid:** Social Graph DB failure → Fan-Out Service crashes → Kafka consumer lag explodes → alert fires → engineer manually scales fan-out → lag drains over next 30 minutes. Mitigation: fan-out consumer is designed to be resumable from any offset, and backpressure prevents overwhelming the DB on recovery.

---

## 7. Notification Service Failure

### 7a. Notification Service crashes

**What breaks:**
- Users don't receive push notifications for likes, follows, mentions.
- Notifications are not a hard dependency for the core experience (reading/posting tweets still works).

**Detection:**
- Notification Kafka consumer lag grows. Alert at > 100K messages of lag.

**Prevention / Recovery:**
- Notifications are fully async via Kafka. If Notification Service is down for 10 minutes, events queue in Kafka (durable).
- On recovery, service processes events in order — users get delayed notifications but no notifications are permanently lost.
- APNs / FCM failure: use retry queues with exponential backoff. APNs stores notifications for up to 30 days for offline devices.
- Graceful degradation: if push notification fails, fall back to in-app notification (shown next time user opens app). This is the existing product behavior.

---

## 8. Search (Elasticsearch) Failure

### 8a. Elasticsearch cluster goes down

**What breaks:**
- Search for tweets/users returns error.
- Trending calculation may be affected if it uses ES.

**Detection:**
- ES health endpoint returns RED status. Alert fires.
- Search API error rate spikes to 100%.

**Prevention:**
- ES cluster: 3+ data nodes, 2 dedicated master nodes. Multi-AZ.
- Trending uses Redis (not ES), so trending is unaffected.

**Recovery:**
- ES is read-heavy and rebuilt from Kafka (search indexer). On recovery, replay missed events from Kafka.
- If ES is down, return empty search results with a "Search is temporarily unavailable" message rather than crashing.
- Search is non-critical path for core feed reading — degrade gracefully.

---

## 9. Complete Data Center / Region Failure

### 9a. Primary region (us-east-1) goes down entirely

**What breaks:**
- Everything in primary region is unavailable.
- Users worldwide cannot use Twitter.

**Prevention / Recovery:**
- Active-Active multi-region: replicate TweetDB (Cassandra multi-DC), Social Graph (cross-region read replicas), Redis feeds (rebuilt on demand in secondary region).
- Global load balancing: Anycast routes users to nearest healthy region. On primary failure, all traffic goes to secondary within 60 seconds.
- Data replication lag: Cassandra async cross-DC replication may lag 1-5 seconds. Tweets written just before the outage may be missing in secondary. This is the CAP theorem in action — system chose Availability over Consistency in a region failure.
- Redis feeds: not replicated cross-region (too expensive). On failover, feeds are rebuilt from TweetDB for active users. Expect 30-60 seconds of slow feed loads immediately after failover.

---

## 10. The Thundering Herd Problem

### Scenario: New tweet from a celebrity goes viral

- Elon Musk tweets something controversial. 50M followers simultaneously open Twitter in the next 60 seconds.
- Timeline Service gets 50M requests for `feed:{user_id}` where each feed includes celebrity's tweet.
- Celebrity's tweet cache (`tweet:{tweet_id}`) is hit 50M times in 60 seconds.

**What breaks:**
- If Redis has the tweet cached: fine — Redis handles millions of ops/sec.
- If tweet cache entry expires at exactly T+3600 (cache stampede): all 50M simultaneous misses go to Cassandra simultaneously → Cassandra overloads → cascade failure.

**Solutions:**
1. **Probabilistic early expiration:** Don't expire all at exactly TTL. Add `random.uniform(-300, +300)` seconds to TTL. Stampede is spread out.
2. **Mutex lock on cache miss:** First thread to get a miss acquires a Redis lock for that tweet_id, fetches from DB, populates cache, releases lock. Other threads wait for lock, then read from cache.
3. **Never evict hot tweets:** For tweets getting >1K reads/minute, refresh TTL on every read (sliding expiration).
4. **Background pre-warming:** Trending Service identifies tweets going viral (>10K retweets in 5 min) → pro-actively ensures they're in cache with long TTL.
