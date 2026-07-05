# URL Shortener — Failure Analysis & Resilience Playbook
**Date:** 2026-05-15  
**Companion to:** URL_Shortener_System_Design_2026_05_15.md

---

## Framework: How to Think About Failures

For every component, ask three questions:
1. **What happens if THIS component fails?** (self-failure)
2. **What happens if the component UPSTREAM fails?** (caller dies)
3. **What happens if the component DOWNSTREAM fails?** (dependency dies)

Then for each: **Detect → Contain → Recover → Prevent**

---

## Component 1: Redirect Service (the hot path)

### Self-failure: Redirect service instance crashes

**Impact:** Traffic that was hitting this instance is lost until LB health check detects it (~30s with default settings).

**Detection:** LB health checks (`GET /health` → must respond 200 in < 500ms). Remove from rotation automatically.

**Containment:** Stateless service — no local state lost. LB redistributes to healthy instances.

**Recovery:**
- Auto-scaling group spins up replacement instance (< 2 min with pre-warmed AMIs)
- Pre-warm L1 cache by replaying top-N URLs from Redis on startup

**Prevention:**
- Run minimum 3 instances across 3 AZs
- Circuit breaker: if instance error rate > 5% in 10s, remove from LB before it fully crashes
- Graceful shutdown: drain in-flight requests before terminating (SIGTERM → 30s drain → SIGKILL)

---

### Upstream failure: API Gateway goes down

**Impact:** All traffic blocked — reads and writes both fail. This is a full outage.

**Containment:** CDN acts as a buffer for cached redirects. Links cached at CDN level still work even if API Gateway is down.

**Recovery:**
- API Gateway should be managed (AWS API GW / Kong cluster) — vendor SLA or HA cluster
- DNS failover to secondary region (Route 53 health checks → failover record)

**Prevention:**
- Multi-region active-active deployment for redirect path
- CDN caching means >60% of traffic never touches API Gateway anyway

---

### Downstream failure: Redis goes down

**Impact:** L2 cache miss on every request → all traffic falls through to DB read replicas. DB load spikes 10x.

**Detection:** Redis client timeout/connection error. Track cache hit rate metric — sudden drop is a signal.

**Containment (Thundering Herd problem):**
- If Redis goes down suddenly, every redirect service instance simultaneously starts hitting the DB
- **Solution 1 — Jitter:** Add random sleep (0-100ms) before DB fallback so requests don't arrive simultaneously
- **Solution 2 — Local L1 Cache:** Each instance holds a small LRU cache (100K entries, ~50MB). Absorbs most traffic for top URLs even without Redis.
- **Solution 3 — Probabilistic Early Expiry:** Before TTL expires, with probability `p`, refresh the cache. Prevents stampede on expiry.

**Recovery:** Redis Cluster promotes replica to primary automatically (Sentinel or Cluster mode). Redirect service starts filling cache again from DB responses.

**Prevention:**
- Redis Cluster with 3 shards × 1 replica each (6 nodes total)
- Sentinel for automatic failover
- Persistent AOF logs so Redis can replay missed writes on restart

> **Real-world example:** GitHub's 2018 outage was partly a cache stampede. When their Memcache went down, MySQL was overwhelmed. They added request coalescing (only one request per key goes to DB, others wait).

---

## Component 2: Primary Database (Write Path)

### Self-failure: Primary DB goes down

**Impact:** No new URLs can be shortened. Reads still work (replicas). This is a write outage.

**Detection:** DB connection pool errors. Alert on write error rate > 0.1%.

**Containment:**
- Reads still served from replicas — redirect functionality survives
- Queue incoming write requests in a temporary buffer (Kafka topic) for replay

**Recovery:**
- Promote a replica to primary (takes 30-60s with automatic failover — AWS RDS Multi-AZ, Postgres Patroni)
- Replay buffered write requests from Kafka

**Prevention:**
- Multi-AZ synchronous replica (RDS Multi-AZ) — automatic failover, RPO ~0, RTO ~60s
- Write-ahead log (WAL) streaming to replica — at most a few seconds of lag
- Never write to primary directly from app — use a write endpoint that the failover manager controls

---

### Downstream failure: DB is slow (not down, just degraded)

This is more insidious than a full failure — partial failures are harder to detect.

**Symptoms:** P99 write latency > 500ms. App threads block waiting for DB. Connection pool exhausts. Entire service hangs.

**Detection:** Monitor P99/P999 DB query latency, not just error rate. Set alert at P99 > 200ms.

**Containment:**
- **Timeouts everywhere:** DB connection timeout = 2s, query timeout = 1s. If exceeded, fail fast, don't wait.
- **Circuit breaker on write path:** After 10 consecutive timeouts, open circuit. Writes go to Kafka "pending writes" topic. Service returns a "your link will be ready in a moment" response.
- **Bulkhead pattern:** DB write connection pool is separate from DB read connection pool. A slow write doesn't steal read connections.

**Recovery:** Once DB recovers, circuit closes, Kafka consumer replays pending writes.

---

## Component 3: Key Generation Service (KGS)

### Self-failure: KGS goes down

**Impact:** No new short codes can be generated → write path fails completely.

**Mitigation — Pre-allocated key pool:**
- Each Shortener Service instance fetches a batch of 10,000 pre-generated keys on startup
- If KGS is down, instance uses its local batch (can sustain ~hours at normal write rate)
- When batch runs low (< 20%), background thread tries to replenish from KGS
- If KGS is still down at batch exhaustion → fail new shortens with 503, return retry-after header

**Recovery:** KGS is a simple stateless service backed by a key table. Auto-restart + multiple replicas.

**Prevention:**
- Run 3 KGS instances behind a load balancer
- The "unused_keys" table should have millions of pre-generated keys
- Key generation is idempotent — restarting KGS doesn't generate duplicates (keys are pre-generated in batch, not on demand)

---

## Component 4: Kafka (Analytics Pipeline)

### Self-failure: Kafka broker goes down

**Impact:** Click events are lost → analytics data becomes incomplete. Redirects still work (Kafka is write-only from redirect service).

**Why this is acceptable:** Analytics is eventually consistent by design. A few lost events during a broker failure is tolerable.

**Mitigation:**
- Kafka topic replication factor = 3 (event survives 2 broker failures)
- Producer config: `acks=1` (leader ack only) — don't slow the redirect path for analytics
- If Kafka is unreachable, fail open: drop the event, log locally, redirect still succeeds

**Prevention:**
- 3-broker Kafka cluster across 3 AZs
- Separate Kafka cluster for analytics vs. critical events (don't mix)

---

### Downstream failure: Analytics Consumer crashes mid-processing

**Impact:** Events accumulate in Kafka. No data loss (Kafka retains events per retention policy). When consumer restarts, it replays from last committed offset.

**Problem:** Duplicate processing — if consumer crashes after processing but before committing offset.

**Solution — Idempotent consumers:**
- Use `event_id` as idempotency key in ClickHouse
- `INSERT INTO click_events ... ON DUPLICATE KEY IGNORE`
- ClickHouse's `ReplacingMergeTree` deduplicates rows with same sort key

---

## Component 5: CDN Layer

### Self-failure: CDN goes down or serves stale content

**Impact (stale content):** Users get redirected to old URL after a link is updated/deleted.

**Mitigation:**
- Use `Cache-Control: max-age=3600, stale-while-revalidate=60` — serve stale for 60s while revalidating in background
- For immediate invalidation (abuse, legal): call CDN purge API (`cloudfront.create_invalidation`)
- For critical deletions: redirect service checks `is_active` flag even for cached responses (hybrid approach)

**Impact (CDN down):** All traffic hits origin. Origin must be sized for 100% traffic (don't assume CDN always absorbs 60%).

**Prevention:** Multi-CDN strategy (CloudFront primary + Fastly secondary with DNS failover) — expensive but used by large systems.

---

## Component 6: Cross-Cutting Failures

### Split-Brain: Two DB nodes think they're primary

**Scenario:** Network partition between primary and replica. Replica promotes itself. Now two primaries accept writes.

**Prevention:**
- Use Patroni/etcd for distributed consensus — only one node can hold the leader lock
- Application always connects via single write endpoint (managed by Patroni) — never connects directly to a node IP
- Read replicas are strictly read-only via `default_transaction_read_only = on`

---

### Data Inconsistency: Cache has stale mapping

**Scenario:** User updates a short link's destination. Cache still has old URL. Users get redirected to wrong place.

**Prevention — Cache invalidation strategy:**
```
On update:
1. Write new URL to DB (primary)
2. Publish "invalidate:{short_code}" message to Kafka
3. All redirect service instances subscribe and evict from L1 cache
4. Redis: DEL shortlink:{short_code} (immediate)
5. CDN: async purge (may take 30s-5min)
```
**Acceptable inconsistency window:** ~5 minutes (CDN TTL) for most users, < 1s for users hitting origin.

---

### Hotspot / "Thundering Herd" on viral URL

**Scenario:** A URL goes viral (e.g., on Reddit front page). 100K req/s hitting a single short code.

**Detection:** Track per-short-code request rate. Alert if any code exceeds 10K/min.

**Solutions:**
1. **Local L1 cache on every redirect instance** — hot URL is in memory of every pod. No Redis needed.
2. **Request coalescing:** If cache miss and URL is already being fetched by another goroutine, wait for that result. Don't send 100 simultaneous DB reads.
3. **CDN caching with longer TTL for GET requests** — viral links benefit from 1-hour CDN caching.
4. **Key-based routing at LB level** — route all requests for a hot short_code to the same pod cluster (pre-warmed L1 cache).

---

## Runbook Summary

| Failure | User Impact | Immediate Action | Recovery |
|---|---|---|---|
| Redirect service instance down | Partial elevated latency | LB auto-removes. Scale out. | Auto-heal via ASG |
| Redis down | Latency spike (DB fallback) | Check L1 cache absorbing. Monitor DB load. | Sentinel failover ~30s |
| Primary DB down | Write outage, reads ok | Promote replica. Drain write queue via Kafka. | RDS Multi-AZ ~60s |
| KGS down | New shortens fail | Local batch sustains hours. Alert on-call. | Restart KGS instances |
| Kafka broker down | Analytics gaps | Fail open (drop events). Check replication. | Kafka auto-leader election |
| CDN stale data | Wrong redirects (rare) | Purge via CDN API for critical links. | TTL expiry or manual purge |
| Viral URL hotspot | Elevated origin load | Verify CDN caching. Check L1 hit rate. | Usually self-resolving |

---

## Chaos Engineering Tests (Prevention)

Following Netflix Chaos Monkey principles:

1. **Kill a redirect service pod** → Verify LB reroutes within 5s
2. **Block Redis connection** → Verify graceful fallback to DB, L1 cache holds
3. **Slow primary DB to 2s queries** → Verify circuit breaker opens, writes queue to Kafka
4. **Kill one Kafka broker** → Verify no event loss (replication factor = 3)
5. **Flood one short_code with 50K req/s** → Verify L1 cache handles it, no DB hammering
6. **Simulate network partition between DB primary and replica** → Verify only one primary elected
