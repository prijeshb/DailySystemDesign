# Pastebin — Failure Analysis
*Date: 2026-06-30*

---

## Failure Topology

```
[Client] → [CDN] → [API Gateway] → [Paste Service] → [Postgres]
                                                    → [Redis]
                                                    → [S3]
                                        ↓
                                   [Key Generator]
                                        ↓
                                   [Kafka] → [Analytics]
```

---

## Case 1: Postgres Primary Fails

**Scenario:** DB primary crashes mid-write or during maintenance. RDS Multi-AZ failover takes 30-60s.

**Impact without mitigation:**
- All paste creates fail (504/503)
- All cache misses fall through to DB → fail
- Reads for popular pastes: Redis hits → OK
- Reads for cold/non-cached pastes: fail

**Chain of failure:**
```
Postgres down
→ Paste Service: connection pool exhausted (all threads waiting on DB timeout)
→ Read requests pile up → Paste Service OOM or thread starvation
→ API Gateway: health checks fail → removes Paste Service instances
→ All traffic unroutable → complete outage
```

**Prevention:**
```
1. DB connection timeout: set connect_timeout=5s, statement_timeout=10s
   → Threads fail fast, not blocked indefinitely

2. Circuit Breaker on DB calls:
   5 consecutive failures in 10s → OPEN
   Fallback: reads → Redis only (cache-only mode)
             writes → 503 with Retry-After: 60

3. RDS Multi-AZ: automatic failover to standby in 30-60s
   → RPO ≈ 0 (synchronous replication), RTO ≈ 60s

4. Connection pool: pgBouncer with pool_size=50 per service pod
   → Limits concurrent DB connections, prevents connection storm on recovery
```

**Recovery:**
```
RDS failover completes → Paste Service circuit half-open → probe succeeds → CLOSED
Write requests resume. Read requests that hit cache continue uninterrupted.
Cold reads missed during 60s window: user sees error, retries.
```

---

## Case 2: Redis Cache Fails

**Scenario:** Redis node crashes (OOM, hardware failure, deployment).

**Impact:**
- All cache reads miss → fall to Postgres
- At 290 reads/sec, Postgres handles ~500 reads/sec capacity → borderline OK
- But if viral paste (10K reads/sec) was being absorbed by cache → Postgres overwhelmed

**Chain of failure:**
```
Redis down → cache miss rate 100%
→ All reads hit Postgres → Postgres CPU spikes to 100%
→ Query latency increases 5-10× → timeouts
→ Read errors cascade → users retry → more load
→ Postgres overloaded → write latency spikes → creates fail too
→ Full service degradation
```

**Prevention:**
```
1. Redis Sentinel (3 nodes): primary dies → automatic failover in ~5s
   ElastiCache: Multi-AZ with automatic failover enabled

2. Cache miss storm protection (dog-pile prevention):
   SET rebuild_lock:{id} NX EX 3 → only 1 thread rebuilds per key
   Others serve stale (if stale-while-revalidate configured) or wait 100ms

3. Local in-process cache (L1):
   LRU cache in each Paste Service pod, 256MB, TTL=30s
   Top 1000 most-read pastes: always in L1 regardless of Redis state
   → Redis failure only affects cold pastes for 30s window

4. Rate limiting at CDN (CloudFront): popular pastes cached at edge 5min
   → Redis and Postgres shielded from read spike for cached content
```

**Recovery:**
```
Redis failover (~5s) or restart (~10s):
  → Cache is cold (empty) → rebuild gradually as reads come in
  → No data loss (Redis is cache, not source of truth)
  → Postgres absorbs extra load for 2-3 minutes during cache warmup
  → Monitor DB CPU; if >80%, enable read replica routing temporarily
```

---

## Case 3: S3 Unavailable

**Scenario:** S3 regional outage or networking issue (e.g., AWS us-east-1 partial outage, ~1-2× per year).

**Impact:**
- Large pastes (>10KB) stored in S3 → unreadable
- Small pastes (≤10KB, ~90% of all pastes) stored inline in Postgres → unaffected

**Prevention:**
```
1. Inline storage threshold (≤10KB in Postgres):
   Covers 90% of pastes. Most users unaffected by S3 outage.

2. S3 Cross-Region Replication:
   Primary: us-east-1. Replica: us-west-2.
   S3 RCR SLA: <15min replication lag.
   On S3 regional issue: switch content_ref prefix to replicated bucket endpoint.

3. For reads: check if content_ref is NULL (inline) first; only call S3 if non-null.
   Circuit breaker on S3 client:
     OPEN → return 503 for large-paste reads; small-paste reads unaffected.

4. Multi-AZ Presigned URLs: generate with 1h expiry, stored in Redis
   → If paste service goes down, CDN can serve content via presigned URL directly
```

**Recovery:**
```
S3 recovers (usually auto, <1hr for regional events):
  → Large paste reads resume automatically
  → No data loss (S3 11-nines durability)
  → No state to rebuild
```

---

## Case 4: Key Generator Service Fails

**Scenario:** The pre-generated key pool service is down. No new keys to issue.

**Impact:**
- All paste creates fail
- Reads unaffected

**Prevention:**
```
1. Paste Service caches a local key buffer (500 keys in memory per pod)
   → Key Gen service down: creates succeed for ~500/pod × N pods more creates
   → At 29 creates/sec: 500 keys / 29 = ~17s of local buffer per pod

2. Key Gen service: 2 replicas + Redis Sorted Set as shared pool
   SPOP keys_available → atomic, no collision between pods
   Pool threshold: when pool < 100K keys → background job refills to 1M

3. Fallback: if Redis pool AND Key Gen both unavailable:
   Use Snowflake ID (timestamp + machine_id + seq) → base62
   May produce 11-char URLs instead of 6 → acceptable degraded mode
   Alert fires; engineering fixes root cause.
```

**Recovery:**
```
Key Gen service restarts → polls Redis pool remaining count → triggers refill job
→ No manual intervention needed if local buffers held for <17s
```

---

## Case 5: CDN Cache Poisoning / Stale Delete

**Scenario:** User deletes a paste. Paste Service deletes from Postgres and Redis. But CDN has the paste cached (max-age=300). For up to 5 minutes, the paste is still publicly accessible via CDN edge.

**Impact:**
- Private/sensitive data deleted from origin but served from CDN for up to 5 min
- GDPR: "right to erasure" means delete must be immediate

**Prevention:**
```
1. Visibility rule: private and unlisted pastes → Cache-Control: no-store
   Only public pastes get CDN caching (max-age=300)
   → Private data never hits CDN

2. For public paste deletion: CloudFront invalidation API
   POST /2020-05-31/distribution/{id}/invalidation
     Paths: ["/p/{paste_id}"]
   SLA: CloudFront invalidation propagates in <30s globally
   Cost: first 1000 invalidations/month free, then $0.005 each

3. Content TTL tuning:
   Default: max-age=300 (5min)
   Adjustable per paste: popular pastes → longer TTL; personal/sensitive → shorter
```

---

## Case 6: Paste Service Pods Fail (All)

**Scenario:** Bad deployment, OOM kill, AZ outage takes all pods offline simultaneously.

**Prevention:**
```
1. Rolling deployment: max-unavailable=1, max-surge=1
   → At least 3 of 4 pods running during deploy

2. Multi-AZ placement: pods spread across us-east-1a, 1b, 1c
   → Single AZ failure: only 1/3 of pods affected

3. Health check endpoint: /health → checks Redis ping and DB ping
   ALB removes unhealthy pod within 10s

4. Pre-warming: on new pod startup → pre-load top-100 paste IDs into L1 cache
   → First reads post-deploy still fast

5. Circuit breaker at API Gateway:
   If >50% of Paste Service instances unhealthy → return 503 immediately
   Rather than queuing requests that will timeout
```

---

## Case 7: Viral Paste (Thundering Read)

**Scenario:** A paste goes viral (Reddit/HN front page). 50K reads/sec suddenly on one paste_id.

**Without mitigation:**
```
CDN: Cache-Control max-age=300 → CDN absorbs >95% of traffic
But: CDN cold start (first 30-60s before CDN populates) → 50K/s hits Paste Service
Redis: GETS saturate → single Redis node ~100K ops/sec → OK
Postgres: if Redis misses → overwhelmed

If CDN edge in a region not yet cached:
→ 50K GETs → Redis → misses (key not yet set by prior reads) → 50K DB reads in 1s
→ Postgres: dead
```

**Prevention:**
```
1. Request coalescing in Paste Service:
   First request for paste_id: acquires lock, fetches from DB, populates Redis
   Concurrent requests: wait on Redis BLPOP or singleflight (Go pattern)
   → 50K requests collapse to 1 DB read

2. CDN warming via pre-signed URL or early cache push:
   On viral detection (>1000 reads/min via analytics): trigger CDN cache warming
   GET /p/{id} from 5 PoPs → CDN primed before organic traffic arrives

3. Always set Redis on first DB read:
   Paste Service: try Redis → miss → DB → SET Redis TTL=24h → return
   → Even without request coalescing: 1s of misses then cache absorbs all subsequent

4. Stale-while-revalidate (CDN):
   Cache-Control: max-age=300, stale-while-revalidate=600
   → CDN serves stale while background-refreshing; origin not hammered on TTL expiry
```

---

## Failure Summary Matrix

| Component | Failure | Read Impact | Write Impact | RTO |
|-----------|---------|-------------|--------------|-----|
| Postgres primary | Crash | Cold reads fail | All creates fail | 60s (Multi-AZ failover) |
| Redis | Crash | All reads → Postgres | No impact | 5-10s (Sentinel failover) |
| S3 | Outage | Large pastes (10%) fail | Large pastes fail | External (AWS SLA) |
| Key Generator | Down | None | Exhausts local buffer in ~17s | 30s (pod restart) |
| CDN | Outage | Traffic hits API directly (10× load) | None | API rate limits protect DB |
| Paste Service | All pods down | No reads | No writes | 30-60s (ECS restart) |
| Viral paste | N/A | Request coalescing + CDN | N/A | Handled automatically |
