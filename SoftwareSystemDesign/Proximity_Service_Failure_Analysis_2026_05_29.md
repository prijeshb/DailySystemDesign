# Proximity Service — Failure Analysis
*Date: 2026-05-29*

---

## Failure-First Mindset

For each component, ask:
1. What if **this component fails**?
2. What if the component **ahead** (upstream) fails?
3. What if the component **behind** (downstream) fails?

---

## Component Map

```
Client → API Gateway → Search Service → [Redis Cache → Geospatial DB → Business DB]
                    → Business Service → [Business DB → Kafka → Index Updater → Geospatial DB]
                    → Review Service → Review DB
```

---

## 1. API Gateway Failure

**Symptom:** All traffic drops. Users see 503.

**Ahead (client):** Client retries with exponential backoff — need idempotent endpoints.  
**Behind (services):** Services are healthy but unreachable.

**Prevention:**
- Multiple gateway instances behind DNS/load balancer
- Health check failover (60s detection → reroute)
- Circuit breaker at client SDK level

**Recovery:** Auto-scaling + blue-green deploy. Stateless gateway = any instance can serve.

---

## 2. Search Service Failure

**Symptom:** /search returns 503. Business pages still work.

**If 1 of N instances fails:**
- Load balancer detects via health check, removes from rotation
- Other instances absorb traffic (ensure headroom: run at 60% capacity)

**If ALL instances fail (bad deploy):**
- Canary deployment: roll out to 5% traffic first
- Rollback trigger: error rate > 1% → auto-rollback

**Ahead (API Gateway) fails:** Search service gets no requests. No impact on state.  
**Behind (Redis) fails:** See Redis failure section below.

---

## 3. Redis Cache Failure

**Scenario A: Single Redis node dies**
- Effect: Cache miss for that geohash shard → traffic falls through to Geospatial DB
- Risk: **Thundering herd** — sudden spike of DB queries

**Prevention:**
- Redis Cluster (3 primary + 3 replica minimum)
- Sentinel for automatic failover (~30s)
- **Request coalescing:** Only one request fetches from DB per cache key; others wait (mutex lock in cache layer)

**Scenario B: Redis cluster partition**
- Some keys unreachable
- Use consistent hashing → only keys on failed shard miss, not all keys

**Scenario C: Redis memory full**
- Eviction policy: `allkeys-lru` (evict least recently used)
- Monitor `used_memory` → alert at 80%

**Trade-off of caching:** Cache hit = fast, stale data. Cache miss = slow, fresh data. For proximity search, 60s staleness is fine. For business "is open now" — refresh more aggressively or skip cache.

---

## 4. Geospatial DB Failure

**Scenario: Primary fails**
- Replica promotion: PostgreSQL streaming replication, ~10s failover with Patroni
- Risk: writes during failover window are lost (RPO = replication lag, typically <1s)

**Read traffic during failover:**
- Route to replica immediately (read-only mode)
- Search still works; business updates queue up

**Scenario: Entire region down**
- Multi-region active-passive: US-East primary, US-West replica
- DNS failover → 60s downtime (acceptable for Yelp, not for emergency services)
- Trade-off: cross-region replication lag vs zero-downtime active-active (much harder)

**Scenario: Data corruption (bad business location written)**
- Geofencing validation: reject lat/lng outside valid range (-90 to 90, -180 to 180)
- Write-ahead log (WAL) → point-in-time recovery
- Soft deletes: `is_active = false` before hard deletes

---

## 5. Kafka / Index Update Queue Failure

**Scenario: Kafka broker down**
- Business update written to DB (sync), Kafka write fails
- Risk: geospatial index is stale — searches return old location

**Solution: Outbox pattern**
```
Transaction:
  1. Write to business DB
  2. Write to outbox table (same transaction)

Separate process:
  3. Read outbox → publish to Kafka → mark as sent
```
This ensures DB and Kafka never diverge. Even if Kafka is down, messages wait in outbox.

**Scenario: Index Updater consumer crashes mid-update**
- Kafka offset not committed → message redelivered
- Index update must be **idempotent**: `UPSERT on business_id` — safe to apply twice

**Scenario: Kafka partition lag grows**
- Search index falls behind reality by minutes/hours
- Alert on consumer lag > 10K messages
- Scale up consumers (Kafka partition count limits parallelism)
- Trade-off: more partitions = better parallelism but more broker overhead

---

## 6. Business DB Failure

**Scenario: Shard failure**
- Only businesses on that shard unreadable
- Search returns partial results (businesses from failed shard missing)
- Degrade gracefully: return available results with a warning header

**Scenario: Hot shard (NYC shard overloaded)**
- Symptoms: high latency on one shard only
- Short-term: add read replica, route reads to replica
- Long-term: split shard (geohash NYC → Manhattan + Brooklyn shards)
- Monitoring: per-shard QPS + latency dashboards

**Scenario: Cross-shard query (a business moved cities)**
- Rare but possible: business relocates, geohash changes
- Handle via: delete from old geohash index, insert to new
- Must be atomic at application level (DB doesn't span shards)
- Use saga pattern: mark old record inactive → create new → if step 2 fails, reactivate old

---

## 7. Geohash Edge Case Failures

**Problem: Business near geohash cell boundary**
```
User at cell boundary of "9q8y"
Business 400m away in cell "9q8x" (neighboring cell)
Query for cell "9q8y" only → business not returned!
```

**Fix: Always query 8 neighbors + current cell (9 total)**

**Problem: Varying geohash precision**
- Radius 5km needs geohash length 4 (39km cells) — too coarse, many false positives
- Radius 50m needs geohash length 8 — too fine, many cells to query

**Fix: Adaptive precision**
```python
def geohash_length(radius_m):
    if radius_m <= 100:   return 7
    if radius_m <= 1000:  return 6
    if radius_m <= 10000: return 5
    return 4
```
Always post-filter with exact haversine distance.

---

## 8. Cascading Failure Scenario

**Full cascade: Redis outage during peak traffic**

```
1. Redis cluster goes down (memory OOM, operator error)
2. All search requests → cache miss → hit Geospatial DB directly
3. Geospatial DB overwhelmed → connection pool exhausted → timeouts
4. Timeouts → retries → more load → DB falls over
5. Search Service returns 503 for all requests
```

**Prevention chain:**
1. **Circuit breaker** in Search Service: if DB error rate > 50% for 10s → open circuit, return cached/stale results or 503 fast-fail
2. **Rate limiting** at API Gateway: cap at 70% of known DB capacity
3. **Redis sentinel** prevents full Redis loss (single-node failure tolerated)
4. **Request coalescing**: deduplicate identical geohash queries in-flight

**Lesson from Foursquare 2010 outage:** MongoDB uneven shard load → one shard hot → cascade. Solution: pre-split shards before they fill up.

---

## 9. Data Staleness Failures

**Business closes permanently, stays in index**
- Nightly reconciliation job: scan all businesses, verify is_active flag
- User-reported "business closed" → soft-flag → manual review queue

**Business moves location**
- Location update → old geohash entry persists until TTL or active cleanup
- Index update must: DELETE old geohash entry + INSERT new geohash entry (in one Kafka message, processed atomically)

---

## Summary: Failure → Solution Map

| Component | Failure | Solution |
|-----------|---------|----------|
| API Gateway | Node failure | Multiple nodes + health check LB |
| Search Service | Bad deploy | Canary + auto-rollback |
| Redis | Node down | Cluster + Sentinel failover |
| Redis | Memory full | LRU eviction + capacity alerts |
| Geospatial DB | Primary fails | Streaming replica + Patroni |
| Geospatial DB | Region down | Multi-region passive replica |
| Kafka | Broker down | Outbox pattern |
| Index Updater | Crash | Idempotent UPSERT + retry |
| Business DB | Hot shard | Read replica + shard split |
| Geohash | Boundary miss | Always query 9 cells |
| Full cascade | Redis → DB overload | Circuit breaker + rate limit |
