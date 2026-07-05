# E-Commerce Search — Failure Analysis
**Date:** 2026-06-14

---

## Failure Taxonomy

```
Components that can fail:
  A. Elasticsearch node / cluster
  B. MongoDB (product DB)
  C. Kafka / Debezium (index pipeline)
  D. Redis (search cache)
  E. Search Service (application)
  F. Index Consumer Service
  G. Load Balancer

Dependency chain:
  User → LB → Search Service → Redis (cache)
                             → ES Cluster (query)
                             → MongoDB (product details enrichment)

  MongoDB → Debezium → Kafka → Index Consumer → ES
```

---

## A. Elasticsearch Node Failure

### Scenario A1: Single Data Node Fails (Most Common)

```
ES Cluster: 18 nodes (6 primary shards × 2 replicas = 18 shards)
Node-7 dies (hardware failure, OOM)

What ES does automatically:
  1. Master node detects Node-7 unresponsive via Zen discovery (~30s default)
  2. Promotes 2 replica shards (on other nodes) to primary
  3. Allocates new replicas on remaining nodes
  4. Cluster status: RED (missing replicas) → YELLOW → GREEN as replicas rebuild

Impact during recovery (~5-10min):
  - 2 shards served by replicas: same latency, no data loss
  - Cluster temporarily under-replicated (only 1 copy of some shards)
```

**Prevention:**
- `min_master_nodes = (total_eligible_masters / 2) + 1` → prevents split-brain
- `discovery.zen.ping_timeout: 30s` → tune down to 10s to detect faster
- Rack awareness: `cluster.routing.allocation.awareness.attributes: rack` → replica never on same rack as primary

**Trade-off of faster failure detection:**
- Shorter timeout = detect real failures faster but may false-trip on GC pauses
- Java GC pause can be 20-30s on JVM heap pressure → tune GC first (G1GC, 30GB heap max)

---

### Scenario A2: ES Master Node Fails

```
ES maintains 3 dedicated master-eligible nodes (not data nodes)
Master election via Raft-like consensus

Master fails:
  1. Remaining 2 masters hold election → new master elected in ~5-30s
  2. During election: cluster is "read-only" — queries still work (data nodes serve)
                      writes are rejected
  3. After election: normal operation resumes

Impact: 5-30s write rejection (index updates fail, buffered in Kafka)
        Reads unaffected
```

**Why dedicated master nodes?**
- Masters handle cluster metadata (shard allocation, node membership)
- If master is also data node: heavy indexing causes GC pause → master unresponsive → false split-brain
- Dedicated masters: small instances (2 vCPU, 8GB), negligible cost

---

### Scenario A3: Full Elasticsearch Cluster Unreachable

```
Causes: AZ-wide network partition, datacenter failure, deployment gone wrong

Search Service behavior:
  Circuit Breaker (wrapping ES calls):
    5 consecutive failures → OPEN
    All searches: fail fast, hit fallback

Fallback layers:
  1. Redis has cached recent popular queries → serve stale results
     (stale-if-error: serve cache even past TTL during ES outage)
  2. Redis empty (cold start) → return "popular products" list
     (pre-cached top-500 products by sales, refreshed daily)
  3. Redis also down → return empty results + "search temporarily unavailable" message
     (never show error stack trace to user)

Circuit breaker probe: every 10s, attempt one ES ping
On success → circuit HALF-OPEN → gradual traffic restoration
```

**Trade-off of "serve stale cache":**
- Users get search results (good UX) but potentially stale (price, stock may differ)
- Acceptable: user clicks product → product page fetches real-time data from MongoDB
- Never serve stale inventory as final truth — always verify at cart/checkout

---

### Scenario A4: ES Heap Pressure / Slow Queries

```
Symptom: P99 search latency spikes from 50ms → 5000ms (not hard failure but degradation)

Root cause analysis:
  1. Too many concurrent "expensive" queries (large aggregations + many facets)
  2. Index too large for available heap (field data cache evictions)
  3. Segment merge storm (too many segments after bulk indexing)

Diagnosis:
  ES Slow Query Log: threshold_ms = 500 → logs queries taking >500ms
  ES Stats API: /_nodes/stats → check jvm.mem.heap_used_percent

Mitigation:
  1. Query timeout: add "timeout": "200ms" to all ES queries
     → partial results returned rather than blocking indefinitely
  2. Reduce aggregation cardinality: limit "size" on term aggregations to 20 (not 1000)
  3. Force merge old indices: POST /products/_forcemerge?max_num_segments=1
     → reduces segment count → faster queries
  4. Add result cache: heavy facet queries cached in Redis by query hash
```

---

## B. MongoDB (Product DB) Failure

### Scenario B1: MongoDB Primary Fails

```
MongoDB Replica Set: 1 Primary + 2 Secondaries
Primary fails:

  Election (via Raft):
    - Secondaries detect primary absence (heartbeat timeout ~10s)
    - Secondary with highest oplog → wins election → becomes new primary
    - Election time: ~10-30s

  During election:
    - All writes rejected (no primary)
    - Reads from secondaries still work (if ReadPreference = secondary)

Impact on search system:
  - Index Consumer: can't read new product updates → Kafka messages buffered
  - Search Service: product detail enrichment reads fail
  
  Search Service behavior:
    - Circuit breaker on MongoDB calls → fallback: serve cached product from Redis
    - Cache TTL for product details: 60s → most enrichments served from cache
```

**Key:** MongoDB failure does NOT break search queries (ES serves those independently).  
Only new product updates stop indexing and product detail enrichment is stale.

---

### Scenario B2: Debezium / CDC Pipeline Fails

```
Debezium is a single service reading MongoDB oplog and publishing to Kafka.

Debezium crashes:
  - MongoDB oplog is a capped collection (rolling window, typically 24-48h)
  - Debezium stores last read oplog position in Kafka Connect offsets
  - On restart: Debezium resumes from stored offset (no data loss if < oplog window)

Risk: if Debezium is down for > oplog retention window (rare: >48h):
  → Oplog rotated past last read position
  → Must do full snapshot of MongoDB → re-publish all docs to Kafka

Prevention:
  - Monitor Debezium lag: alert if last_event_age > 60s
  - Increase oplog retention: MongoDB oplogSizeMB or TTL
  - Run 2 Debezium instances (active-passive): Kafka Connect handles failover
```

---

## C. Kafka Failure

### Scenario C1: Kafka Broker Fails

```
Kafka: 6 brokers, replication factor 3

1 broker fails:
  - Leader partitions on failed broker: ISR (in-sync replica) takes over as leader
  - Election: ~5-10s
  - During election: producers buffer in-memory (configurable: max.block.ms=60s)
  - No message loss (already replicated to 2 other brokers)

Impact: up to 10s of product updates not published → accumulated in buffer → sent when leader elected
```

### Scenario C2: Index Consumer Lag (Slow Consumption)

```
Cause: ES overwhelmed → bulk indexing timeouts → consumer backs off → lag grows

Symptoms:
  - product.changes consumer lag grows
  - Product updates visible in MongoDB but not in search for >30s

Monitoring:
  Kafka consumer lag alert: lag > 10,000 messages (>5s behind at 2K msg/s)

Mitigation:
  - Scale out Index Consumer: add more consumer instances (up to partition count)
  - Reduce ES bulk size: 500 → 100 docs per batch (less timeout risk)
  - ES circuit breaker: if ES bulk fails → exponential backoff + Kafka offset not committed
    → same docs re-processed on recovery (ES upsert = idempotent)
  
Consumer idempotency:
  ES upsert by product_id is idempotent:
    PUT /products/_doc/{product_id}  → overwrite if exists, create if not
    Processing same message twice → same final state. Safe.
```

---

## D. Redis Cache Failure

### Scenario D1: Redis Node Fails (Redis Sentinel Setup)

```
Redis Sentinel setup: 1 master + 2 replicas, 3 Sentinel nodes

Master fails:
  - Sentinels detect (down-after-milliseconds: 5000)
  - Majority vote (2 of 3 Sentinels agree)
  - Promote replica to master
  - Failover: ~10-30s

During failover:
  - All cache reads/writes fail
  - Search Service: all queries go to ES directly
  - ES can handle: 100K RPS at full load was designed for; cache normally absorbs ~60%
    → ES under 2.5× load temporarily
```

**Critical trade-off:** Redis failure → 2.5× spike on ES → ES may slow down → user sees high latency.

**Mitigation:**
- ES replica nodes: auto-balance search load
- Query timeout: 200ms → prevent thread pile-up
- Rate limiting at API Gateway: cap at 80K RPS during degraded mode (shed 20% traffic)
- Monitor: P99 > 300ms → alert ops

### Scenario D2: Cache Stampede on Cache Restart

```
Cache restarts (or all keys expired simultaneously) → 100K req/s all hit ES
Thundering Herd

Fix: Probabilistic Early Expiration (PER)
  Instead of all keys expiring exactly at TTL, each read slightly extends TTL:
    remaining_ttl = actual_ttl - (random() * delta)
  So keys don't all expire at same instant.

Plus: Cache Mutex (same as Ticketmaster playbook):
  On miss:
    SET rebuild_lock:{key} NX EX 2  → only 1 request rebuilds
    Others: wait 50ms → retry → served from cache
```

---

## E. Search Service Crash

```
Search Service is stateless (no local state) → crash = restart

If all instances crash (bad deploy):
  LB health checks fail → routes 0 traffic (rather than hitting dead instances)
  K8s: deployment rollback triggers automatically (readiness probe fails)
  
Rolling deploy strategy (always):
  Replace 1 instance at a time
  New instance must pass readiness probe before old instance killed
  If readiness fails → rollout paused → alert
```

---

## F. Partial Failure: ES Returns Partial Results

```
Scenario: 1 of 6 primary shards unreachable during query

ES behavior:
  - Query times out waiting for that shard
  - Returns results from 5/6 shards (partial)
  - Response includes: "_shards": {"total": 6, "successful": 5, "failed": 1}

Search Service behavior:
  - Log the partial shard failure
  - Return results to user (5/6 = ~83% of products still searched)
  - Better than: returning error and 0 results

User sees: slightly reduced result count (rare, ~16% products missing)
           virtually undetectable in practice
```

---

## G. Upstream: Load Balancer Fails

```
Multi-LB with DNS-based failover:
  - 2 LBs active-active
  - DNS TTL = 30s → fast cutover
  - If one LB fails: DNS removes it from rotation

On LB failure:
  - 30s of some users getting DNS-cached old LB IP
  - Those requests fail → client retry → resolved to healthy LB
  - Impact: <30s of elevated errors, ~0.1% requests dropped
```

---

## Failure Summary Matrix

| Component | Detection Time | Recovery Time | Impact | Mitigation |
|-----------|---------------|---------------|--------|------------|
| ES data node | 10-30s | 5-10min (replica rebuild) | Query latency +20% | Rack awareness, 2 replicas |
| ES master node | 5-30s | 5-30s | Writes rejected, reads OK | 3 dedicated masters |
| ES cluster total | 5s (circuit breaker) | Ops-dependent | Serve stale/popular cache | Circuit breaker + Redis fallback |
| MongoDB primary | 10-30s (election) | 10-30s | Write fail, reads from secondary | Replica set, cache enrichment |
| Debezium | Immediate | Minutes (restart) | Index lag grows | Monitoring, resume from offset |
| Kafka broker | 5-10s | 5-10s (leader election) | Brief producer block | RF=3, ISR |
| Index Consumer | Immediate | Minutes (restart) | Index lag grows | Idempotent upserts, alert on lag |
| Redis | 5-30s (sentinel) | 10-30s | Cache miss storm → ES spike | PER, mutex, rate limit |
| Search Service | Immediate (LB probe) | Seconds (K8s restart) | Rolling deploy unaffected | Stateless, rolling deploys |

---

## Key Failure Design Principles Applied

1. **ES is secondary** — MongoDB is truth. ES can rebuild from Kafka/MongoDB. Never the reverse.
2. **Circuit breaker everywhere** — ES calls, MongoDB calls, Redis calls all wrapped. Fail fast, fallback gracefully.
3. **Idempotent writes** — ES upsert by product_id. Kafka re-delivery safe.
4. **Defense in depth** — Redis cache → ES → popular products fallback → degraded message. Never a blank page.
5. **Observe before act** — Slow query log, consumer lag metrics, ES heap monitoring. Alert before crash.
