# Ad Click Aggregator — Failure Analysis
**Date:** 2026-06-06  
**System:** Ad Click Aggregation  
**Approach:** Failure-first — every component, what breaks upstream/downstream

---

## Failure Map

```
[SDK] → [API Gateway] → [Ingest Service] → [Kafka] → [Flink] → [ClickHouse] → [Query Service]
                              │                                       │
                           [Redis]                                  [S3]
                           (dedup)                              (raw events)
```

---

## 1. SDK / Client Fails to Send Click

**Scenario:** Browser closes tab immediately after click (before POST completes). Mobile app is offline.

**Impact:** Click lost → advertiser undercharged (real revenue loss), user sees valid ad but click not counted.

**Detection:** Client-side — can't detect from server side.

### Solutions

**Beacon API (browser):**
```javascript
// navigator.sendBeacon fires even after page unload
navigator.sendBeacon('/v1/clicks', JSON.stringify(clickPayload));
// Browser queues and sends even if page closes
```

**Mobile offline queue:**
```
SDK stores click in local SQLite
Retry with exponential backoff when network available
click_id (UUID) ensures dedup on server — safe to retry
```

**Trade-off:** Beacon API doesn't support custom headers — use POST body for auth tokens. Mobile retry queue may deliver clicks hours later — Flink watermark must accommodate or route to late-event handler.

---

## 2. API Gateway Fails

**Scenario A:** Gateway crash — all ingestion stops.

**Impact:** All clicks dropped. Billing gap. Revenue loss.

**Prevention:**
- Multi-AZ deployment (AWS ALB spans AZs automatically)
- Health checks every 5s, auto-replace unhealthy instance
- Client SDK retries with exponential backoff (using same click_id for idempotency)

**Scenario B:** Gateway overloaded (traffic spike — Super Bowl ad goes viral).

**Impact:** 503 errors, clicks dropped.

**Prevention:**
- Rate limiting per IP at gateway (protect Ingest Service)
- Auto-scaling Ingest Service (trigger on Kafka consumer lag)
- Kafka as shock absorber — even if Ingest Service scales slowly, Kafka buffers

---

## 3. Ingest Service Fails

**Scenario A:** Ingest Service crashes mid-request (after Kafka publish, before dedup Redis write).

**Timeline:**
```
1. Receive click
2. Publish to Kafka ✓
3. CRASH — Redis SET never happens
4. Client retries same click_id
5. Redis: MISS (key was never set) → publish to Kafka again → DUPLICATE
```

**Impact:** Double-count in billing.

**Fix — Consumer-side dedup as second layer:**
```
Flink consumer:
  Before counting click:
    Check Cassandra: SELECT 1 FROM processed_clicks WHERE click_id = ?
    HIT → skip (already counted)
    MISS → count + INSERT click_id (TTL 7 days)
```

Two-layer dedup: Redis (fast, memory) + Cassandra (durable, persistent). Redis failure → Cassandra catches it.

**Scenario B:** Ingest Service rejects valid click (bug, bad validation).

**Impact:** Click dropped silently.

**Fix:**
- Dead-letter queue: failed events → Kafka topic `clicks-failed`
- Alert on DLQ depth > 0
- Replay DLQ after fix

---

## 4. Redis (Dedup Store) Fails

**Scenario:** Redis cluster goes down.

**Impact:** Dedup check fails → every click goes through without dedup → duplicates flood Kafka.

**Strategy: Fail open vs fail closed?**
- **Fail closed** (reject all clicks if Redis down) → miss real clicks, revenue loss. Bad.
- **Fail open** (bypass dedup, publish to Kafka anyway) → potential duplicates. Acceptable because Cassandra catches them downstream.

```
try:
    is_dup = redis.get(f"dedup:{click_id}")
    if is_dup: return 202  # already processed
except RedisUnavailable:
    # Fail open — let it through, Cassandra will dedup
    pass

kafka.publish(click_event)
```

**Additional protection:** Redis Cluster (3 primaries, 3 replicas). One node failure → cluster re-elects. Full cluster failure is rare; Cassandra layer handles it.

---

## 5. Kafka Fails

**Scenario A:** Kafka broker crash (1 of N brokers).

**Impact:** Partitions on that broker become unavailable temporarily.

**Prevention:**
- Replication factor = 3 (default: RF=1 is dangerous)
- `min.insync.replicas = 2`: write only succeeds if 2+ replicas acknowledge
- Kafka controller re-elects new leader within ~30s
- Producer: `acks=all` + retry with idempotent producer (`enable.idempotence=true`)

**Scenario B:** Kafka full (disk exhausted).

**Impact:** Producers block → Ingest Service backs up → timeouts → click loss.

**Fix:**
- Alert at 70% disk usage
- Tiered storage (Kafka → offload older segments to S3 automatically)
- Retention policy: 7 days or 5TB, whichever first

**Scenario C:** Consumer (Flink) falls behind — consumer lag grows.

**Impact:** Dashboard shows stale data. Advertiser sees incorrect real-time counts.

**Detection:**
```
Alert: kafka_consumer_lag{topic="raw-clicks"} > 1,000,000
```

**Fix:**
- Auto-scale Flink task managers
- Flink checkpointing: if Flink crashes, resumes from last checkpoint (no re-ingestion needed)
- SLA: "near-real-time" defined as ≤2min; lag alert fires if lag > 2min worth of events

---

## 6. Flink Stream Processor Fails

**Scenario:** Flink job crashes mid-window.

**Impact:** Partially aggregated window data lost → count gap → undercounting.

**Prevention — Flink Checkpointing:**
```
Every 30 seconds:
  Flink saves state (partial window aggregations) to S3
  If crash → restart from last checkpoint
  At-least-once delivery: events after checkpoint re-processed
  → Count-Min Sketch may slightly over-count (acceptable for dashboard)
```

**For billing:** Flink over/under-counts don't matter. Batch layer (S3 → Spark) is the source of truth. Billing always uses batch counts.

**Scenario: Flink processes event twice after restart.**
- SummingMergeTree in ClickHouse is idempotent for same (ad_id, window_start, window_size, country, device) key
- Over-writes just merge, don't double-count at query time

---

## 7. ClickHouse Fails

**Scenario:** ClickHouse node crash.

**Impact:** Aggregated counts unavailable → dashboard goes dark.

**Prevention:**
- ClickHouse cluster with replication (ReplicatedSummingMergeTree)
- ZooKeeper / ClickHouse Keeper for coordination
- Query service reads from any replica

**Scenario: ClickHouse write falls behind (Flink writes faster than CH can absorb).**

**Impact:** Aggregations delayed, dashboard lags.

**Fix:**
- ClickHouse native format batch writes (not row-by-row)
- Buffer table: writes go to in-memory buffer → flushed to actual table every 10s
- Alert on insert queue depth

---

## 8. S3 (Raw Event Store) Fails

**Scenario:** S3 write fails — raw event not persisted.

**Impact:** Can still aggregate in real-time (Kafka + Flink). But if batch layer needs to reprocess, event is missing → billing reconciliation gap.

**Prevention:**
- S3 is 11-nines durability — near impossible to lose written data
- Kafka 7-day retention acts as backup: if S3 write fails, Kafka has the event for 7 days
- Kafka→S3 connector (Kafka Connect S3 Sink): on failure, retries automatically

**Scenario: Batch job (Spark) reads stale/incomplete S3 data.**

**Prevention:**
- Batch job runs at 2 AM, 24h after data date — all events should be in S3 by then
- Late event Kafka topic (`clicks-late`) also flushed to S3 before batch job runs

---

## 9. Query Service Fails

**Scenario:** Query Service crash.

**Impact:** Dashboard, billing UI, top-K API all unavailable.

**Prevention:**
- Stateless service → horizontal scaling, any instance handles any request
- LB routes around failed instances
- ClickHouse queries are fast (<100ms) — no caching needed for most queries
- Top-K: Redis cache → if Query Service can't reach ClickHouse, serves stale top-K from Redis (with cache TTL caveat)

---

## 10. Clock Skew — The Silent Killer

**Scenario:** Ad server in Tokyo sends click with timestamp 500ms in the future (NTP drift). Flink assigns it to the wrong 1-minute window.

**Impact:** Click counted in wrong bucket → dashboard shows wrong spike, correct window undercounts.

**Fix:**
```
Flink watermark strategy: allow up to 30s out-of-order events
Accept events within ±30s of processing time
Events older than 30s → late event topic → batch correction

Ingestion Service:
  If |server_time - event_timestamp| > 5min → reject (likely clock error or replay attack)
```

**Two timestamps:**
```
AdClick:
  event_time    ← client-reported (may drift, use for windowing)
  ingest_time   ← server-set (trusted, use for fraud/ordering)
```

---

## 11. Cascade Failure Scenario

**"Super Bowl Ad goes viral — 100× normal traffic"**

```
Phase 1: Spike hits API Gateway → Ingest Service overwhelmed
  → Ingest Service autoscales (2min lag)
  → Kafka buffers the backlog (10-20min of events)

Phase 2: Flink consumer lag grows
  → Dashboard shows stale data (up to 20min behind)
  → Alert fires → Flink task managers autoscale

Phase 3: ClickHouse write throughput spikes
  → Buffer tables absorb burst
  → Disk I/O becomes bottleneck
  → Horizontal shard: add ClickHouse shard for this campaign

Recovery:
  → Kafka retention means no events lost during backlog
  → Once Flink catches up, dashboard becomes current
  → Batch job still accurate (uses S3, not Flink)

SLA impact:
  → Real-time dashboard: degraded (stale by up to 20min)
  → Billing: unaffected (batch layer)
  → Click ingestion: no data loss (Kafka buffered)
```

**Key insight:** Design so that ingestion never fails even when processing lags. Kafka is the safety valve.

---

## Failure Prevention Summary

| Component | Primary Risk | Prevention | Recovery |
|-----------|-------------|------------|---------|
| SDK | Click lost on page close | Beacon API + mobile queue | Client retry with same click_id |
| API Gateway | Overload | Rate limiting + autoscale | Client retry |
| Ingest Service | Crash mid-dedup | Consumer-side Cassandra dedup | Idempotent retry |
| Redis dedup | Crash | Fail open + Cassandra layer | Cassandra catches duplicates |
| Kafka | Broker crash | RF=3, min.insync=2, idempotent producer | Auto leader election |
| Flink | Job crash | Checkpointing to S3 every 30s | Restart from checkpoint |
| ClickHouse | Node crash | Replication + any-replica reads | Auto failover |
| S3 | Write failure | Kafka 7-day retention as backup | Replay from Kafka |
| Clock skew | Wrong window assignment | 30s watermark + late event topic | Batch correction |
