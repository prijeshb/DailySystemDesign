# Ad Click Aggregation System Design
**Date:** 2026-06-06  
**Difficulty:** Hard  
**Real-world refs:** Google Ads, Meta Ads, Amazon Advertising, Criteo

---

## 0. First Principles — Do We Even Need This?

**The problem:** Advertisers pay per click. They need:
1. Real-time dashboards ("how many clicks in the last 5 min?")
2. Billing (charge correctly — can't miss or double-count)
3. Fraud detection (detect click farms immediately)
4. Campaign analytics (which ad performed best today?)

**Why not just query the DB directly?**
- 10B clicks/day = ~115K clicks/sec at peak
- `SELECT COUNT(*) WHERE ad_id=X AND time > now()-5min` on raw events = table scan on billions of rows = 💀
- We need **pre-aggregated** results, not raw queries

**Why not just increment a counter per click?**
- Works at small scale, but fails under:
  - Network failures → lost counts
  - Duplicate delivery → inflated counts
  - Multi-region → counter divergence
- We need a pipeline that is **fault-tolerant**, **exactly-counted**, and **queryable at multiple time granularities**

---

## 1. Scope & Requirements

### Functional
- Record every ad click (ad_id, user_id, timestamp, geo, device)
- Query click counts by: `ad_id`, `time window` (last 1m / 5m / 1h / 1d)
- Query top-K most-clicked ads in a time window
- Support filtering by: country, device type, campaign_id
- Aggregated data available within **≤1 minute** of click event

### Non-Functional
- **Scale:** 10 billion clicks/day → ~115K clicks/sec (peak: 3×)
- **Availability:** 99.99% for click ingestion (losing clicks = lost revenue)
- **Latency:** Query response < 1s for aggregated data
- **Exactly-once counting** for billing (duplicates cost money, drops cost money)
- **Idempotent ingestion** — retries must not double-count

### Out of scope
- Ad serving (which ad to show) — separate system
- User targeting / ML — separate system
- Click-through rate prediction

---

## 2. Entities

```
AdClick
  id            UUID (client-generated, idempotency key)
  ad_id         UUID
  campaign_id   UUID
  user_id       UUID (nullable for anonymous)
  timestamp     epoch_ms
  ip_address    string
  country       string (ISO 3166)
  device        enum(MOBILE, DESKTOP, TABLET)
  user_agent    string
  referrer_url  string

Ad
  id            UUID
  campaign_id   UUID
  advertiser_id UUID
  name          string

Campaign
  id            UUID
  advertiser_id UUID
  budget_usd    decimal
  status        enum(ACTIVE, PAUSED, ENDED)

AggregatedClickCount  (materialized, not raw)
  ad_id         UUID
  window_start  timestamp
  window_size   enum(1m, 5m, 1h, 1d)
  count         bigint
  country       string (nullable for non-filtered)
  device        string (nullable)
```

---

## 3. Actions / API

```
# Click Ingestion (called by ad-serving SDK in browser/app)
POST /v1/clicks
  Body: { click_id, ad_id, campaign_id, user_id?, timestamp, geo, device, referrer }
  Response: 202 Accepted  (async — don't block the user's page load)
  Idempotency: click_id is client-generated UUID, server deduplicates

# Query aggregated counts
GET /v1/stats/clicks
  ?ad_id=X&window=5m&end_time=now
  → { ad_id, count, window_start, window_end }

GET /v1/stats/clicks/top-k
  ?k=10&window=1h&campaign_id=Y
  → [{ ad_id, count }, ...]

# Internal (billing)
GET /v1/billing/clicks/daily
  ?advertiser_id=X&date=2026-06-06
  → { advertiser_id, ad_breakdown: [{ad_id, count, cost}] }
```

---

## 4. Data Flow

```
[Browser / App SDK]
      │
      │  POST /clicks (fire-and-forget, async)
      ▼
[API Gateway + Load Balancer]
      │
      ▼
[Click Ingestion Service]
  - Validate (ad_id exists, not expired campaign)
  - Deduplicate (check click_id in Redis, TTL=24h)
  - Write to Kafka topic: "raw-clicks"
      │
      ▼
[Kafka: raw-clicks]  ← partitioned by ad_id (ensures ordering per ad)
      │
      ├──────────────────────────────────────────────┐
      ▼                                              ▼
[Stream Processor]                         [Raw Event Store]
(Flink / Spark Streaming)                  (Kafka → S3/HDFS)
  - Aggregate by ad_id + time window         stores raw events
  - Compute sliding + tumbling windows       for batch reprocessing
  - Write to TimeSeries DB (ClickHouse)      and audit
      │
      ▼
[ClickHouse / TimeSeries DB]
  - Stores: ad_id, window_start, count, filters
  - Queried by dashboard, billing, top-K API
      │
      ▼
[Query Service]  ←── serves dashboard + billing APIs
```

### Why this flow?
- **Kafka as buffer:** Ingestion Service never blocks on DB — decouples click capture from aggregation
- **Stream processor:** Near-real-time aggregation (< 1min latency)
- **Raw events also persisted:** Enables reprocessing if aggregation has a bug (correctness > storage cost)

---

## 5. High Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│                                                                 │
│  Browser/App SDK  ──►  API Gateway  ──►  Click Ingest Service   │
│                                              │  (dedupe+validate)│
│                                              │                   │
│                                         Kafka: raw-clicks        │
└──────────────────────────────────────────────┼──────────────────┘
                                               │
               ┌───────────────────────────────┤
               │                               │
               ▼                               ▼
┌──────────────────────┐            ┌──────────────────────────┐
│   SPEED LAYER        │            │   BATCH LAYER            │
│   (Flink Streaming)  │            │   (Spark / nightly job)  │
│                      │            │                          │
│  Tumbling windows:   │            │  Reads raw events from   │
│  1m, 5m aggregates   │            │  S3, recomputes exact    │
│                      │            │  daily/monthly totals    │
│  Writes to:          │            │  (for billing accuracy)  │
│  ClickHouse (fast)   │            │  Writes to:              │
│                      │            │  Postgres (billing DB)   │
└──────────┬───────────┘            └──────────────────────────┘
           │
           ▼
┌──────────────────────┐
│  SERVING LAYER       │
│                      │
│  Query Service       │
│  ├── Dashboard API   │
│  ├── Billing API     │
│  └── Top-K API       │
│                      │
│  Redis (top-K cache) │
└──────────────────────┘
```

**Lambda Architecture:** Two parallel paths:
- **Speed layer** = approximate, low-latency (stream processing, available in <1min)
- **Batch layer** = exact, high-latency (batch reprocessing, available next day)
- **Serving layer** merges both: dashboard uses speed layer; billing uses batch layer

**Trade-off:** Lambda adds operational complexity (two codebases). Kappa Architecture (stream-only, replay from Kafka) is simpler but requires Kafka retention to be long (expensive). For billing accuracy, Lambda wins here.

---

## 6. Low Level Design

### 6.1 Click Ingestion Service — Deduplication

```
POST /v1/clicks
  body: { click_id: "uuid-abc", ad_id: "X", ... }

1. Check Redis:
   GET dedup:{click_id}
   → HIT?  → return 202 (already processed, idempotent)
   → MISS? → continue

2. Publish to Kafka:
   producer.send("raw-clicks", key=ad_id, value=click_event)
   → On Kafka ack → SET dedup:{click_id} "1" EX 86400

3. Return 202 Accepted
```

**Why Redis for dedup, not DB?**
- 115K/sec writes — Redis handles this; Postgres would struggle
- TTL cleanup is automatic
- We only need dedup for 24h (same click can't be retried after 24h)

**Trade-off:** Redis is in-memory. If Redis crashes before we set the key, duplicate might slip through → Kafka consumer-side dedup as second layer (check Cassandra before writing to ClickHouse).

### 6.2 Kafka Partitioning Strategy

```
Partition key = ad_id

Why?
- All events for ad_id=X go to same partition → same consumer
- Aggregation for ad_id=X happens in one place → no cross-partition merge needed
- Ordering guaranteed per ad_id within partition

Risk: Hot partition if one ad gets viral traffic
Fix: Composite key = ad_id + shard_id (0-9, random)
     Aggregation layer merges 10 shards → final count
```

### 6.3 Flink Stream Processing — Time Windows

```
Tumbling Window (non-overlapping):
  ├── 1-minute buckets: [10:00:00-10:00:59], [10:01:00-10:01:59]
  └── Use: real-time dashboard, fraud detection

Sliding Window (overlapping):
  ├── "last 5 minutes" updated every 1 minute
  └── Use: "clicks in last 5m" query

Session Window:
  └── User activity burst, gap-based — not needed here
```

```java
// Flink pseudo-code
DataStream<Click> clicks = kafkaSource("raw-clicks");

clicks
  .keyBy(click -> click.getAdId())
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(new ClickCountAggregator())
  .addSink(clickHouseSink);

// Late events (up to 30s) handled via watermark
clicks.assignTimestampsAndWatermarks(
  WatermarkStrategy.<Click>forBoundedOutOfOrderness(Duration.ofSeconds(30))
)
```

**Late data problem:** Clicks can arrive late (mobile offline → reconnects). Flink watermark allows 30s late arrival. After that, late events are routed to a "late events" Kafka topic → batch job picks them up and corrects daily totals.

### 6.4 ClickHouse Schema (TimeSeries DB)

```sql
-- Raw aggregated table (stream processor writes here)
CREATE TABLE click_counts (
  ad_id        UUID,
  campaign_id  UUID,
  window_start DateTime,
  window_size  Enum8('1m'=1, '5m'=5, '1h'=60, '1d'=1440),
  country      LowCardinality(String),
  device       LowCardinality(String),
  count        UInt64
) ENGINE = SummingMergeTree()
  PARTITION BY toYYYYMM(window_start)
  ORDER BY (ad_id, window_start, window_size, country, device);
```

**Why ClickHouse?**
- Columnar storage → blazing-fast aggregation queries (sums, group-by)
- SummingMergeTree: if two rows have same ORDER BY key → automatically sums count column (idempotent writes)
- Compression: 10B rows/day ≈ ~200GB/day raw; ClickHouse compresses 5-10x → manageable

**Trade-off vs Postgres:** ClickHouse can't do JOINs well, can't do point lookups efficiently. Keep billing reconciliation in Postgres; use ClickHouse only for aggregation queries.

### 6.5 Top-K Most Clicked Ads

**Naive approach:** `SELECT ad_id, SUM(count) ... GROUP BY ad_id ORDER BY SUM DESC LIMIT 10`
→ Fine for hourly/daily. Too slow if called every second.

**Better approach — Count-Min Sketch + Redis Sorted Set:**

```
On each click event (in Flink):
  1. Update Count-Min Sketch (approximate frequency estimation)
  2. If sketch estimate for ad_id > threshold → ZINCRBY topk:1h {ad_id} 1

Redis:
  ZREVRANGE topk:1h 0 9 WITHSCORES → top 10 ads (in-memory, O(log N))
  TTL = 1h (auto-expire old windows)
```

**Count-Min Sketch:** Probabilistic structure, O(1) update, never under-counts, may slightly over-count. For top-K advertising dashboards, slight over-count is acceptable (vs exact but slow).

**Trade-off:** If exact top-K needed (billing) → ClickHouse query nightly. For dashboard, sketch is fine.

### 6.6 Billing — Batch Layer (Exact Counts)

```
Nightly Spark job:
1. Read raw events from S3 (Parquet, partitioned by date)
2. Deduplicate by click_id (exact)
3. Aggregate: COUNT per ad_id per day
4. Write to Postgres billing table
5. Reconcile against stream counts (alert if >0.1% discrepancy)

Reconciliation query:
  SELECT 
    b.ad_id,
    b.stream_count,
    b.batch_count,
    ABS(b.stream_count - b.batch_count) / b.batch_count AS discrepancy_pct
  FROM daily_reconciliation b
  WHERE discrepancy_pct > 0.001  -- alert if >0.1%
```

This is why we persist raw events to S3 — ground truth for billing disputes.

---

## 7. Key Trade-offs

| Decision | Chosen | Alternative | Why chosen |
|----------|--------|-------------|------------|
| Stream processor | Apache Flink | Spark Streaming | Flink: true streaming (row-by-row), lower latency; Spark: micro-batch (1-2s delay) |
| Aggregation DB | ClickHouse | Druid / TimescaleDB | ClickHouse: simpler ops, better compression, SummingMergeTree auto-merges |
| Top-K | Count-Min Sketch + Redis | Exact ClickHouse query | Sketch: O(1), ~1s latency; Exact: expensive per call |
| Dedup | Redis (fast) + Cassandra (durable) | DB-only | Redis handles burst; Cassandra gives durability |
| Architecture | Lambda (speed + batch) | Kappa (stream only) | Billing needs exact counts; batch layer provides correctness guarantee |
| Partition key | ad_id (+ shard suffix) | user_id | ad_id groups data for aggregation; user_id would scatter it |

---

## 8. Scale Estimates

```
Clicks/day:        10 billion
Clicks/sec (avg):  115,000
Clicks/sec (peak): 345,000 (3× spike)

Kafka:
  Partitions = 345K / 1K (per partition) = 345 → round to 512 partitions
  Retention: 7 days (for reprocessing)
  Storage: 10B × 200 bytes = 2TB/day × 7 = 14TB

ClickHouse:
  Rows/day (aggregated, 1m windows): 10B clicks / avg 100 clicks per ad per min ≈ manageable
  Actually: #ads × windows × dimensions ≈ 1M ads × 1440 min × few dims = ~2B rows/day
  With 10× compression: ~40GB/day

S3 raw events:
  10B × 200 bytes = 2TB/day (Parquet ≈ 400GB/day compressed)

Redis top-K:
  1M ads × 8 bytes score = 8MB per window → negligible
```

---

## 9. Fraud Detection (Bonus)

Click fraud = bots clicking ads to drain advertiser budget.

```
Real-time signals (Flink, same pipeline):
  - Same IP + ad_id > 10 clicks/min → flag
  - User-agent pattern matching (known bot signatures)
  - Click velocity per device_id
  - Geographic anomaly (100K clicks from same /24 subnet)

Action:
  → Publish to "fraud-signals" Kafka topic
  → Fraud Service reads, marks click as INVALID in Redis
  → Billing batch job ignores INVALID clicks
  → Advertiser not charged for fraudulent clicks
```

**Key insight:** Don't drop clicks at ingestion (might be wrong). Mark as suspicious, exclude from billing. Keep raw event for audit.
