# Ad Click Aggregator — Interview Q&A
**Date:** 2026-06-06  
**System:** Ad Click Aggregation  
**Format:** Interviewer prompt → strong answer → common follow-ups

---

## Opening Questions

**Q: Design an ad click aggregation system.**

Strong opening move:
> "Before diving in — let me clarify scope. Are we focusing on click ingestion, real-time aggregation, billing, or all three? And at what scale — 10M or 10B clicks/day? The architecture differs significantly."

Then establish:
- 10B clicks/day
- Need real-time dashboard (<1min latency) AND accurate billing
- Dedup/idempotency required (billing can't double-count)

---

## Section 1 — Ingestion

**Q: How do you handle a user clicking the same ad multiple times?**

> "Two layers of deduplication. First, the client SDK generates a UUID (click_id) per click intent and sends it with every retry. The ingestion service checks Redis — if click_id already seen, return 202 and skip. If Redis is unavailable (fail open), the Flink consumer checks Cassandra before counting. Cassandra is the durable fallback — slightly slower but survives Redis restarts. TTL is 24h on both — no legitimate click retry survives that long."

Follow-up: **Why client-generated UUID, not server-generated?**
> "Server-generated requires an extra round trip. If the network fails before the client receives the key, the problem shifts one level up — now the client doesn't know its key and can't deduplicate. Client-generated UUID is pre-computed before the first attempt, so every retry carries the same key from the start."

---

**Q: What if two different users click the same ad at the same time?**

> "Two different users = two different click_ids (UUID). They're independent events and both get counted. Dedup is per-intent, not per-ad. This is the correct behavior — two separate clicks should count twice."

---

**Q: How do you handle mobile users who are offline when they click?**

> "SDK queues the click in local SQLite with the same click_id. When connectivity restores, it retries with exponential backoff. The server deduplicates using the click_id, so even if the retry comes 2 hours later, it's only counted once. One caveat: Flink's late-event watermark is typically 30 seconds. An event arriving 2 hours late goes to a late-events Kafka topic and is processed by the nightly batch job, not the stream layer. Dashboard might not show it in real-time, but billing will be accurate."

---

## Section 2 — Stream Processing

**Q: How would you compute 'clicks in the last 5 minutes'?**

> "Sliding window in Flink. A 5-minute window that slides every 1 minute produces 5 overlapping buckets at any point. Each bucket holds a partial aggregate. When a user queries 'last 5 minutes,' we sum the relevant buckets. In ClickHouse, this means querying rows WHERE window_start >= now()-5min AND window_size='1m' and summing counts. The stream processor writes 1-minute tumbling window aggregates; the query layer computes sliding views over them."

Follow-up: **Why not compute sliding windows in Flink directly?**
> "You can — Flink supports sliding windows natively. But storing pre-computed 1-minute tumbling windows gives flexibility: you can reconstruct any window size at query time without re-running the stream job. Trade-off is that query-time aggregation is slightly slower, but ClickHouse's columnar engine handles this in milliseconds."

---

**Q: What is a watermark and why do you need it?**

> "A watermark is Flink's best estimate of 'all events up to time T have arrived.' It's needed because events can arrive out of order — a mobile click from 5pm might arrive at 5:01pm due to network delay. Without watermarks, Flink doesn't know when to close a time window. Setting watermark = event_time minus 30 seconds tells Flink: 'wait 30 extra seconds for late arrivals before finalizing the window.' Events later than that go to a late-event handler rather than being silently dropped."

---

**Q: What happens if your Flink job crashes?**

> "Flink checkpoints its state — partial window aggregates — to S3 every 30 seconds. On restart, it replays from the last checkpoint. Events between the checkpoint and crash are re-read from Kafka (which has them, since Kafka consumer offsets weren't committed past the checkpoint). This gives at-least-once processing. For dashboard, slight over-count during recovery is acceptable. For billing, we use the batch layer (Spark on S3 raw events) which is independent of Flink's state."

---

## Section 3 — Storage & Querying

**Q: Why ClickHouse instead of Postgres for aggregated data?**

> "At our scale, we're writing millions of aggregated rows per day and running queries like 'sum of clicks for ad_id X across all 1-minute windows in the last hour.' Postgres is row-oriented — it reads entire rows even if you only need the count column. ClickHouse is columnar — it reads only the count column, which is orders of magnitude faster for aggregations. SummingMergeTree is a bonus: if Flink writes the same (ad_id, window_start) row twice (e.g., after a crash/restart), ClickHouse merges them by summing counts rather than duplicating — idempotent by design."

Follow-up: **What's the downside of ClickHouse?**
> "ClickHouse is terrible at JOINs and point lookups (fetch one specific row by ID). It's also operationally more complex than Postgres. For billing reconciliation where we need to JOIN clicks with campaign budgets and advertisers, I'd keep that in Postgres and use ClickHouse only as the aggregation store."

---

**Q: How do you compute top-K most-clicked ads efficiently?**

> "Two approaches: approximate and exact. For real-time dashboard, I use a Count-Min Sketch in Flink to estimate frequencies, then push top candidates into a Redis Sorted Set keyed by time window. ZREVRANGE returns top-K in O(log N). For exact billing-level top-K, the nightly Spark batch job computes it precisely from raw S3 events. The approximate approach trades accuracy for O(1) updates — for a dashboard showing 'top 10 ads this hour,' a 1-2% overcount is fine."

Follow-up: **What is a Count-Min Sketch?**
> "It's a probabilistic data structure — a 2D array of counters with multiple hash functions. On each element, you increment d counters (one per hash function). To estimate frequency, take the minimum across those d counters. It never undercounts, may slightly overcount due to hash collisions. Space is O(d × w) regardless of how many distinct elements — very memory efficient for tracking millions of ad_ids."

---

## Section 4 — Architecture

**Q: Why Lambda architecture? Isn't it complex to maintain two codebases?**

> "It is complex — that's the real trade-off. The reason I choose Lambda here is billing accuracy. Stream processing with Flink gives us near-real-time results (dashboard), but Flink can over-count during restarts or when watermarks cause late event issues. For billing, we cannot over/undercharge — even 0.01% error on 10B clicks/day = 1M clicks = real money. The batch layer (Spark on raw S3 events) is the source of billing truth. It's slower (runs nightly) but exact. If billing accuracy weren't a requirement, I'd use Kappa Architecture — stream only, replay from Kafka for corrections — much simpler."

Follow-up: **What is Kappa Architecture?**
> "Kappa removes the batch layer. Instead of S3 + Spark for corrections, you replay raw events from Kafka. When you discover a bug in your stream logic, you fix it, create a new Kafka consumer group, and replay from the beginning. The new output overwrites the old. Simpler operationally (one codebase), but requires long Kafka retention (expensive) and replay time can be hours for weeks of data."

---

**Q: How do you handle hot partitions in Kafka (one viral ad getting 90% of traffic)?**

> "Hot partition problem: if partition key = ad_id, one viral ad maps to one partition → that partition's consumer is bottlenecked. Fix: composite key = ad_id + random_shard (0-9). This distributes one ad's clicks across 10 partitions. The Flink aggregation layer merges the 10 shards' counts to get the final total. Trade-off: slightly more complex aggregation logic, but eliminates the hot partition bottleneck."

---

## Section 5 — Failure & Edge Cases

**Q: How do you detect and handle click fraud?**

> "Real-time signals in the same Flink pipeline: same IP clicking same ad >10 times/minute, known bot user-agent strings, click velocity anomalies, geographic clustering (1000 clicks from one /24 subnet). Important design choice: don't drop suspicious clicks at ingestion — you might be wrong. Instead, mark them as SUSPICIOUS in a side-stream and write to a fraud-signals Kafka topic. The fraud service makes the final call. Billing batch job excludes INVALID clicks. This way, false positives can be reversed — you have the raw event to audit."

---

**Q: What if the aggregated dashboard shows different numbers than the billing report?**

> "Expected — they use different layers. Dashboard uses the speed layer (Flink + ClickHouse): fast but may have slight over/undercounts from late events or restarts. Billing uses the batch layer (Spark on raw S3): slower but exact. I'd add a reconciliation step: nightly, compare stream counts vs batch counts per ad_id. If discrepancy > 0.1%, alert and investigate. The batch number is always authoritative for billing disputes."

---

**Q: What if an advertiser claims they were charged for clicks they didn't get?**

> "This is exactly why we persist raw events to S3. We can reprocess: read every click event for that advertiser on that day, apply dedup by click_id, count exactly. This is auditable and deterministic. We'd also check the fraud pipeline — were any clicks from that ad marked INVALID? If yes, they were excluded from billing already. If the advertiser disputes that, we have IP/user-agent/timestamp data to show the clicks were genuine."

---

## Section 6 — Scaling

**Q: How do you scale the ingestion layer from 115K/s to 1M/s?**

> "The ingestion path is stateless (validate + dedup check + Kafka publish), so it scales horizontally. Steps:
> 1. Add Ingest Service instances (behind load balancer, auto-scaling group)
> 2. Add Kafka partitions (increase from 512 to 2048 — requires rolling restart)
> 3. Redis cluster scales horizontally (cluster mode shards keyspace)
> 4. Cassandra (dedup) is linearly scalable — add nodes
> 5. Flink: add task managers
> 6. ClickHouse: add shards
> The weakest link is usually Kafka partition count — plan capacity headroom upfront."

---

## Quick-Fire Round

**Q: Why not just use a time-series DB like InfluxDB instead of ClickHouse?**
> "InfluxDB is great for device metrics (one metric per time series). We have millions of ad_ids × multiple dimensions — ClickHouse handles high cardinality better and its SummingMergeTree is purpose-built for our use case."

**Q: Can you use DynamoDB for aggregated counts?**
> "DynamoDB with atomic increment (UpdateExpression: ADD #count :one) works for simple counts. But ad-hoc queries like 'top K across all ad_ids' require a full scan → expensive. ClickHouse's columnar scans are much faster for analytical patterns. DynamoDB shines for point lookups; ClickHouse shines for aggregations."

**Q: How do you handle time zones in windowing?**
> "Always store and compute in UTC. Convert to advertiser's timezone only at display time. Mixing timezones in window computation leads to off-by-one-hour bugs at DST boundaries. Advertiser dashboards can select their timezone, but the underlying data is always UTC."

**Q: What's your RPO and RTO for this system?**
> "For billing data: RPO = 0 (no data loss — Kafka RF=3 + S3 replication), RTO = 4h (time to replay 24h of events from Kafka if ClickHouse is destroyed). For real-time dashboard: RPO = 30s (last Flink checkpoint), RTO = 5min (Flink restart time). Click ingestion: RPO = 0 (Kafka durable), RTO = 2min (Ingest Service autoscale)."
