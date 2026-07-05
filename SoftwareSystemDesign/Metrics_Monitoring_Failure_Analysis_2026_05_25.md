# Metrics & Monitoring — Failure Analysis
**Date:** 2026-05-25 | Companion to: Metrics_Monitoring_System_Design_2026_05_25.md

---

## Failure-First Framework

For each component: **What if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?**

---

## 1. Service / Exporter Fails (metric source goes down)

### What happens?
- Scraper gets `connection refused` or timeout on `/metrics`
- Metrics simply stop flowing — gap in TSDB
- Alert evaluators see stale/missing data

### Problems:
- **Silent failure**: graphs show flat line, looks like "zero errors" — actually no data
- **Alert doesn't fire**: `rate(errors[5m]) > 0` needs data to evaluate
- **False positives**: `absent()` or `up == 0` alerts fire for every restart/deploy

### Solutions:
1. **`up` metric**: Scraper emits `up{job="api"} = 0` when scrape fails. Alert on `up == 0` for >2 min.
2. **Distinguish "no data" from "zero"**: Use `absent(metric)` alerts separately. "No data for 5 min" = different alert severity than "metric is zero."
3. **Stale markers**: TSDB writes a NaN stale marker after 2 missed scrapes — prevents last-known-value from polluting rate calculations.
4. **Graceful exporter restarts**: Exporters should reset counters cleanly; otherwise rate() spikes on restart.

---

## 2. Scraper / Collector Fails

### What happens?
- All targets go unscraped for duration of outage
- Data gap in TSDB — cannot be backfilled (pull model)
- Ongoing incidents may go undetected

### Problems:
- Loss of observability during highest-risk moments (deploys often trigger scraper restarts)
- Alert evaluator has no data → may flip to "no data" state

### Solutions:
1. **HA scraper pair**: Two scrapers independently scrape all targets. Deduplication at query layer (Thanos). Tolerate one scraper down with zero data loss.
2. **Scraper health metric**: Scrapers emit their own `scrape_duration_seconds` and `scrape_samples_scraped`. Alert if scraper itself goes silent.
3. **Push gateway fallback**: Critical services can push to a stateless gateway — survives scraper downtime.
4. **WAL replay**: Scraper buffers last N minutes to WAL on disk; on restart, replays to TSDB if gap < retention window.

---

## 3. Ingestion Layer Fails

### What happens?
- Scraped data cannot be written to TSDB
- Back-pressure builds up in scraper buffers

### Problems:
- If ingestion is stateless (scales horizontally), routing hash changes → data goes to wrong shard → gaps
- Buffer overflow in scraper → oldest samples dropped silently

### Solutions:
1. **Stateless ingestion + consistent hashing**: Use consistent hash ring so adding/removing nodes doesn't cause mass re-routing. Only ~1/N series affected.
2. **Circuit breaker on scraper**: If ingestion responds with 5xx, scraper backs off (exponential) rather than hammering. Prevents cascade.
3. **WAL at ingestion**: Kafka or local WAL acts as durable buffer. Ingestion reads from Kafka; lag is visible. TSDB writes from here. Max data loss = Kafka retention (hours).
4. **Overflow alerting**: Alert when scraper buffer utilization > 80% — gives time to scale ingestion before data loss.

---

## 4. Time-Series DB (TSDB) Fails

### What happens?
- Writes queue up → eventually overflow
- Queries fail or return partial results
- Alerts cannot evaluate → silently stop firing

### Problems:
- **Hot shard**: One shard gets all traffic for a popular metric (`cpu_usage`) — uneven load
- **Compaction storm**: Background compaction jobs spike I/O → write latency spikes → backpressure
- **Chunk corruption**: Power loss during write → WAL partially written → data loss on replay

### Solutions:
1. **WAL (Write-Ahead Log)**: All writes go to WAL first (sequential, fast). TSDB replays WAL on restart. Max loss = last WAL flush interval (default 2s in Prometheus).
2. **Shard by series ID hash**: Prevents hot shards. Series are distributed ~uniformly if hash function is good (MurmurHash3).
3. **Compaction throttling**: Limit compaction I/O to X MB/s. Prometheus `--storage.tsdb.max-block-duration` controls chunk size to avoid I/O spikes.
4. **Read replica**: Separate read and write paths. Writes go to primary; reads from replica (slight staleness acceptable for dashboards).
5. **TSDB isolation from alert evaluator**: Alert evaluator reads from dedicated replica so dashboard traffic can't starve alerting.

---

## 5. Query Engine Fails

### What happens?
- Dashboards go blank — users lose visibility
- Alert evaluations may time out

### Problems:
- Expensive queries (full table scan over 1M series) cause OOM
- Query fanout: one dashboard opens 50 queries → cascades to TSDB

### Solutions:
1. **Query timeout**: Hard limit 30s per query. Return partial results with warning rather than timeout error.
2. **Query cardinality limits**: Reject queries that would return >10K series before executing. "Your query matches 500K series, please add a label filter."
3. **Query result cache (Redis)**: Cache recent query results. 80%+ dashboard queries are repeated (same query, slightly shifted time range → bucket to nearest 15s → cache hit).
4. **Rate limiting per user/team**: Prevent one noisy dashboard from starving others.
5. **Circuit breaker on TSDB**: If TSDB error rate > 50%, query engine returns cached/stale results with a banner rather than blank dashboards.

---

## 6. Alert Evaluator Fails

### What happens?
- Active alerts stop being re-evaluated
- Firing alerts may not resolve (stuck in firing)
- New threshold breaches are not detected

### Problems:
- **Split-brain**: Two evaluator instances both page for the same alert — double pages
- **Clock skew**: Evaluator's clock drifts → `for: 2m` duration calculated incorrectly

### Solutions:
1. **Persistent alert state**: Evaluator writes `{alert_name, labels, state, since}` to Redis/Postgres. On restart, reloads state — avoids re-firing already-resolved alerts.
2. **Deduplication at notification router**: Even if two evaluators both fire, dedup window (4h) ensures one page. Dedup key = `hash(alert_name + labels)`.
3. **Active-passive HA**: One evaluator active, one hot standby. Fencing via distributed lock (Redis SET NX). Failover < 5s.
4. **Dead man's switch**: Evaluator fires a "watchdog" alert every minute. Notification router expects it. If watchdog stops → page on-call about monitoring system itself.

---

## 7. Notification Router / Alertmanager Fails

### What happens?
- Alerts fire but pages are never sent
- On-call team unaware of incidents

### This is the most dangerous single point of failure — you're blind AND deaf.**

### Solutions:
1. **Redundant Alertmanager cluster**: Alertmanager runs as 3-node cluster with gossip protocol. Any node can route pages. Uses mesh networking (similar to consul).
2. **Heartbeat to PagerDuty**: Alertmanager sends a heartbeat every 1 min to PagerDuty's Dead Man's Snitch. If PagerDuty stops receiving it → triggers alert independently.
3. **Direct webhook fallback**: Critical alerts have two routes: Alertmanager AND a direct webhook to PagerDuty (bypassing Alertmanager). Belt and suspenders.
4. **Alert delivery acknowledgment**: Notification router marks alerts as "delivered" only after 2xx from PagerDuty. Retries with backoff on failure.

---

## 8. Cold Storage (S3) Fails

### What happens?
- Historical queries (>15 days) fail
- Downsampled data unavailable

### Solutions:
1. **Hot TSDB is independent**: Short-term queries (<15 days) unaffected. Cold storage failure doesn't impact real-time monitoring.
2. **S3 multi-region replication**: Enable cross-region replication on the metrics bucket. RPO near-zero, RTO minutes.
3. **Partial result graceful degradation**: Query router returns hot data with "historical data unavailable" banner — operators still see current state.

---

## Cascading Failure Scenario: Deploy Goes Wrong

```
1. Engineer deploys new service version
2. New version has a bug → throws exceptions at 5000 rps
3. Exception labels include request_id → cardinality explosion
4. 10M new series created in seconds → TSDB OOM killer
5. TSDB crashes → alert evaluator has no data
6. Alertmanager watchdog fires (dead man's snitch)
7. On-call paged about monitoring failure BEFORE service alert
8. Engineer investigates → finds service + monitoring both down
```

**Prevention chain:**
- Cardinality enforcement at ingestion (step 3 blocked → 4 never happens)
- TSDB memory limits with graceful rejection (step 4 isolated)
- Alert evaluator on separate replica (step 5 isolated)
- Dead man's snitch as last resort (step 6 works)

---

## Failure Severity Matrix

| Component | Blast Radius | Detection | Recovery Time |
|-----------|-------------|-----------|---------------|
| Single exporter down | 1 service blind | `up==0` alert, 2min | Immediate (redeploy) |
| Scraper down | All targets blind | Self-monitoring | < 30s (HA pair) |
| Ingestion down | Data gap | Buffer overflow alert | < 60s (stateless restart) |
| TSDB shard down | 1/N metrics unavailable | Query errors | 1–5 min (WAL replay) |
| Alert evaluator down | No new alerts fire | Dead man's snitch | < 30s (failover) |
| Alertmanager down | Pages not sent | PagerDuty heartbeat | < 60s (cluster) |
| Cold storage down | No historical data | Query error banner | Minutes (S3 multi-region) |
