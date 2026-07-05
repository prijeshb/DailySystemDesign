# Metrics & Monitoring System — System Design
**Date:** 2026-05-25 | **Difficulty:** Hard | **Real-world:** Prometheus, Datadog, Grafana, InfluxDB

---

## First Principles: Do We Even Need This?

> "If you can't measure it, you can't improve it."

Every production system breaks. Without metrics, you find out from customers. With metrics, you find out before they do — or never. A monitoring system answers: **What is my infra doing right now, was it different yesterday, and should I wake someone up?**

We need it because:
- Services fail silently (CPU looks fine, but DB pool is exhausted)
- Performance degrades gradually (p99 latency creeps up over weeks)
- Business metrics need visibility (orders/sec dropped 20% at 2 AM)

---

## Entities

| Entity | Description |
|--------|-------------|
| **Metric** | Named time-series value (e.g., `http_requests_total{service="api", status="200"}`) |
| **Label/Tag** | Key-value dimensions on a metric (high cardinality risk) |
| **DataPoint** | `(timestamp, value)` pair |
| **Target** | A scrape endpoint (host:port/metrics) |
| **Alert Rule** | Condition + threshold + notification channel |
| **Dashboard** | Visual grouping of panels/graphs |
| **User/Team** | Consumer of dashboards and alerts |

---

## Actions

- Emit metric (counter, gauge, histogram, summary)
- Collect/scrape metrics from targets
- Store data points efficiently
- Query data with aggregations (avg, sum, rate, quantile)
- Evaluate alert rules continuously
- Send notifications (PagerDuty, Slack, email)
- Visualize on dashboards

---

## Data Flow

```
[Services / Infra]
      │  expose /metrics (pull) OR push to agent
      ▼
[Collector / Scraper]  ── service discovery (Consul, k8s) ──► [Target Registry]
      │
      │  batch ingest
      ▼
[Ingestion Layer]  ── validates, labels, deduplicates
      │
      ▼
[Time-Series DB (TSDB)]  ── chunked blocks, compressed
      │
      ├──► [Query Engine]  ◄── Dashboard / API clients
      │
      └──► [Alert Evaluator]  ──► [Notification Router]  ──► PagerDuty / Slack
```

---

## Scale Estimations

- 10,000 services × 200 metrics each = **2M active time-series**
- Scrape interval: 15s → 133K data points/sec ingested
- Each point: ~16 bytes → **~2 MB/s raw**, ~200 GB/day before compression
- TSDB compression (Gorilla/delta-of-delta): **10–20× reduction** → ~10–20 GB/day
- Retention: 15 days hot, 1 year cold (downsampled)

---

## High-Level Design

```
┌─────────────────────────────────────────────────────────┐
│                     Clients / Services                   │
│   [App Instrumentation]   [Node Exporter]   [StatsD]    │
└───────────┬──────────────────────┬──────────────────────┘
            │ /metrics (HTTP)      │ UDP push
            ▼                      ▼
     ┌─────────────┐        ┌─────────────┐
     │  Scraper    │        │  Push GW    │
     │  (pull)     │        │  (buffer)   │
     └──────┬──────┘        └──────┬──────┘
            └──────────┬───────────┘
                       ▼
              ┌─────────────────┐
              │  Ingestion API  │  (validation, labeling, routing)
              └────────┬────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
   ┌─────────────┐          ┌─────────────┐
   │  Hot TSDB   │          │  Cold Store │
   │ (15 days)   │          │ (S3 Parquet │
   │  in-memory  │          │  1 year)    │
   └──────┬──────┘          └─────────────┘
          │
   ┌──────┴──────────────┐
   ▼                      ▼
[Query Engine]      [Alert Evaluator]
   │                      │
   ▼                      ▼
[Grafana /          [Notification
 Dashboard]          Router]
```

---

## Low-Level Design

### 1. Storage: Time-Series DB

**Why not a regular relational DB?**
- Metrics are append-only (no updates)
- Queries are always time-range + label filters
- Need high write throughput (100K+ points/sec)

**Approach — Prometheus TSDB model:**
```
Each series has a unique ID → int64
Series index: label-set → series ID  (inverted index)

Data stored as 2-hour chunks:
  chunk = [(t₀,v₀), (t₁,v₁), ...]
  compression: XOR delta encoding (Gorilla paper, Facebook 2015)
  avg 1.37 bytes/sample vs 16 bytes raw → 12× compression
```

**Block structure on disk:**
```
/data/
  01GXYZ.../          ← 2h block
    chunks/           ← compressed series data
    index             ← series → chunk mapping
    meta.json         ← time range, stats
  WAL/                ← Write-Ahead Log (crash recovery)
```

**Trade-off: Pull vs Push**
| Pull (Prometheus) | Push (StatsD, InfluxDB) |
|---|---|
| Collector controls scrape rate | Service controls emit rate |
| Easy to detect target down | Simpler for ephemeral jobs |
| Centralized config | Works behind firewalls |
| Can miss short-lived metrics | Flood risk if misconfigured |

**Decision:** Use pull as primary, push gateway for batch jobs/lambdas.

---

### 2. Query Engine

**PromQL-style query:**
```
rate(http_requests_total{status="500"}[5m])  -- per-second rate over 5m
histogram_quantile(0.99, http_latency_bucket) -- p99 latency
```

**Execution:**
1. Parse query → AST
2. Label matcher → fetch matching series IDs from index
3. Load chunks for time range from TSDB
4. Apply functions (rate, avg, quantile) in memory
5. Return result matrix

**Caching layer (Redis):**
- Cache query results keyed by `hash(query + time_range)`
- TTL = scrape interval (15s) — no stale data risk
- Trade-off: reduces read load 80%+, but cache miss on new label combos

---

### 3. Cardinality Problem

**The #1 scaling killer in monitoring.**

Bad: `http_requests_total{user_id="u123456"}` → 1M users = 1M series
Good: `http_requests_total{service="api", status="200"}` → ~20 series

**Enforcement:**
- Ingestion layer rejects metrics with >10K unique label values per key
- Alert if a new series explosion is detected (series created/min threshold)
- Quotas per team on active series count

---

### 4. Alerting Pipeline

```
Alert Rule (stored in config):
  expr: rate(http_errors[5m]) > 0.01
  for: 2m          ← must fire for 2 min (avoids flapping)
  severity: critical

Evaluator loop (every 15s):
  1. Execute PromQL expression
  2. Compare against threshold
  3. Transition: inactive → pending → firing → resolved
  4. "for" duration prevents flap alerts

Notification Router:
  - Deduplication window (same alert = 1 page in 4h)
  - Grouping (route all DB alerts to DBA on-call)
  - Inhibition (don't page for services if datacenter is down)
```

---

### 5. Long-Term Storage & Downsampling

Hot TSDB keeps 15 days at raw resolution.

For historical queries (months), raw is too large:
```
Compaction job (runs every 2h):
  Raw (15s) → 5m averages  → S3 (30 days)
  5m avg    → 1h averages  → S3 (1 year)
```

Thanos / Cortex pattern: query router checks if time range is in hot TSDB or cold S3 and merges results transparently.

---

### 6. High Availability

**Prometheus HA pair:** Two identical scrapers run in parallel — both scrape every target. Deduplication at query time (Thanos dedup by timestamp+label).

**Ingestion layer:** Stateless, horizontally scalable. Route by `hash(series_id) % N` for consistent routing to same TSDB shard.

**TSDB sharding:** Shard by metric name prefix or consistent hash of series ID. Each shard handles ~200K series.

---

## Key Trade-offs

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Pull vs Push | Pull primary | Miss short-lived jobs; solved with push gateway |
| In-process vs remote TSDB | Remote (Thanos) | More latency, gain horizontal scale |
| Cache query results | Yes (Redis) | Stale reads possible within 1 scrape interval |
| Exact vs approximate cardinality | HyperLogLog for counting unique series | ~2% error, saves 100× memory |
| Strong vs eventual consistency | Eventual (async WAL flush) | May lose <15s of data on crash |
| Downsampling | Yes after 30 days | Lose sub-minute granularity for old data |

---

## Real-World References

- **Prometheus TSDB design** — Fabian Reinartz blog (2017): chunk encoding, WAL, compaction
- **Gorilla paper** (Facebook/Meta 2015): XOR delta-of-delta compression for time-series
- **Thanos** (Improbable Engineering): HA Prometheus + object storage
- **Cortex/Mimir** (Grafana Labs): multi-tenant Prometheus at scale
- **VictoriaMetrics** blog: merge-tree storage for TSDB, better than Prometheus for high cardinality
- **Datadog architecture** blog: DogStatsD, intake pipeline, tiered storage

---

## Summary

A monitoring system is fundamentally a **high-write, time-range-query, label-filtered** workload. The TSDB with compressed chunks is the core. Cardinality control is the hardest operational challenge. Alerting must be stateful (pending→firing) and deduplicated to prevent alert storms.
