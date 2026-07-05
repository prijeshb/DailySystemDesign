# Metrics & Monitoring — Interview Q&A
**Date:** 2026-05-25 | Companion to: Metrics_Monitoring_System_Design_2026_05_25.md

---

## Opening Questions (First 5 Minutes)

**Q: Design a monitoring system like Prometheus/Datadog.**
> Start with clarifying: "Are we monitoring infra (CPU/mem) or business metrics (orders/sec) or both? What's the scale — 100 services or 100,000? Do we need real-time alerting or dashboards or both?"

**Q: What are the core requirements?**
> Functional: collect metrics, store time-series, query with aggregation, alert on thresholds, visualize.
> Non-functional: high write throughput (100K pts/sec), low query latency (<1s for dashboards), 99.9% uptime for alerting, 1-year retention.

---

## Deep-Dive Questions

**Q: Why use a time-series DB instead of MySQL or Postgres?**
> TSDB is optimized for append-only, time-range-filtered, label-matched queries. Relational DBs have row overhead, indexes not suited for time + label combos, and MVCC is wasteful for immutable data. TSDB's chunk encoding compresses 16-byte points to ~1.4 bytes. MySQL at 100K writes/sec would need massive sharding; TSDB handles it natively.

**Q: Pull vs push — which would you choose and why?**
> Pull: scraper controls rate, easy to detect target down, works with service discovery. Problem: can't scrape short-lived jobs (crons, lambdas) — use a push gateway for those. Push: simpler for ephemeral jobs, works behind NAT. Risk: flood risk if client emits too frequently. **I'd choose pull as primary + push gateway for batch jobs.** Prometheus, Datadog agent, and VictoriaMetrics all use this hybrid model.

**Q: What is cardinality and why does it matter?**
> Cardinality = number of unique label value combinations = number of active time-series. High cardinality (e.g., a label per user_id) means millions of series — each needs memory for chunk head, series index entry, and WAL buffer. 1M series × 5KB each = 5GB just in memory. Prometheus OOMs at ~2M series on 32GB. **Never put high-cardinality values (user IDs, request IDs, trace IDs) in metric labels.**

**Q: How do you handle the "no data" vs "zero" problem in alerts?**
> They look the same on a graph but mean very different things. Use `absent(metric{job="api"})` to detect missing data as a separate alert. Use `up == 0` to detect scrape failures. For alert conditions, `rate(errors[5m]) > 0.01` won't fire if there's no data — which is wrong. Wrap with `(rate(errors[5m]) or vector(0)) > 0.01` if you want zero to be safe, but still use `absent()` separately.

**Q: How would you scale the storage layer?**
> Shard the TSDB by consistent hash of series ID. Each shard owns ~200K series. Consistent hashing means adding a shard only re-routes 1/N series, not a full resharding. For long-term: compact raw (15s) to 5m averages to 1h averages, store in object storage (S3). Query router transparently merges results from hot TSDB and cold S3. This is exactly what Thanos/Cortex do.

**Q: How do you prevent alert storms during an outage?**
> Three mechanisms: (1) **Inhibition** — if datacenter-down alert fires, inhibit all alerts for services in that DC. (2) **Grouping** — route all alerts with the same cluster/team label as one grouped page, not 50 individual pages. (3) **Deduplication window** — same alert doesn't re-page for 4 hours. Alertmanager implements all three natively.

---

## Follow-Up / Pressure Questions

**Q: Your monitoring system itself goes down during an incident. How do you handle this?**
> Defense in depth: (1) HA scraper pair — one down, other covers. (2) Alert evaluator HA with persistent state. (3) Alertmanager 3-node cluster. (4) Dead man's snitch — Alertmanager sends heartbeat to PagerDuty; if heartbeat stops, PagerDuty pages independently. (5) For true catastrophic failure, have a secondary simpler monitoring stack (e.g., just CloudWatch/basic pings) as last resort.

**Q: A single metric suddenly creates 10 million new series. What happens and how do you prevent it?**
> TSDB memory usage explodes (OOM risk). Write throughput spikes (WAL fills up). Index lookup slows (more series to scan). **Prevention:** Ingestion layer enforces cardinality limits per label key (e.g., max 10K unique values). Alert when new series creation rate exceeds threshold. Reject writes that would exceed quota with a clear error message. This is table stakes at any company with many teams emitting metrics.

**Q: How would you add multi-tenancy (different teams, different data isolation)?**
> Add a `tenant_id` at ingestion. Each tenant gets: separate storage namespace (separate TSDB or separate keyspace), separate query path (tenant can only query their own series), separate alert rules and dashboards, quotas on active series count. Cortex (now Grafana Mimir) is the reference implementation for multi-tenant Prometheus.

**Q: What's the trade-off between 15-second and 60-second scrape intervals?**
> Shorter interval: more data, detect faster-changing metrics (CPU spikes last 30s), but 4× more storage and 4× more write load. Longer interval: cheaper, but miss transient spikes. **Decision:** Use 15s for critical services (payment, auth), 60s for stable infra metrics (disk usage changes slowly). Different scrape configs per job in Prometheus.

**Q: How do you handle metric backfill? E.g., you discover a calculation error and need to re-ingest 7 days of data.**
> Pull model doesn't support backfill natively (scraper fetches current values only). Options: (1) If raw logs exist, run offline processing to generate a backfill file and use Prometheus `promtool tsdb create-blocks-from rules`. (2) Store raw events in S3 separately; recompute metrics from source of truth. (3) Accept the data loss with an annotation on the dashboard explaining the gap. **Real answer:** backfill is hard in Prometheus; Datadog has a backfill API. Design for immutable raw events separately if backfill is critical.

---

## Architecture Trade-off Questions

**Q: Would you use Prometheus or InfluxDB?**
> Prometheus: pull-based, better for k8s/service discovery, large community, PromQL is expressive. InfluxDB: push-based, better for IoT/high frequency, has built-in downsampling. For typical cloud infra monitoring, Prometheus wins. For high-frequency sensor data (1ms intervals), InfluxDB or TimescaleDB is better.

**Q: When would you choose VictoriaMetrics over Prometheus?**
> When you have >2M active time-series (Prometheus struggles), need horizontal scalability without Thanos complexity, or need better compression. VictoriaMetrics uses a merge-tree (similar to LSM) rather than chunk-based storage — handles higher cardinality and write throughput, uses ~70% less disk than Prometheus.

**Q: Should dashboards query live TSDB or a read replica?**
> Read replica. Dashboards are often poorly optimized (wide time ranges, many panels). Without isolation, a heavy dashboard query can starve the alert evaluator of TSDB I/O — your monitoring system's most critical path. **Alert evaluator always gets dedicated read path.** Dashboard users get best-effort.

---

## Behavioral / Design Sense Questions

**Q: A team complains their alerts are too noisy — they get 200 pages a week. How do you help?**
> Walk through their alert rules: (1) Are thresholds set to business impact, not technical convenience? (2) Is there a `for: Xm` duration to prevent flapping? (3) Are related alerts grouped or causing separate pages? (4) Are low-severity alerts going to Slack (not PagerDuty)? (5) Is the `absent()` check too aggressive for services that deploy frequently? Fix the signal-to-noise ratio before operators stop trusting alerts entirely.

**Q: How do you know if your monitoring system is actually working?**
> The dead man's snitch pattern: fire a synthetic alert called `watchdog` every minute. If it stops firing → monitoring is broken. Also: track `scrape_success` rate across all targets, `alertmanager_notifications_failed_total`, and evaluator latency. Monitor the monitoring system. At Datadog scale, they have a separate internal monitoring stack that watches the customer-facing one.

---

## Key Numbers to Remember

| Metric | Value |
|--------|-------|
| Prometheus active series limit (practical) | ~2M per instance |
| Gorilla compression ratio | ~12× (1.37 bytes/sample) |
| Typical scrape interval | 15s |
| Alert dedup window | 4h |
| Hot retention | 15 days |
| WAL flush interval | ~2s (Prometheus default) |
| Max cardinality per label key (recommended) | 10K |
