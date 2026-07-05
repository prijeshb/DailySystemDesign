# Interview Q&A: Distributed Logging & Observability System
> Date: 2026-06-19

---

## Opening / Scoping Questions (Interviewer tests if you clarify before diving in)

**Q: Design a logging system for a microservices architecture.**

Don't start drawing boxes. Ask:
1. "How many services / logs per second are we talking?" → scale determines Kafka vs direct
2. "What are the primary use cases — debugging, alerting, compliance?" → shapes storage
3. "Do we need distributed tracing, or just log search?" → traces = different storage
4. "Any compliance requirements (PII, retention)?" → shapes masking + retention
5. "What's the budget / existing stack?" → Kafka+Flink vs Vector.dev

*Interviewer wants to hear:* You don't assume — you clarify requirements first.

---

## Core Design Questions

**Q: How would you handle 2.5M log events per second?**

Directly to Elasticsearch → ES dies (it handles ~100K writes/sec/node).

Correct path:
1. Log agents batch + compress → Kafka (designed for 10M+ msg/sec)
2. Kafka decouples write rate from processing rate
3. Flink consumers process at their own pace, catch up on burst
4. ES only sees enriched, filtered, sampled subset

*Key numbers to cite:*
- Kafka: 10M+ msg/sec, sequential disk I/O, batch compression
- Elasticsearch: 100-500K docs/sec per node (with proper sizing)
- Without Kafka: 2.5M/sec would need ~20 ES nodes just for ingest

---

**Q: How do you search logs from 3 weeks ago efficiently?**

3-tier answer:
```
<7 days: Elasticsearch (hot) — full-text search, seconds
7-30 days: S3/Parquet + Athena — SQL queries, 10-60 seconds
>30 days: Glacier — async restore (4-12 hours), rare
```

Explain partition strategy for Athena:
```sql
-- Athena query only scans needed partitions:
SELECT * FROM logs_warm
WHERE year='2026' AND month='05' AND day='28'  -- partition pruning
  AND service='payment-service'
  AND level='ERROR'
```
Without partitioning → full 200TB scan every query = $1,000/query.

---

**Q: How do you correlate a request across 12 microservices?**

Distributed tracing via trace context propagation:
```
API Gateway generates trace_id = random 128-bit UUID
Passes to all downstream calls via HTTP header:
  traceparent: 00-{trace_id}-{span_id}-{flags}

Each service:
  Reads parent trace_id from header
  Generates own span_id
  Logs: { trace_id: X, span_id: Y, parent_span_id: Z }
  Passes updated traceparent downstream
```

*Follow-up: What if a service doesn't propagate the trace_id?*
The trace breaks at that service — you see two disconnected trees instead of one.
Fix: enforce via middleware in all service templates (not optional).

---

**Q: How do you prevent sensitive data (PII) from ending up in logs?**

Three layers:
1. **Developer discipline** (weakest): coding standards, log review
2. **Application framework** (better): structured logging, never log `.toJson()` on user objects — only log explicit safe fields
3. **Pipeline masking** (strongest): Flink processor applies regex patterns before any storage:
   - Credit cards: `\b\d{13,16}\b` → `[CARD_REDACTED]`
   - Emails in free-text: hashed with SHA256
   - SSNs: `\b\d{3}-\d{2}-\d{4}\b` → `[SSN_REDACTED]`

*Never rely on layer 1 alone.*

*Follow-up: What about GDPR right to erasure?*
Logs are immutable by design — you cannot delete one user's logs.
Solution: never store PII in logs (use internal user_id hashes, not email/name).
Then erasure of logs is unnecessary — they contain no identifiable data.

---

**Q: How do you do sampling without losing important logs?**

**Wrong answer:** Random N% sampling at the agent.

**Right answer:** Tail-based sampling:
1. Buffer all spans/logs for a trace for 30 seconds
2. When trace completes, decision:
   - ANY span has error → keep ALL spans for this trace (100%)
   - P99 latency > threshold → keep (100%)
   - Otherwise → keep 1%

*Why tail-based?* You need to know the outcome before deciding. Head-based (decided at entry) can't know if the request will error.

*Cost:* Requires 30-second in-memory buffer (or Kafka window). At 2.5M spans/sec × 30s × 500 bytes = 37.5GB buffer. Use Flink tumbling windows (disk-spillable).

---

## Follow-up / Deep Dive Questions

**Q: Your Elasticsearch cluster is getting too expensive. What do you do?**

Prioritized options (cheapest first):

1. **Reduce retention in ES:** 7d → 3d (move to S3/Athena faster). 57% cost savings.
2. **Increase sampling:** 1% info logs instead of 5% (reduce write volume)
3. **Switch to ClickHouse:** 5-10× cheaper than ES for structured logs at same query speed for SQL queries (only loses full-text)
4. **Use ES Frozen tier:** cheaper node types for day 4-7 (read-only, slower but cheaper)
5. **Switch to Loki (Grafana):** label-based log store, no inverted index, 80% cheaper — but loses full-text search capability

*Trade-off conversation:* Full-text search vs cost. If your engineers search by `service=X level=ERROR` (structured fields) → ClickHouse is sufficient. If they grep for `"NullPointerException" in message` → need ES/Loki.

---

**Q: How do you handle a burst when a bad deploy sends 100× normal log volume?**

Defense in depth:
```
Level 1 — Application: rate limiter in logger (max 1K lines/sec per class)
Level 2 — Agent: max throughput cap (50MB/sec), excess drop_oldest
Level 3 — Kafka: 48h retention absorbs the burst regardless of downstream speed
Level 4 — Flink: auto-scale on lag metric (add task managers)
Level 5 — ES: index rate limiting (don't overwhelm cluster)
Level 6 — Monitor: alert on log volume spike >5× baseline → trigger oncall even before errors appear
```

*Key insight:* The burst itself is a signal — a 100× log spike means something is very wrong, even before errors appear. Alert on it.

---

**Q: How would you debug: "users are seeing errors but I don't see anything in logs"?**

Systematic check:
1. **Is sampling dropping the error?** → Check if error-level logs are sampled at all. Should be 100% for ERROR.
2. **Is the agent running?** → `kubectl get pods -n logging` — check DaemonSet health
3. **Is Kafka consumer lagging?** → Check consumer lag dashboard. Lag = delayed, not lost.
4. **Is Elasticsearch rejecting writes?** → Check ES ingest rate, cluster health, disk usage
5. **Is the service logging the right level?** → Maybe error is logged as WARN. Check logger config.
6. **Is PII masker corrupting JSON?** → Regex can malform JSON if not careful. Check raw Kafka messages.
7. **Is the log at the right service?** → The error might be in a dependency. Use trace_id to find which service.

---

**Q: How do you alert when the alerting system itself is broken?**

Dead man's switch pattern:
```
Alert rule: "Expected alert should fire every 30 minutes"
→ Synthetic event published every 30min to Kafka
→ Alert engine should fire P3 alert for it (test alert)
→ A second system (external, e.g. PagerDuty's heartbeat) watches for this test alert
→ If test alert doesn't arrive within 35 min → page on-call

"The alert system is being monitored by a system that is monitored by a system you trust"
```

*Real-world:* PagerDuty has a "dead man's snitch" feature exactly for this.

---

**Q: How is this different from designing S3 (yesterday's design)?**

S3 is a storage system — it answers: "store and retrieve arbitrary blobs."
Logging is a pipeline system — it answers: "observe system behavior at scale."

Key differences:
- Logs must be searchable (not just retrievable by key)
- Logs need sampling (you can't keep everything)
- Logs need stream processing (real-time alerting)
- Logs have retention policies and compliance requirements
- Traces need correlation across services (structural relationship, not just storage)

Both use S3 internally — but the design problems are fundamentally different.

---

## Senior-Level Questions

**Q: How would you design log-based anomaly detection?**

Two approaches:

**Statistical (simple, interpretable):**
```
Baseline: error rate per service, averaged over 7 days, same hour-of-day
Alert: if current_rate > baseline × 3σ → anomaly
```

**ML-based (Datadog AIOps style):**
```
Features per 1-min window per service:
  [error_rate, latency_p99, log_volume, unique_error_messages]

Model: Isolation Forest (unsupervised anomaly detection)
  Train on 30 days of normal data
  Score new windows in real-time
  High anomaly score → alert

Advantage: catches compound anomalies (latency up + volume down + error up)
           that threshold alerts miss individually
```

Cost: ML model runs on aggregated ClickHouse data (1-min rollups), not raw logs. Feasible.

---

**Q: How does your system handle multi-tenancy (SaaS logging product)?**

New requirements:
- Data isolation: tenant A cannot see tenant B's logs
- Noisy neighbor: tenant A can't starve tenant B's ingest
- Per-tenant retention policies (some customers paid for 1yr, others 30d)

Design changes:
```
Kafka: topic-per-tenant (or partition-per-tenant within topic)
ES: index-per-tenant (different ILM policies)
   OR tenant_id field + document-level security (DLS in ES)
Ingest: per-tenant rate limiter at API gateway (token bucket, quota per tier)
Query API: middleware injects WHERE tenant_id=X on every query (auth token maps to tenant)
Billing: ClickHouse tracks ingested bytes per tenant per day → invoice
```

*Hard problem:* A tenant who ingests 100× their quota: reject at ingest (lose logs) or queue and process slowly (affects other tenants)?
Solution: Hard throttle at ingest + buffer small burst + reject with 429 if sustained.

---

## One-Line Concept Tests

**Q: Why use Parquet on S3 instead of JSON?**
Columnar format → query `WHERE level='ERROR'` reads only level column, skips rest. 10-100× less data scanned.

**Q: Why is tail-based sampling hard to implement?**
Must buffer all events for a trace (30s window), then decide. Requires stateful stream processing (Flink) and careful memory management at scale.

**Q: What is an ILM policy?**
Index Lifecycle Management: automatically move ES indices from hot → warm → cold → delete based on age/size. Eliminates manual shard management.

**Q: Why NOT put everything in one Kafka topic?**
Single topic → single consumer group reads in order. Can't parallelize by service. Hot services block cold services in the same partition. Separate topics (or partition by service_name) allows independent consumer scaling.

**Q: What does "log at WARN, alert at ERROR" mean?**
WARN = something unusual happened but system recovered. ERROR = something failed and may require action. If you alert on WARN, alert fatigue. If you only track ERROR, you miss degraded-but-working states.
