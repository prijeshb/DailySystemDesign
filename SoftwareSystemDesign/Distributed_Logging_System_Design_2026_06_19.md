# System Design: Distributed Logging & Observability System
> Date: 2026-06-19 | Think: Datadog / ELK Stack / CloudWatch

---

## First Principles: Do We Even Need This?

**Question:** Can we just use `print()` and SSH into servers?

At 1 server: yes.  
At 10 servers: painful but possible.  
At 100+ services, 50M events/day: impossible. You cannot:
- Correlate a request across 12 microservices
- Find all errors in the last 5 min across the fleet
- Know if a deploy caused a spike in latency
- Wake up the right engineer at 3am before users notice

**What we're building:** A system that ingests, stores, and queries structured log events + distributed traces across a fleet of services — with alerting and dashboards.

**Real-world reference:** Datadog ($4B ARR), ELK Stack (Elasticsearch + Logstash + Kibana), Splunk, AWS CloudWatch.

---

## Entities

| Entity | Description |
|--------|-------------|
| **Log Event** | Single structured record: `{timestamp, service, level, trace_id, message, fields}` |
| **Trace** | End-to-end journey of one request across services |
| **Span** | One unit of work within a trace (one service's contribution) |
| **Alert Rule** | Condition + threshold + notification target |
| **Dashboard** | Saved queries + visualizations |
| **Retention Policy** | Per-service rules for hot/warm/cold storage |

---

## Actions

1. **Ingest** — Service emits log; agent collects + ships to pipeline
2. **Sample** — Filter which logs to keep (cost control)
3. **Index** — Make logs searchable by time + fields + full text
4. **Query** — User searches across N days of logs
5. **Alert** — Stream processor fires alert when condition met
6. **Trace** — Correlate spans across services into full request trace
7. **Archive** — Move old logs to cheaper storage tiers

---

## Capacity Estimation

**Assumptions:**
- 500 microservices, each emitting 1,000 log lines/sec average
- Average log size: 500 bytes (structured JSON, gzip ~150 bytes)
- Peak multiplier: 5×

**Write throughput:**
```
500 services × 1,000 logs/sec = 500,000 logs/sec (steady state)
Peak: 2.5M logs/sec

Daily volume:
  500K/sec × 86,400 sec = 43.2B logs/day (raw)
  × 500 bytes = 21.6 TB/day (raw)
  × 0.3 gzip ratio = 6.5 TB/day (compressed)
```

**Storage by tier (Concepts_Index #70 — Tiered Log Storage):**
```
Hot  (ES, 7 days):    6.5 TB/day × 7  =  45.5 TB  — indexed, fast search
Warm (S3/Parquet, 30 days): 6.5 TB/day × 30 = 195 TB — compressed, slow search
Cold (Glacier, 365 days): 6.5 TB/day × 365 = 2.4 PB — archive, rare access
```

**AWS Cost (Budget Constraint Example):**
```
ES hot tier:
  45 TB on r6g.2xlarge ES cluster (3-node) = ~$4,000/month
  
S3 warm tier:
  195 TB × $0.023/GB = $4,485/month + Athena queries $5/TB
  
Glacier cold:
  2.4 PB × $0.004/GB = $9,830/month

Data transfer (ingest 6.5 TB/day):
  EC2 → ES (same region): free
  EC2 → S3: free (same region)

Total storage cost: ~$18,315/month

At $20K/month budget for logging alone:
  → Barely fits. Must sample aggressively.
  
Budget optimization (60% cost reduction):
  Sampling: 100% errors, 5% warn, 0.1% info → 6.5 TB/day → 1.8 TB/day
  Reduced hot tier from 7 days → 3 days for info/debug
  → Total: ~$7,200/month ✓
```

**Trade-off — sampling saves cost but you lose rare bugs that only show in debug logs.**  
*Decision: tail-based sampling (keep logs for requests that errored, even if initially sampled out).*

---

## Data Flow

```
[Service]
    │  structured JSON log + trace context
    ▼
[Log Agent: Fluentd/Filebeat] (runs as sidecar/DaemonSet)
    │  • Adds: host, pod_id, k8s_namespace
    │  • Buffers (disk-backed): handles downstream unavailability
    │  • Batches: 1,000 logs / 200ms, whichever comes first
    ▼
[Kafka] (3 topics: logs.raw, logs.errors, traces.spans)
    │  • Partitioned by service_name (ordering within service)
    │  • Retention: 48h (buffer for processor lag)
    ▼
[Log Processor: Flink cluster]
    │  • Parse, validate, enrich (geo from IP, PII masking)
    │  • Sample: drop excess info/debug per sampling policy
    │  • Route: errors → logs.indexed + PagerDuty topic
    │            traces → trace assembler
    │            all → S3 sink (Parquet, partitioned by date/service)
    ▼                      ▼
[Elasticsearch]      [S3/Parquet]        [ClickHouse - metrics aggregates]
  hot: 7d              warm: 30d           alert eval: pre-aggregated
  full-text search     Athena queries      1min rollups
    ▼
[Query API]
    │  • ES for recent (<7d)
    │  • Athena for historical (7-30d)
    │  • Glacier retrieval request for >30d (async, hours)
    ▼
[Dashboard UI + Alert Engine]
```

---

## High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION TIER                           │
│  Service ──► Agent ──► Kafka (logs.raw)                        │
│               │                                                 │
│           [disk buffer]  ← if Kafka down, won't lose logs      │
└─────────────────────────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────────────────────────┐
│                       PROCESSING TIER                           │
│  Flink ──► Enrich + Sample + Route                             │
│        ──► PII Masker (credit cards, SSNs, emails)             │
│        ──► Alert Evaluator (stream processing, no storage needed│
└─────────────────────────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────────────────────────┐
│                        STORAGE TIER                             │
│  Elasticsearch (hot, 7d)  →  S3/Parquet (warm, 30d)           │
│  ClickHouse (metrics, 90d)    Glacier (cold, 1yr)              │
│  Cassandra (trace storage, 7d — optimized for trace_id lookup) │
└─────────────────────────────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────────────────────────────┐
│                         QUERY TIER                              │
│  Query Router → ES or Athena depending on time range           │
│  Trace Viewer → Cassandra lookup by trace_id                   │
│  Dashboard → ClickHouse for metric aggregates                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Low-Level Design

### Log Schema (Structured JSON)
```json
{
  "timestamp":   "2026-06-19T10:30:45.123Z",
  "service":     "payment-service",
  "version":     "v2.3.1",
  "level":       "ERROR",
  "trace_id":    "4bf92f3577b34da6",
  "span_id":     "00f067aa0ba902b7",
  "message":     "Payment declined: insufficient funds",
  "user_id":     "u_82729",
  "order_id":    "ord_99821",
  "duration_ms": 145,
  "host":        "payment-pod-4d8f9",
  "environment": "production"
}
```

**Key design decision:** `trace_id` is how you correlate a log across 12 services. Every service must propagate it via HTTP headers (W3C TraceContext: `traceparent`).

### Distributed Tracing Model
```
Request: POST /checkout
  trace_id: 4bf92f3577b34da6

  Span 1: API Gateway        (0-5ms)   span_id: aa001
  Span 2: ├── Order Service  (5-50ms)  span_id: bb002  parent: aa001
  Span 3: │   ├── DB write   (5-15ms)  span_id: cc003  parent: bb002
  Span 4: └── Payment Svc   (50-195ms) span_id: dd004  parent: aa001
  Span 5:     └── Stripe API (50-180ms) span_id: ee005 parent: dd004

Trace assembled by: collecting all spans with same trace_id,
                    sorting by parent_span_id to build tree
```

**Storage:** Cassandra, primary key = `(trace_id, span_id)`. Lookup by trace_id = partition scan = fast.

### Sampling Strategy

```
Head-based (decision at first service):
  Random 10% of all requests → propagate sample flag in traceparent
  Problem: 10% error requests also sampled → lose errors
  ✗ Don't use for error-critical systems

Tail-based (decision after trace completes):
  Buffer spans in memory for 30s (trace timeout window)
  On trace complete:
    Has error span?    → KEEP (100%)
    P99 latency > 2s? → KEEP (100%)
    Otherwise:        → KEEP 1%, DROP 99%
  ✓ Better: captures all errors, samples happy-path

Adaptive (Datadog's approach):
  Per-service rate targets: max 100 traces/sec/service
  Dynamically lower sample rate if ingestion too high
  Always keep 100% of errors
```

**Trade-off:**
- Tail-based requires buffering spans in memory (30s × 2.5M spans/sec = huge RAM)
- Solution: stream spans to Kafka, let Flink window them by trace_id (30s tumbling window)
- Flink decides keep/drop, only then writes to Cassandra

### Alert Engine

```
Rule example:
  error_rate(service="payment", window=1m) > 1%
  → PagerDuty alert: "Payment service error rate 3.2%"

Implementation:
  Flink reads from logs.errors topic (already filtered upstream)
  Tumbling 1-min windows, count by service
  Evaluate rule: count / total > threshold?
  Write to alert_events table (Cassandra) + notify via PagerDuty/Slack

Alert fatigue prevention:
  Deduplication: if same alert fired in last 15min → suppress
  Grouping: same service errors → one alert, not N separate ones
  Severity: P1 (page), P2 (Slack), P3 (dashboard only)
```

### Query Routing
```python
def query_logs(q: Query) -> Results:
    now = datetime.utcnow()
    
    if q.end_time > now - timedelta(days=7):
        # ES: full-text search, fast, indexed
        return elasticsearch.search(q.to_es_query())
    
    elif q.end_time > now - timedelta(days=30):
        # Athena: S3/Parquet scan, slower, cheaper
        return athena.query(q.to_sql(), partitions=q.date_range())
    
    else:
        # Glacier: async, must request restore first
        job_id = glacier.initiate_restore(q.date_range())
        return {"status": "async", "job_id": job_id, "eta": "4-12 hours"}
```

### Index Design (Elasticsearch)
```
Index: logs-{YYYY.MM.DD}  (one per day, rolled over at midnight)
Shards: 5 primary × 3 days hot = 15 shards active
Replicas: 1 (HA, doubles storage)

Mappings (partial):
  timestamp:   date (indexed)
  service:     keyword (exact match, faceted)
  level:       keyword
  trace_id:    keyword (not analyzed, exact lookup)
  message:     text (full-text indexed, analyzer: standard)
  duration_ms: integer (range queries)
  user_id:     keyword (filtered, hashed for PII)

ILM Policy (Index Lifecycle Management):
  Hot phase (0-7d):   shard on 3 ES data-hot nodes
  Warm phase (7-30d): move to S3 (ES Searchable Snapshots, read-only)
  Delete (>30d):      ILM deletes from ES (already in S3)
```

---

## Budget-Constrained Design (Startup at $8K/month for logging)

**Constraint:** $8K/month total for observability.

**What you cut:**
```
Cut Elasticsearch entirely:
  Use ClickHouse instead (10× cheaper for log storage + querying)
  ClickHouse: columnar, compressed, SQL, fast for aggregates
  $800/month for 3-node ClickHouse (vs $4,000 for ES)

Cut Flink:
  Use Vector.dev (lightweight agent + processor in one)
  No Kafka for <50K logs/sec
  Vector: $0 (open source), runs on 2 t3.large = $130/month

Cut Glacier:
  Retain only 60 days total (not 1 year)
  Archive to S3 Standard-IA after 7 days

Cut distributed tracing:
  Use open-source Jaeger (self-hosted) instead of Datadog APM

Result:
  ClickHouse (3×r6g.xlarge): $1,200/month
  S3 (60d × 1.8TB/day):      $2,500/month
  Vector agents:               $130/month
  Jaeger:                      $200/month
  Total:                      ~$4,030/month ✓

Limitation: No real-time full-text search on >7d logs.
           Query historical = Athena SQL only (minutes latency, not seconds).
           No ML anomaly detection (Datadog's AIOps).
```

---

## Component Trade-offs

| Choice | Pros | Cons | Use When |
|--------|------|------|----------|
| **Elasticsearch** for logs | Full-text search, fast, Kibana UI | Expensive, operationally complex, not columnar | Logs with free-text queries, budget allows |
| **ClickHouse** for logs | Columnar, cheap, SQL, fast aggregates | No full-text search (only LIKE), less ecosystem | Structured logs, budget-constrained |
| **Kafka** for ingestion | Durable, replay, high throughput | Complex, another system to operate | >10K logs/sec, need replay guarantee |
| **Vector.dev** (direct to storage) | Simple, low ops, low cost | No replay, if S3/CH down → logs lost | <10K logs/sec, startup |
| **Tail-based sampling** | Catches all errors | RAM-heavy (30s buffer), complex | Error-critical systems |
| **Head-based sampling** | Simple, cheap | Misses rare errors | Non-critical telemetry |
| **Fluentd** agent | Battle-tested, rich plugins | Heavy (Ruby), ~40MB RAM/agent | Legacy, many custom sources |
| **Filebeat/Vector** agent | Lightweight (~10MB RAM), fast | Fewer plugins | Kubernetes, modern stacks |

---

## Key Design Decisions & Rationale

**Why Kafka between agent and processor?**
- Agents write at burst speed; Flink processes at steady state
- If Flink restarts, Kafka buffers the gap (no log loss)
- Multiple consumers (Flink + S3 sink + alert engine) without agent knowing

**Why Cassandra for traces (not ES)?**
- Trace lookup is always by `trace_id` (known key) — no full-text search needed
- Cassandra excels at key-value reads at scale; O(1) partition lookup
- ES would waste inverted index overhead for known-key lookups

**Why ClickHouse for metrics aggregates (not PostgreSQL)?**
- Aggregating 500K log events/sec for "error rate per service per minute"
- ClickHouse: columnar compression, vectorized execution → 100× faster than Postgres for this pattern
- PostgreSQL would need pre-aggregation or struggle at this write rate

**Why S3/Parquet for warm tier (not ES searchable snapshots)?**
- ES searchable snapshots: queries still need ES nodes (warm ES data nodes ~$2K/month)
- S3 + Athena: pay per query, no standing compute, 3× cheaper for rare historical queries
- Trade-off: Athena queries take 10-60 seconds vs ES seconds
