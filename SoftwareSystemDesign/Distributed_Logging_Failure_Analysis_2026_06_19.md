# Failure Analysis: Distributed Logging & Observability System
> Date: 2026-06-19

---

## Failure Mode Matrix

| Component | Fails How | Impact | Detection | Recovery |
|-----------|-----------|--------|-----------|----------|
| Log Agent | Crash / OOM | Logs lost until restart | Agent health check (missing heartbeat) | Auto-restart (k8s DaemonSet), disk buffer drains on restart |
| Kafka | Broker down | Logs queue in agent disk buffer | Kafka JMX: under-replicated partitions | Leader re-election (~30s), consumer reconnects |
| Kafka full (disk) | Log writes start failing | Agents can't ship → disk buffer fills → agent drops | Disk usage alert >80% | Increase retention, add broker, enable log compaction |
| Flink | Task manager crash | Lag builds on Kafka | Kafka consumer lag >N | Flink auto-restores from checkpoint (~30s downtime) |
| Elasticsearch | Node crash | Search degraded (replica takes over) | ES cluster health: YELLOW | ES reshards automatically, no manual action if replicas exist |
| Elasticsearch | Cluster RED (majority down) | Search unavailable | Cluster health API | Restart nodes, restore from snapshot |
| S3 | Eventual (very rare: AWS outage) | Warm tier writes fail | S3 error rate | Retry with exponential backoff; S3 SLA 99.99% |
| Athena | Query timeout | Historical search fails | Query latency metric | Retry, partition pruning, reduce scan size |
| Alert Engine | Crash mid-evaluation | Missed alerts during outage | Watchdog: "alert engine hasn't fired in 5min" | Auto-restart + catch up from Kafka (replay from offset) |
| PagerDuty/Slack | Down | Alerts computed but not delivered | Delivery callback fails | Secondary channel (email fallback), dead letter queue for alert events |

---

## Failure Scenarios — In Depth

### Scenario 1: Log Agent OOM (Most Common)

**What happens:**
```
High-traffic deploy → service logs 10× normal volume
Agent buffer fills (2GB disk limit reached)
Agent starts dropping oldest buffered logs (tail-drop)
Agent process OOM if buffer in memory instead
```

**Why it fails:**
- Agent not configured with disk-backed buffer (default: in-memory)
- No backpressure to the application (logs are fire-and-forget)

**Prevention:**
```
1. Always use disk-backed buffer for Fluentd/Filebeat:
   <buffer>
     @type file
     path /var/log/fluentd-buffer
     total_limit_size 5GB
     overflow_action drop_oldest_chunk  ← not throw_exception (kills agent)
   </buffer>

2. Agent resource limits: request 128MB, limit 512MB (k8s)
   OOMKilled → k8s restarts agent automatically

3. Sampling at application level:
   if log_rate > 50K/sec: enable aggressive sampling immediately
   Use log4j/logback rate limiter: max 1000 log lines/sec per logger
```

**Recovery:**
- Agent restarts (k8s restarts automatically, ~5s)
- Resumes from disk buffer (no lost logs in buffer, only overflow is lost)
- Alert: "log agent buffer utilization > 80%" → investigate before overflow

---

### Scenario 2: Kafka Consumer (Flink) Fails

**What happens:**
```
Flink task manager crashes (OOM, node failure, bad deploy)
Consumer group stops reading → Kafka partition offsets freeze
Logs still arrive into Kafka → pile up unconsumed
No enrichment, no sampling, no writes to ES or S3 during outage
```

**Three sub-cases:**

**A — Single task manager crash (partial failure):**
```
Flink Job Manager detects: task manager heartbeat gone
Reschedules task on surviving task managers
Resumes from last checkpoint offset (default checkpoint every 10min)
→ 10 min × 2.5M logs/sec = 1.5B logs to catch up
→ Catch-up lag can take 30-60 min depending on parallelism
```

**B — Full Flink cluster crash:**
```
All task managers down simultaneously (bad deploy, datacenter event)
Job Manager also down → no auto-recovery
Manual intervention required: restart Flink cluster
Resume from last committed Kafka offset (stored in Kafka __consumer_offsets)
Recovery time: 5-30 min depending on how quickly cluster restarts
Kafka retains up to 48h → no log loss as long as cluster back within 48h
```

**C — Flink crash DURING checkpoint write:**
```
Checkpoint is transactional:
  If checkpoint incomplete → Flink reverts to PREVIOUS successful checkpoint
  May replay up to 2 checkpoints worth of events (up to 20 min at default interval)
  Consumer outputs (ES writes, S3 writes) must be idempotent to handle replays
```

**Why it fails:**
- Long checkpoint interval (10min default) → large replay window on crash
- Alert engine depends on Flink output topic → alerts also stop during Flink outage
- ES/S3 writes not idempotent → duplicate log entries after replay

**Prevention:**
```
1. Checkpoint interval: 30s (not 10min)
   StreamExecutionEnvironment.enableCheckpointing(30_000)
   → Max replay window = 30s × 2.5M logs = 75M logs (manageable)

2. Alert engine: separate consumer group reading raw Kafka directly
   → Flink crash does NOT break alerting
   → Alerts evaluate on unprocessed logs (no enrichment, but still fires on errors)

3. Idempotent writes:
   ES: use log's {service + timestamp + hash(message)} as document _id
       → duplicate replay = ES upsert, no duplicate entries
   S3: Flink writes to temp path, renames on checkpoint commit (atomic)

4. Flink HA: Job Manager on 3 nodes (ZooKeeper/Kubernetes leader election)
   → Job Manager crash doesn't bring down task managers
   → New Job Manager elected in <10s, resumes coordination

5. Kafka retention 48h:
   → Flink can be down up to 2 days before log loss
   → Alert: consumer_lag > 1h worth of data → page before approaching limit
```

**What you lose during Flink outage:**
```
✗ PII masking (logs go un-redacted to S3 if using bypass path)
✗ Sampling (all logs, not just sampled subset, must be handled at recovery)
✗ Enrichment (geo, k8s metadata missing)
✓ Alerting (if alert engine reads Kafka directly — preserved)
✓ Raw log durability (Kafka buffer holds everything)
```

**Emergency bypass (for long outages >4h):**
```
Direct sink: configure Filebeat agents to write DIRECTLY to S3 (unprocessed)
Bypass Kafka + Flink entirely
Logs safe in S3 (no enrichment/masking)
After Flink recovers: backfill enrichment from S3 if needed
```

**Detection:**
```
Alert: consumer_lag{group="flink-log-processor"} > 10M  (P1)
Alert: flink_job_status != RUNNING  (P1, dead man's switch via Flink REST API)
Alert: ES ingest rate drops to 0  (P2)
```

---

### Scenario 3: Kafka Cluster Down

**What happens:**
```
Agent tries to produce → "Leader Not Available" / connection refused
Kafka brokers unreachable → agent falls back to disk buffer
Flink consumer: no new messages (paused, not crashed)
Alert engine: completely blind — no events flowing through Kafka
```

**Three sub-cases:**

**A — Single broker down (most common):**
```
Kafka has 3 brokers, replication factor = 3
Affected partitions: leader election triggered (~15-30s)
During election: producers get LeaderNotAvailableException
Agent retries with backoff → reconnects after election completes
Impact: ~30s of buffering in agent disk buffer
No log loss (disk buffer absorbs it)
```

**B — Majority of brokers down (quorum lost):**
```
KRaft controller cannot elect new leaders (no quorum)
Existing partition leaders: stop serving writes (to prevent split-brain)
Producers: all writes rejected
Agent disk buffer starts filling:
  At 500K logs/sec × 500 bytes = 250MB/sec into disk buffer
  5GB disk buffer = 20 seconds of capacity
  After 20s: agent drops oldest chunks → LOG LOSS

Kafka is down 10 minutes → hours of logs lost
```

**C — Network partition between agents and Kafka (agents see Kafka as down, brokers healthy):**
```
Same outcome as B from agent perspective
Brokers still accept writes from anything that can reach them
But agents cannot → disk buffer fills → drops

Fix: place Kafka brokers in same AZ as agent pods (or same VPC)
     minimize network hops between agent and broker
```

**The critical blind spot — alerting goes dark:**
```
Alert engine reads from Kafka → Kafka down → alert engine sees nothing
During Kafka outage, if your services are also failing:
  → Errors generated → logged by service → buffered in agent disk
  → NOT in Kafka yet → alert engine sees 0 events
  → No alerts fire → on-call not paged
  → Outage goes undetected via logging system

This is why you need:
  1. Dead man's switch (Concept #73): alert if Kafka consumer lag stops changing
  2. Independent health checks on services (not log-based) — APM, synthetic monitors
  3. Kafka cluster health alert via JMX/CloudWatch (doesn't go through Kafka itself)
```

**Prevention:**
```
1. Kafka replication factor = 3, min.insync.replicas = 2
   → Can lose 1 broker with zero impact
   → Can lose 2nd broker: reads still work, writes require 2 ISR → may block

2. Multi-AZ Kafka deployment:
   Broker 1: AZ-a, Broker 2: AZ-b, Broker 3: AZ-c
   → Single AZ failure = 1 broker down → leader election, no quorum loss

3. Larger agent disk buffer:
   Default 5GB → increase to 20GB (buy ~80s at peak load, hours at steady state)
   This is cheap insurance (EBS cost ~$1/GB/month)

4. Producer acks=1 vs acks=all:
   acks=1 (default for logs): write ACK'd when leader receives it
   → Broker crash before replica sync = potential loss
   acks=all: write ACK'd only when all ISR replicas confirm
   → Zero loss, but higher latency (~5-10ms extra)
   → For logs: acks=1 is acceptable (losing <1s of logs on broker crash is fine)

5. Kafka cluster health monitoring via JMX (not via Kafka itself):
   under-replicated-partitions > 0 → broker issue → P2 alert
   offline-partitions > 0         → leader election failed → P1 alert
   These metrics exported via JMX → Prometheus → independent of Kafka message flow
```

**Recovery sequence:**
```
1. Kafka brokers restart → leader elections complete (~30s per partition)
2. Agents: detect Kafka available → flush disk buffer (oldest first, preserving order)
3. Buffer flush rate: agents send at max throughput (rate-limited to not overwhelm Kafka)
   20GB buffer / 50MB/s flush = ~400s = 6 min catch-up
4. Flink: resumes consuming from last committed offset (no action needed)
5. Alert engine: starts receiving events again — fires any backlogged alert conditions
6. Check: disk buffer fully drained before declaring incident resolved
```

**What you lose:**
```
✗ Logs generated during outage (if disk buffer exhausted)
✗ Alerting during entire Kafka outage (blind window)
✗ Real-time search in ES (Flink wasn't writing)
✓ Logs buffered in agent disk (if outage short enough)
✓ Service health (if you have independent APM/synthetic monitors)
```

**Key rule:**  
> Kafka down = your entire observability pipeline is blind. This is why observability infrastructure itself needs independent monitoring that doesn't route through the system being observed.

---

### Scenario 4 (was 3): Elasticsearch Splits (Hot Shard Imbalance)

**What happens:**
```
service="payment-service" logs 100× more than others
All payment logs → same ES shard (partitioned by service_name)
One shard = hot, CPU-pegged → ES rejects writes → logs lost
```

**Why it fails:**
- Naive sharding by `service_name` → hot partitions for high-volume services

**Prevention:**
```
Option A: Shard by time bucket + service hash:
  Kafka partition key = hash(service_name + timestamp_bucket_1min)
  → distributes payment logs across multiple partitions/shards

Option B: ES write via ingest pipeline with routing:
  If log.service IN high_volume_services:
    route to index logs-highvolume-{date}  (more shards)
  Else:
    route to logs-default-{date}

Option C: ES data stream with dynamic shard count:
  ES auto-adjusts shard count based on ingestion rate
  → Rollover when shard hits 50GB or 1B docs
```

**Detection:**
```
ES shard size alert: any shard > 50GB
ES hot thread alert: node CPU > 90% sustained 5min
Ingestion rejection rate > 0
```

---

### Scenario 4: Kafka Disk Full (Cascading)

**What happens:**
```
Large deploy → log spike → Kafka disk fills to 95%
Kafka starts rejecting produce requests
Agent disk buffers fill (5GB limit)
Agent starts dropping logs
Alert fires AFTER logs already dropped (logs about the issue are gone)
```

**This is particularly bad because: the logs you need to debug the outage are the ones being dropped.**

**Prevention:**
```
1. Alert at 70% (not 90%) — give 4-6 hours of headroom
   disk_usage{kafka_broker} > 70% → PagerDuty P2

2. Kafka retention: time-based, not size-based:
   log.retention.hours=48       ← drop after 48h regardless
   NOT log.retention.bytes (unreliable as throttle)
   
3. Separate high-volume topics:
   logs.errors   → 7 day retention (small volume)
   logs.all      → 48h retention (huge volume)
   
4. Agent-level rate limiting:
   Max throughput per agent = 50MB/s
   Excess: sample + log a "sampling active" warning (meta-log, always kept)
```

**Recovery:**
```
Emergency: increase Kafka disk (EBS resize, live — AWS supports this)
Emergency: lower retention to 12h temporarily (free space quickly)
Post-incident: add more Kafka brokers, re-partition
```

---

### Scenario 5: PII Leak (Logs Contain Sensitive Data)

**Not a availability failure — a compliance/security failure. Often overlooked.**

**What happens:**
```
Developer adds: log.info("Processing payment for user: {}", user.toJson())
user.toJson() includes: card_number, CVV, SSN
Now PII is in plain-text logs, indexed in Elasticsearch, searchable by any engineer
GDPR violation, SOC2 failure, potential breach notification required
```

**Prevention:**
```
1. PII Masker in Flink (mandatory, not optional):
   Pattern: credit card = \b\d{13,16}\b → replace with [CARD_REDACTED]
   Pattern: SSN = \b\d{3}-\d{2}-\d{4}\b → [SSN_REDACTED]
   Pattern: email in message field → hash (SHA256)
   
   Problem: can't catch custom serializations (user.toJson())
   
2. Structured logging discipline:
   NEVER log objects — log specific safe fields only:
   log.info("Payment processed", "user_id", user.getId(),  // safe: internal ID
                                 "order_id", order.getId(), // safe
                                 "amount_cents", amount);   // safe (no card data)
   
3. OpenTelemetry semantic conventions:
   Use standard attribute names: db.statement, http.url (sanitized)
   http.url should never include query params with tokens

4. Log sink encryption:
   S3 server-side encryption (SSE-S3 or SSE-KMS)
   ES indices encrypted at rest
   
5. GDPR "right to erasure":
   user_id stored as hash (SHA256 + salt) in logs
   Can't retroactively remove logs (immutable) — by design don't store PII
```

---

### Scenario 6: Alert Storm During Deployment

**What happens:**
```
Rolling deploy: 20 services restart in 5 minutes
Each restart: brief spike in error rate (initialization errors, health check failures)
Alert engine: fires 200 alerts in 5 minutes
On-call engineer: woken up 200 times, alert fatigue → misses real issue
```

**Prevention:**
```
1. Deployment-aware alerting:
   Deployment event (from CI/CD) → suppress alerts for service for 5min post-deploy
   Alert rule: ONLY fire if error_rate elevated for >5min continuous (not transient)

2. Alert deduplication window:
   Same alert + same service: suppress for 15min after first fire
   
3. Alert grouping:
   "5 services degraded during deploy-xyz" → 1 alert, not 5

4. Separate alert thresholds:
   During deploy: threshold = 10% error rate (expect transient)
   Normal:        threshold = 1% error rate
   
5. Dead-man's switch:
   If 0 alerts fire in 1 hour → alert "alert system may be down"
   Ensures the alerting system itself is healthy
```

---

### Scenario 7: Component Ahead Fails — Service Can't Reach Log Agent

**What happens:**
```
Log Agent Unix socket unavailable (agent restarting)
Service's log appender configured to write to agent socket
Socket write blocks → service threads block → service latency spikes
```

**Prevention:**
```
Application logging MUST be async + non-blocking + best-effort:
  Logback AsyncAppender: 
    <appender name="ASYNC_FILE" class="ch.qos.logback.classic.AsyncAppender">
      <discardingThreshold>20</discardingThreshold>  <!-- drop when 80% full -->
      <queueSize>1000</queueSize>
      <neverBlock>true</neverBlock>   ← CRITICAL: never block calling thread
      <appender-ref ref="FILE" />
    </appender>

Rule: Logging infrastructure failure must NEVER cause application failure.
      Logs are observability — they support the product, they ARE NOT the product.
```

---

### Scenario 8: Component Behind Fails — Elasticsearch Rejects Writes

**What happens:**
```
ES cluster RED (write quorum lost)
Flink: write to ES fails
Flink: retries with backoff → accumulates pending writes in memory
Flink memory fills → OOM → task manager crash → replay from checkpoint
```

**Prevention:**
```
1. Circuit breaker around ES writes (Concepts_Index #32):
   After 5 failed writes → open circuit → stop writing to ES
   Fallback: write ONLY to S3 (always available, even if ES is down)
   ES recovers → circuit closes → backfill from S3

2. Flink checkpoint before ES write:
   Offset committed to Kafka only AFTER S3 write succeeds (not ES)
   ES write is best-effort (can be rebuilt from S3 snapshot)

3. ES circuit breaker config:
   PUT _cluster/settings {
     "persistent": {
       "indices.breaker.total.limit": "70%"  ← ES rejects writes at 70% heap
     }
   }
   Alert at 60% heap → add nodes before breaker trips

Principle: S3 is your source of truth. ES is a search index.
           If ES burns down, you can rebuild it from S3 (costly, but possible).
           If S3 burns down, you've lost logs permanently.
```

---

## Defense in Depth Summary

```
Layer 1: Agent disk buffer         → absorbs Kafka unavailability (up to hours)
Layer 2: Kafka 48h retention       → absorbs Flink crash (catch up from offset)
Layer 3: S3 as primary store       → ES/ClickHouse can be rebuilt from S3
Layer 4: Async non-blocking logging → logging failure never kills application
Layer 5: Alert engine reads Kafka  → independent of Flink lag
Layer 6: Circuit breaker on ES     → ES failure doesn't cascade to Flink OOM
Layer 7: PII masker in pipeline    → compliance not dependent on developer discipline
Layer 8: Tail-based sampling       → errors always captured regardless of sample rate
```
