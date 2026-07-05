# Search Autocomplete — Failure Analysis
*Date: 2026-05-21 | Failure-First Approach*

---

## Failure Taxonomy

For each component, we ask:
1. **What fails?** (the fault)
2. **What's the blast radius?** (user impact)
3. **Prevention** (design)
4. **Detection** (monitoring)
5. **Recovery** (runbook)

---

## 1. Trie Server Fails

### Case A: Single Trie Server Crashes

**Blast radius**: All queries for that prefix range → no suggestions (or stale cache serves).

**Prevention**:
- Each prefix shard has **1 primary + 2 replicas** (read from any replica)
- Replicas updated via periodic snapshot sync (every 5min) + delta log
- Client-side: if primary times out in 50ms → retry on replica

**Detection**:
- Health check every 10s from load balancer
- Alert if p99 latency > 200ms or error rate > 0.1%

**Recovery**:
- Load balancer marks server unhealthy → traffic shifts to replicas
- Dead server restarts, loads latest trie snapshot from S3
- Rebuild time from snapshot: ~2min (acceptable)

### Case B: ALL Trie Servers for a Shard Fail

**Blast radius**: No suggestions for that prefix range (e.g., all "s*" queries).

**Prevention**:
- Maintain **stale Redis cache** as last-resort fallback (TTL extended to 1hr on failure)
- Return last-known-good suggestions rather than empty

**Recovery**:
- PagerDuty alert → ops restores from S3 snapshot
- Graceful degradation: API returns `{"suggestions": [], "degraded": true}`
- Do NOT return 500 — return empty suggestions with flag

---

## 2. Redis Cache Fails

**Fault**: Redis cluster goes down (OOM, network partition, node failure).

**Blast radius**:
- Cache miss storm → all 87k RPS hits Trie Servers directly
- Trie servers designed for ~20k RPS → overload → cascade failure risk

**Prevention**:
- Redis cluster: 3-shard, each with 1 primary + 1 replica
- Circuit breaker in Autocomplete Service: if Redis error rate > 50% → bypass cache, go direct to trie (with rate limiting)
- Client-side debounce (150ms) naturally smooths burst

**Detection**:
- Redis memory usage alert at 80%
- Latency spike alert on cache layer

**Recovery**:
- Redis replication: replica promoted to primary in < 30s (Redis Sentinel)
- Cache warms up naturally from traffic within 5min

---

## 3. Kafka Fails (Write Pipeline)

**Fault**: Kafka brokers unavailable → `raw-queries` topic unwritable.

**Blast radius**:
- **Read path unaffected** (Kafka is write-only for frequency data)
- Frequency counts stop accumulating → trie becomes stale over time
- No immediate UX impact; becomes noticeable after 24-48hrs

**Prevention**:
- Kafka cluster: 3 brokers, replication factor 3
- Producer: `acks=all`, `retries=5`, exponential backoff
- If Kafka unavailable → write to **local buffer file** (fallback producer)
- Buffer replays when Kafka recovers

**Detection**:
- Consumer lag alert: if `raw-queries` consumer lag > 1M messages → alert
- Producer error rate metric

**Recovery**:
- Kafka broker restart < 2min
- Unprocessed buffer replays → Spark processes backlog
- Idempotent processing: Spark uses `query + window_start` as dedupe key

---

## 4. Spark Frequency Aggregation Fails

**Fault**: Spark job crashes mid-run.

**Blast radius**:
- Frequency window partially aggregated → some queries under-counted
- Trie rebuild uses stale frequencies

**Prevention**:
- Spark Streaming uses **checkpointing** to S3 every 1min
- Exactly-once processing via Kafka transactions
- On failure: restart from last checkpoint

**Recovery**:
- Spark auto-restart via YARN/Kubernetes restart policy
- Worst case: 1min of lost aggregation → negligible at daily scale

---

## 5. CDN Edge Cache Stale During Trending Event

**Fault**: Breaking news causes "hurricane florida" to spike 1000x. CDN serves stale top-k (no "hurricane" suggestions) for up to 1hr.

**Blast radius**: Poor UX for exactly the queries users care most about during a news event.

**Prevention — Trending Detection System**:
```
Kafka raw-queries
    │
    ▼
[Trending Detector (Flink, 1-min windows)]
    │ if query_count > 5x baseline for that prefix
    ▼
[Trending Cache Layer] ← separate from main trie
    │
    ▼
Autocomplete Service: merge trie results + trending results
    │ if trending query exists
    ▼
[CDN Cache Bypass + Purge API call for affected prefix]
```

**Trade-off**: Trending layer adds latency for affected prefixes (~+20ms). Acceptable for edge-case.

---

## 6. Network Partition (Between API Servers and Trie Servers)

**Fault**: Network segment failure isolates some API servers from Trie servers.

**Blast radius**: Those API servers can't reach Trie → fall back to Redis → Redis also potentially unreachable → return empty.

**Prevention** (CAP Theorem consideration):
- Autocomplete chooses **Availability over Consistency** (AP system)
- Always return *something* — stale > empty
- API servers cache last 1000 results locally (in-process, 30s TTL)
- This micro-cache handles network blip

**Detection**:
- API server logs: `trie_timeout_ms > 100` → alert
- Cross-datacenter latency dashboards

---

## 7. Trie Rebuild Corrupts Data

**Fault**: Bug in Trie Builder writes malformed trie. All trie servers reload bad data.

**Blast radius**: Garbage suggestions or crashes for all users.

**Prevention — Blue/Green Trie Deployment**:
```
Trie Builder completes new trie
    │
    ▼
Deploy to STAGING trie servers (10% of fleet)
    │
    ▼
Run automated validation:
  - Top 1000 prefixes return expected results?
  - No null/empty responses for common prefixes?
  - p99 latency < 50ms?
    │
    ├── PASS → rolling deploy to remaining 90%
    └── FAIL → keep old trie, alert on-call, discard new build
```

**Recovery**:
- Each server keeps **2 trie snapshots** on disk (current + previous)
- On bad deploy: `trie-server rollback --version=prev`

---

## 8. Thundering Herd on Cache Miss

**Scenario**: Redis TTL expires for a popular prefix simultaneously across many keys. 10k requests all miss cache and hit Trie servers at once.

**Prevention — Probabilistic Early Expiration**:
```python
# Instead of hard TTL, use staggered expiry
base_ttl = 300  # 5 min
jitter = random.randint(-30, 30)  # ±30s
actual_ttl = base_ttl + jitter
```

Also: **Cache-aside with mutex lock** — first request acquires a lock, fetches from Trie, populates cache. Other concurrent requests wait (short timeout) rather than all querying Trie.

---

## 9. Data Poisoning (Autocomplete Manipulation)

**Fault**: Bad actors spam queries to make malicious/inappropriate terms trend.

**Prevention**:
- Query filter pipeline: block profanity, hate speech before writing to Kafka
- Anomaly detection: flag accounts submitting >1000 identical queries/hour
- Manual review queue for newly trending terms before they surface

---

## Failure Summary Matrix

| Component | Fail Type | User Impact | Recovery Time | Fallback |
|-----------|-----------|-------------|---------------|----------|
| Trie server (single) | Crash | None (replicas) | <30s | Replica |
| Trie server (all shards) | Full outage | Empty suggestions | 2-5min | Stale Redis |
| Redis | OOM/crash | Trie overload | 30s (failover) | Direct trie + rate limit |
| Kafka | Broker down | Stale frequencies (hours) | 2min | Local buffer |
| Spark | Job crash | Stale trie | Minutes | Checkpoint restart |
| CDN | Stale during trending | Missed trends | 1hr | Trending bypass layer |
| Trie rebuild | Bad data | Garbage suggestions | Minutes | Blue/green + rollback |
