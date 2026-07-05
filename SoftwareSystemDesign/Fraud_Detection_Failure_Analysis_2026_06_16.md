# Fraud Detection System — Failure Analysis
**Date:** 2026-06-16

---

## Failure-First Map

```
Payment API
    │
    ▼
Fraud Gateway ──► [FAILURE 1: Gateway crash]
    │
    ├──► Redis (Feature Store) ──► [FAILURE 2: Redis down]
    │
    ├──► Postgres (Account Profile) ──► [FAILURE 3: DB slow/down]
    │
    ├──► Rule Engine ──► [FAILURE 4: Rule engine crash / bad rule]
    │
    └──► ML Model Server ──► [FAILURE 5: Model server down / stale model]
            │
            ▼
       Decision Engine ──► [FAILURE 6: Incorrect thresholds]
            │
            ▼
       Kafka ──► [FAILURE 7: Kafka down]
            │
            ▼
       Flink Consumers ──► [FAILURE 8: Consumer lag / crash]
            │
       Neo4j (Graph) ──► [FAILURE 9: Graph DB overloaded]
```

---

## FAILURE 1: Fraud Gateway Crashes

**Scenario:** Fraud gateway pod dies mid-request during peak (Diwali sale, 10K TPS).

**What breaks:** Payment API is waiting for fraud decision. Gateway is unreachable.

**Impact:** All transactions blocked (payment API waits for 100ms then times out → return error to user).

### Prevention
```
- Deploy 3+ gateway pods behind load balancer
- Health checks: /health endpoint, pod restart on failure (<5s recovery)
- k8s HPA: auto-scale on CPU >70%
- Horizontal pod autoscaler with headroom: run at 60% capacity normally
```

### Recovery (Fail-Open vs Fail-Closed)

Critical design decision:

| Mode | Behavior on Gateway Failure | Risk |
|------|----------------------------|------|
| **Fail-closed** | DENY all transactions | Zero fraud during outage BUT revenue loss. 10K TPS × $50 avg = $500K/sec blocked |
| **Fail-open** | APPROVE all transactions | Fraud gets through during outage, but revenue preserved |
| **Fail-safe** | Apply hardcoded minimal rules, then APPROVE | Best: block obvious fraud (stolen card list cached locally), approve rest |

**Chosen: Fail-safe.**

```python
# Fallback when gateway is degraded
def fraud_check_fallback(tx: Transaction) -> Decision:
    # Step 1: Check local in-memory block list (refreshed every 60s from Redis)
    if tx.card_id in local_block_list_cache:
        return Decision(DENY, "stolen_card_local_cache")
    
    # Step 2: Basic velocity (from local counter if Redis is up, skip if not)
    if local_velocity_counter.get(tx.account_id, 0) > 50:  # very high threshold
        return Decision(DENY, "velocity_circuit_open")
    
    # Step 3: Approve with flag for async review
    return Decision(APPROVE, "degraded_mode", flag_for_review=True)
```

All transactions approved in degraded mode are queued for retrospective fraud scan when system recovers.

---

## FAILURE 2: Redis Cluster Down

**Scenario:** Redis primary fails. Sentinel election takes 30s. All feature fetches return errors.

**Impact:** Velocity counters unavailable. Device profiles unavailable. Enrichment step fails.

### What Fails
- Velocity checks → cannot enforce per-account limits → velocity fraud possible
- Device profile → cannot detect new device anomaly
- Block list → cannot check stolen cards (if stored in Redis)

### Prevention
```
Redis Sentinel: 1 primary + 2 replicas + 3 sentinels
  Primary fails → sentinel votes → replica promoted in ~30s

Redis Cluster: 6 nodes (3 primary + 3 replica), hash slots
  Single shard failure → 1/3 velocity data unavailable → rest works
  Full cluster down → very unlikely (would require 3+ node simultaneous failure)

Block list specifically: also store in local memory (LRU cache, 10K entries, refresh every 60s)
  → Stolen card check still works even if Redis is down
```

### Recovery Steps
```
1. Redis Sentinel promotes replica → new primary available
2. Velocity counters: stale for 30s window
   → Reconstruct from Kafka (re-read last 1h events to Flink → rebuild counters)
3. Circuit breaker on Redis:
   → 5 consecutive failures → OPEN → serve fallback (default: approve with REVIEW flag)
   → Probe every 10s → HALF-OPEN → if Redis responds → CLOSED
```

### Staleness Risk During Failover

```
T=0:    Primary has: account:123 tx_count_1h = 15
T=5s:   Primary crashes
T=35s:  Replica promoted (was 5s behind at crash time, so: tx_count_1h = 14)

During T=5s to T=35s: velocity unchecked → attacker could push 10 more txns
T=35s+: promoted replica shows tx_count_1h=14 → 1 transaction "erased"

Risk: minor. Gap is 30s. Attacker would need 30s window to exploit.
Mitigation: immediately cap all approve decisions at 50% of normal velocity limit during recovery mode
```

---

## FAILURE 3: Postgres (Account Profile) Slow

**Scenario:** Postgres replica lag spikes to 5s during bulk analytics query. Feature fetch timeout.

**Impact:** Account age, KYC level unavailable → model runs with missing features → score unreliable.

### Why This Happens
- Analytics team runs expensive query on same read replica used for fraud features
- Replica WAL replay slows → lag spikes

### Prevention
```
- Separate read replicas: fraud service reads from dedicated replica (tagged: "fraud-replica")
- Analytics team writes to OLAP (BigQuery), never queries production replica
- PgBouncer connection pool: max 50 connections to fraud replica → analytics cannot starve fraud
- Query timeout for fraud: 10ms. If Postgres doesn't respond in 10ms → fallback value
```

### Fallback Values
```python
def fetch_account_profile_with_fallback(account_id: str) -> AccountProfile:
    try:
        return fetch_from_postgres(account_id, timeout_ms=10)
    except TimeoutError:
        # Return conservative defaults (treat as unknown/new account)
        return AccountProfile(
            account_age_days=0,       # treat as new → higher risk score
            kyc_level=KYC.UNVERIFIED, # worst case
            source="fallback"
        )
```

Conservative fallback = model scores higher → more REVIEW decisions during outage. Acceptable trade-off (more analyst work) vs fraud through.

---

## FAILURE 4: Bad Rule Deployed

**Scenario:** Ops team deploys rule: "DENY all transactions > ₹1000 on weekends." Typo in condition → denies ALL transactions on weekends.

**Impact:** 100% DENY rate → revenue disaster. 5 minutes = millions in blocked transactions.

### Prevention
```
Rule deployment pipeline:
  1. Shadow mode: new rule evaluated but decision not applied → log only
  2. Monitor: if new rule would trigger on >X% of traffic → auto-block deployment
  3. A/B mode: apply to 1% of traffic for 10 minutes → compare DENY rate vs control
  4. Full rollout if DENY rate within ±2% of baseline

Rule validation before save:
  - Test against last 10K transactions
  - Show: would have triggered N times (N%), DENY N%, REVIEW N%
  - If >2% additional DENY → require manual confirmation with reason
```

### Fast Rollback
```
Rules stored in Redis (primary source) + Postgres (backup/audit)
  Redis TTL: rules cached in gateway for 60s
  
Rollback:
  DELETE rule from Redis → propagates within 60s to all gateway instances
  No code deploy needed
  Total blast radius: <60s
```

### Kill Switch
```
Circuit breaker on rule engine:
  If rule engine DENY rate exceeds 50% in 60s → auto-disable rule engine → run ML only
  Alert PagerDuty: "Rule engine DENY rate anomaly: 85% (baseline: 5%)"
```

---

## FAILURE 5: ML Model Server Down / Stale Model

**Scenario A:** TF Serving pod crashes. All ML score requests fail.

**Scenario B:** Bad model deployed. False positive rate spikes to 5% (from 0.02% baseline) → users get wrongly denied.

### Scenario A: Model Server Down

```
Prevention:
  3 TF Serving pods behind LB (k8s)
  Health check: /v1/models/fraud_model → if down → pod restart
  
Fallback (model unavailable):
  - Apply rules only (no ML score)
  - Default score = 50 (gray zone → REVIEW)
  - Alert: "ML scorer unavailable, running rule-only mode"
  - Analyst queue fills up during outage → SLA breach risk if >4h
```

### Scenario B: Bad Model in Production

**Detection:**
```
Real-time monitoring:
  - DENY rate per hour (alert if > 2× baseline)
  - False positive rate from instant chargebacks (user calls support, legit txn denied)
  - Score distribution: if mean score shifts >10 points → anomaly

Dashboard metrics every 5 minutes:
  - APPROVE %: 85% (expected: ~90%)
  - REVIEW %:  10% (expected: ~7%)
  - DENY %:    5%  (expected: ~3%) ← ALERT: 5% >> 3%
```

**Rollback:**
```
Model registry (MLflow):
  v42 (current, bad)  →  v41 (previous, good)
  
Rollback:
  1. Update model alias: "fraud_model" → v41
  2. TF Serving hot-reloads model (no restart, <30s)
  3. Monitor DENY rate → should recover within 5 minutes

Champion/Challenger testing prevents this:
  - New model runs as "challenger" at 5% traffic for 24h
  - Compare: AUC, FPR, TPR vs champion
  - Only promote if both AUC improves AND FPR within acceptable range
```

---

## FAILURE 6: Flink Consumer Lag (Velocity Counters Stale)

**Scenario:** Flink consumer for velocity counter updates falls behind. At 10K TPS, if consumer processes at 8K TPS → lag grows at 2K events/sec → after 10 minutes = 1.2M events behind.

**Impact:** Velocity counters 10+ minutes stale → cannot detect accounts making 100 transactions in last hour (lag hides recent txns).

### Why Flink Lags
- Flink node dies → rebalance takes 2 minutes
- GC pause on Flink worker → processing stalls
- Spike in transaction volume without proportional Flink parallelism

### Prevention
```
Kafka partition count = Flink parallelism (default: 12 partitions, 12 Flink tasks)
Auto-scaling: Flink reactive mode → scale tasks on consumer lag metric
  Lag > 10K events → add 4 tasks
  Lag < 1K events → scale back

Backpressure handling: Flink job monitors lag via Kafka metrics → alerts
Alert threshold: lag > 30 seconds → PagerDuty
```

### Recovery
```
Flink job restarts from last committed checkpoint (every 30s):
  At most 30s of re-processed events → idempotent ZADD (sorted set, duplicate key = no-op)
  
After restart, catch-up mode:
  Process at 3× normal throughput (Flink reads from Kafka at full speed)
  ~3-4 minutes to catch up 10 minutes of lag
  
During catchup: velocity counters may undercount → slightly more fraud through
  Acceptable: brief window, hard rules still protect
```

### Optimistic Velocity Counter (Prevent Stale During Normal Ops)

```
Instead of async-only update:
  Write path: increment Redis counter synchronously (before Kafka) → then Kafka for durability
  
  Risk: Redis write fails → counter lost
  Mitigation: Periodic reconciliation (every 5 minutes, Flink emits reconciliation event
              → Redis SET counter = exact_count_from_kafka_state)
```

---

## FAILURE 7: What If a Component Ahead Fails (Payment API)

**Scenario:** Payment API goes down. Fraud gateway is healthy but receives no traffic.

**Impact on Fraud System:** None directly. Fraud system is downstream — if no request comes, nothing to evaluate.

**But:** When Payment API recovers, it may replay missed transactions (if using Kafka internally):
```
Payment API recovery → burst of transactions hits fraud gateway simultaneously
10K normal TPS + 50K replay TPS = 60K TPS → 6× normal load

Prevention:
  - Rate limit at API gateway: 15K TPS max to fraud gateway
  - Replay transactions in order with throttle: max 20K TPS replay burst
  - Fraud gateway HPA triggers: add pods in <60s if CPU > 70%
```

---

## FAILURE 8: What If Component Behind Fails (Case Management)

**Scenario:** Case Management UI goes down. Analysts can't review REVIEW cases.

**Impact:** REVIEW queue fills up. Transactions in REVIEW are already held (not approved or denied).

```
REVIEW SLA: decision within 2 hours
If Case Management down for >2h:
  Auto-escalation: transactions > 2h in queue → auto-APPROVE (if low-risk features)
                                               → auto-DENY (if high-risk features: amount > ₹1L AND new device)

Queue backup: Redis list for pending cases, Postgres for persistence
If UI down: analysts can still query Postgres directly (manual SQL) → emergency workaround
```

### Analyst Queue Saturation

```
Normal: 7% REVIEW rate × 10K TPS = 700 cases/sec → 2.5M cases/hour
Analysts: 50 analysts × 200 cases/hour = 10K cases/hour capacity → NOT ENOUGH

Reality check:
  - Most REVIEW cases auto-resolved by secondary ML pass (after 10 minutes of velocity data)
  - "Auto-approval ML": after 10 minutes, re-score with fresh velocity → if score drops to <30 → APPROVE
  - Human review only for: high-amount (>₹1L), new account (<7 days), device mismatch

Realistic human queue: ~5% of REVIEW → 0.35% of total TPS = 35 cases/sec
50 analysts × 200/hour = 10K/hour = 2.7/sec → comfortable
```

---

## FAILURE 9: Graph DB (Neo4j) Overloaded

**Scenario:** Fraud ring investigation query traverses 6-hop graph on 10M node graph → query takes 45s → Neo4j query queue fills → all graph queries time out.

**Impact:** Graph-based fraud ring detection down. Individual transaction scoring unaffected (graph is async).

### Prevention
```
- Limit traversal depth: max 3 hops (covers 99% of rings, 6-hop is rare)
- Index all node properties used in traversal (account_id, device_id, card_id)
- Separate query pools: analyst investigation (low priority) vs automated fraud ring scan (high priority)
- Read-only replicas for investigation queries; primary for write + automated scan
```

### Timeout and Fallback
```
Graph query timeout: 5s
  If timeout → return partial results (nodes found so far)
  Alert: "Graph query timeout for account {id}, flagging for manual investigation"
  
Neo4j OOM (large fraud ring investigation):
  Kill query → alert analyst → export to BigQuery for offline analysis
  BigQuery can handle full-graph analysis (hours, but complete)
```

---

## Defense Depth Summary

```
Layer 1: Local in-memory cache (block list, rules) → survives Redis outage
Layer 2: Circuit breaker on each external call → fail-fast, fallback values
Layer 3: Fail-safe mode → approve with REVIEW flag, not fail-closed
Layer 4: Retrospective scan → when system recovers, scan all transactions approved in degraded mode
Layer 5: Monitoring → DENY rate, REVIEW rate, FPR — any anomaly → PagerDuty in <5 min
Layer 6: Manual kill switches → disable rule engine, disable ML, apply rules-only mode
Layer 7: Model rollback in <30s → MLflow registry + TF Serving hot reload
```
