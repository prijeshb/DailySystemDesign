# Fraud Detection System — System Design
**Date:** 2026-06-16  
**Difficulty:** Hard  
**Companies:** Stripe, PayPal, Razorpay, UPI/NPCI, Visa, Mastercard

---

## First Principles Thinking

**Do we really need a dedicated fraud detection system?**

Every payment processor faces fraudulent transactions. Without detection:
- Chargebacks → merchant loses revenue + pays chargeback fee (~$25/incident)
- Account takeovers → lose user trust, regulatory fines (PCI-DSS)
- Friendly fraud → users dispute legitimate charges

Can we outsource it? Stripe Radar, Sift exist. But at Razorpay/UPI scale (~100M tx/day), licensing cost > build cost. Also proprietary data about Indian fraud patterns = better models than generic SaaS.

**So yes: build it. Core requirement = real-time decision before money moves.**

---

## Entities

| Entity | Key Attributes |
|--------|---------------|
| **Transaction** | tx_id, amount, currency, merchant_id, card_id, device_id, ip, timestamp, status |
| **Account** | account_id, created_at, kyc_level, linked_devices, linked_cards |
| **Card** | card_id, account_id, bin, last4, issuer, type (credit/debit/prepaid) |
| **Device** | device_id, fingerprint, os, app_version, ip_history |
| **Merchant** | merchant_id, category (MCC), risk_tier, chargeback_rate |
| **Rule** | rule_id, condition_expr, action, priority |
| **Decision** | tx_id, score, triggered_rules, action (APPROVE/DENY/REVIEW), latency_ms |
| **Case** | case_id, tx_id, analyst_id, resolution (fraud/legit), feedback_ts |

---

## Actions

- Receive transaction event → enrich → evaluate rules → score risk → decide
- Analyst reviews flagged transactions → resolves case → feeds back to model
- Ops team creates/updates rules (no code deploy needed)
- Batch retraining of ML model from resolved cases
- Retrospective fraud scan (card compromised at T+6h, scan past 24h)

---

## Data Flow

```
Payment API
    │ (sync, <100ms budget)
    ▼
Fraud Gateway (enrichment + feature fetch)
    │
    ├──► Feature Store (online) → velocity, account history, device profile
    │
    ├──► Rule Engine → deterministic rules (velocity limits, block lists)
    │
    └──► ML Scorer → risk score 0–100
            │
            ▼
       Decision Engine
            │
    ┌───────┴──────────┐
    ▼                  ▼                  ▼
APPROVE             DENY              REVIEW (queue)
(score < 30)    (score > 70 OR      (score 30-70,
                 hard rule hit)      human review)
            │
            ▼
    Case Management UI
            │
    (analyst resolves)
            ▼
    Feedback Loop → Kafka → Feature Store update + model retraining
```

**Async path** (after decision sent):
```
Transaction event → Kafka → Stream Processor (Flink)
  → update velocity counters (Redis)
  → enrich data warehouse (Cassandra/BigQuery)
  → graph analytics (Neo4j/Neptune) — fraud ring detection
```

---

## High Level Design

```
                         ┌─────────────────────────────────────┐
                         │          Fraud Detection Platform    │
                         │                                      │
Payment API ──(gRPC)──►  │  ┌──────────────┐                   │
                         │  │Fraud Gateway  │                   │
                         │  │(enrichment)   │                   │
                         │  └──────┬───────┘                   │
                         │         │ parallel                   │
                         │    ┌────┴─────┐  ┌──────────────┐   │
                         │    │  Rule    │  │  ML Scorer   │   │
                         │    │  Engine  │  │ (model server│   │
                         │    │ (Drools) │  │  TF Serving) │   │
                         │    └────┬─────┘  └──────┬───────┘   │
                         │         └────────┬───────┘           │
                         │                  ▼                   │
                         │          Decision Engine             │
                         └─────────────────┬───────────────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                       ▼
                APPROVE                 DENY                   REVIEW
                                                                   │
                                                         ┌─────────▼──────────┐
                                                         │  Case Management   │
                                                         │  (Analyst UI)      │
                                                         └─────────┬──────────┘
                                                                   │ feedback
                                                                   ▼
                                                         ┌──────────────────┐
                                                         │  Feature Store   │
                                                         │  + Model Retrain │
                                                         └──────────────────┘
```

**Async Pipeline:**
```
All transactions ──► Kafka ──► Flink ──► Redis (velocity counters)
                                     ──► Cassandra (tx history)
                                     ──► Neo4j (fraud ring graph)
                                     ──► BigQuery (analytics, model training data)
```

---

## Low Level Design

### 1. Feature Extraction (Enrichment Layer)

Before rules/ML can run, we need features. Done in <10ms.

```python
def enrich_transaction(tx: Transaction) -> Features:
    # Parallel fetches (async, all at once)
    results = await asyncio.gather(
        fetch_velocity(tx.account_id, tx.card_id, tx.device_id),  # Redis
        fetch_account_profile(tx.account_id),                       # Postgres
        fetch_device_profile(tx.device_id),                         # Redis/Cassandra
        fetch_merchant_risk(tx.merchant_id),                        # Redis/Postgres
        fetch_card_history(tx.card_id, window_24h),                 # Redis
    )
    return Features(
        # Velocity
        tx_count_1h=results[0]['tx_count_1h'],
        tx_amount_1h=results[0]['tx_amount_1h'],
        unique_merchants_24h=results[0]['unique_merchants_24h'],
        
        # Account
        account_age_days=results[1]['account_age_days'],
        kyc_level=results[1]['kyc_level'],
        
        # Device
        device_seen_before=results[2]['first_seen_ts'] < tx.timestamp,
        device_account_count=results[2]['account_count'],  # mule accounts share device
        
        # Merchant
        merchant_risk_tier=results[3]['risk_tier'],
        merchant_chargeback_rate=results[3]['chargeback_rate'],
        
        # Card
        card_tx_count_24h=results[4]['tx_count'],
        card_new_merchant=tx.merchant_id not in results[4]['known_merchants'],
        
        # Derived
        amount_vs_avg=tx.amount / (results[0]['avg_amount_30d'] + 1e-6),
        hour_of_day=tx.timestamp.hour,
        is_weekend=tx.timestamp.weekday() >= 5,
    )
```

**Why parallel fetches?** Sequential = 5 × 2ms = 10ms. Parallel = max(2ms) = 2ms.

### 2. Velocity Checks (Redis Sliding Window)

```
Key pattern: velocity:{entity}:{entity_id}:{window}
  velocity:account:acc_123:1h   → sorted set of (tx_id, timestamp)
  velocity:card:card_456:24h    → sorted set
  velocity:device:dev_789:1h    → sorted set
  velocity:ip:1.2.3.4:1h        → sorted set

On each transaction (async, after decision):
  ZADD velocity:account:{id}:1h {timestamp} {tx_id}
  ZREMRANGEBYSCORE velocity:account:{id}:1h -inf {now - 3600}  # prune old
  ZCARD velocity:account:{id}:1h  → count in last 1h

For amount sum:
  Use Redis sorted set with amount as score, sum via ZRANGEBYSCORE sum
  Or: separate hash: HINCRBYFLOAT velocity:amount:account:{id}:1h {amount}
```

**Sliding window vs tumbling:** Sliding window is accurate but expensive (prune on each check). For velocity, Redis sorted set prune is O(log N) — acceptable.

**Trade-off:** Exact sliding window uses ~1KB/user/window. For 50M active users × 5 windows = 250GB Redis. Use approximation (2-bucket tumbling: current minute + previous minute weighted) for large user bases.

### 3. Rule Engine

Rules are DAG-evaluated, not sequential. High-priority hard rules evaluated first (fail fast).

```python
# Hard rules (immediate DENY, no ML needed)
rules = [
    Rule(
        id="R001",
        priority=1,  # highest
        condition=lambda f: f.card_id in global_block_list,
        action=DENY,
        reason="stolen_card"
    ),
    Rule(
        id="R002",
        priority=1,
        condition=lambda f: f.tx_count_1h > 20,
        action=DENY,
        reason="velocity_limit_exceeded"
    ),
    Rule(
        id="R003",
        priority=2,
        condition=lambda f: f.amount > 100000 and f.account_age_days < 7,
        action=REVIEW,
        reason="high_amount_new_account"
    ),
    Rule(
        id="R004",
        priority=3,
        condition=lambda f: f.merchant_risk_tier == "HIGH" and f.amount > 50000,
        action=REVIEW,
        reason="high_risk_merchant_large_amount"
    ),
]

def evaluate_rules(features: Features) -> Optional[Decision]:
    for rule in sorted(rules, key=lambda r: r.priority):
        if rule.condition(features):
            return Decision(action=rule.action, reason=rule.reason, triggered_by=rule.id)
    return None  # no rule triggered → proceed to ML
```

**Rules stored in DB, loaded to in-memory cache (TTL 60s).** Ops team updates via UI → propagates within 60s. No code deploy.

**Why rules before ML?** Hard block-list checks are O(1), deterministic, explainable. Reduces ML call volume by ~30%.

### 4. ML Scorer (Risk Score 0–100)

**Model:** Gradient Boosted Trees (XGBoost/LightGBM) — industry standard for tabular fraud features.
- Why not deep learning? Tabular data doesn't benefit much from DL. GBT is faster inference, more explainable, easier to debug.
- SHAP values for explainability (which features drove this score → analyst sees why).

```python
# Feature vector for model (same features as above, encoded)
feature_vector = [
    tx_count_1h,          # float
    tx_amount_1h,         # float
    amount_vs_avg,        # float
    account_age_days,     # float
    device_seen_before,   # 0/1
    device_account_count, # int
    merchant_risk_tier,   # one-hot: [LOW=0, MEDIUM=1, HIGH=2]
    card_new_merchant,    # 0/1
    hour_of_day,          # int
    is_weekend,           # 0/1
    ...                   # ~50 features total
]

score = model.predict(feature_vector)  # returns 0.0–1.0 → multiply by 100
```

**Model serving:** TensorFlow Serving or Triton Inference Server.
- Latency: ~5ms for GBT inference (vs ~20ms for DNN)
- Batching: single inference per request (can't batch; transactions arrive one at a time at <100ms budget)

**Model retraining:**
```
Offline (batch, nightly):
  Resolved cases from last 30 days → feature extraction → label (fraud=1, legit=0)
  Retrain model → A/B shadow mode (new model scores alongside old, decision from old)
  Evaluate: AUC, precision@recall_90, false positive rate
  If better: promote to production (blue-green model swap in model server)
```

### 5. Decision Engine

```python
def decide(tx: Transaction, features: Features) -> Decision:
    # Hard rules first (fast path, no ML needed)
    rule_decision = evaluate_rules(features)
    if rule_decision and rule_decision.action == DENY:
        return rule_decision  # short-circuit
    
    # ML Score
    score = ml_scorer.score(features)  # 0-100
    
    # Combine rule soft-signals + ML score
    if rule_decision and rule_decision.action == REVIEW:
        score = max(score, 60)  # floor score if rule flagged
    
    # Threshold-based decision
    if score < 30:
        return Decision(action=APPROVE, score=score)
    elif score > 70:
        return Decision(action=DENY, score=score)
    else:
        return Decision(action=REVIEW, score=score)
```

**Thresholds are configurable:** Risk team adjusts 30/70 based on:
- False positive rate (legitimate transactions blocked → lost revenue)
- False negative rate (fraud missed → chargebacks)
- Business decision: card-present (lower risk, tighter thresholds) vs card-not-present (higher risk, looser)

### 6. Feature Store

Two tiers:
```
Online Store (Redis):         ← <2ms fetch, used by real-time path
  - velocity counters
  - device fingerprint profile
  - account summary stats

Offline Store (BigQuery/S3):  ← used for model training
  - historical feature snapshots at time-of-transaction
  - point-in-time correct (no future leakage during training)
```

**Point-in-time correctness matters:**
```
Transaction at T=10:00
Model trains using features at T=10:01 (post-outcome) → future leakage → overfit
Correct: snapshot features at T=09:59 → honest training
```

Feature Store (Feast/Tecton) solves this: stores feature snapshots with timestamp → can serve features "as of any past time."

### 7. Graph Analysis (Fraud Ring Detection)

Individual transaction checks miss coordinated fraud:
- Multiple stolen cards used from same device
- Money mule network: card A → merchant B → card C (layering)
- Synthetic identity fraud: fabricated accounts with same phone/email hash

```
Nodes: Account, Card, Device, Phone, Email, IP
Edges: 
  ACCOUNT──uses──CARD
  ACCOUNT──uses──DEVICE
  ACCOUNT──registered_with──PHONE
  DEVICE──has_ip──IP

Graph query (at REVIEW time, not real-time):
  Given fraudulent account A:
  Find all accounts within 2 hops that share device OR card OR phone
  → Flag as potential fraud ring
  → Auto-review all their pending transactions

Graph DB: Neo4j (self-hosted) or Amazon Neptune
Query (Cypher):
  MATCH (a:Account {id: $suspicious_id})-[:USES*1..2]-(related:Account)
  RETURN related.id
  LIMIT 100
```

**Why not real-time?** Graph traversal takes 50-200ms — too slow for <100ms budget. Run async after APPROVE or trigger on REVIEW cases.

---

## Latency Budget

```
Total budget: 100ms (payment API waits for fraud decision)

Breakdown:
  Network (payment API → fraud gateway):   5ms
  Feature fetch (parallel, Redis):         8ms
  Rule engine evaluation:                  2ms
  ML inference (GBT, TF Serving):          5ms
  Decision engine + response:              2ms
  Network back:                            5ms
  ─────────────────────────────────────
  Total:                                  27ms  (p50)
  p99 target:                             80ms  (leave buffer)

Async (fire-and-forget, after response):
  Kafka publish:                           2ms  (non-blocking)
  Velocity counter update:                 5ms  (Redis, consumer)
  Graph update:                           50ms  (Neo4j, consumer)
```

---

## Scale Estimates

```
UPI volume peak: 10,000 TPS (transactions per second)
                 ~864M/day at peak season

Per transaction:
  Feature fetch: 5 Redis reads, 1 Postgres read
  ML inference:  50 features, single GBT model

Redis:
  Velocity keys: 50M active accounts × 5 windows × 200 bytes = 50GB
  Device profiles: 100M devices × 500 bytes = 50GB
  Total: ~200GB Redis cluster (3× replication = 600GB, comfortable on r6g.4xlarge)

ML Model Server:
  10,000 RPS × 5ms/inference = 50,000ms of compute/sec
  = 50 cores needed for inference
  = 5× 10-core TF Serving instances (3× for HA)

Kafka:
  10,000 events/sec × 500 bytes/event = 5MB/s → trivial
  Partitioned by account_id (locality for velocity consumer)

Cassandra (transaction history, 90-day hot):
  864M tx/day × 365 days × 500 bytes = ~158TB/year → shard by account_id
```

---

## Trade-offs

### Rule Engine vs Pure ML

| | Rules Only | ML Only | Hybrid (chosen) |
|--|---|---|---|
| Explainability | Full | Black box (SHAP helps) | Both: rule reason OR feature importance |
| Adaptability | Manual updates | Auto-learns from data | Rules for known patterns, ML for novel |
| New fraud pattern | Miss until rule written | Adapts within 24h (retraining) | ML catches first, rule codifies after |
| False positive | Tunable | Harder to tune | Rule blocks are precise, ML catches gray |

**Chosen: Hybrid.** Rules handle deterministic cases (stolen card list, velocity hard limits). ML handles nuanced patterns (amount + merchant + time + account_age combination).

### Threshold Sensitivity

```
Lower DENY threshold (e.g., 60 instead of 70):
  Pro: catch more fraud
  Con: more false positives → legitimate users blocked → revenue loss, user frustration

Business metric: False Positive Rate = FP / (FP + TN)
Target: FPR < 0.1% (1 in 1000 legitimate transactions blocked)
At 0.1% FPR with 10,000 TPS and 99.9% legitimate = 10 false blocks/sec = 864K/day
→ Still too high. Real-world FPR target: <0.02% (2 in 10,000)
```

### Feature Store — Online vs Offline Sync Lag

```
Fraud happens → Kafka → Flink → Redis update: ~500ms lag
During this 500ms: next transaction from same card sees stale velocity counter
Solution: optimistic counter (increment synchronously in Redis before Kafka, then Kafka for persistence)

Risk: Redis write fails after fraud decision sent → velocity counter stale forever
Fix: TTL + periodic reconciliation from Cassandra (nightly batch corrects drift)
```

---

## Real-World Examples

- **Stripe Radar:** ML + rules, <100ms, scores every Stripe transaction
- **PayPal:** Graph analysis for fraud rings, ~10B tx/year screened
- **UPI/NPCI:** Rule-based initially (2016), added ML layer 2019, <50ms decision
- **Visa Advanced Authorization:** <1ms decision using proprietary AI model, 500+ variables
- **Netflix (account sharing detection):** Similar pattern — velocity + device fingerprinting
