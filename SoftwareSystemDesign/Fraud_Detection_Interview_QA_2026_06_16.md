# Fraud Detection System — Interview Q&A
**Date:** 2026-06-16

---

## Opening Questions

**Q: Design a fraud detection system for a payment gateway like Razorpay.**

Strong opener answer (2-3 sentences, not a monologue):
> "Before I dive in, let me clarify scope. Are we talking about card-present fraud, card-not-present (online), account takeover, or all three? And what's the latency requirement — does the payment API wait for fraud decision or can it be async?"

Expected interviewer answer: "Card-not-present online payments, real-time, <100ms."

Then structure: entities → data flow → high-level → low-level.

---

## Core Design Questions

**Q: What's the most important constraint in this system?**

> Latency + accuracy trade-off. We need a decision in <100ms while being accurate enough that:
> - False negative rate is low (fraud slips through → chargebacks)
> - False positive rate is very low (<0.02%) — blocking legitimate transactions loses revenue and destroys trust faster than fraud itself.
> This drives every architectural decision: why we use GBT not deep learning, why we cache features in Redis not fetch from DB, why rules evaluate before ML.

---

**Q: Walk me through what happens when a transaction comes in.**

> 1. Payment API sends transaction to fraud gateway (gRPC, <5ms)
> 2. Gateway fetches features in parallel: velocity counters from Redis, account profile from Postgres, device fingerprint from Redis — all parallel, ~8ms
> 3. Rule engine evaluates deterministic rules first: is card on block list? Is velocity > 20 tx/hr? — ~2ms
> 4. If no hard DENY rule: ML scorer runs GBT inference on ~50 features → risk score 0-100 — ~5ms
> 5. Decision engine applies thresholds: <30=APPROVE, >70=DENY, else REVIEW
> 6. Response back to payment API — <30ms total p50
> 7. Async: publish to Kafka → Flink updates velocity counters, enriches graph, writes to data warehouse

---

**Q: Why GBT (Gradient Boosted Trees) over a neural network for fraud detection?**

> Three reasons:
> 1. **Speed:** GBT inference is ~5ms vs ~20ms for a DNN of comparable accuracy. In a <100ms budget, every ms matters.
> 2. **Tabular data:** Fraud features are tabular (amount, velocity, account age) — GBT consistently beats DNN on tabular data in benchmarks. DNNs excel at unstructured data (image, text).
> 3. **Explainability:** SHAP values on GBT are fast and interpretable. An analyst reviewing a case needs to know WHY — "score was 78 because: unusual merchant category (40%), high velocity (30%), new device (20%), high amount (10%)" is actionable. DNNs require LIME or more complex explanation methods.

---

**Q: How do you handle the cold-start problem for new accounts?**

> New account has no history → most features are null → model uncertainty.
> Three approaches:
> 1. **Default conservative features:** account_age_days=0, tx_count_1h=0 (no history = treated as zero → higher risk by default)
> 2. **New account rules:** separate rule layer for accounts <7 days old: lower velocity thresholds, high-amount transactions auto-REVIEW, new device + new merchant + high amount = auto-DENY
> 3. **Identity signal:** KYC level (verified phone, verified PAN, linked bank) compensates for lack of transaction history. KYC_FULL + new account = lower risk than KYC_NONE + new account.

---

**Q: How do you prevent velocity attacks — someone making 1000 small transactions to test a stolen card?**

> Velocity checks at multiple granularities:
> - Per account: tx_count_1h, tx_count_24h
> - Per card: card_tx_count_1h (catches card testing even if different accounts)
> - Per device: device_tx_count_1h (catches one person testing multiple cards)
> - Per IP: ip_tx_count_1h (catches bot farms with limited IPs)
> - Amount pattern: if 10 transactions all between ₹1-₹100 (testing for live cards) → flag
> 
> These are sliding window counters in Redis sorted sets (ZADD timestamp → ZCOUNT last_hour). Hard rule: card tx_count_1h > 10 → DENY. Why 10? Card testing attacks typically try 50-100 cards at $1 each. 10 per hour from one card is already suspicious.

---

**Q: What happens if your ML model is wrong — high false positive rate?**

> Monitoring catches it fast:
> - Real-time: DENY rate per 5-minute window. Baseline 3%. If 6%+ → PagerDuty alert in <5 min.
> - User feedback: support tickets "my payment was declined" spike → proxy for false positives.
> - Chargeback rate (T+30 days): if disputes don't match DENY reasons → model diverged.
> 
> Rollback: model registry (MLflow). Previous version tagged. TF Serving hot-reloads in <30s. No restart, no downtime.
> 
> Prevention: champion/challenger testing. New model runs at 5% traffic for 24h in shadow mode. Only promotes if AUC improves AND FPR stays within 0.05% of champion.

---

## Follow-up / Deep Dive Questions

**Q: How does fraud ring detection work? Give a concrete example.**

> Suppose account A is confirmed fraud (analyst resolved case). We want to find related accounts.
>
> Graph model: nodes = Account, Card, Device, Phone, Email. Edges = uses relationship.
>
> Example fraud ring:
> ```
> Account A ──uses──► Device D1
>                          │
>                     Account B ──uses──► Card C1
>                                              │
>                                         Account C
> ```
> A and B share device D1 → suspicious. B and C share card C1 → C is a mule account.
>
> Cypher query on Neo4j:
> ```
> MATCH (a:Account {id: "A"})-[:USES*1..2]-(related:Account)
> RETURN DISTINCT related.id
> ```
> → Returns B and C → auto-flag for review.
>
> This is async (50-200ms, too slow for real-time path). Triggered when case resolved as fraud or when score > 85.

---

**Q: How do you handle the trade-off between fail-open and fail-closed when the fraud system goes down?**

> This is a business decision, not purely technical, but the technical answer must support both:
>
> Pure **fail-closed** (deny everything): zero fraud during outage, but at 10K TPS × ₹500 avg = ₹50M/minute in blocked revenue. Unacceptable for >30 seconds.
>
> Pure **fail-open** (approve everything): fraud gets through during 30-minute outage. At 3% fraud rate × 10K TPS × ₹500 × 30min = ₹27M in fraud exposure. Also bad.
>
> **Fail-safe (chosen):** local in-memory cache of top 10K stolen cards (refreshed every 60s). Apply only hard block-list check + very aggressive velocity limit (50 tx/hr, not normal 20). Approve the rest with REVIEW flag. Retrospective scan when system recovers.
>
> Result: catches ~80% of fraud (major stolen cards + extreme velocity) during outage, blocks nothing legitimate.

---

**Q: How do you ensure the velocity counter is accurate?**

> It's eventually consistent, not strongly consistent. That's a deliberate trade-off.
>
> Synchronous (accurate but slower): before responding to payment, update Redis counter, read back new count. Adds 3-5ms.
>
> Async (fast but stale): update via Kafka consumer after decision. Counter lags by ~500ms.
>
> We use **optimistic sync:** increment Redis counter synchronously in the hot path (before Kafka, 2ms), then publish to Kafka for durability. If Redis is temporarily unavailable, fall back to async.
>
> For accuracy guarantee: Flink emits periodic reconciliation events every 5 minutes → Redis SET counter = exact count from Kafka state. Corrects any drift.

---

**Q: How do you train the model? What's the training data?**

> Label source: resolved cases from Case Management.
>   - Analyst marks case as "fraud" or "legit" → label
>   - Chargebacks (T+30 days) → retrospective labels for missed fraud
>
> Point-in-time feature extraction (critical):
>   Features must be "as of the time of transaction" — not current state.
>   Feature Store stores snapshots with timestamp. Training job: for each transaction, retrieve features at transaction timestamp.
>   Without this: "future leakage" — model trains on features that weren't available at decision time → overfit → bad production performance.
>
> Training cycle: nightly batch retrain on last 30 days.
> Why 30 days? Fraud patterns drift. Model trained on 1-year-old data misses new attack patterns. 30 days = fresh patterns, enough data volume.
>
> Class imbalance: 3% fraud rate. Train with class weights (fraud class weight = 33×) or SMOTE oversampling to prevent model from just predicting "legit" always.

---

**Q: How would you handle a scenario where a user's card was stolen and fraudulent transactions happened before you knew about it?**

> Retrospective scan.
>
> T=0h: Attacker uses stolen card → 10 transactions approved (below velocity threshold, small amounts).
> T+6h: User reports card stolen to issuer → issuer notifies us.
>
> When we learn card is compromised:
> 1. Immediately add card to block list → all future transactions denied.
> 2. Trigger retrospective scan: query all transactions on this card in last 24h.
> 3. For each past approved transaction: re-evaluate with updated features (card now known stolen → would have scored 90+).
> 4. Flag all as potential fraud → submit chargebacks to merchants.
> 5. Alert account holder: "suspicious activity detected on your card" + immediate card block.
>
> Retrospective scan on Cassandra: indexed by card_id + timestamp. 24h × 10 tx = trivial. At scale (100M cards), this is triggered only on confirmed stolen cards — still manageable.

---

**Q: How do you measure model performance in production?**

> Two classes of metrics:
>
> **Online (real-time, <5min lag):**
> - DENY rate, REVIEW rate, APPROVE rate per hour
> - DENY by reason (rule triggered vs ML score)
> - Score distribution (histogram of scores)
> - P99 latency of fraud decision
>
> **Offline (T+30 days, when chargebacks and resolutions come in):**
> - True positive rate (TPR): what % of actual fraud did we catch
> - False positive rate (FPR): what % of legitimate transactions did we wrongly deny
> - AUC-ROC: overall model discrimination ability
> - Precision@Recall=90%: at 90% recall (catching 90% of fraud), what's precision?
>
> Production target:
> - FPR < 0.02% (2 in 10,000 legitimate transactions blocked)
> - TPR > 90% (catch 90% of fraud)
> - These conflict → tune DENY threshold to balance based on business priority

---

## Rapid Fire

**Q: Why not just use a simple rule: deny all transactions > ₹1L?**
> High-value legitimate transactions exist (B2B payments, large purchases). FPR would be unacceptably high. Better: high amount + new device + new merchant + new account = deny.

**Q: Why Cassandra for transaction history?**
> Write-heavy (every transaction appended), time-series access pattern (query by account_id + time range), no joins needed. Cassandra: partition by account_id → O(1) fetch of last 30 days for a given account.

**Q: How do you prevent your own analysts from being socially engineered (bribed to approve fraud)?**
> 4-eyes principle for high-value cases. Audit log of every analyst decision. Randomized re-review: 5% of resolved cases randomly re-reviewed by different analyst. Analyst performance metrics tracked; anomalies (e.g., analyst approves 80% vs team avg 20%) flagged.

**Q: What's the difference between Account Takeover (ATO) fraud and Card-Not-Present (CNP) fraud?**
> CNP: stolen card used by someone other than the cardholder. Detection: device mismatch, velocity on card, unusual merchant.
> ATO: attacker gains access to the legitimate account and makes transactions. Harder: device may match (attacker logged into real account). Detection: login anomaly (new device, new IP, impossible travel), sudden account data change (new phone number, new shipping address), then high-value transaction.

**Q: How would you scale this to 100K TPS (10× current)?**
> Redis cluster: already sharded, add more shards. Linear scale.
> Fraud Gateway: stateless, add pods. k8s HPA handles automatically.
> ML Model Server: bottleneck. GBT at 5ms × 100K = 500,000 ms compute/sec = 500 cores. Solution: model quantization (INT8 vs FP32, 4× speedup), model parallelism across GPUs, or switch to ONNX Runtime (faster than TF Serving for GBT).
> Kafka/Flink: add partitions, add Flink workers. Linear scale.
> Neo4j: read replicas for analysis. Graph writes batched (not every transaction writes to graph).
