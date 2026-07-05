# Payment System — Failure Analysis — 2026-05-20

> Failure-first: What breaks, what cascades, how to prevent.

---

## Failure Map (Component by Component)

### 1. API Gateway Fails

**Symptoms:** All payment requests return 502/503

**Cascades:** Complete outage. No payments possible.

**Prevention:**
- Multi-AZ deployment (ALB across 3 AZs)
- Health checks every 5s, auto-replace unhealthy instances
- Circuit breaker: if >50% requests fail in 10s window, open circuit

**Recovery:**
- DNS failover to secondary region (Route53 health checks)
- Target: RTO < 30s

---

### 2. Payment Service Pod Crashes Mid-Request

**Scenario:** Order written to DB, gateway called, pod dies before saving response

**Cascades:** 
- User charged but order shows PENDING
- Client retries → duplicate charge if no idempotency

**Prevention:**
- Idempotency key stored in Redis **before** gateway call
- Gateway transaction ID stored in DB as soon as received
- State machine: INITIATED → PROCESSING → SUCCESS/FAILED (no skipped states)

**Recovery:**
- Background reconciliation job: query gateway for PROCESSING orders older than 2 min
- If gateway says SUCCESS → update order, emit event
- If gateway says FAILED → refund + update order

---

### 3. Payment Database (Primary) Crashes

**Scenario:** Postgres primary goes down during high traffic

**Cascades:**
- All writes fail (new payments, status updates)
- Reads still work from replica (eventual consistency lag)

**Prevention:**
- Synchronous streaming replica in same AZ (RPO ~0)
- Async replica in different AZ/region (RPO ~1s)
- PgBouncer for connection pooling (reduces reconnect storm)

**Recovery:**
- Auto-failover via Patroni/RDS Multi-AZ (~30s)
- During failover window: queue payment requests in Redis (write-behind)
- Replay queued requests after failover

**Risk:** 30s downtime = ~2500 failed payments at 5M/day scale. Accept or add regional fallback.

---

### 4. Redis (Idempotency Store) Crashes

**Scenario:** Redis cluster fails

**Cascades:**
- Cannot check idempotency → risk of duplicate payments
- Cannot enforce rate limits → DDoS risk

**Prevention:**
- Redis Cluster with 3 master + 3 replica nodes
- Sentinel for automatic failover

**Recovery Fallback:**
```python
def check_idempotency(key):
    try:
        return redis.get(key)
    except RedisConnectionError:
        # Fallback: check DB for existing order with same idempotency_key
        return db.query("SELECT * FROM orders WHERE idempotency_key = ?", key)
```

**Trade-off:** DB fallback is ~10x slower (10ms vs 1ms) but prevents duplicates.

---

### 5. Payment Gateway (Stripe/Razorpay) Fails

**Scenario:** Gateway returns 5xx or times out after charging user

**The hardest failure in payments.**

**State after failure:**
```
Case A: Gateway charged user, response lost in transit
Case B: Gateway never processed, timeout on our side
Case C: Gateway processing, response delayed
```

**Prevention — Never rely on synchronous response alone:**

```
1. Store gateway_txn_id immediately when received (even partial response)
2. All gateway calls are async + webhook-confirmed
3. Reconciliation job runs every 5 min:
   - Find orders in PROCESSING state > 3 min
   - Query gateway API: GET /charges/{our_order_id}
   - Update status based on gateway source of truth
```

**Retry logic:**
```python
for attempt in range(1, 4):
    try:
        result = gateway.charge(order_id=order.id, amount=order.amount)
        break
    except GatewayTimeout:
        # CRITICAL: do NOT retry blind — query status first
        status = gateway.get_charge_status(reference_id=order.id)
        if status == "SUCCESS":
            break  # already charged, stop retrying
        time.sleep(2 ** attempt)  # exponential backoff
```

---

### 6. Kafka (Event Queue) Fails

**Scenario:** Kafka cluster unavailable; webhook events not processed

**Cascades:**
- Ledger not updated → balance incorrect
- Settlement delayed
- Merchant not notified

**Prevention:**
- Outbox pattern: events survive in DB even if Kafka is down
- Kafka replication factor = 3; min.insync.replicas = 2
- Consumer group lag alerting (>10k messages = alert)

**Recovery:**
- Outbox poller switches to direct processing during Kafka outage
- After recovery: replay from outbox (idempotent consumers prevent duplicates)

---

### 7. Webhook Handler Fails (Gateway Callback Missed)

**Scenario:** Stripe sends webhook, our handler is down, Stripe stops retrying after 3 days

**Prevention:**
- Webhook endpoint separate from main service (dedicated pods)
- Immediately acknowledge (200 OK) and queue for async processing
- Never do heavy processing synchronously in webhook handler

```python
@app.post("/webhooks/gateway")
def handle_webhook(payload):
    db.insert("webhook_inbox", {"payload": payload, "processed": False})
    return 200  # acknowledge immediately

# Separate worker processes webhook_inbox
```

**Recovery:**
- Reconciliation job as safety net (covers missed webhooks)
- Stripe dashboard: manually trigger webhook replay for critical events

---

### 8. Settlement Service Fails

**Scenario:** T+1 settlement batch job crashes; merchants not paid

**Cascades:**
- Merchant account underfunded
- Trust/contractual breach

**Prevention:**
- Settlement is idempotent: `settlement_id` prevents double-pay
- Job checkpointing: mark each merchant as settled before moving to next
- Dead letter queue for failed settlements with alerting

**Recovery:**
- Re-run failed settlements from checkpoint (idempotent — safe to retry)
- Manual override for high-value merchants

---

### 9. Fraud Detection Service Slow/Down

**Scenario:** Fraud service takes >2s or returns 503

**Cascades:** Payment latency spikes; user abandons checkout

**Prevention:**
- Fraud check is **async + non-blocking** for low-risk transactions
- Circuit breaker: if fraud service is down, fall back to rule-based checks (amount thresholds)
- SLA: fraud service must respond < 200ms or be bypassed

**Trade-off:** Bypassing fraud check = slight increase in fraud rate. Acceptable for < 1min outage.

---

### 10. Network Partition (Split-Brain)

**Scenario:** DB primary and replica lose connectivity; replica promoted as new primary

**Risk:** Two primaries, both accepting writes → split-brain

**Prevention:**
- Fencing tokens: new primary gets higher epoch; old primary rejects writes
- Patroni uses Etcd for distributed consensus (quorum required for promotion)
- Application: writes go to primary only (read replica is read-only)

---

## Failure Priority Matrix

| Failure | Probability | Impact | Priority |
|---|---|---|---|
| Gateway timeout mid-charge | High | Critical (money lost) | P0 |
| DB primary crash | Medium | High (all writes fail) | P1 |
| Idempotency store crash | Low | Critical (double charge) | P0 |
| Webhook missed | Medium | Medium (delayed settlement) | P2 |
| Kafka lag | Medium | Low (eventual) | P3 |
| Fraud service down | Low | Low (bypass acceptable) | P3 |

---

## Reconciliation: The Safety Net

Runs every **5 minutes**. Catches all other failure modes.

```
1. SELECT orders WHERE status = 'PROCESSING' AND updated_at < NOW() - 3min
2. For each order: query gateway API
3. If gateway = SUCCESS → mark SUCCESS, emit events
4. If gateway = FAILED → mark FAILED, trigger refund
5. If gateway = UNKNOWN → alert ops team
```

This is the **last line of defense** — assume everything else can fail.
