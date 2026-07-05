# Distributed Payment Processing System — 2026-05-20

> First-principles approach · Failure-first · Interview ready

---

## 0. Do We Even Need It? (First Principles)

**Problem:** User wants to pay ₹500 to a merchant. Why can't we just do a DB write?

- Money movement touches **two parties** (debit + credit) — single DB write can't span banks
- Regulatory requirements: audit trail, idempotency, reconciliation
- Scale: Razorpay processes ~5M txns/day; Stripe ~1B+ txns/day
- Failures are **unacceptable** — losing money or double-charging destroys trust

→ We need a **dedicated payment service** with strong consistency guarantees, audit logs, and idempotency.

---

## 1. Entities

| Entity | Fields |
|---|---|
| User | id, name, email, kyc_status |
| Merchant | id, name, settlement_account, mcc_code |
| PaymentOrder | id, user_id, merchant_id, amount, currency, status, idempotency_key, created_at |
| Transaction | id, order_id, gateway_txn_id, type(DEBIT/CREDIT/REFUND), status, processed_at |
| Ledger | id, txn_id, account_id, amount, direction(DR/CR), balance_after |
| Wallet | id, user_id, balance, currency, version (for optimistic lock) |
| PaymentMethod | id, user_id, type(CARD/UPI/WALLET), tokenized_data, is_default |

---

## 2. Actions (API Surface)

```
POST /payments/initiate          → creates PaymentOrder (returns order_id)
POST /payments/{order_id}/pay    → executes payment (idempotent)
GET  /payments/{order_id}/status → poll status
POST /payments/{order_id}/refund → full/partial refund
POST /webhooks/gateway           → receive async gateway callbacks
GET  /payments/history?user_id=  → paginated history
```

---

## 3. Data Flow

```
User → API Gateway → Payment Service
                          │
              ┌───────────┼─────────────────┐
              ▼           ▼                 ▼
         Idempotency   Fraud Check      Rate Limiter
           Store       Service          (Redis)
              │           │
              └─────┬─────┘
                    ▼
             Order DB (write)
                    │
                    ▼
          Payment Gateway (Razorpay/Stripe)
                    │
             [async callback]
                    ▼
          Webhook Handler → Update Order Status
                    │
                    ▼
           Ledger Service → Dual-entry bookkeeping
                    │
                    ▼
           Settlement Queue → Bank Transfer (T+1/T+2)
```

---

## 4. High-Level Design

```
┌─────────────────────────────────────────────────────┐
│                    API Gateway                       │
│              (Auth, Rate Limit, SSL)                 │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
  Payment Service             Webhook Service
  (stateless pods)            (gateway callbacks)
          │                         │
    ┌─────┴──────┐           ┌──────┴──────┐
    ▼            ▼           ▼             ▼
 Orders DB   Idempotency   Orders DB    Event Queue
 (Postgres   Store         (same)       (Kafka)
  sharded)   (Redis)
    │
    ▼
 Gateway SDK
 (Stripe/Razorpay)
    │
    ▼ (async)
 Webhook → Kafka → Ledger Service → Ledger DB
                              │
                              ▼
                     Settlement Service → Bank API
```

**Key design choices:**
- Payments DB: **PostgreSQL** (ACID, strong consistency)
- Idempotency store: **Redis** (fast key lookup, TTL-based expiry)
- Async events: **Kafka** (durable, replayable)
- Ledger: **Append-only** table, never update rows

---

## 5. Low-Level Design

### 5.1 Idempotency (most critical concept)

**Problem:** Client retries → double charge  
**Solution:** Idempotency key per request

```python
def initiate_payment(request, idempotency_key):
    # Check Redis first
    cached = redis.get(f"idem:{idempotency_key}")
    if cached:
        return cached  # return same response, no re-processing

    # Acquire distributed lock
    lock = redis.set(f"lock:{idempotency_key}", "1", nx=True, ex=30)
    if not lock:
        raise ConflictError("Request in progress")

    try:
        order = create_order(request)
        response = process_payment(order)
        redis.set(f"idem:{idempotency_key}", response, ex=86400)  # 24h TTL
        return response
    except Exception:
        redis.delete(f"lock:{idempotency_key}")
        raise
```

**Trade-off:** Redis adds latency (~1ms) but prevents catastrophic double-charges.

---

### 5.2 Database Sharding

**Shard key:** `user_id` (mod N)  
**Why user_id:** All user's payments stay on one shard → no cross-shard joins for history queries

```
Shard 0: user_ids 0–999,999
Shard 1: user_ids 1M–1,999,999
...
```

**Problem:** Hot users (high-volume merchants)  
**Solution:** Separate merchant shards by `merchant_id`; cross-reference via order_id

**Trade-off:**
| Option | Pro | Con |
|---|---|---|
| Shard by user_id | Simple history queries | Hot shard if user is Flipkart |
| Shard by txn_id (hash) | Even distribution | Cross-shard queries for user history |
| Shard by date | Easy archiving | Write hot-spot on current day shard |

→ Use `user_id % N` for consumer payments; separate merchant DB for B2B.

---

### 5.3 Two-Phase Commit Alternative (Saga Pattern)

**Problem:** Debit user wallet AND credit merchant wallet atomically across services

**Two-Phase Commit:** Too slow, coordinator becomes SPOF

**Saga Pattern (chosen):**
```
1. DEBIT user wallet       → emit "user_debited" event
2. CREDIT merchant wallet  → emit "merchant_credited" event
3. UPDATE order status = SUCCESS

If step 2 fails:
  → Compensating txn: REFUND user wallet
  → UPDATE order status = FAILED
```

Implemented via Kafka + outbox pattern — DB and event publish in same transaction.

---

### 5.4 Outbox Pattern (Reliable Event Publishing)

**Problem:** Write to DB succeeds, Kafka publish fails → inconsistent state

```sql
-- Same DB transaction:
INSERT INTO payment_orders (...) VALUES (...);
INSERT INTO outbox (event_type, payload) VALUES ('ORDER_CREATED', '{"order_id":...}');
COMMIT;

-- Outbox poller (separate process):
SELECT * FROM outbox WHERE published = false ORDER BY created_at LIMIT 100;
-- publish to Kafka
UPDATE outbox SET published = true WHERE id IN (...);
```

**Trade-off:** Extra DB writes + poller process, but guarantees at-least-once delivery.

---

### 5.5 Caching Strategy

| Cache Layer | What | TTL | Trade-off |
|---|---|---|---|
| Redis | Idempotency keys | 24h | Stale key = block retries |
| Redis | Rate limit counters | 1min | Redis down = no rate limiting |
| Redis | User payment methods | 15min | Stale data if card deleted |
| CDN | Static assets only | — | No payment data on CDN |

**What NOT to cache:** Account balances (always read from DB — stale balance = fraud vector)

---

### 5.6 Ledger Design (Double-Entry Bookkeeping)

Every payment creates **2 ledger entries** (always):

```
Payment of ₹500:
  DR  user_account    ₹500  (money leaving user)
  CR  merchant_account ₹500  (money arriving at merchant)

Refund of ₹500:
  DR  merchant_account ₹500
  CR  user_account    ₹500
```

```sql
CREATE TABLE ledger (
  id          BIGSERIAL PRIMARY KEY,
  txn_id      UUID NOT NULL,
  account_id  UUID NOT NULL,
  amount      BIGINT NOT NULL,  -- store paise, never float
  direction   CHAR(2) NOT NULL CHECK (direction IN ('DR','CR')),
  balance_after BIGINT NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
-- Append-only: no UPDATE or DELETE ever
```

**Why BIGINT for money?** Float arithmetic causes rounding errors. Store in smallest unit (paise/cents).

---

### 5.7 Wallet Concurrent Update (Optimistic Locking)

```sql
-- Read
SELECT balance, version FROM wallets WHERE id = ?;
-- Returns: balance=1000, version=5

-- Write (only if version unchanged)
UPDATE wallets
SET balance = balance - 500, version = version + 1
WHERE id = ? AND version = 5;

-- If 0 rows updated → concurrent modification → retry
```

**Alternative:** Pessimistic lock (`SELECT FOR UPDATE`) — simpler but kills throughput.

---

## 6. Reliability Numbers

| Component | Target | Mechanism |
|---|---|---|
| Payment API | 99.99% uptime | Multi-AZ, health checks |
| Payment DB | RPO < 1s | Synchronous replica |
| Idempotency store | 99.9% | Redis cluster + fallback to DB |
| Webhook delivery | at-least-once | Kafka + retry with backoff |
| Settlement | T+1 | Batch job with retry |

---

## 7. Real-World References

- **Stripe:** Uses idempotency keys as first-class API concept; stores in Redis with 24h window
- **Razorpay engineering blog:** Outbox pattern for event publishing; Saga for distributed txns
- **PayPal:** Double-entry ledger with append-only design; balance computed from ledger sum
- **Google Pay:** Shards by VPA (UPI ID) hash; uses Spanner for cross-region consistency
