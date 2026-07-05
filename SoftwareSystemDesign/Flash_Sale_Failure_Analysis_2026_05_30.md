# Flash Sale — Failure Analysis
**Date:** 2026-05-30

---

## Component Failure Map

### 1. Redis Fails (Stock Counter)

**Impact:** Can't reserve stock → entire sale blocked.

**Before Redis fails:**
- Client gets connection timeout on DECR
- Reserve Service has no fallback → returns 500 to user

**Solutions:**
- **Redis Cluster** (3 masters, 3 replicas) — automatic failover ~30s
- **Sentinel** for HA in simpler setups
- **Emergency fallback:** DB-level atomic update (`UPDATE SET stock=stock-1 WHERE stock>0`) — slower but survives Redis outage. Switch via feature flag.
- Pre-sale: test Redis connection, abort sale start if unhealthy

**After Redis restores:**
- Stock counter may be stale (if sweeper ran or payments failed during downtime)
- **Fix:** On restart, recompute from DB: `total_allocated_stock = COUNT(reservations WHERE status IN (PENDING, CONFIRMED))`; Redis stock = total_stock - allocated

---

### 2. Reserve Service Pod Crashes (mid-request)

**Scenario:** DECR Redis succeeded, DB write for reservation failed.

**Impact:** Stock decremented but no reservation record → stock leaked (never returns to pool).

**Solution:**
- Write reservation to DB **before** returning to user. If DB write fails → INCR Redis to rollback.
- Use try/finally pattern:
  ```
  decr_result = redis.DECR(stock_key)
  if decr_result < 0: redis.INCR(stock_key); return SOLD_OUT
  try:
      db.insert(reservation)
  except:
      redis.INCR(stock_key)  # rollback
      raise
  ```
- **Idempotency:** If pod crashes after DECR but before INCR rollback, sweeper reconciliation (runs every 30s) will catch orphaned stock.

---

### 3. Payment Service Fails

**Scenario:** Reservation confirmed, payment charge call fails or times out.

**Impact:** User charged but order not created, OR user not charged but reservation consumed.

**Solutions:**
- Payment calls use **idempotency key = reservation_id**
- On timeout: poll payment provider status before retrying
- Reservation status remains PENDING until payment explicitly confirmed
- If payment fails → release reservation (INCR Redis stock)
- **Saga pattern:** Reserve → Pay → Confirm. Each step has compensating action.

---

### 4. Reservation Sweeper Fails

**Scenario:** Sweeper crashes → expired reservations not released → stock stuck.

**Impact:** Users get "sold out" but items never actually purchased.

**Solutions:**
- Run sweeper on multiple pods, use DB-level locking or `SELECT FOR UPDATE SKIP LOCKED` to avoid double-processing
- **Lease-based:** Each sweeper claims a batch with a timestamp lock
- Alert if sweeper hasn't run in >2min (monitor via last_run timestamp in DB)
- Self-healing: INCR Redis stock is idempotent if run twice (would over-release). **Fix:** Use Redis SET instead of INCR for known expired reservations, or track with a processed set.

---

### 5. Thundering Herd at Sale Start

**Scenario:** 500K users hit "Buy Now" at exact start time.

**Impact:** API Gateway/Reserve Service overwhelmed → cascading failure.

**Solutions:**
- **Queue-based entry:** At sale start, users enter a virtual waiting room (randomized position). Release to purchase in batches.
  - Real-world: Nike SNKRS, Ticketmaster use this
- **Pre-sale registration:** Allow users to register interest 24h before → randomized selection
- **Horizontal scaling:** Pre-scale Reserve Service pods before sale (schedule-based HPA in K8s)
- **Circuit breaker:** If error rate >10% → return "queue full, try in 30s" vs letting requests pile up

---

### 6. Database Overload

**Scenario:** Thousands of reservation writes/sec overwhelm Postgres.

**Solutions:**
- Write reservations to Kafka first, batch-write to DB (eventual consistency acceptable here — reservation_id exists in Redis immediately)
- Separate DB for flash-sale reservations (isolated from main product DB)
- Connection pooling via PgBouncer

---

### 7. Oversell (Critical Correctness Failure)

**Scenario:** Due to a bug, more items sold than available.

**Causes:**
- DECR result checked incorrectly (off-by-one)
- Sweeper releases stock but DB still shows CONFIRMED reservation
- Multi-region Redis with split-brain

**Prevention:**
- **Invariant check:** After sale ends, `confirmed_orders + pending_reservations <= total_stock`. Alert if violated.
- For multi-region: sell from single Redis region (sacrifice latency for correctness). Or use Redis WATCH/transaction.
- **Hard cap:** At order creation time, also check DB: `SELECT COUNT(*) FROM orders WHERE sale_id = ?` < total_stock. Adds latency but prevents oversell as last line of defense.

---

### 8. Duplicate Order (Idempotency Failure)

**Scenario:** User clicks "Confirm" twice → two orders created.

**Solution:**
- Idempotency key on `POST /orders/confirm` = `reservation_id`
- DB unique constraint: `UNIQUE(reservation_id)` on orders table
- API Gateway caches responses for 10s per idempotency key

---

## Monitoring Checklist

| Metric | Alert Threshold |
|--------|----------------|
| Redis stock value | Alert if < 0 |
| Sweeper last_run_at | Alert if > 2 min ago |
| Reserve Service error rate | Alert if > 5% |
| Reservation PENDING count | Alert if growing after sale ends |
| Payment timeout rate | Alert if > 2% |
| Orders confirmed vs stock sold | Alert on mismatch |
