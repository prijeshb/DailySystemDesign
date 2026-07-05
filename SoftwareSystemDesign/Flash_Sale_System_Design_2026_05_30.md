# Flash Sale System Design
**Date:** 2026-05-30  
**Pattern:** High-concurrency inventory + order processing  
**Real-world:** Amazon Lightning Deals, Flipkart Big Billion Day, Nike SNKRS drops

---

## First Principles Check

**Do we even need a separate system?**  
Regular e-commerce can handle ~1K orders/sec. A flash sale for a limited-stock item (e.g., 1000 units, 500K concurrent users) creates a thundering herd. Normal DB can't handle it → yes, special design needed.

**Core problem:** Prevent overselling (sell exactly N items, not N+1) under extreme concurrency, while keeping UX responsive.

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| Product | product_id, name, price, total_stock |
| FlashSale | sale_id, product_id, start_time, end_time, sale_price, allocated_stock |
| Reservation | reservation_id, user_id, product_id, status (PENDING/CONFIRMED/EXPIRED), expires_at |
| Order | order_id, user_id, reservation_id, payment_status, created_at |
| User | user_id, email, rate_limit_bucket |

---

## Actions

- User views sale page (read-heavy, cached)
- User clicks "Buy Now" → **reserve** stock (atomic, critical path)
- User completes payment → **confirm** reservation → create order
- Timer expires → **release** reservation back to pool
- Admin creates/manages flash sale

---

## Data Flow

```
User → CDN (sale page) → API Gateway → Rate Limiter
                                           ↓
                                    Reserve Service
                                     ↓          ↓
                               Redis (stock)   DB (write reservation)
                                           ↓
                                    Payment Service
                                           ↓
                                    Order Service → DB
                                           ↓
                                    Notification Queue
```

---

## High Level Design

### Components

1. **CDN** — Static sale page, product images, countdown timer
2. **API Gateway** — Auth, rate limiting (per-user + global)
3. **Reserve Service** — Core: atomically decrement stock, create reservation
4. **Payment Service** — Process payment, confirm/release reservation
5. **Order Service** — Create final order record
6. **Redis** — Stock counter (DECR atomic op), reservation TTL, user dedup
7. **Postgres** — Source of truth for orders, reservations
8. **Kafka** — Async: order confirmations, notifications, inventory sync
9. **Reservation Sweeper** — Cron/worker to expire timed-out reservations

### Stock Pre-loading
Before sale starts: load `flash_sale:{sale_id}:stock = 1000` into Redis.  
**Why Redis?** DECR is atomic, single-threaded per key → no race condition.

---

## Low Level Design

### Reserve Flow (Critical Path)

```
1. Rate limit check (Redis sliding window, 1 req/sec per user)
2. Dedup check: SETNX user_dedup:{user_id}:{sale_id} = 1 (TTL 5min)
   → If exists: return "already in queue"
3. Stock decrement: DECR flash_sale:{sale_id}:stock
   → If result < 0: INCR (rollback), return "sold out"
4. Write reservation to DB (async, via Kafka or direct write):
   reservation_id, user_id, sale_id, status=PENDING, expires_at=now+10min
5. Return reservation_id to user → redirect to payment
```

### Why DECR not DB compare-and-swap?
DB: SELECT stock → check > 0 → UPDATE stock-1 requires row-level lock.  
Under 100K concurrent requests → lock contention, deadlocks, timeouts.  
Redis DECR: O(1), no locks, handles 100K+ ops/sec per shard.

### Reservation Expiry
- Redis TTL on reservation key (10 min)
- Sweeper runs every 30s: `SELECT * FROM reservations WHERE status=PENDING AND expires_at < NOW()`
- For each expired: INCR Redis stock + UPDATE reservation status=EXPIRED
- **Idempotency:** sweeper marks status EXPIRED only if still PENDING (atomic UPDATE WHERE status=PENDING)

### Payment Confirmation
```
POST /orders/confirm {reservation_id, payment_token}
→ Verify reservation is PENDING and not expired
→ Charge payment (idempotent key = reservation_id)
→ UPDATE reservation SET status=CONFIRMED
→ INSERT into orders
→ Publish to Kafka: order.created
```

---

## Trade-offs

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Redis for stock | ✅ Atomic, fast | Risk: Redis down = can't sell. Mitigation: Redis Sentinel/Cluster |
| Reserve-then-pay | ✅ User experience | Stock held but payment may fail → need expiry sweeper |
| In-memory dedup | ✅ Prevents double-buy fast | Redis restart loses dedup state (TTL-bounded, acceptable) |
| Pre-load stock to Redis | ✅ Avoids DB at hot path | Must sync back to DB after sale ends |
| Async order write | ✅ Keeps reserve path fast | Order may not exist instantly → use reservation_id as source of truth |
| Rate limit per user | ✅ Fairness | Determined users can use multiple accounts (partial mitigation) |

---

## Failure Analysis → See Flash_Sale_Failure_Analysis_2026_05_30.md
## Interview Q&A → See Flash_Sale_Interview_QA_2026_05_30.md
