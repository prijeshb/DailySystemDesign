# Flash Sale — Interview Q&A
**Date:** 2026-05-30

---

## Opening Questions

**Q: Design a flash sale system where 1000 items sell in seconds to 500K concurrent users.**

Good start:
- Clarify: How many items? How long is the sale? Is payment inline or async?
- State the core challenge: prevent oversell under concurrency
- Mention you'll think from first principles before jumping to components

---

## Round 1 — Scoping & Entities

**Q: What are the core requirements?**
- Functional: Reserve item, complete payment, handle sold-out gracefully
- Non-functional: Exactly N items sold (correctness), low latency on reserve, high availability

**Q: What entities do you model?**
- FlashSale, Reservation, Order, User
- Key: Reservation is separate from Order — it's a temporary hold

**Q: Why separate Reservation from Order?**
- Reserve is fast/cheap, order is after payment
- Allows expiry without touching order table
- Maps to real-world: "item in cart" vs "purchased"

---

## Round 2 — Core Design

**Q: How do you prevent overselling?**
- Use Redis DECR (atomic) on a stock counter
- If result < 0: rollback INCR + return sold-out
- Why not DB? Lock contention under 100K concurrent requests

**Q: What if two requests DECR simultaneously and both get result ≥ 0?**
- Redis is single-threaded per command — DECR is truly atomic. No race condition.
- Follow-up: This breaks in multi-master Redis setups → use single Redis primary or WAIT command

**Q: How do you handle the reservation expiring?**
- Set `expires_at` in DB
- Sweeper job queries expired PENDING reservations every 30s
- INCR Redis stock + mark EXPIRED
- **Follow-up:** What if sweeper is slow? User gets timeout, item stays held. Mitigation: short TTL (5-10 min), multiple sweeper instances with `SELECT FOR UPDATE SKIP LOCKED`

---

## Round 3 — Concurrency & Idempotency

**Q: What if a user submits the payment twice?**
- Idempotency key = reservation_id on payment API
- DB unique constraint on `orders(reservation_id)`
- Return cached 200 for duplicate within 10s window

**Q: What if the reserve service crashes after DECR but before DB write?**
- Stock decremented, no reservation record → leaked stock
- Fix: Wrap in try/finally — INCR on DB failure
- Remaining gap (process kill): sweeper reconciliation will catch after sale ends

**Q: Is your system idempotent end-to-end?**
- Reserve: SETNX dedup key prevents double-reserve per user
- Payment: idempotency key prevents double-charge
- Sweeper: UPDATE WHERE status=PENDING prevents double-release
- Order creation: unique constraint on reservation_id

---

## Round 4 — Scalability

**Q: How do you handle 500K concurrent users hitting "Buy Now" at T=0?**
- Virtual queue / waiting room — randomize entry
- Pre-scale pods before sale (K8s scheduled HPA)
- Rate limit per user (1 req/sec) + global cap
- CDN serves sale page, countdown — only API calls hit backend

**Q: How do you scale the Reserve Service?**
- Stateless — scale horizontally
- All state in Redis (stock counter, dedup set)
- Redis Cluster for HA

**Q: What if you have multiple flash sales running simultaneously?**
- Partition Redis keys by sale_id: `flash_sale:{sale_id}:stock`
- No cross-sale contention

---

## Round 5 — Trade-offs & Follow-ups

**Q: You chose reserve-then-pay. What's the downside?**
- Stock held by users who never complete payment
- Mitigation: Short TTL (10 min), sweeper releases quickly
- Alternative: Pay-then-reserve — worse UX (charged before guaranteed item)

**Q: Why not just use database SELECT FOR UPDATE?**
- Works at small scale
- At 100K concurrent: row lock contention → queuing → latency spikes → cascading timeouts
- Redis DECR avoids locks entirely

**Q: What if Redis is unavailable?**
- Fallback to DB atomic update: `UPDATE flash_sale SET stock=stock-1 WHERE stock>0 AND sale_id=?`
- Check rows_affected = 1 (success) or 0 (sold out)
- ~10x slower but correct
- Implement via feature flag, auto-switch on Redis health check failure

**Q: How do you ensure the sale doesn't sell more than allocated, even with bugs?**
- **Defense in depth:**
  1. Redis DECR (primary guard)
  2. DB constraint: `CHECK (stock >= 0)` on flash_sale table (if using DB fallback)
  3. Post-sale reconciliation: count orders vs allocated_stock, alert on mismatch
  4. Order creation: SELECT COUNT to verify before INSERT (last resort)

---

## Round 6 — Real World Reference

**Amazon Lightning Deals:**
- Shows "X% claimed" bar — this is approximate (cached), not real-time
- Adds item to cart (reservation) for 15 min
- Uses internal queue systems for peak traffic

**Nike SNKRS:**
- Randomized draw, not first-come-first-served
- Eliminates thundering herd problem entirely

**Ticketmaster:**
- Virtual waiting room
- Reservation holds with expiry
- Known for failures under load (Taylor Swift) → lessons: underestimating concurrency, single-region bottlenecks

---

## Quick Concepts Recap

| Concept | Application Here |
|---------|----------------|
| Idempotency | Reservation dedup, payment key, sweeper WHERE clause |
| Atomicity | Redis DECR for stock, DB unique constraints |
| Cache | Redis stock counter (hot path), CDN for sale page |
| Failure isolation | Sweeper separate from reserve path |
| Backpressure | Virtual queue, rate limiting |
| Eventual consistency | Async order write via Kafka (OK since reservation_id is source of truth) |
| Saga pattern | Reserve → Pay → Confirm with compensating actions |
