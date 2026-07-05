# Ticketmaster — Interview Q&A
**Date:** 2026-06-08  
**System:** Online Event Ticket Booking

---

## How Interviewers Open This Problem

> "Design a ticket booking system like Ticketmaster."  
> "Design BookMyShow."  
> "Design a system where users can buy seats for events."

They want to see: concurrency handling, seat uniqueness, fairness at peak load.

---

## Round 1 — Requirements & Scoping

**Q: What's the core challenge here vs a regular e-commerce system?**  
A: Inventory is finite and unique. Seat 3A exists once. You can't oversell it. E-commerce can have 100 identical phones — here each item is distinct. That forces strong consistency at hold/purchase time, not eventual consistency.

**Q: How many users are you designing for?**  
A: Baseline: thousands of concurrent users. Peak (hot event onsale): 2M users in 60 seconds. That peak is the hard problem — it's a thundering herd, not steady load.

**Q: Do users need real-time seat availability?**  
A: For browsing — no. 5s stale is fine. At the moment of selecting a seat — yes, we need to read from primary DB, not replica. Showing a seat as available when it's taken is a minor UX issue. Letting two users both "buy" the same seat is a hard bug.

---

## Round 2 — Core Design

**Q: Walk me through the seat hold flow.**  
A:
1. User selects seats and submits hold
2. We run: `UPDATE seats SET status='HELD' WHERE id=$seat AND status='AVAILABLE'` — one atomic statement per seat, all inside a transaction
3. rows_affected=0 means someone else took it → return 409
4. On success, create PENDING order, set 10-min hold TTL in DB, update Redis cache
5. User has 10 minutes to pay

**Q: Why use a DB UPDATE instead of Redis SETNX for the lock?**  
A: Redis is fast but can lose data. If the service crashes after the Redis SETNX but before writing to DB, the lock exists but no DB record — phantom hold. DB `UPDATE WHERE status='AVAILABLE'` is durable and atomic. Redis is updated after the DB write as a cache only.

**Q: What if two users click the same seat at the same time?**  
A: Both send `UPDATE seats SET status='HELD' WHERE id=$seat AND status='AVAILABLE'`. Postgres serializes row-level: one wins (rows_affected=1), one loses (rows_affected=0). The loser gets a 409. No application-level mutex needed — the DB handles it.

**Q: How does the hold expire?**  
A: Background worker runs every 30 seconds: finds seats with `hold_expires_at < now()`, resets status to AVAILABLE, cancels associated PENDING orders, invalidates Redis cache. The trade-off: max 30s overhang past expiry. Acceptable — it's not a real booking.

**Q: What if the user pays right as the hold expires?**  
A: Race condition — user is charged, seat released before DB write. Prevention: before initiating payment, extend hold by 5 minutes with `UPDATE WHERE held_by_user=$user AND hold_expires_at > now()`. If rows_affected=0 → hold already expired → abort before charging. Closes the race window without a grace period hack.

---

## Round 3 — Scale & Hot Events

**Q: How do you handle 2 million users hitting buy at the same moment for Taylor Swift?**  
A: Virtual waiting queue. Users arrive → assigned position in Redis sorted set (score = arrival timestamp). A drain worker releases N users every 30s, issuing each a signed JWT (valid 15 min). Seat Service only accepts holds with a valid queue token. This caps DB throughput at a controlled rate instead of letting all 2M hit simultaneously.

**Q: What's the drain rate based on?**  
A: Seat Service throughput. If we can handle 500 holds/sec, drain 500 users/sec. Scale Seat Service horizontally to increase drain rate.

**Q: Isn't a 30-60 minute queue terrible UX?**  
A: For hot events, yes — but the alternative is "everyone gets a 503" or "bots win." Queue is fairer, gives users an honest wait time, and doesn't overload the system. Ticketmaster, Queue-It, and IRCTC all use this pattern.

**Q: What if the waiting queue service itself goes down?**  
A: Fail-closed: reject holds without queue token → nobody buys until queue recovers. Fail-open: allow requests → DB overloaded → total outage. For hot events, fail-closed is the right call. Fall back to global API rate limiting as a minimum protection.

**Q: How do you handle a hot seat — every user trying to hold seat 3A?**  
A: The virtual queue already limits concurrent hold attempts. For extreme cases (center front row in a 50K arena), a per-seat queue using a Redis List — serialize hold attempts for that seat_id — prevents thundering herd on one DB row.

---

## Round 4 — Payment & Consistency

**Q: How do you prevent double-charging on payment retry?**  
A: Client generates UUID before first payment attempt and sends it on every retry. Order Service passes this as idempotency_key to Payment Service, which forwards it as Stripe-Idempotency-Key. Stripe deduplicates — same key = same response, no second charge.

**Q: What if payment succeeds but the DB write fails?**  
A: Reconciliation worker runs every 5 minutes. Finds PENDING orders older than 15 minutes. Queries Payment Service with idempotency_key:  
- Got charged? → write CONFIRMED + SOLD, issue ticket  
- Not charged? → CANCELLED, release seat  
This closes the "user charged, no ticket" window.

**Q: Should we read seat availability from a replica?**  
A: For browsing — yes, replicas fine. For the hold check and payment check — primary only. Replica lag means a replica might show a seat as AVAILABLE when it's been HELD 1s ago on primary. Reading from primary for writes eliminates this. "Never check available balance from replica before deducting" — same principle.

---

## Round 5 — Data Model & Tricky Details

**Q: Why is `hold_expires_at` stored on the `seats` table and not a separate `holds` table?**  
A: Colocation. Hold expiry query, availability check, and status update all touch the same row. One table = one index scan. Separate table = JOIN. For 80K seats per event queried every 30 seconds, colocation wins. Trade-off: seat row gets wider; acceptable since seats table is modest size.

**Q: How do you prevent a user from holding 200 seats?**  
A: Check before hold: `SELECT COUNT(*) FROM seats WHERE event_id=$1 AND held_by_user=$2 AND status='HELD'`. If count + requested > limit (e.g. 4) → reject. This read is on primary (to be safe). Add a DB-level CHECK or application-level enforcement.

**Q: How does the QR code work and how do you prevent duplicate entry?**  
A: QR = HMAC-signed payload (ticket_id, seat_id, event_id, user_id). Gate verifies signature offline in <5ms. To prevent duplicate scan: `SADD scanned:{event_id} {ticket_id}` — Redis atomic SADD returns 1 (first scan) or 0 (already scanned). All gates share same Redis cluster → consistent across gates.

**Q: How do you generate the seat map?**  
A: Two-layer approach. Static layout (section/row/seat positions) = JSON in S3/CDN, cache max-age=86400. Venue layout never changes. Dynamic overlay (which seats are HELD/SOLD) = separate endpoint, 5s TTL, client polls every 3s and overlays on static map. Decouples the never-changing from the frequently-changing.

---

## Round 6 — Follow-ups & Edge Cases

**Q: What about inventory waves — releasing seats in batches?**  
A: Admin sets seats status='AVAILABLE' in controlled batches. Event Service publishes InventoryReleased event → cache invalidated → seat count updated → waiting queue drain rate can increase. Admins hold back e.g. 500 front-row seats for VIP sale, release later.

**Q: A user's internet cuts out during payment. They reopen the app. What happens?**  
A: App reads order_id from local state → GET /orders/{id} → see status: PENDING, hold_expires_at still in future → user continues payment. If hold expired: order=CANCELLED → user must reselect. If payment succeeded but user never saw confirmation: reconciliation worker + polling on order status → returns CONFIRMED.

**Q: Design the notification flow for "your tickets are confirmed."**  
A: OrderConfirmed event on Kafka → Notification Service consumes → sends email + push. Ticket PDF generated asynchronously (not on hot path). Notification Service is idempotent on event_id — if Kafka redelivers, it checks: already sent this event_id? → skip.

**Q: How would you add a resale marketplace?**  
A: Seller lists ticket → creates a new Listing entity with ticket_id, price. Buyer purchases listing → same hold/pay flow but Listing as inventory (status=LISTED/HELD/SOLD). Transfer ownership: UPDATE tickets SET user_id=$buyer in same transaction as payment CONFIRMED. Original QR invalidated, new QR issued.

**Q: How would you shard the database?**  
A: Shard by event_id. All seats, orders, tickets for an event land on the same shard — no cross-shard joins for the hot path (hold + pay). Hot event problem: one shard is hammered; mitigate with more read replicas per shard and the virtual queue controlling write throughput to primary.
