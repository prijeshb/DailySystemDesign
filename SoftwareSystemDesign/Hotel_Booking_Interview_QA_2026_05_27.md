# Hotel Booking System — Interview Q&A
**Date:** 2026-05-27

---

## Opening Questions (Scoping)

**Q: Walk me through how you'd design a hotel booking system like Booking.com.**

Start by clarifying scope:
- Read-heavy (browsing) vs write-critical (booking)
- Scale: global, millions of hotels, thousands of concurrent bookings
- Key constraint: **no double-booking** — this drives most design decisions

Core flow: Search → View availability → Reserve → Pay → Confirm

---

## Round 1: Core Design

**Q: How do you prevent two users from booking the same room on the same night?**

Optimistic locking on the inventory table:
```sql
UPDATE inventory SET available = available - 1
WHERE room_id=? AND date=? AND available > 0
```
If `rows_affected == 0` → conflict → retry or "sold out".

For multi-night booking, wrap all nights in a single transaction. If any night fails, rollback all.

*Interviewer follow-up: Why not pessimistic locking?*
Pessimistic locks hold DB connections during payment (~3-5s). At scale, this kills throughput. Optimistic works because actual conflicts are rare — most users book different rooms.

---

**Q: How do you handle the case where a user is on the payment page for 10 minutes?**

Use a **temporary hold**: decrement inventory immediately when user hits "Book", store a Redis key with 15-min TTL. If payment succeeds → confirm. If payment fails or TTL expires → background job restores inventory.

*Follow-up: What if Redis crashes and holds are lost?*
Two defenses: (1) Redis AOF persistence — recover on restart. (2) Fallback: store holds in DB table, switch to DB-based holds if Redis unreachable.

---

**Q: How does search work? Do you query inventory for every search result?**

Two-phase:
1. **Elasticsearch** returns candidate hotels matching geo, filters, price range (fast, ~10ms)
2. **Inventory Service** checks real-time availability for the candidates

Search results can be slightly stale (Redis cache, 60s TTL). Final availability check happens at booking time — always from primary DB.

*Follow-up: What if Elasticsearch is down?*
Circuit breaker detects failure → route to degraded Postgres search (no geo scoring, slower). Alert on-call. ES recovers from its own persistence.

---

## Round 2: Distributed Systems

**Q: A user retries their booking request after a network timeout. How do you handle it?**

**Idempotency keys.** Every booking request includes a client-generated UUID header. We check Redis before processing:
- If key exists → return cached response
- If key is new → process → store result in Redis (TTL 24h)

Also send idempotency key to payment provider (Stripe supports this natively).

*Follow-up: What if Redis crashes between processing and storing the idempotency result?*
Fall back to DB: before processing, query booking table for matching `idempotency_key` column. DB is source of truth; Redis is a speed cache.

---

**Q: Payment times out — we don't know if the charge went through. What do you do?**

1. Query payment provider's status API with the idempotency key
2. If CHARGED → confirm inventory, write booking as CONFIRMED
3. If NOT CHARGED → release inventory, return error to user
4. If STILL UNKNOWN → mark booking PENDING, retry in background job every 60s for up to 30 min, then escalate to support

Never assume failure and charge again — always query first.

---

**Q: How do you handle the saga pattern here? Why not 2PC?**

Steps: Reserve Inventory → Charge Payment → Confirm Inventory

**2PC is rejected** because it requires a coordinator and holds locks across services — too slow, single point of failure.

**Saga (choreography):** Each step has a compensating transaction. If payment fails → call Release Inventory. If confirm inventory fails → issue refund. Compensating transactions must be idempotent and retried until they succeed (via DLQ/Kafka retry).

---

## Round 3: Scale & Edge Cases

**Q: How would you handle a flash sale — a famous hotel at 90% discount for 1 hour?**

Normal optimistic locking may be overwhelmed. Solutions:
1. **Queue bookings per room-date**: Redis list, process serially → no contention, just queuing
2. **Switch to pessimistic locking** for that specific hotel during the sale window
3. **Rate limit booking attempts**: Redis counter per room-date, max 10 requests/second
4. **Virtual waiting room**: hold users in queue, admit N at a time

*Follow-up: How do you decide which approach?*
Depends on expected volume. For moderate scale → rate limiting is enough. For viral scale → queue serialization.

---

**Q: How do you scale the inventory DB for 500M hotels?**

**Shard by hotel_id.** All rooms of a hotel go to the same shard → multi-night transactions are single-shard (no distributed transactions).

Shard routing: consistent hashing on hotel_id. Booking Service knows which shard to contact. Add shard metadata table for routing.

*Follow-up: What's the hotspot problem?*
Popular hotels get more writes. Mitigate: pre-shard popular hotels onto dedicated shards, or use a separate hot-hotel shard pool.

---

**Q: How do you handle cancellations?**

```
Cancel Booking → Release Inventory → Trigger Refund → Notify User
```

- Release inventory: `UPDATE inventory SET available = available + 1 WHERE room_id=? AND date=?`
- Refund: call payment provider with booking's charge ID
- Cancellation policies: stored as rules, applied before refund amount is calculated
- Idempotent: cancelling an already-cancelled booking returns success (no-op)

---

**Q: How would you add pricing that changes dynamically (surge pricing)?**

- Price calculation happens at search time (not stored per inventory row)
- Pricing Service takes: room_id, dates, current_occupancy, season, local_events → returns price
- **Snapshot price at booking creation** — never change what user agreed to pay
- Price validity: "lock" quoted price for 15 min (same window as inventory hold)

---

## Round 4: Deep Dives

**Q: What's the schema for the inventory table? How does it handle a 30-day booking?**

```sql
CREATE TABLE inventory (
  room_id    BIGINT,
  date       DATE,
  total      INT,          -- physical rooms of this type
  available  INT,          -- rooms available on this date
  PRIMARY KEY (room_id, date)
);
```

30-day booking = 30 rows, one per night. Transaction updates all 30 atomically.

*Follow-up: Isn't this a lot of rows?*
~500M hotels × 10 room types × 365 days = ~1.8T rows/year. Partition by date range (monthly partitions), archive old dates. Actually manageable with partitioning.

---

**Q: How do you make the booking flow observable/debuggable?**

- Every booking has a `booking_id` (UUID) as correlation ID — passed through all services
- Structured logging with booking_id → query in ELK
- Distributed tracing (Jaeger/Zipkin) — visualize latency across services
- Key metrics: booking success rate, inventory conflict rate, payment failure rate, p99 latency per service

---

## Common Follow-Up Themes

| Topic | Quick Answer |
|-------|-------------|
| CAP theorem choice | CP for inventory (consistency critical), AP for search (availability fine) |
| Why not NoSQL for bookings? | Need ACID transactions for multi-night atomic updates |
| How to handle multi-currency? | Store prices in smallest unit of local currency; display layer converts |
| Reviews system | Async, write to Cassandra, async compute aggregate rating |
| Overbooking (like airlines do)? | Hotels don't typically; system enforces hard limits unless hotel explicitly opts in |
| How to handle group bookings? | All rooms in single transaction, or distributed saga with rollback |
