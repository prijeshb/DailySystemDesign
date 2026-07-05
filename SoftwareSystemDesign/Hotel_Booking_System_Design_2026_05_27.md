# Hotel Booking System Design
**Date:** 2026-05-27 | **Difficulty:** Hard

---

## 1. First Principles — Do We Need It?

**Problem:** Users want to search, reserve, and pay for hotel rooms across millions of properties worldwide. Hotels need to manage inventory in real-time and avoid double-booking.

**Core challenges:**
- Inventory is finite and time-bound (a room on a specific date)
- Concurrency: multiple users booking the same room simultaneously
- Search is complex: geo, filters, availability, pricing
- Consistency vs availability trade-off (booking must be exact, search can be stale)

---

## 2. Entities

| Entity | Key Fields |
|--------|-----------|
| **Hotel** | id, name, location (lat/lng), amenities, star_rating |
| **Room** | id, hotel_id, type, capacity, base_price, total_count |
| **Inventory** | room_id, date, available_count, reserved_count |
| **Booking** | id, user_id, room_id, check_in, check_out, status, total_price, idempotency_key |
| **User** | id, email, payment_methods |
| **Payment** | id, booking_id, amount, status, provider_ref |
| **Review** | id, hotel_id, user_id, rating, text |

---

## 3. Actions & APIs

### User-Facing
```
GET  /search?lat=&lng=&radius=&checkin=&checkout=&guests=&filters=
GET  /hotels/{id}
GET  /hotels/{id}/rooms?checkin=&checkout=
POST /bookings                    # create booking
GET  /bookings/{id}
DEL  /bookings/{id}               # cancel
```

### Internal
```
POST /inventory/reserve           # temp hold
POST /inventory/confirm           # finalize
POST /inventory/release           # rollback
```

---

## 4. Data Flow

### Search Flow
```
User → API Gateway → Search Service
  → Elasticsearch (geo + filters)
  → Inventory Service (availability check per result)
  → Cache results (TTL: 60s, stale OK)
  → Return ranked results
```

### Booking Flow (Critical Path)
```
User → API Gateway → Booking Service
  1. Validate request
  2. Check idempotency key (Redis) → return cached if exists
  3. Inventory Service: TryReserve (temp lock, TTL=15min)
     → Optimistic lock on inventory row
  4. Payment Service → charge card
     → if fail: release inventory → return error
  5. Confirm inventory (decrement available_count)
  6. Write Booking record (status=CONFIRMED)
  7. Send confirmation event → Notification Service
  8. Store idempotency result in Redis (TTL=24h)
```

---

## 5. High-Level Design

```
                     ┌─────────────────────────────┐
                     │         API Gateway          │
                     │   (auth, rate limit, route)  │
                     └──┬─────┬──────┬──────┬───────┘
                        │     │      │      │
               ┌────────▼─┐ ┌─▼────┐ ┌▼──────────┐ ┌▼──────────┐
               │  Search  │ │Booking│ │ Inventory  │ │  Payment  │
               │ Service  │ │Service│ │  Service   │ │  Service  │
               └────┬─────┘ └──┬───┘ └─────┬──────┘ └─────┬─────┘
                    │          │            │               │
           ┌────────▼──┐  ┌───▼────┐  ┌───▼────┐    ┌─────▼─────┐
           │Elasticsearch│ │Booking │  │Postgres│    │Stripe/    │
           │(hotels,   │ │Postgres│  │(inventory│   │Adyen/     │
           │geo-index) │ └────────┘  │ + locks) │   │Braintree  │
           └───────────┘             └──────────┘   └───────────┘
                    │                     │
               ┌────▼────┐          ┌─────▼──────┐
               │  Redis  │          │   Redis     │
               │(search  │          │(temp holds/ │
               │ cache)  │          │idempotency) │
               └─────────┘          └────────────┘
                                          │
                                   ┌──────▼──────┐
                                   │  Kafka       │
                                   │(booking evts)│
                                   └──────┬───────┘
                                          │
                              ┌───────────▼──────────┐
                              │  Notification Service │
                              │  Analytics Service    │
                              └──────────────────────┘
```

---

## 6. Low-Level Design

### 6.1 Inventory: Preventing Double Booking

**Inventory table:**
```sql
CREATE TABLE inventory (
  room_id    BIGINT,
  date       DATE,
  total      INT,
  available  INT,
  PRIMARY KEY (room_id, date)
);
```

**Optimistic Lock (preferred for read-heavy):**
```sql
UPDATE inventory
SET available = available - 1
WHERE room_id = ? AND date BETWEEN ? AND ?
  AND available > 0;
-- Rows affected == 0 → conflict, retry or fail
```

**Why optimistic over pessimistic?**
- Hotel booking: reads >> writes (browsing vs actual booking)
- Pessimistic lock holds DB connections → poor scalability
- Trade-off: optimistic has retry overhead on high contention (flash sales), use pessimistic only for that scenario

**Multi-date booking — must be atomic:**
```sql
BEGIN TRANSACTION;
  -- Check and decrement all nights
  UPDATE inventory SET available = available - 1
  WHERE room_id = ? AND date IN (night1, night2, ...) AND available > 0;
  -- If affected_rows != num_nights → ROLLBACK
COMMIT;
```

### 6.2 Temporary Holds (15-min lock)

When user enters payment page:
```
Redis SETEX hold:{room_id}:{date}:{booking_id} 900 "user_id"
```
- Inventory decremented immediately in DB
- If payment fails or TTL expires: background job restores inventory
- This prevents "cart abandon" from blocking others indefinitely

### 6.3 Search — Elasticsearch Index

```json
{
  "hotel_id": "123",
  "location": { "lat": 40.7, "lon": -74.0 },
  "star_rating": 4,
  "amenities": ["pool", "wifi", "parking"],
  "min_price": 120,
  "avg_rating": 4.3
}
```

- Availability **not** stored in ES (too volatile) — ES returns candidates, Inventory Service filters in real-time
- Cache search results in Redis (TTL=60s) — stale is OK for search, exact check at booking time

### 6.4 Idempotency

Every booking request carries `Idempotency-Key: <uuid>` header.
```
Key: idempotent:{key}
Value: {booking_id, status, response_body}
TTL: 86400s
```
- Before processing: check Redis → return cached response if exists
- After success: store in Redis
- Prevents duplicate charges on network retry

### 6.5 Overbooking Prevention — Rate of Reservations

For popular hotels, use a **distributed lock** on room+date:
```
SETNX lock:reserve:{room_id}:{date} {server_id}  TTL=5s
```
Ensures only one reservation attempt per room-date at a time during high contention.

### 6.6 Pricing Engine

- Base price stored per room
- Dynamic pricing rules: occupancy %, season, lead time, events
- Prices snapshotted at booking creation — never retroactively change a confirmed booking price

---

## 7. Trade-Off Analysis

| Decision | Chosen | Alternative | Why / Cost |
|----------|--------|-------------|------------|
| Optimistic locking | ✅ | Pessimistic | Better throughput; retries on contention |
| Elasticsearch for search | ✅ | Postgres full-text | Geo queries, facets, ranking; eventual consistency |
| Temp holds in Redis | ✅ | DB row locks | Fast TTL management; risk: Redis crash loses holds |
| Separate inventory service | ✅ | Monolith | Independent scaling; network overhead |
| Kafka for notifications | ✅ | Sync call | Decoupled; delayed notification possible |
| Cache search (60s stale) | ✅ | Real-time | Performance; user may see stale availability |
| DB for bookings (not NoSQL) | ✅ | DynamoDB | ACID transactions needed; harder to scale writes |

---

## 8. Scale Estimates

- 500M hotels globally, ~1B room-nights/year
- Peak: Black Friday, holidays → 100x normal traffic
- Search QPS: ~50K, Booking QPS: ~5K
- Inventory writes: ~5K/s (one row per room per night)

**Scaling Strategy:**
- Shard Inventory DB by hotel_id (co-locate all rooms of a hotel)
- Read replicas for hotel metadata
- ES: 5 primary shards, geo-based routing
- Booking DB: partition by user_id for user-facing reads

---

## 9. Key Design Principles Applied

1. **Separation of availability check and booking** — never trust search-time availability at booking time
2. **Idempotency everywhere** — safe retries for payments and inventory ops
3. **Fail-safe defaults** — if inventory service is down, reject bookings (don't accept and overbook)
4. **Event-driven side effects** — notifications, analytics via Kafka, not in booking transaction
5. **Compensating transactions** — no distributed 2PC; use saga pattern (reserve → charge → confirm, with explicit rollbacks)
