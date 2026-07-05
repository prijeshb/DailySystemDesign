# Ticketmaster — Online Event Ticket Booking
**Date:** 2026-06-08  
**Difficulty:** Hard  
**Real-world refs:** Ticketmaster, BookMyShow, StubHub, IRCTC

---

## 0. First Principles — Do We Even Need This?

**The problem:** A concert has 50,000 seats. 2 million fans try to buy tickets the moment sales open. Every seat is unique (Section A, Row 3, Seat 12). Two people cannot own the same seat.

**Why not just use a simple e-commerce system?**
- E-commerce: unlimited inventory (100 phones in stock → sell 100 to 100 people)
- Ticketing: *finite, unique* inventory — seat 3A exists exactly once
- E-commerce allows eventual consistency for stock; ticketing requires *strict* consistency — double-selling seat 3A is unacceptable

**Do we need exact real-time seat availability for everyone?**
- No. If 500K people are browsing, showing slightly stale availability (~1-5s) is fine
- But at the moment of *hold* (user commits to a seat), availability must be exact — checked from primary DB, not replica

**Why a temporary "hold" instead of immediate purchase?**
- Payment takes 5–10s. Without hold, User B could see the same seat, select it, and pay faster than User A — User A's payment succeeds but seat is gone
- Hold = reserve seat for N minutes while user pays → prevents race

---

## 1. Scope & Requirements

### Functional
- Users browse events (search by city, date, artist, venue)
- View seat map with real-time availability (available / held / sold)
- Hold 1–N seats for a fixed window (10 minutes)
- Complete purchase (payment → booking confirmed)
- Cancel hold or booking within rules
- View/download e-tickets (PDF or QR)
- Admins create events, set pricing tiers, release inventory in waves

### Non-Functional
- **Scale:** 50K concurrent seat hold requests at peak (Taylor Swift onsale)
- **Availability:** 99.99% — downtime = lost revenue + PR disaster
- **Seat uniqueness:** Zero double-sells (hard constraint)
- **Hold duration:** 10 minutes, auto-expire
- **Fairness for hot events:** Virtual waiting queue, no first-cracker-gets-all advantage
- **Consistency:** Seat status must be strongly consistent at hold/purchase time

### Out of Scope
- Ticket resale / secondary market
- Event recommendation ML
- Venue gate scanning — separate QR validation service
- Dynamic pricing engine

---

## 2. Entities

```
User
  id              UUID
  email           string
  payment_methods [card_id]

Event
  id              UUID
  name            string
  artist_id       UUID
  venue_id        UUID
  event_date      timestamp
  status          enum(DRAFT, ON_SALE, SOLD_OUT, CANCELLED)
  sale_start_at   timestamp

Venue
  id              UUID
  name            string
  city            string
  total_capacity  int
  seat_map_url    string   (static JSON on CDN)

Section
  id              UUID
  venue_id        UUID
  name            string   (e.g. "Floor A", "Upper Tier")
  capacity        int

Seat
  id              UUID
  section_id      UUID
  row             string
  number          int
  status          enum(AVAILABLE, HELD, SOLD)    ← source of truth
  hold_expires_at timestamp (nullable)
  held_by_user    UUID (nullable)

Ticket
  id              UUID
  event_id        UUID
  seat_id         UUID
  order_id        UUID
  qr_code         string   (signed token)
  issued_at       timestamp

Order
  id              UUID
  user_id         UUID
  event_id        UUID
  status          enum(PENDING, CONFIRMED, CANCELLED, REFUNDED)
  total_amount    decimal
  idempotency_key UUID     (client-generated)
  created_at      timestamp

OrderItem
  id              UUID
  order_id        UUID
  seat_id         UUID
  price           decimal
```

---

## 3. Actions & Data Flow

### 3.1 Browse Events
```
User → CDN (cached event pages)
     → Search Service (Elasticsearch for city/date/artist search)
     → Event Service (fetch event details from Postgres)
```
Elasticsearch synced via CDC: Debezium → Kafka → ES consumer (~1s stale). Acceptable for browsing. Always confirm event exists in Postgres before hold.

### 3.2 View Seat Map
```
User → Seat Service → Redis (seat status cache, 5s TTL)
                    → Postgres primary (on cache miss)

Redis key: seat_status:{event_id} → hash of seat_id → status
TTL: 5s
```
Trade-off: 5s stale map is fine for browsing. Real availability confirmed atomically at hold time.

**Why not push each seat change via WebSocket?**  
50K viewers × seat changes every second = too much fan-out. Polling every 3s is sufficient and vastly simpler.

### 3.3 Hold Seat (Critical Path)

```
POST /hold { event_id, seat_ids:[3], idempotency_key }

Seat Service:
  1. Verify waiting queue token (hot events only — §7)
  2. BEGIN TRANSACTION
       For each seat_id:
         UPDATE seats
         SET status='HELD',
             held_by_user=$user,
             hold_expires_at = now() + interval '10 min'
         WHERE id=$seat_id
           AND event_id=$event_id
           AND status='AVAILABLE'      ← atomic check + write, no separate SELECT
         RETURNING id;

         rows_affected = 0? → ROLLBACK all → 409 Conflict
     COMMIT
  3. Create PENDING order
  4. Update Redis cache for held seats
  5. Return order_id
```

**Why DB atomic UPDATE, not Redis SETNX?**
- Redis can lose data on crash (even with AOF, small window exists)
- DB `UPDATE ... WHERE status='AVAILABLE'` = single atomic statement, row-locked by Postgres
- Redis updated *after* DB as a cache, never as the source of lock

**Multi-seat all-or-nothing:**  
Wrap all seat UPDATEs in a single transaction. Any 0-row result → ROLLBACK all. User sees partial failure cleanly.

### 3.4 Hold Expiry

```
Expiry Worker (every 30s):
  SELECT id FROM seats
  WHERE status='HELD' AND hold_expires_at < now()
  
  UPDATE seats
  SET status='AVAILABLE', held_by_user=NULL, hold_expires_at=NULL
  WHERE id IN (expired_ids)
  
  → Update order status='CANCELLED' for associated PENDING orders
  → Invalidate Redis cache
  → Publish SeatReleased to Kafka → SSE to users browsing that event
```

30s interval = max 30s extra hold beyond expiry. Acceptable. Reduces DB load vs 1s polling.

### 3.5 Checkout / Payment

```
POST /orders/{id}/pay { card_token, idempotency_key }

Order Service:
  1. SELECT hold_expires_at FROM seats WHERE id IN (order seats) — from PRIMARY
     → any expired? → 400 "Hold expired, please reselect"
  2. Verify order.status = 'PENDING'
  3. Call Payment Service with idempotency_key
       → forwards to Stripe with Stripe-Idempotency-Key
  4. Payment SUCCESS:
       BEGIN;
         UPDATE seats SET status='SOLD' WHERE id IN (...) AND status='HELD'
         UPDATE orders SET status='CONFIRMED'
         INSERT INTO tickets (seat_id, order_id, qr_code, ...)
       COMMIT;
       Publish OrderConfirmed → Kafka
  5. Payment FAIL:
       UPDATE seats SET status='AVAILABLE', held_by_user=NULL ...
       UPDATE orders SET status='CANCELLED'
       Return error
```

**Payment idempotency:** Client generates UUID before first attempt. On retry, same key → Payment Service returns cached result → no double charge.

### 3.6 Order State Machine
```
PENDING ──→ CONFIRMED  (payment success)
PENDING ──→ CANCELLED  (hold expired | payment failed | user cancelled)
CONFIRMED → REFUNDED   (cancellation within policy)
```
All transitions enforced: `UPDATE orders SET status='CONFIRMED' WHERE id=$1 AND status='PENDING'` → 0 rows = already transitioned.

---

## 4. High-Level Design

```
Users ──────────→ CDN (event pages, seat map JSON)
        │
        └───────→ API Gateway (auth, rate limit, TLS)
                        │
         ┌──────────────┼──────────────┬──────────────┐
         │              │              │              │
   Event Service   Seat Service   Order Service  Search Service
         │              │              │              │
      Postgres      Postgres       Postgres      Elasticsearch
      (events,      PRIMARY        (orders,      (event index)
       venues)     (seats ←        tickets)
                   source of                    
                   truth)         
                        │
                      Redis
                  (seat cache 5s,
                   queue tokens,
                   inventory count)
                        │
                      Kafka
               ┌────────┴────────┐
          Notification        Analytics
          Service             → BigQuery
```

---

## 5. Low-Level Design

### 5.1 Seat Table Indexing

```sql
-- Seat hold query: event + status
CREATE INDEX idx_seats_event_status ON seats(event_id, status);

-- Expiry job: only held seats with past expiry
CREATE INDEX idx_seats_expiry ON seats(hold_expires_at)
  WHERE status = 'HELD';   -- partial index, much smaller

-- Uniqueness constraint: no two holds on same seat
-- Enforced by status column logic, not UNIQUE index (seat changes state, not key)
```

**Why these indexes, and not others?**

**`idx_seats_event_status (event_id, status)`**  
The most common query: "show me all available seats for event X." Without this index, Postgres scans every seat row in the table (could be millions across all events). With the composite index, it jumps straight to rows where `event_id = X AND status = 'AVAILABLE'`. Order matters — `event_id` first because it's the high-cardinality filter that eliminates the most rows.

**`idx_seats_expiry` — partial index with `WHERE status = 'HELD'`**  
The expiry worker queries: "find all HELD seats past their expiry time." At any given moment, only a tiny fraction of seats are HELD (e.g., 200 out of 50,000). A full index on `hold_expires_at` would index all 50,000 rows including AVAILABLE and SOLD rows that the expiry job never touches. The partial index only indexes rows where `status = 'HELD'` — much smaller, faster to scan, cheaper to maintain.

**Why no `UNIQUE` index for "one hold per seat"?**  
A UNIQUE index enforces that a column value appears only once across all rows. But `seat_id` is not what changes — `status` is. The same seat row transitions: AVAILABLE → HELD → SOLD → AVAILABLE. You can't put a UNIQUE constraint on "only one row with status=HELD per seat" using a standard UNIQUE index (Postgres partial unique indexes can approximate it, but it's complex). Instead, the atomic `UPDATE WHERE status='AVAILABLE'` enforces the invariant: the DB can only flip one row to HELD at a time — row-level locking handles the rest.

### 5.2 Fast Availability Pre-Check (Redis Counter)

```
event:{id}:available_count  →  integer

On hold:   DECR event:{id}:available_count  (atomic)
On expiry: INCR event:{id}:available_count  (atomic)
On sale open: SET event:{id}:available_count <total_seats>

Before DB query:
  count = GET event:{id}:available_count
  if count <= 0: return 409 immediately (no DB hit)
```

Resync every 5 min from DB to prevent drift:
```sql
SELECT COUNT(*) FROM seats WHERE event_id=$1 AND status='AVAILABLE'
```

Trade-off: Counter slightly stale (up to 5 min drift after crash). But worst case: we do a DB query anyway when counter says 0 but DB has seats — acceptable false negative, not a false positive.

### 5.3 Seat Map: Static + Live Overlay

**Static layout** (CDN, aggressive cache):
```json
{
  "venue_id": "...",
  "sections": [
    { "id": "floor_a", "rows": [{ "row": "1", "seats": [1,2,...,20] }] }
  ]
}
```
Layout never changes → Cache-Control: max-age=86400.

**Availability overlay** (live, 5s TTL):
```json
{
  "event_id": "...",
  "as_of": "2026-06-08T10:00:05Z",
  "held":  ["seat_101", "seat_205"],
  "sold":  ["seat_310"]
}
```
Client polls overlay every 3s. Renders map = layout + overlay. Decouples static from dynamic.

### 5.4 QR Code / E-Ticket

```
payload   = base64url({ ticket_id, seat_id, event_id, user_id, issued_at })
signature = HMAC_SHA256(payload, SERVER_SECRET)
qr_data   = payload + "." + signature
```

Gate scanner: verify HMAC (< 1ms, no DB call) → check Redis set `scanned:{event_id}` for duplicate scan → SADD returns 0 = already scanned → reject.

### 5.5 Database Sharding

Shard key: `event_id`
- All seats, orders, tickets for one event → same shard → no cross-shard joins during hold
- Hot event (Taylor Swift) = one shard hammered → mitigate with read replicas for browsing; primary for all writes

Alternative — shard by `venue_id`: natural geographic split, similar trade-off on hot events.

---

## 6. Virtual Waiting Queue (Hot Events)

**Problem:** 2M users hit buy simultaneously → DB overwhelmed → no one gets tickets.

**Pattern: Queue-It style virtual room**

```
Step 1: User arrives → Waiting Room Service
  ZADD queue:{event_id} <arrival_timestamp> <user_id>
  Return: { position: 45230, estimated_wait: "23 min" }

Step 2: Worker drains queue at controlled rate
  ZPOPMIN queue:{event_id} <batch_size>   -- drain N users every 30s
  Issue each user a signed JWT:
    { user_id, event_id, valid_until: now+15min }

Step 3: User presents JWT with hold request
  Seat Service: verify JWT signature → allow hold

Step 4: User doesn't act within 15 min → JWT expires → re-queue
```

**Drain rate = f(DB throughput)**  
If Seat Service handles 500 hold requests/sec and each hold takes 20ms → drain 500 users/sec from queue.

**Trade-off:** Queue adds 20–60 min wait for hot events. Fairer than "fastest internet wins." Add CAPTCHA at queue entry to filter bots.

### Alternate Options

**Option B: API Gateway Rate Limiting (Token Bucket)**
```
API Gateway enforces: max 500 hold requests/sec globally
Excess requests → 429 Too Many Requests + Retry-After header
Client retries after N seconds
```
Pros: zero extra infrastructure, simple to configure.  
Cons: no queue position → user has no idea when to retry → frustrating UX. Bots retry aggressively and win over humans. No fairness guarantee — pure luck of retry timing.  
Use when: you don't care about fairness, just protecting the DB. Bad fit for ticket onsales.

**Option C: SQS FIFO Queue**
```
User arrives → publish message to SQS FIFO queue (MessageGroupId = event_id)
Worker polls SQS FIFO → processes in order → issues JWT token → notifies user via WebSocket/email
```
Pros: managed, durable, guaranteed FIFO ordering within group, no Redis ops to maintain.  
Cons: no instant position query (`ZRANK` equivalent doesn't exist in SQS) → can't tell user "you're #45,230 in line." SQS has throughput limits per MessageGroup (~3,000 messages/sec). Adds 1–2s polling latency vs Redis.  
Use when: fairness matters but position visibility doesn't, or event is not extremely hot.

**Option D: Dedicated SaaS (Queue-It, Fastly Waiting Room, Cloudflare Waiting Room)**
```
DNS / CDN layer redirects traffic through SaaS waiting room
SaaS handles queue, position display, token issuance, bot detection
Your backend only sees pre-qualified users
```
Pros: zero engineering effort, battle-tested at Ticketmaster scale, built-in CAPTCHA and bot detection, handles millions of concurrent users.  
Cons: cost (per-event pricing), vendor dependency, less control over queue logic, data leaves your infra.  
Use when: you don't want to build this yourself. In an interview, mention this as the real-world answer, then describe Option A as "how it works internally."

**Comparison**

| Option | Position Visibility | Fairness | Complexity | Bot Resistance |
|--------|-------------------|----------|------------|----------------|
| A — Redis ZADD + JWT | Yes (`ZRANK`) | High (timestamp order) | Medium | Needs CAPTCHA |
| B — Rate Limit | No | None (luck of retry) | Low | Low |
| C — SQS FIFO | No | High (FIFO) | Low-Medium | Low |
| D — SaaS (Queue-It) | Yes | High | Low (integration only) | High (built-in) |

---

## 7. Trade-offs Summary

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Seat hold mechanism | DB atomic `UPDATE WHERE status='AVAILABLE'` | Redis SETNX | DB = durable, atomic. Redis = fast but lossy on crash. |
| Seat map refresh | Client polls every 3s | WebSocket push per seat | 50K viewers × updates = WS fan-out explosion. Polling is predictable. |
| Hold expiry | Background worker every 30s | DB trigger / TTL column | Worker = controlled burst; doesn't fire during high-load DB transactions. |
| Waiting queue | Redis sorted set (ZADD score=timestamp) | SQS FIFO | ZADD supports instant position query (`ZRANK`); SQS has no position visibility. |
| Inventory pre-check | Redis counter + periodic resync | Always query DB | 99% of "sold out" checks never hit DB. Drift is bounded and safe (false 0 → DB fallback). |
| Seat map layout | Static CDN JSON + live overlay | Full dynamic render | Layout never changes → CDN hit. Overlay is tiny and separate. |

---

## 8. Key Numbers

| Metric | Value |
|--------|-------|
| Hold attempts at onsale (hot event) | ~2M in first 60s |
| Seats per event | 5K – 80K |
| Hold TTL | 10 min |
| Expiry worker interval | 30s |
| Seat availability cache TTL | 5s |
| DB shard key | event_id |
| QR verify latency | <5ms (HMAC only, no DB) |
| Waiting queue drain rate | ~500 users/sec per Seat Service instance |
