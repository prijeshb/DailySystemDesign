# Airbnb — Rental Property Booking System Design
*Date: 2026-06-30*

---

## First Principles Check

> Do we really need a booking system? Yes — rental marketplaces require:
> - Searchable property inventory with availability
> - Guaranteed exclusive access for a time window (booking)
> - Trust infrastructure (reviews, identity, payments)
> - The hard problem: prevent double-bookings under concurrent requests

---

## Scale Assumptions

| Metric | Value |
|--------|-------|
| Registered users | 150M |
| Active listings | 7M properties |
| Searches/day | 5M |
| Bookings/day | 500K |
| Peak search RPS | ~250 |
| Peak booking RPS | ~30 |
| Avg listing photos | 20 images × 2MB = 40MB/listing |
| Total image storage | 7M × 40MB = 280TB |

---

## Back-of-Envelope (AWS Costs)

### Storage
```
Property images (S3):     280TB × $0.023/GB      = $6,440/month
CDN (CloudFront):         assume 50TB transfer    = $425/month
Postgres (bookings/users): r6g.2xlarge × 2        = $730/month
ElastiCache Redis (3-node cluster): r6g.large × 3 = $375/month
OpenSearch (3-node):      r6g.large.search × 3   = $540/month
```

### Compute
```
Search API:   10 × c6g.xlarge (spot) = $400/month
Booking API:  5  × c6g.xlarge        = $200/month
Image Service: 3 × c6g.large         = $90/month
```

### Total Estimate: ~**$10,000–15,000/month** at mid-scale

### Budget Constraint Example
**Startup at 1/10th scale (~$1,500/month budget):**
- Skip dedicated OpenSearch → use Postgres full-text search (pg_trgm)
- Limitation: no geo-radius search, slow text queries above 500K listings
- Skip Redis cluster → single Redis node (no HA)
- Limitation: cache miss on Redis restart drops to 10× DB load briefly
- Skip CDN → serve images directly from S3
- Limitation: image load 300ms vs 20ms; no edge caching

---

## Entities

```
User         { id, email, phone, is_host, identity_verified, created_at }
Listing      { id, host_id, title, description, type, lat, lng,
               max_guests, bedrooms, bathrooms, amenities[], status }
Room         { id, listing_id, name }          // for multi-room properties
Availability { listing_id, date, status }      // AVAILABLE | BLOCKED | BOOKED
PriceRule    { listing_id, start_date, end_date, nightly_price, min_nights }
Booking      { id, listing_id, guest_id, check_in, check_out,
               guests, total_price, status, idempotency_key }
Payment      { id, booking_id, amount, currency, provider_ref, status }
Review       { id, booking_id, reviewer_id, reviewee_id, rating, text }
Message      { id, booking_id, sender_id, content, created_at }
```

---

## Actions

**Guest:**
- Search (location + dates + filters)
- Check availability
- Request / Instant Book
- Cancel booking
- Leave review

**Host:**
- Create/edit listing + photos
- Set availability calendar
- Set pricing rules (weekend rates, min stay)
- Accept / Decline request
- Leave review

---

## Data Flow

### Search Flow
```
Guest → Search API
  → parse location → geocode (lat/lng)
  → OpenSearch:
      geo_distance filter (radius km)
      + date availability pre-filter
      + price range, amenities filters
      → ranked listing IDs
  → Fetch listing details from Redis / Postgres
  → Return results with thumbnail URLs (CloudFront)
```

### Booking Flow
```
Guest selects listing → Check availability (DB read, primary)
  → Attempt hold: SET hold:{listing_id}:{check_in}:{check_out} guest_id NX EX 600
  → Got hold? → Create booking record (status=PENDING_PAYMENT)
              → Charge payment (Stripe)
                → Success: UPDATE booking status=CONFIRMED
                           UPDATE availability table (date rows → BOOKED)
                           DELETE hold key
                           → Notify host (push/email)
                → Failure: UPDATE booking status=FAILED
                           DELETE hold key
                           → Availability freed
  → No hold? → Return "dates unavailable, try again"
```

### Review Flow
```
check_out + 24h → scheduled job → send review prompt
Guest submits review → store (pending_moderation)
  → spam/content check (async)
  → publish after 14 days or when both parties review
  → Update listing avg_rating (incremental: running_sum / count)
```

---

## High-Level Design

```
                        ┌─────────────┐
     Client             │ CloudFront  │──► S3 (property images)
      │                 │   (CDN)     │
      ▼                 └─────────────┘
  [API Gateway] ──► rate limit, auth (JWT), routing
      │
  ┌───┴───────────────────────────────────────┐
  │           │           │                  │
[Search    [Listing    [Booking           [Message
 Service]   Service]   Service]           Service]
  │           │           │                  │
  │     [Postgres]   [Postgres]          [Postgres]
  │     (listings)  (bookings/        (messages)
  │                  payments)
  │
[OpenSearch]          [Redis]
(search index)        (availability holds,
                       session cache,
                       listing cache)
      │                      │
  [Kafka] ◄──── CDC (Debezium from Postgres)
      │
  ├── Notification Service (email/push)
  ├── Search Index Sync Service
  └── Analytics (S3 + Athena)
```

---

## Low-Level Design

### 1. Availability Calendar

**Problem:** 7M listings × 365 days = 2.5B rows if naive per-day rows. Too much.

**Better: Sparse row model (only store non-default state)**
```sql
CREATE TABLE availability (
  listing_id   BIGINT,
  date         DATE,
  status       ENUM('BLOCKED', 'BOOKED'),  -- AVAILABLE = absence of row
  booking_id   BIGINT REFERENCES bookings(id),
  PRIMARY KEY (listing_id, date)
);

-- Check available: dates NOT IN (SELECT date FROM availability WHERE listing_id=X)
-- Block: INSERT date rows with status=BLOCKED
-- Book:  INSERT date rows with status=BOOKED (within booking transaction)
```

Index: `(listing_id, date)` — composite PK covers range queries efficiently.

**Alternative (host-friendly):** iCal-style blocks stored as date ranges.
```sql
CREATE TABLE blocked_ranges (
  listing_id  BIGINT,
  start_date  DATE,
  end_date    DATE,
  reason      TEXT,   -- 'host_blocked' | booking_id
  PRIMARY KEY (listing_id, start_date)
);
```
Fewer rows, but overlap detection requires range query. Use `&&` operator with `daterange` type in Postgres.

**Trade-off:** Sparse rows (fast point lookups) vs date ranges (fewer rows, complex overlap detection). Use sparse rows for operational availability checks; ranges for host calendar UI.

---

### 2. Double-Booking Prevention (TOCTOU Fix)

**Wrong approach:**
```
1. SELECT available dates → looks good
2. INSERT booking
// concurrent request also passed step 1 → double booking
```

**Correct approach — INSERT with conflict detection:**
```sql
-- Atomic: insert availability rows inside booking transaction
BEGIN;

-- Lock the listing's dates (skip locked = don't block, just skip)
SELECT listing_id FROM availability
WHERE listing_id = $listing_id
  AND date BETWEEN $check_in AND $check_out - 1
FOR UPDATE;  -- row-level lock on any existing rows

-- If any existing row found → dates already blocked/booked → ROLLBACK
-- If no rows → safe to proceed

INSERT INTO bookings (listing_id, guest_id, check_in, check_out, status, ...)
VALUES (...) RETURNING id;

-- Insert availability rows atomically
INSERT INTO availability (listing_id, date, status, booking_id)
  SELECT $listing_id, generate_series($check_in, $check_out - 1, '1 day'), 'BOOKED', $booking_id
  ON CONFLICT (listing_id, date) DO NOTHING
  RETURNING date;  -- if fewer rows returned than expected → conflict → ROLLBACK

COMMIT;
```

DB-level constraint as last defense:
```sql
ALTER TABLE availability ADD CONSTRAINT unique_listing_date UNIQUE (listing_id, date);
```

---

### 3. Search: OpenSearch Schema

```json
{
  "id": "listing_123",
  "title": "Cozy flat in Bandra",
  "description": "...",
  "type": "apartment",
  "location": { "lat": 19.05, "lon": 72.84 },
  "price_min": 3500,
  "bedrooms": 2,
  "max_guests": 4,
  "amenities": ["wifi", "pool", "kitchen"],
  "avg_rating": 4.8,
  "review_count": 127,
  "available_dates": ["2026-07-01", "2026-07-02", ...]  // pre-computed for next 90 days
}
```

**Search query:**
```json
{
  "query": {
    "bool": {
      "filter": [
        { "geo_distance": { "distance": "20km", "location": { "lat": 19.05, "lon": 72.84 } } },
        { "terms": { "available_dates": ["2026-07-10", "2026-07-11", "2026-07-12"] } },
        { "range": { "price_min": { "lte": 5000 } } },
        { "term": { "max_guests": { "gte": 2 } } }
      ]
    }
  },
  "sort": [{ "_score": "desc" }, { "avg_rating": "desc" }]
}
```

**Trade-off:** `available_dates` field in ES is 90-day pre-computed. Updated async via Kafka when availability changes. Lag of ~1s — user might see slightly stale results. **Final availability check always happens against DB at booking time.**

---

### 4. Dynamic Pricing

```sql
CREATE TABLE price_rules (
  id          BIGINT PRIMARY KEY,
  listing_id  BIGINT,
  rule_type   ENUM('base', 'weekend', 'seasonal', 'long_stay_discount'),
  start_date  DATE,
  end_date    DATE,
  value       DECIMAL(10,2),  -- absolute price or % discount
  min_nights  INT DEFAULT 1,
  priority    INT             -- higher = overrides lower
);
```

**Price resolution at quote time:**
```
1. Find all matching rules for requested dates (ordered by priority DESC)
2. Apply base nightly rate
3. Apply overlapping rules (weekend +20%, holiday season +50%, weekly discount -10%)
4. Multiply by nights
5. Add: cleaning fee (flat), service fee (14%), taxes (GST 18%)
```

**Trade-off:** Pre-computing prices vs real-time calculation.
- Pre-compute: fast search display, stale on rule change
- Real-time: always accurate, slower search
- **Decision:** Display in search uses pre-computed nightly_from price. Exact quote computed real-time when user selects dates.

---

### 5. Identity & Trust

**Problem:** Airbnb needs identity verification to build trust.

```
Guest verification flow:
  Signup → email/phone OTP
  Booking attempt → prompt govt ID upload (Aadhaar/Passport)
  ID image → stored in S3 (private, host-access controlled)
  ID check → manual review OR automated OCR + face match
  Verified badge stored on user record

Host listing rules:
  can_require_id_verification: boolean per listing
  At search time: filter by user.identity_verified if required
```

---

## Component Trade-offs

| Decision | Choice | Why | Cost |
|----------|--------|-----|------|
| Availability check | Postgres primary (no replica) | No stale reads → no double booking | Extra primary load |
| Search index | OpenSearch + async sync | Fast geo+text+filters | ~1s lag vs DB |
| Hold mechanism | Redis SET NX | Atomic, auto-expiry | Lost if Redis restarts during hold |
| Image serving | S3 + CloudFront | Cheap, global, durable | CDN cache takes time to warm |
| Pricing | Real-time at quote | Always accurate | Slower than pre-computed |
| Availability field in ES | 90-day pre-computed | Fast filter in search | Can show false availability |

---

## Real-World References

- **Airbnb Engineering Blog** — "How We Scaled to 100M Listings" describes sharding by geo-region
- **Booking.com** — Uses Cassandra for availability at extreme scale (immutable slot model)
- **VRBO** — iCal standard for cross-platform calendar sync (hosts can sync from other platforms)
- **Airbnb's Calendar** — uses `daterange` in Postgres with GiST index for overlap detection in production

---

## Related Concepts
- Concept #1: Geo Search (OpenSearch geo_distance)
- Concept #13: TOCTOU (double booking prevention)
- Concept #22: Seat/Resource Hold Pattern (availability hold via Redis)
- Concept #4: Idempotency (booking idempotency key)
- Concept #8: Caching Patterns (listing cache, availability in ES)
- Concept #9: Outbox / Kafka (availability sync to search index)
- Concept #10: State Machines (booking status transitions)
