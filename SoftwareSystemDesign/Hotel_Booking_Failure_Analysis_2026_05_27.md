# Hotel Booking — Failure Analysis
**Date:** 2026-05-27

---

## Failure-First Mindset

For every component ask: **What happens if THIS fails?** → **What fails upstream?** → **What fails downstream?**

---

## 1. Inventory Service Fails

### Scenario A: Inventory Service Crashes During Reserve
- User submits booking → Booking Service calls Inventory Service → no response
- **Risk:** Booking Service doesn't know if inventory was decremented or not
- **Prevention:**
  - Idempotent reserve operation: retry with same `booking_id` is safe (DB: INSERT IGNORE or ON CONFLICT DO NOTHING on hold table)
  - Booking Service retries with exponential backoff (3x, 100ms→400ms→1600ms)
  - If all retries fail → return 503 to user, no partial state

### Scenario B: Inventory Service Crashes After Reserve, Before Payment
- Inventory decremented, hold created in Redis, payment never charged
- **Risk:** Room blocked indefinitely
- **Prevention:**
  - Redis TTL on hold (15 min) → background job restores inventory on expiry
  - Reconciliation job: every 5 min, find holds with no corresponding confirmed booking → release

### Scenario C: Inventory DB Overloaded (High Contention on Popular Room)
- Many users booking same room → UPDATE contention → lock wait timeouts
- **Prevention:**
  - Optimistic locking with retry limit (max 3 retries, then fail fast with "sold out")
  - Rate limit booking attempts per room-date via Redis counter
  - Queue bookings per room-date using Redis list (serialize writes)

---

## 2. Payment Service Fails

### Scenario A: Payment Timeout (no response)
- Inventory reserved, payment outcome unknown
- **Risk:** Double charge if retry succeeds; inventory held if we don't retry
- **Prevention:**
  - Idempotency key sent to payment provider — retry is safe
  - Query payment provider for status before retrying charge
  - If still unknown after 30s: mark booking as PENDING, async resolution job

### Scenario B: Payment Declined
- **Flow:** Payment fails → Inventory Service release inventory → return 402 to user
- Compensating transaction must always succeed; if release fails → retry with dead-letter queue

### Scenario C: Payment Provider Fully Down
- **Prevention:**
  - Circuit breaker: after 5 failures in 10s → open circuit → return "payment unavailable" immediately
  - Fallback: queue booking for manual processing (enterprise/B2B use case) or redirect to backup provider

---

## 3. Database Failures

### Scenario A: Primary DB Goes Down (Booking DB)
- **Prevention:**
  - Synchronous replication to standby (Postgres streaming replication)
  - Automatic failover via Patroni/pgBouncer (<30s downtime)
  - During failover: queue incoming bookings or return 503

### Scenario B: Inventory DB Replica Lag
- Read replica is behind → search shows stale availability
- **Accepted:** Search can show stale data (eventual consistency)
- **Guarded:** Final booking always hits primary DB

### Scenario C: Partial DB Write (crash mid-transaction)
- **Prevention:** All booking + inventory changes in single ACID transaction
- Postgres WAL ensures crash recovery — partial writes rolled back on restart

---

## 4. Redis (Cache/Holds) Fails

### Scenario A: Redis Crashes — Holds Lost
- All 15-min temp holds disappear
- **Risk:** Rooms that were "held" become bookable again → could lead to overbooking if payment was in-flight
- **Prevention:**
  - Redis persistence (AOF) — recover holds on restart
  - Redis Sentinel/Cluster for HA
  - Fallback: use DB table for holds (slower but durable); switch automatically if Redis unreachable

### Scenario B: Redis Crashes — Idempotency Keys Lost
- User retries booking → processed again → potential double charge
- **Prevention:**
  - Check Booking DB for existing booking with same idempotency_key before processing
  - DB is source of truth; Redis is cache for speed

### Scenario C: Redis Cache Miss (search cache evicted)
- **Impact:** Increased load on Elasticsearch — not catastrophic
- **Prevention:** Cache warming on startup; graceful degradation (cache miss = live query)

---

## 5. Search Service (Elasticsearch) Fails

### Scenario A: ES Cluster Down
- Users can't search for hotels
- **Prevention:**
  - ES cluster: 3-node minimum, shard replication factor 2
  - Fallback: degraded search via Postgres (no geo ranking, slower)
  - Circuit breaker: detect ES failure → route to fallback immediately

### Scenario B: ES Index Stale (hotel metadata not updated)
- Hotel changes pricing/amenities → ES still shows old data
- **Prevention:**
  - Write-through sync: hotel update → Kafka event → ES consumer updates index (eventually consistent, <5s lag)
  - Acceptable: search shows slightly stale amenity info, exact check at booking

---

## 6. API Gateway / Network Failures

### Scenario A: API Gateway Overloaded
- **Prevention:**
  - Rate limiting per user (100 req/min) and per IP
  - Request queuing with backpressure
  - Auto-scaling API Gateway instances

### Scenario B: Network Partition Between Services
- Booking Service can't reach Inventory Service
- **Prevention:**
  - Service mesh (Istio) handles retries and circuit breaking
  - Fail closed: if can't confirm inventory, don't proceed with payment

---

## 7. Kafka (Event Bus) Fails

### Scenario A: Kafka Down — Notification Not Sent
- Booking confirmed in DB but email/SMS not sent
- **Impact:** Low severity — booking exists, just notification delayed
- **Prevention:**
  - Kafka replication factor 3
  - Outbox pattern: write notification event to DB table in same booking transaction → separate Kafka producer reads from outbox → retries until Kafka available
  - At-least-once delivery; notification service deduplicates

---

## 8. Booking Service Itself Crashes

### Scenario A: Crash After Inventory Reserved, Before Writing Booking Record
- **State:** Inventory decremented, hold in Redis, no Booking row
- **Recovery:**
  - Reconciliation job: holds with no confirmed booking after 15min → release inventory
  - Booking ID generated before any external calls → idempotent on retry

### Scenario B: Multiple Instances — Race Condition
- Two Booking Service instances receive same idempotency key
- **Prevention:**
  - Redis distributed lock on idempotency key during processing (SETNX, TTL=30s)
  - Only one instance proceeds; other returns 409 Conflict or waits

---

## 9. Reconciliation & Consistency Jobs

| Job | Frequency | Action |
|-----|-----------|--------|
| Release expired holds | Every 5 min | Find Redis holds with no confirmed booking → restore inventory |
| Payment resolution | Every 1 min | Find PENDING bookings → query payment provider → confirm/cancel |
| ES sync check | Every 1 hour | Compare hotel count in ES vs DB → re-index drift |
| Overbooking audit | Daily | available_count < 0 anywhere → alert + manual review |

---

## 10. Failure Severity Matrix

| Failure | Severity | User Impact | Recovery Time |
|---------|----------|-------------|---------------|
| Inventory DB primary down | P0 | Bookings fail | <30s (failover) |
| Payment provider down | P0 | No new bookings | Circuit break → 503 |
| Redis down | P1 | Slower bookings, holds lost | <2min (Sentinel) |
| Elasticsearch down | P1 | Search fails | Fallback to Postgres |
| Kafka down | P2 | Delayed notifications | Outbox retries |
| Single service instance crash | P2 | Transient errors | Auto-restart (<10s) |
| Cache miss storm | P3 | Slower search | Self-healing |
