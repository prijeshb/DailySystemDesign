# Ticketmaster — Failure Analysis
**Date:** 2026-06-08  
**System:** Online Event Ticket Booking  
**Approach:** Failure-first — every component, upstream/downstream failures

---

## Failure Map

```
[Client] → [API Gateway] → [Waiting Queue] → [Seat Service] → [Postgres Primary]
                                                     │
                                                  [Redis]
                                                     │
                                             [Order Service] → [Payment Service] → [Stripe]
                                                     │
                                                  [Kafka]
                                          ┌──────────┘
                                  [Notification]  [Analytics]
```

---

## 1. Client Fails to Complete Hold Request

**Scenario:** User selects seats, submits hold — network drops before response arrives.

**Impact:** Hold may or may not have been created. User retries → possible duplicate hold attempt.

### Without idempotency
- First request: seat held, order_id=X created
- Retry: seat already HELD → 409 → user thinks it failed → confused

### With idempotency_key (UUID, client-generated)
```
Client stores: idempotency_key = UUID() before first attempt
Every retry sends same key

Seat Service:
  SELECT order_id FROM holds WHERE idempotency_key=$key
  Found? → return same order_id (seat already held, just confirm it)
  Not found? → proceed with hold
```
Retry returns same result. User sees consistent state.

**Trade-off:** Key must be stored in client local storage. If user clears storage and retries, new UUID → second hold attempt. Seat Service must enforce per-user-per-event hold limits (max 4 seats per user).

**How per-user hold limits are enforced:**

Three layers, each catching what the previous misses:

**Layer 1: COUNT check before hold (application layer)**
```sql
-- Before attempting the hold, check existing held seats for this user+event
SELECT COUNT(*) FROM seats
WHERE event_id = $event_id
  AND held_by_user = $user_id
  AND status = 'HELD'
  AND hold_expires_at > now();   -- only count active holds

-- requested = number of seats in this request
IF existing_count + requested > 4:
    return 409 "Hold limit reached"
```
Read from primary (not replica) — replica lag could show 2 when primary has 4.

**Layer 2: Atomic check inside the transaction (race-safe)**

The COUNT check above has a TOCTOU gap — two concurrent requests from the same user both pass the check before either commits. Fix: enforce inside the hold transaction itself:

```sql
BEGIN;
  -- Re-count inside transaction (sees locked rows)
  SELECT COUNT(*) FROM seats
  WHERE event_id=$event_id AND held_by_user=$user_id
    AND status='HELD' AND hold_expires_at > now()
  FOR UPDATE;   -- lock these rows so concurrent txn waits

  IF count + requested > 4: ROLLBACK;

  -- proceed with seat UPDATEs
COMMIT;
```
`FOR UPDATE` on the count query means a concurrent hold from the same user must wait for this transaction to finish before it can count. Serializes per-user holds.

**Layer 3: DB partial unique index (last line of defense)**
```sql
-- At most 4 active holds per user per event
-- Can't express "max 4" as a UNIQUE constraint, but can catch obvious bugs:
CREATE UNIQUE INDEX idx_one_active_order_per_user_event
  ON orders(user_id, event_id)
  WHERE status = 'PENDING';
-- Prevents two simultaneous PENDING orders for same user+event entirely
```
This is a blunter constraint (one pending order, not 4 seats) but catches the "user opened two browser tabs" case cleanly.

**Why read from primary for the count:**
```
Replica lag scenario:
  User holds 4 seats → primary shows 4 HELD
  User clears storage, retries immediately
  Replica still shows 0-2 HELD (lag)
  COUNT from replica = 2 → check passes → user holds 4 more → 8 seats held
```
Primary read for limit checks is non-negotiable.

---

## 2. Seat Service Crashes Mid-Hold Transaction

**Scenario:** DB transaction starts, Seat Service crashes before COMMIT.

**Impact:** Postgres rolls back automatically on connection close. Seats return to AVAILABLE. No phantom holds.

**What about Redis cache?**
- Redis updated *after* COMMIT. If crash before update → Redis may show seat as AVAILABLE when it's actually HELD, or vice versa.
- Not a correctness problem: Redis is cache only, hold truth is in Postgres. Next read refreshes Redis.

**Recovery:** Client retries with same idempotency_key → no hold found → attempt fresh hold.

---

## 3. Hold Expiry Worker Fails

**Scenario:** Expiry worker crashes for 10 minutes.

**Impact:** Expired holds persist beyond 10-minute TTL. Seats show HELD even though time is up. Other users can't select those seats.

**Prevention:**
- Multiple expiry worker instances (only one runs via distributed lock: `SET expiry_lock NX EX 60`)
- On worker startup: immediately process all expired holds (catch-up run)
- **DB constraint as backstop:** Hold query includes `AND hold_expires_at > now()` — even if status=HELD, payment at checkout re-validates expiry from primary DB

**Worst case:** 10-min outage of worker → 10-min "phantom holds" → users see fewer available seats but no false sales. Recovers automatically when worker restarts.

---

## 4. Payment Service Fails After Seat Is Held

**Scenario:** User pays. Order Service calls Payment Service. Payment Service crashes.

**Outcome by failure timing:**

| When crash happens | Seat status | Order status | Action needed |
|---|---|---|---|
| Before charge attempt | HELD | PENDING | User retries — payment service issues refund idempotently |
| After charge, before Order DB write | HELD + user charged | PENDING | Idempotency key resolves on retry — detect charge exists, write CONFIRMED |
| After Order DB write CONFIRMED | SOLD | CONFIRMED | Correct — idempotency key returns cached CONFIRMED result |

**Async recovery for middle case:**
```
Reconciliation Worker (runs every 5 min):
  SELECT orders WHERE status='PENDING' AND created_at < now()-15min
  For each: query Payment Service with order's idempotency_key
    → charged? → mark CONFIRMED + SOLD
    → not charged? → mark CANCELLED + release seat
```

**Trade-off:** 15-min window before reconciliation. User may not know status for 15 min. Send "processing" email immediately; final status email after reconciliation.

---

## 5. Payment Succeeds but DB Write Fails (CONFIRMED)

**Scenario:** Stripe charges card. `UPDATE seats SET status='SOLD'` fails (DB crash, deadlock).

**Impact:** User charged, seat still HELD (expires in ~7 min), no ticket issued.

**Detection:**
- Same reconciliation worker: finds PENDING order + confirmed payment → retry DB write
- DB write is idempotent: `UPDATE seats SET status='SOLD' WHERE id=$1 AND status IN ('HELD','SOLD')` — running twice = same result

**Prevention:**
- Outbox pattern: write CONFIRMED + outbox event in same transaction
- Outbox publisher re-attempts Kafka publish on recovery
- Ticket generation triggered by Kafka event → survives crash

---

## 6. Waiting Queue Service Fails

**Scenario:** Queue Service is down when Taylor Swift tickets go on sale.

**Impact depends on queue bypass policy:**

**Option A: Fail open** — allow requests without queue token  
→ 2M requests flood Seat Service → DB overloaded → total outage

**Option B: Fail closed** — reject requests without valid queue token  
→ No one buys tickets until queue recovers → bad UX but safe

**Option C: Circuit breaker with rate limit**  
→ Queue down → fall back to global rate limit (e.g., 1000 req/sec at API Gateway) → controlled load even without queue ordering

**Best practice:** Option C for short outages, Option B for extended. Never Option A for hot events.

**Queue Redis failure:**
- Redis cluster with AOF persistence + replicas
- On Redis primary failure: replica promoted (Redis Sentinel, ~30s)
- During 30s: queue frozen → no new tokens issued → existing tokens still valid (JWT, verified without Redis)

---

## 7. Postgres Primary Fails During Peak Onsale

**Scenario:** Postgres primary dies while 10K seat hold transactions/sec are in flight.

**Impact:** All ongoing transactions rolled back. In-flight holds lost.

**Recovery:**
- Automatic failover: Patroni/pg_auto_failover promotes replica (~15–30s)
- Read replicas promoted to primary; new primary begins accepting writes
- All in-flight transactions rolled back → clients retry → hold attempt fails or succeeds on new primary

**Mitigation:**
- Idempotency keys: retries are safe
- API Gateway: 503 response with `Retry-After: 30` header → clients wait and retry
- Waiting queue: acts as buffer — doesn't issue new tokens during failover, resumes after

**Data loss risk:**  
Replica may be ~1s behind primary at time of failure. In-flight transactions that committed on primary but not replicated → lost. Users get 503 → retry → clean new hold. No double-charge risk (payment not initiated for lost holds).

---

## 8. Hot Seat DB Row Contention

**Scenario:** Seat 3A, Floor A, Taylor Swift. 50K concurrent UPDATE requests on same row.

**Impact:** Postgres serializes at row level. 50K requests queue up on that row lock → massive wait → timeouts.

**Why this happens:** Every user who clicks seat 3A on the map sends an UPDATE. Even if we have 100 DB replicas, all writes go to primary, and all fight for the same row.

**Solutions:**

**A: Funnel through queue per seat**
```
Redis List: seat_queue:{seat_id}
  LPUSH seat_queue:{seat_id} {user_id, idempotency_key}
  Worker pops FIFO → one DB write at a time → O(1) contention per seat
```
Trade-off: extra latency (~100ms per user in queue). Prevents thundering herd.

**B: Virtual waiting queue (already described in §6 of design)**  
Limits concurrent hold attempts across all seats → seat-level contention naturally capped.

**C: Optimistic locking** (less effective here)  
High contention = many retries = same problem.

**Best answer:** Virtual waiting queue caps overall throughput → most contention avoided before DB. Seat-level queue as backstop for extremely popular individual seats.

---

## 9. Elasticsearch Event Index Becomes Stale

**Scenario:** CDC pipeline (Debezium → Kafka → ES) lags 5 minutes. User searches "Taylor Swift" — event shows ON_SALE but it's actually SOLD_OUT.

**Impact:** User clicks event, goes to seat map, all seats show SOLD → confusion + frustration.

**Prevention:**
- ES is for search/discovery only. Event detail page always fetches from Postgres Event Service.
- "SOLD OUT" badge on event page = read from Postgres (real-time), not ES
- Acceptable: ES search results 5 min stale. Not acceptable: showing wrong status on detail/purchase page.

**Rule:** Search index = find events. Transaction DB = everything at purchase time. Never trust ES for purchase-critical state.

---

## 10. QR Code Double-Scan at Venue Gate

**Scenario:** Scalper duplicates QR image. Two people present same QR at two gates simultaneously.

**Impact:** Both valid HMAC signatures → both gates might admit.

**Prevention:**
```
Gate scanner → SADD scanned:{event_id} {ticket_id}
  Returns 1 → first scan → ADMIT
  Returns 0 → already scanned → REJECT

Redis set is shared across all gates (same cluster)
```

**Race condition:** Two gates scan at exact same time → Redis SADD is atomic → only one returns 1.

**Fallback if Redis down:** Gate scanner falls back to local set (per-device) → duplicate scan via different gate admitted. Trade-off: prefer availability (admit valid ticket) over perfect dedup on Redis failure.

---

## 11. Inventory Counter Drift (Redis)

**Scenario:** Seat Service crashes after DB hold but before `DECR event:{id}:available_count`. Counter shows 1 seat available, but DB shows 0.

**Impact:** One extra hold request gets past the Redis pre-check → hits DB → DB correctly returns 0 rows → 409 returned. No false positive sale, just one extra DB query.

**Why this is safe:** Redis counter is a pre-filter only. DB is authoritative. Drift causes at most a few extra DB queries, never a double-sell.

**Resync:** Every 5 minutes, worker recalculates true count from DB and overwrites Redis. Bounded drift.

---

## 12. Hold Expiry Race: TTL vs Payment Completion

**Scenario:** User is 9:55 into their 10-min hold. Starts payment at 9:50. Payment takes 15 seconds. By the time payment response arrives, hold has expired.

**Timeline:**
```
t=0:00  Hold created, hold_expires_at = t+10:00
t=9:50  User submits payment
t=10:00 Expiry worker runs → sets seat status='AVAILABLE'
t=10:05 Payment service returns SUCCESS
t=10:05 Order Service: UPDATE seats SET status='SOLD' WHERE status='HELD' → 0 rows
```

**Impact:** User charged, seat released, ticket not issued.

**Prevention:**

**Option A: Grace period in expiry worker**  
```sql
WHERE status='HELD'
  AND hold_expires_at < now() - interval '2 min'   -- 2 min grace
```
Trade-off: seats held 12 min total instead of 10. Slight overclaim on inventory.

**Option B: Payment pre-check and extend hold**  
```
Before initiating payment:
  UPDATE seats SET hold_expires_at = now() + interval '5 min'
  WHERE id IN (...) AND status='HELD' AND held_by_user=$user
  AND hold_expires_at > now()   -- still valid
```
If rows_affected=0 → hold already expired → reject before charging card.

**Option C: Sell even if "HELD by someone else"** (wrong — allows double-sell)

**Best answer:** Option B — extend hold TTL at payment start. If already expired (0 rows), abort before charge. This closes the race window.
