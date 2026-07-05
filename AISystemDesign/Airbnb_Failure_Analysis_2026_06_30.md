# Airbnb — Failure Analysis
*Date: 2026-06-30*

---

## Failure-First Thinking

Every component can fail. The question is: what is the blast radius and how do we contain it?

---

## Component Failure Map

```
Client
  └─► API Gateway
        ├─► Search Service ──► OpenSearch
        │                  └─► Redis (listing cache)
        ├─► Listing Service ──► Postgres (listings)
        │                   └─► S3 (images)
        ├─► Booking Service ──► Postgres (bookings) ─► Redis (holds)
        │                   └─► Stripe (payments)
        └─► Notification Service ──► Kafka
                                 └─► Email/Push provider
```

---

## Case 1: API Gateway Fails

### Scenario
API Gateway crashes or becomes unreachable (ELB misconfiguration, deployment gone wrong).

### Impact
- All traffic blocked — 100% outage

### Detection
- CloudWatch: 5xx spike, TCP connection failures, latency P99 > 10s

### Recovery
**Immediate:** ELB health checks auto-route to healthy instances (multi-AZ).

**If entire ELB fails:**
- Failover DNS to secondary region (Route 53 health check → secondary ALB)
- TTL on Route 53 should be 60s max, not 300s default

**Prevention:**
- Blue/green deployments — swap only after health checks pass
- Canary: route 1% traffic to new gateway before full rollout
- Never deploy gateway changes during peak hours (Fri–Sun)

---

## Case 2: OpenSearch (Search Index) Fails

### Scenario
OpenSearch cluster goes down — all 3 nodes lose quorum, or master election fails.

### Impact
- Search returns 0 results or 503
- Users cannot find listings
- Hosts lose organic discovery

### Detection
- Search API 5xx rate > 1% → alert

### Recovery
**Short-term fallback:** Return last-known cached result from Redis (30-min TTL key per popular search).

**If cache miss:** Fall back to Postgres `listings` table with:
```sql
SELECT id, title, lat, lng, price_min FROM listings
WHERE ST_DWithin(location, ST_MakePoint($lng, $lat), $radius)
  AND status = 'active'
ORDER BY avg_rating DESC
LIMIT 50;
```
- Slower (200ms vs 20ms) but functional
- No amenity/availability filter in fallback — show results, let user filter on listing page

**What if Postgres also can't handle search load?**
- Rate-limit search to 10 RPS (serve degraded results or queue)
- Disable non-essential search filters (amenities, bed count) to reduce cardinality
- Enable maintenance page for new searches; keep booking flow alive

**Recovery of OpenSearch:**
- If 1-2 nodes fail: cluster auto-rebalances (green within 5–15 min)
- If all nodes fail: rebuild index from Postgres CDC stream; ETA ~2-4 hours for 7M listings
- Always keep a cold snapshot in S3 (daily snapshot → restore in <30 min)

**Prevention:**
- Minimum 3 nodes across 3 AZs
- Replica shards (1 primary + 1 replica minimum)
- Monitor: JVM heap > 80%, unassigned shards > 0

---

## Case 3: Redis (Availability Hold) Fails

### Scenario
Redis crashes while a guest has an active hold on dates (NX lock key).

### Impact
- Existing holds are lost → hold is treated as released
- Concurrent booking requests now compete with no lock
- Double booking possible if both get to DB write simultaneously

### Detection
- Redis connection errors from Booking Service

### Recovery
**Why we're still safe:**
- DB-level UNIQUE constraint on `(listing_id, date)` is the last line of defense
- Even without Redis hold, the `INSERT INTO availability … ON CONFLICT DO NOTHING` at DB level will fail for the second concurrent request
- The loser gets a conflict response → returns "dates unavailable"

**Steps after Redis recovery:**
1. All in-flight holds were lost — send advisory email: "Your hold expired, rebook now"
2. Redis restarts cold (no data) — no stale holds linger
3. Booking service retries connecting to Redis (exponential backoff)

**What if Redis is down for 30+ minutes?**
- Skip hold step; go directly to DB transaction with row-level lock
- Slightly worse UX (no "hold while paying" guarantee) but no incorrect behavior

**Prevention:**
- Redis Sentinel (3-node: 1 primary + 2 replicas + Sentinel) or Redis Cluster
- Persistence: RDB snapshot every 60s + AOF for hold keys (though holds are ephemeral — acceptable to lose)
- If using ElastiCache: Multi-AZ with automatic failover (~30s failover time)

---

## Case 4: Postgres (Bookings DB) Fails

### Scenario
Primary Postgres instance crashes mid-transaction.

### Impact
- Booking in progress is rolled back (ACID guarantee) — no phantom booking
- New booking attempts fail until failover completes

### Detection
- Booking service connection errors, 503s spike

### Recovery
**Failover (RDS Multi-AZ):**
- RDS automatically promotes standby → typically 60–120s
- DNS endpoint is same; applications reconnect automatically after retry
- In-flight transactions at crash moment: rolled back cleanly

**Read replicas:**
- Search for "my bookings" → hits read replica (acceptable lag)
- Booking writes → always primary only

**What if primary and standby both fail?**
- Restore from latest RDS automated snapshot (< 5-minute RPO with continuous backup)
- Restore Point-in-Time to last consistent state
- RTO: ~15-30 min

**Prevention:**
- RDS Multi-AZ (synchronous replication — zero data loss)
- Automated backups: 7-day retention
- Weekly restore drills (actually restore to test environment)
- Connection pooling (PgBouncer) to avoid connection thundering herd on failover

---

## Case 5: Stripe (Payment Provider) Fails

### Scenario
Stripe API returns 503 or times out mid-payment during booking.

### Impact
- Guest is charged but booking status unknown
- Or: guest not charged but booking appears confirmed (worst case)

### Detection
- Stripe webhook status vs internal booking status divergence

### Recovery
**Idempotency key saves us:**
```
POST /v1/charges
Idempotency-Key: booking_{booking_id}_{attempt_count}
```
- Retry same request → Stripe returns same result (deduplication)
- Same key = safe to retry on timeout

**State machine for booking:**
```
PENDING_PAYMENT → {PAYMENT_SUCCESS → CONFIRMED} | {PAYMENT_FAILED → CANCELLED}
```
- Background job: every 5 min, scan bookings stuck in PENDING_PAYMENT > 10 min
- Query Stripe API to check actual charge status
- Reconcile: charge succeeded → CONFIRMED; charge absent → CANCELLED

**If Stripe is fully down (rare, but happened in 2022):**
- Display: "Payment processing delayed — your booking is reserved for 15 minutes"
- Queue payment job with Kafka; process when Stripe recovers
- Do NOT release hold during Stripe outage (extend Redis TTL)

**Prevention:**
- Webhook endpoint must be idempotent (same event arrives twice → no double-confirm)
- Always use idempotency keys
- Circuit breaker: if Stripe error rate > 20%, show "try again in 5 min" instead of hammering

---

## Case 6: Kafka (Event Bus) Fails

### Scenario
Kafka cluster is unavailable — notifications and search index sync stop.

### Impact
- Booking confirmation emails not sent
- Search index not updated (new listings / availability changes lag)
- Analytics data temporarily lost

### Detection
- Kafka consumer lag spikes, producer errors

### Recovery
**Critical vs non-critical consumers:**
| Consumer | Critical? | Fallback |
|----------|-----------|---------|
| Notification Service | No | Queue in DB; batch send after recovery |
| Search Index Sync | No | Mark dirty listings; full re-sync after recovery |
| Analytics | No | Accept loss; replay from CDC WAL after recovery |
| Payment reconciliation | Yes | Use Stripe webhook directly (secondary path) |

**Notification fallback:**
```sql
CREATE TABLE notification_outbox (
  id BIGINT PRIMARY KEY,
  booking_id BIGINT,
  type TEXT,
  payload JSONB,
  sent_at TIMESTAMP,  -- NULL = not yet sent
  created_at TIMESTAMP DEFAULT now()
);
-- Booking service writes here synchronously before returning success
-- Background job polls: WHERE sent_at IS NULL → send directly via SES
```

**Prevention:**
- Kafka: 3-broker cluster, replication factor 3, min.insync.replicas=2
- Producer: `acks=all` for notification topics
- Consumer lag monitoring: alert if lag > 10K messages

---

## Case 7: S3 (Image Storage) Fails

### Scenario
S3 returns errors for image reads/writes (extremely rare; 99.999999999% durability but regional outages possible).

### Impact
- Property images don't load → poor UX, lower bookings
- New listing creation fails (can't upload photos)

### Recovery
**Read failures (images not loading):**
- CloudFront has edge cache — cached images continue serving (TTL typically 24h)
- Stale listings still browsable
- After TTL, show placeholder image

**Write failures (host can't upload):**
- Queue upload request, retry when S3 recovers
- Allow listing creation without photos temporarily; flag listing as "photos pending"

**Cross-region replication:**
- S3 CRR (Cross-Region Replication): us-east-1 → us-west-2
- If us-east-1 has outage, CloudFront origin failover → us-west-2 bucket
- Replication lag ~minutes; very recent uploads may not be in secondary

**Prevention:**
- S3 standard (not S3-IA) for active listing images
- Versioning on bucket to recover accidental deletes
- CloudFront origin shield to reduce S3 direct requests

---

## Case 8: Double Booking Under Load

### Scenario
500 concurrent guests attempt to book the same popular listing for the same week (New Year's Eve property).

### Impact
- Without proper locks: multiple bookings created for same dates
- Revenue damage, host reputation, guest compensation

### Detection
- DB constraint violation spike (UNIQUE violation on `availability`)

### Why This Cannot Happen (Defense in Depth):

**Layer 1 — Redis hold (NX):**
- Only 1 request gets `SET hold:{listing}:{dates} NX` → rest immediately see "unavailable"
- Hold is short-lived (10 min) — prevents holding without paying

**Layer 2 — DB row lock (FOR UPDATE):**
- Within the booking transaction, `SELECT … FOR UPDATE` on existing availability rows
- Second transaction waits or detects conflict

**Layer 3 — UNIQUE constraint:**
- Even if layers 1 and 2 somehow both passed: `INSERT INTO availability ON CONFLICT DO NOTHING`
- Returns fewer rows inserted → transaction detects conflict → ROLLBACK

**Layer 4 — Post-check:**
```sql
-- After INSERT, verify all dates were inserted
SELECT COUNT(*) FROM availability
WHERE listing_id = $id AND date BETWEEN $in AND $out
  AND booking_id = $booking_id;
-- If count < expected nights → conflict → ROLLBACK
```

**Result:** Exactly one booking succeeds. All others get "dates unavailable."

---

## Case 9: Search Index Out of Sync with DB

### Scenario
Kafka consumer lag causes OpenSearch to show available listing that is already booked.

### Impact
- Guest sees listing as available → clicks → clicks Book → "Sorry, dates no longer available"
- Frustrating UX but not a data integrity issue (final check is always DB)

### Detection
- Monitor: compare random sample of ES availability vs DB (hourly audit job)
- Alert if divergence > 1%

### Recovery
**Per-listing refresh:**
```
Booking Service → publishes "listing.availability_changed" event → ES sync consumer
→ recompute available_dates[] for next 90 days → update ES document
```

**Full re-sync fallback:**
```bash
# Backfill all listings modified in last 24h
SELECT id FROM listings WHERE updated_at > now() - interval '24 hours'
→ batch update ES (100 listings/batch, 5 workers)
```

**Prevention:**
- Kafka: `acks=all` + `min.insync.replicas=2` ensures event is durable before commit
- Consumer: mark-and-retry on ES update failure (with 3 retries, then DLQ)
- Always show "Confirm availability" button on listing page → DB check before booking page loads

---

## Failure Severity Matrix

| Failure | Severity | Auto-Recovery? | User Impact |
|---------|----------|---------------|-------------|
| OpenSearch down | High | Partial (fallback to PG) | Degraded search |
| Redis hold lost | Medium | Yes (DB constraint saves) | Minor UX — re-enter payment |
| Postgres primary fails | Critical | Yes (RDS failover ~60s) | 1-2 min booking outage |
| Stripe timeout | High | Yes (idempotency + retry) | Delayed confirmation |
| Kafka down | Low | Yes (outbox pattern) | Delayed notifications |
| S3 read failure | Low | Yes (CDN cache) | Images not loading |
| S3 write failure | Medium | Partial (queue retry) | Can't upload new photos |
| API Gateway down | Critical | Yes (multi-AZ ELB) | Full outage if all AZs fail |

---

## Real-World References

- **Airbnb 2018 incident:** Double booking edge case under Black Friday load → led to DB-level constraint addition
- **Stripe 2022 outage:** Caused payment failures across many platforms → idempotency keys allowed safe retry
- **AWS us-east-1 2021:** S3/EC2 outage → importance of cross-region replication and CloudFront edge caching
- **Booking.com:** Uses Cassandra's lightweight transactions (LWT) for atomic availability writes at scale

---

## Related Concepts
- Concept #13: TOCTOU (double booking prevention)
- Concept #22: Seat/Resource Hold Pattern
- Concept #4: Idempotency (Stripe payments)
- Concept #9: Outbox Pattern (Kafka fallback)
- Concept #32: Circuit Breaker (Stripe circuit breaker)
- Concept #59: Fail-Open vs Fail-Closed (availability fallback)
