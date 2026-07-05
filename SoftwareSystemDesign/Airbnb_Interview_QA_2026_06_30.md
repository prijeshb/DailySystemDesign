# Airbnb — Interview Q&A
*Date: 2026-06-30*

---

## How Interviewers Open This Problem

> "Design Airbnb" or "Design a vacation rental marketplace" or  
> "Design a property booking system like Booking.com"

**Clarifying questions you should ask first:**
1. "Should I focus on the search + booking core, or also host onboarding and reviews?"
2. "Are we building for global scale or a specific region?"
3. "Is instant booking required, or host-approval flow?"
4. "Any budget or infrastructure constraints?"

---

## Round 1: Core Design Questions

**Q1: How do you prevent double bookings?**

> Expected: Multi-layer defense

"First, Redis NX hold gives us optimistic locking at the application layer — only one request gets the hold. Then within the booking transaction, we do a `SELECT … FOR UPDATE` on existing availability rows to detect conflicts at the DB layer. Finally, a UNIQUE constraint on `(listing_id, date)` is the last line of defense. If the INSERT returns fewer rows than expected, we rollback. Three layers means we never get a double booking."

Follow-up: *"What if Redis is down?"*

"Redis is an optimization, not a correctness guarantee. If Redis is down, we skip the hold step and go directly to the DB transaction. The DB constraints ensure exactly one booking succeeds."

---

**Q2: How would you model availability — 2.5 billion rows seems like a lot?**

> Expected: Sparse row model or date-range model

"Storing a row per (listing, date) for all possible dates is too expensive. Instead I use a sparse model — AVAILABLE is the absence of a row. We only store BLOCKED and BOOKED rows. For 7M listings at realistic occupancy (~30%), that's ~750M rows at peak season, indexed on `(listing_id, date)`. Alternatively, use a date-range model — store contiguous blocked/booked ranges as `(start_date, end_date)`. Fewer rows, but overlap detection requires a range query with `&&` on Postgres daterange type."

---

**Q3: How does your search handle date availability filtering?**

> Expected: Pre-computed field in ES + DB validation at booking

"OpenSearch indexes an `available_dates` field: an array of available dates for the next 90 days, pre-computed and updated async via Kafka. Search query uses a `terms` filter on this array. The tradeoff: ~1s lag between a booking and ES update — so search might briefly show a just-booked listing as available. That's acceptable because the final availability check always happens against Postgres before the booking is created."

Follow-up: *"What if the Kafka consumer is behind?"*

"We show a stale result in search — worst case, user clicks on it and sees 'dates unavailable' at booking time. We make this clear with 'Check availability' language rather than '100% available.' For high-demand listings, we could add a real-time pre-check before showing the 'Book Now' button."

---

**Q4: Walk me through the booking state machine.**

> Expected: Full status flow

```
INITIATED → HOLD_ACQUIRED → PENDING_PAYMENT → CONFIRMED
                                           → PAYMENT_FAILED → CANCELLED
CONFIRMED → CANCELLED_BY_GUEST
CONFIRMED → CANCELLED_BY_HOST
CONFIRMED → COMPLETED (after checkout)
```

"Each transition is logged. A background job scans for bookings stuck in PENDING_PAYMENT > 10 minutes and queries Stripe to reconcile. This handles the case where Stripe succeeded but we crashed before updating our DB."

---

**Q5: How do you handle dynamic pricing?**

> Expected: Rule-based at quote time, pre-computed for display

"Search displays a `price_from` (cheapest nightly rate in next 30 days). When a user selects specific dates, we compute exact price in real-time: base rate + matching price rules (weekend premium, seasonal, long-stay discount, early-bird) applied in priority order, then add cleaning fee, service fee (14%), and taxes. We don't pre-compute all price permutations — too many combinations. But we cache the result per `(listing_id, check_in, check_out)` in Redis with 5-min TTL."

---

## Round 2: Deep-Dive Follow-ups

**Q6: How do you scale search to 250 RPS?**

"OpenSearch with 3 nodes handles this comfortably — ES can do 5,000+ search RPS per node. The heavier work is the geo-distance computation at query time. We pre-index listings with `geo_point` type and use `geo_distance` filter — ES does this in native binary; very fast. We also cache popular search queries (same location + same date range hit by many users) in Redis with 30s TTL."

Follow-up: *"What about a new city launch — the index for that city is cold, no cache hits."*

"Pre-warm: when we launch in a new city, we seed the search cache with a script that fires the top 50 date combinations for that city. We also set OpenSearch replica count higher temporarily to handle the launch spike, then scale back."

---

**Q7: How would you handle the calendar sync feature (hosts syncing from iCal/VRBO/other platforms)?**

"Hosts often list on multiple platforms and need to avoid double bookings across them. We support iCal import: the host provides an iCal URL from another platform. A background job fetches and parses the iCal every 15-30 minutes, converts VEVENT blocks to blocked date ranges, and upserts into our `availability` table. The tricky part: iCal is eventually consistent — there's a window where both platforms could accept a booking. We mitigate by syncing more frequently for active listings (every 5 min) and by warning hosts about the sync lag."

---

**Q8: A host has 100+ properties (property management company). How do you handle their scale?**

"For bulk operations — blocking 100 listings for the same date range — naive single-row operations won't work. We need a batch API:
```
POST /api/listings/bulk-availability
{ listing_ids: [...], start: "2026-07-01", end: "2026-07-10", status: "BLOCKED" }
```
This runs as a single transaction or a Kafka-batched job. For search, we might partition by host_id to colocate their listings on the same Postgres shard, so bulk queries stay local."

---

**Q9: How do you handle the review system — specifically, preventing fake/biased reviews?**

"Key constraints: only guests who completed a stay can review. Reviews are mutual (host reviews guest, guest reviews host) and both are hidden until either both submit or 14 days pass. This prevents strategic revenge reviewing. For spam: content goes through a classifier before publishing. For rating inflation: we show both average and distribution, and use Bayesian smoothing on listings with < 5 reviews to prevent new listings from gaming with 1 five-star review."

---

**Q10: How does your system handle the peak load of a major holiday (New Year's Eve)?**

"Several mechanisms:
1. **Virtual waiting queue** (Concept #21): For highly sought listings, instead of all users hammering the booking API, we issue queue tokens. Users get a position and a callback when it's their turn.
2. **Search cache aggressiveness**: increase TTL from 30s to 5 min during peak — slightly stale results but much less DB load.
3. **Read replicas**: bump read replica count from 2 to 5 via RDS auto-scaling. All read operations (check booking, search my trips) go to replicas.
4. **Async payment**: for peak, accept booking immediately (hold secured), queue Stripe charge, confirm asynchronously. Guest sees 'Booking in progress' state for up to 5 min.
5. **Pre-scale**: CloudWatch schedule-based scaling — add Search API instances at 6 PM on Dec 26 (when NYE searches spike)."

---

## Round 3: System Trade-off Questions

**Q11: Why Postgres and not Cassandra for bookings?**

"Bookings require strong consistency and multi-row transactions — exactly what Cassandra is bad at. We need: check availability + insert booking + insert availability rows atomically. Cassandra's lightweight transactions (LWT) are available but slow and limited to single partition. Postgres gives us ACID, row-level locking, and complex queries. The volume (500K bookings/day = ~6 writes/second) is well within Postgres's range. We'd switch to Cassandra only if we hit >100K writes/second, which Airbnb's actual scale doesn't require for bookings."

---

**Q12: Why OpenSearch and not Postgres full-text search for property search?**

"Postgres FTS (pg_trgm) works at small scale but lacks native geo-distance search, faceted aggregations (count by amenity), and relevance scoring at 7M documents. OpenSearch handles all three natively and is horizontally scalable. The cost is eventual consistency (1s lag). At startup scale (<500K listings), pg_trgm + PostGIS is a valid choice — saves $540/month in infra."

---

**Q13: What's your biggest failure mode?**

"The availability hold in Redis is ephemeral — if Redis goes down during a user's payment flow, the hold is lost. Another user could potentially race to the same dates. Our DB constraint prevents actual double booking, but the second user now experiences a confusing 'dates unavailable' error after entering payment info. Fix: make the hold TTL slightly longer (15 min) and give a grace period by re-checking availability after Redis restarts before sending the 'unavailable' error. Longer term: explore using Postgres advisory locks as a fallback for the hold mechanism."

---

## Common Mistakes Candidates Make

| Mistake | Better Answer |
|---------|--------------|
| Using Redis as the only double-booking guard | Explain DB constraint as last defense |
| Storing all (listing, date) combinations | Sparse row model or date ranges |
| Real-time availability filter in search | Pre-computed 90-day window in ES |
| Not mentioning idempotency for payments | Stripe idempotency key + reconciliation job |
| Making OpenSearch the source of truth | It's an index; Postgres is source of truth |
| Ignoring the iCal sync use case | Multi-platform hosts are a real product need |

---

## Estimation Questions

**Q: How many rows in the availability table at full scale?**
```
7M listings × avg 30% occupancy × 365 days
= 7,000,000 × 0.30 × 365
= ~767M rows

Plus host-blocked dates (assume 20%):
= 767M + (7M × 0.20 × 365) = ~1.28B rows

Sparse model: with Postgres partitioning by listing_id range, manageable.
Index size: ~60 bytes/row × 1.3B = ~78GB (fits on modern SSD)
```

**Q: How much bandwidth for image serving?**
```
5M searches/day × 20 listings shown × 1 thumbnail (50KB each)
= 5M × 20 × 50KB = 5TB/day in thumbnail traffic

CloudFront CDN: 5TB × $0.085/GB = $425/month
Origin (S3): cache hit ratio ~95% → only 250GB/day to origin = ~$6/month
```

---

## Related Concepts
- Concept #1: Geo Search
- Concept #4: Idempotency
- Concept #8: Caching Patterns
- Concept #10: State Machines
- Concept #13: TOCTOU
- Concept #21: Virtual Waiting Queue
- Concept #22: Seat/Resource Hold Pattern
- Concept #29: Database Replication
