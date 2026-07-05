# Food Delivery System — Interview Q&A
**Date:** 2026-06-04

---

## How Interviewers Open This Problem

> "Design a food delivery app like DoorDash."

Or more specific hooks:
> - "How would you handle driver assignment at scale?"
> - "Design the real-time order tracking feature."
> - "How do you ensure a payment is never double-charged?"

---

## Round 1: Scoping (First 5 minutes)

**Q: Before you dive in, what clarifying questions would you ask?**

A: I'd ask:
1. Scale — how many cities, daily orders? (determines if one DB is enough or need sharding)
2. Who are the actors? Just customers + restaurants, or also drivers in the same system?
3. Any specific feature to deep-dive? (real-time tracking, payment, driver matching?)
4. Consistency requirements — is it okay if a user briefly sees an out-of-stock item?
5. Latency targets — order placement SLA?

---

## Round 2: Core Design Questions

**Q: How do you find nearby restaurants for a user?**

A: Three options: PostGIS `ST_DWithin`, Geohash prefix search in Redis, or S2 geometry cells.

For an interview: **PostGIS + Redis cache.** PostGIS for correctness, Redis Geohash for caching results per zone (60s TTL). At DoorDash scale, they use S2 cells — but PostGIS is correct and interview-friendly.

Mention the edge case: user at cell boundary needs to check neighboring cells too.

---

**Q: How does driver matching work?**

A: When order is accepted by restaurant:
1. Query Redis GEORADIUS around restaurant location (3km, sorted by distance).
2. Filter: `status == AVAILABLE`.
3. Score = `ETA to restaurant` (accounting for remaining prep time).
4. Offer to top driver, 30s timeout, if declined → next.

Follow-up they'll ask: **"What if no drivers are available?"**
- Expand search radius (3km → 5km → 8km).
- Show user estimated wait time honestly.
- Surge pricing to incentivize offline drivers to come online.

---

**Q: How do you implement real-time order tracking?**

A: WebSocket between driver app and Location Service. Driver sends location every 3 seconds.

Server-side:
- Location Service writes to Redis (`GEOADD`) and publishes to Redis pub/sub channel `order:{order_id}`.
- Tracking Service subscribes, pushes to user's WebSocket.

Follow-up: **"Why not polling?"**
- 5M active orders × poll every 5s = 1M RPS just for tracking. WebSocket = persistent connection, server pushes only on update.

Follow-up: **"What if WebSocket drops?"**
- Graceful degradation: SSE → long-polling.
- Client reconnects with exponential backoff, resumes last known state.

---

**Q: How do you prevent double-charging a user?**

A: Idempotency keys.

1. Client generates a unique key per order attempt (UUID, stored client-side).
2. On payment request: check if key exists in DB → return cached result.
3. If new: call PSP with `Stripe-Idempotency-Key` header.
4. Persist `(idempotency_key, result)` before returning to client.

Result: Even if client retries 10 times, PSP sees same key → executes once.

Follow-up: **"What if our system crashes after PSP charges but before we write to DB?"**
- PSP sends webhook. We reconcile on recovery: find payments with no order → create order or refund.
- Outbox pattern: write payment intent + order in one DB transaction.

---

**Q: Walk me through what happens from "user taps Order" to "food delivered."**

Good answer (show the state machine):

1. `CART → PLACED`: Order + payment auth in one transaction. Publish `order.placed` to Kafka.
2. Restaurant Service consumes → push notification to restaurant tablet.
3. Restaurant taps Accept → `PLACED → ACCEPTED`. Publish `order.accepted`.
4. Driver Matching consumes → finds nearest driver → `ACCEPTED → DRIVER_ASSIGNED`.
5. Driver picks up → `DRIVER_ASSIGNED → PICKED_UP`. Location updates stream to user.
6. Driver delivers → `PICKED_UP → DELIVERED`. Payment captured. Notifications sent.
7. Auto-confirm after 2min → `DELIVERED → COMPLETED`.

---

**Q: How do you handle an item going out of stock after the user sees the menu?**

A: Two-phase check:

1. Menu cache (Redis) has `available: true/false` per item. Users see this on browse.
2. At order placement: re-check availability against DB (not cache). If unavailable → reject with "item no longer available."

Restaurant can also mark items unavailable in real-time → cache invalidation via pub/sub.

---

**Q: How would you handle a surge on Friday evening (10x normal traffic)?**

A: Layered defense:
1. **CDN** absorbs static content + cached restaurant pages.
2. **Rate limiting** per user at API Gateway.
3. **Auto-scaling** (HPA) — pods scale with CPU/RPS.
4. **Circuit breakers** — fail fast, don't let one slow service cascade.
5. **Load shedding** — deprioritize non-critical reads (order history, reviews) under load.
6. **Queue** incoming orders in Kafka — process at steady rate, don't overwhelm downstream.

---

## Round 3: Deep-Dive Follow-ups

**Q: How would you shard the orders database as it grows?**

A: Shard by `user_id` (consistent with how users query their orders) or by `restaurant_id` (for restaurant dashboard). But order queries need both…

Better: Shard by `user_id`. For restaurant queries (different access pattern), use a separate read replica or Elasticsearch for restaurant-side analytics.

At extreme scale: Cassandra or DynamoDB with `user_id` as partition key, `order_id` as sort key.

---

**Q: How do you calculate ETA for delivery?**

A: Three components:
1. `restaurant_prep_time`: restaurant provides estimate, system learns via moving average.
2. `driver_to_restaurant`: Google Maps / Mapbox API for real road distance.
3. `restaurant_to_customer`: same routing API.

ETA = current_time + max(prep_time_remaining, driver_travel_to_restaurant) + driver_to_customer

Update ETA live as driver moves. If driver deviates from route or traffic changes → recalculate.

---

**Q: How does the system handle restaurant cancelling mid-preparation?**

A:
1. Restaurant sends cancel event → Order Service: `ACCEPTED → CANCELLED`.
2. Kafka: `order.cancelled` event.
3. Payment Service: void authorization (no charge).
4. Notification Service: push to user ("Restaurant cancelled. Full refund in 3-5 days.").
5. Loyalty Service: issue credit/coupon.

If driver already dispatched and en route:
- Notify driver to abort pickup.
- Mark driver back as AVAILABLE.

---

**Q: Design the restaurant-side tablet app flow.**

A: The tablet is a thin client. Order Service pushes via WebSocket or push notification.

States the restaurant sees:
- `New Order` → Accept / Reject (60s timeout, auto-accept or auto-reject)
- `Preparing` → tap when ready
- `Ready for pickup` → driver notified

The Accept must be transactional: `UPDATE orders SET status='ACCEPTED', accepted_at=now() WHERE id=$1 AND status='PLACED'` — prevents race condition if same order somehow duplicated.

---

**Q: How do you ensure drivers aren't assigned multiple orders simultaneously?**

A: Driver status as distributed lock:

```
When assigning:
  SET driver:{driver_id}:status BUSY NX EX 3600
  -- NX = only set if not exists (atomic check-and-set)
  -- If returns OK → assignment succeeded
  -- If returns nil → driver already assigned, pick next
```

On delivery complete: SET status back to AVAILABLE. On timeout (no heartbeat for 5min): auto-release to AVAILABLE.

---

## Common Mistakes to Avoid

| Mistake | Better Approach |
|---------|----------------|
| Using DB for driver location polling | Redis GEOADD — in-memory, geo-native |
| Synchronous order-to-restaurant notification | Kafka async — restaurant service can be slow/down |
| No idempotency on payment retry | Always use idempotency keys |
| Single monolithic "order" service | Separate concerns: order, payment, matching, tracking |
| Forgetting the cancel/refund flow | State machine must have explicit cancel transitions |
| Ignoring driver disconnect | Geofence auto-confirm + SMS fallback |
| Not mentioning load at scale | Always estimate: X orders/s, Y drivers, Z location updates |

---

## Key Numbers to Remember

| Metric | Value |
|--------|-------|
| Driver location update frequency | Every 3s |
| Driver matching radius (initial) | 3km |
| Driver offer timeout | 30s |
| Menu cache TTL | 5min |
| Restaurant list cache TTL | 60s |
| Payment auth validity (Stripe) | 7 days |
| Order auto-confirm after delivery | 2-5min |
| WebSocket ping interval | 30s (keep-alive) |
