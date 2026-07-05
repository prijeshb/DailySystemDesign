# Food Delivery — Failure Analysis
**Date:** 2026-06-04

---

## Failure Matrix

For each component: What fails? What's the blast radius? How do we detect, recover, prevent?

---

## 1. Restaurant Service Fails

**Scenario:** Restaurant catalog DB is down or Restaurant Service crashes.

**Blast radius:** Users can't browse restaurants. No new orders possible.

**Detection:** Health check → alert. DB connection pool exhausted → 500s.

**Immediate mitigation:**
- Serve **stale cache** (Redis/CDN) for restaurant list — users see cached data, not error.
- Mark service degraded in API Gateway → return cached response with `X-Stale-Data: true` header.

**Recovery:**
- Auto-restart (K8s pod restart policy).
- DB failover to read replica for reads; writes queue in Kafka until primary recovers.

**Prevention:**
- Circuit breaker (Hystrix/Resilience4j): after 5 failures in 10s, open circuit → return cache.
- Read replicas for restaurant catalog (read-heavy, write-rarely).
- CDN-cache restaurant pages (TTL=60s). CDN serves even if origin is down.

**Edge case — What if cache is also cold (first deploy)?**
- Pre-warm cache on deploy via background job before routing traffic.

---

## 2. Driver Matching Service Fails

**Scenario:** Driver Matching Service crashes mid-assignment.

**Blast radius:** Orders stuck in ACCEPTED state, no driver assigned. Restaurants preparing food with no pickup.

**Detection:** Order in ACCEPTED state > 5min without driver_id → alert.

**Mitigation:**
- Kafka consumer group: another instance picks up `order.accepted` event on restart (at-least-once delivery).
- **Idempotent matching:** Before assigning, check DB: `order.driver_id IS NULL`. Prevents double-assignment on retry.

**What if ALL matching service instances are down?**
- Orders queue in Kafka (durable). When service recovers, process backlog.
- TTL per Kafka message = 30min. After that, auto-cancel order, refund user, notify restaurant to stop prep.

**Edge case — Driver assigned but crashes before accepting:**
- 30s offer timeout. If no ACK → re-run matching → next driver.
- Track "offer sent" in Redis with TTL=30s. On expiry, trigger re-match.

**Prevention:**
- At least 3 replicas. K8s PodDisruptionBudget: always ≥ 2 available.
- Separate matching workers per city/zone → failure in one zone doesn't cascade.

---

## 3. Location Service Fails (Driver Tracking)

**Scenario:** Redis cluster for driver locations becomes unavailable.

**Blast radius:**
- No driver location updates → stale map for users.
- Driver matching falls back to last known location → poor assignment quality.
- Tracking WebSocket can't push updates.

**Detection:** Redis connection errors spike. Driver location age > 10s → alert.

**Mitigation (degraded mode):**
- Driver app buffers location locally; sends burst when connection restores.
- Tracking Service: if no update for 10s → show "Tracking temporarily unavailable" to user.
- Driver matching: use last-known location (stale up to 30s) — still better than nothing.

**Recovery:**
- Redis Cluster with automatic failover (Sentinel or Cluster mode). Typical recovery < 30s.
- Driver app: exponential backoff on reconnect (1s, 2s, 4s... max 30s).

**What if Redis is down during peak?**
- Fallback: write driver locations to Postgres (lower frequency, 10s intervals) as backup store.
- Matching quality degrades but orders still flow.

**Prevention:**
- Redis Cluster (3 primaries, 3 replicas, auto-failover).
- Location data is ephemeral — Redis AOF disabled for location keys (speed over durability). Geo data regenerates from driver pings.

---

## 4. Payment Service Fails

**Scenario A: PSP (Stripe) returns 500 or times out.**

**Blast radius:** Orders can't be placed. Revenue loss + user frustration.

**Mitigation:**
- **Retry with idempotency key** (Stripe-Idempotency-Key header). Safe to retry same charge.
- Max 3 retries with exponential backoff.
- If all retries fail: mark order as PAYMENT_FAILED → notify user → release restaurant slot.

**Scenario B: Payment confirmed by PSP but our system crashes before writing to DB.**

**Blast radius:** User charged, order not created → ghost charge.

**Mitigation:**
- After PSP confirms, write `payment_confirmations` table FIRST, then create order.
- On recovery, scan `payment_confirmations` with no matching order → auto-create or auto-refund.
- PSP webhook for async confirmation (fallback if sync call drops).

**Scenario C: Payment authorized but delivery never happens (restaurant rejects, driver unavailable).**

**Mitigation:**
- Cancel authorized hold within 7 days (Stripe auth validity).
- Order cancellation flow → trigger PSP void/refund within 24h SLA.
- Kafka event `order.cancelled` → Payment Service → refund.

**Prevention:**
- Two PSP integrations (Stripe primary, Razorpay backup) — switch on circuit open.
- Separate payment service with its own DB → failure doesn't cascade to order service.

---

## 5. Order Service Fails

**Scenario:** Order Service crashes mid-order (after payment, before sending to restaurant).

**Blast radius:** User charged, restaurant never notified. Order in limbo.

**Mitigation:**
- Outbox pattern: write order to DB + write to `outbox` table in same transaction.
- Separate Outbox Publisher reads outbox table → publishes to Kafka → marks as published.
- Even if Order Service crashes: outbox row persists. On recovery, re-publish.

```
Transaction:
  INSERT orders (id, status=PLACED, ...)
  INSERT outbox (order_id, event=order.placed, published=false)
COMMIT

Outbox Publisher (separate process):
  SELECT * FROM outbox WHERE published=false
  FOR EACH: publish to Kafka
  UPDATE outbox SET published=true
```

**Recovery:**
- K8s restarts pod. Outbox Publisher catches up on any unpublished events.
- Idempotent restaurant notification: check if already notified before sending again.

**What if Order Service crashes DURING a transaction?**
- Postgres ACID: transaction rolls back. Order not created, payment not captured (only authorized). Auth auto-voids after 7 days.

---

## 6. Kafka Fails

**Scenario:** Kafka cluster unavailable.

**Blast radius:** All async communication breaks. Orders queue up, notifications stop, driver matching stops.

**Mitigation:**
- Kafka replication factor = 3. Min in-sync replicas = 2. Leader election within seconds.
- Producers: retry with backoff. If Kafka unreachable for 10s → write to fallback queue (DB table).
- Consumers: pause, retry, resume when Kafka recovers.

**Fallback for critical path (order placement):**
- Order Service → direct HTTP call to Restaurant Service as fallback (synchronous, slower, but works).
- Trade-off: loses decoupling, increases coupling, but order goes through.

**Prevention:**
- 3 Kafka brokers across 3 AZs.
- Topic replication = 3.
- Monitor consumer lag — alert if lag > 10K messages.

---

## 7. Driver App Network Disconnect

**Scenario:** Driver loses connectivity for 2-3 minutes (tunnel, dead zone).

**Blast radius:** Location updates stop. Tracking breaks for user. Order system can't confirm delivery.

**Mitigation:**
- Driver app: buffer all location updates locally during disconnect.
- On reconnect: flush buffered locations with original timestamps.
- If disconnect > 5min: order system marks driver as "unreachable" → dispatch alternative driver offer.
- Driver confirms delivery via SMS fallback (USSD) if app can't connect.

**What if driver delivers but never marks as delivered?**
- Geo-fence trigger: driver's last known location == delivery address → auto-confirm after 5min.
- SMS confirmation: send link to driver for offline confirmation.

---

## 8. Restaurant Rejects Order (Late Failure)

**Scenario:** Order placed and paid, then restaurant rejects (too busy, item out of stock).

**Blast radius:** User charged, food not coming.

**Mitigation:**
- Immediate full refund trigger (Stripe void or refund).
- Offer alternative restaurant (if same item available nearby).
- Push notification with apology + credit (₹50 coupon).

**Prevention:**
- Restaurants set capacity limit (max concurrent orders). Order Service enforces via counter in Redis.
- Menu item real-time availability: restaurant marks items unavailable → cache invalidation → users can't order that item.

---

## 9. Cascading Failure (Friday 8pm — Peak Traffic)

**Scenario:** Traffic spike → Restaurant Service slow → timeout → API Gateway retries → 3x traffic → DB overwhelmed → full outage.

**Prevention (Defense in depth):**
1. **Rate limiting** at API Gateway (per user, per endpoint).
2. **Circuit breaker** between services — fail fast, don't let slow service cascade.
3. **Timeout budgets** — Restaurant Service call: 200ms timeout. Never wait more.
4. **Load shedding** — reject non-critical requests (browse history, reviews) under high load. Protect order flow.
5. **Horizontal auto-scaling** — HPA in K8s scales pods when CPU > 70%.
6. **Chaos engineering** — Regular GameDay drills to test failure modes before they hit production.

---

## Failure Mode Quick Reference

| Component | Failure | User Impact | Recovery Time | Mitigation |
|-----------|---------|-------------|---------------|------------|
| Restaurant Service | Down | Can't browse | <30s (cache) | Stale cache serve |
| Driver Matching | Down | No driver assigned | <2min (Kafka retry) | Idempotent re-match |
| Location/Redis | Down | No tracking | <30s (failover) | Last-known location |
| Payment/PSP | Timeout | Can't place order | 3 retries ~10s | Idempotency + retry |
| Order Service | Crash | Order in limbo | <60s (outbox) | Outbox pattern |
| Kafka | Partial fail | Delayed events | <10s (failover) | 3-broker cluster |
| Driver disconnects | Network | Stale tracking | Auto | Geofence confirm |
| Restaurant rejects | Business | No food | Immediate | Instant refund + credit |
