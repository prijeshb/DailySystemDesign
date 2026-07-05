# Ride-Sharing System — Failure Analysis
*Date: 2026-06-01*

---

## Failure-First Approach

For each component: What if **it** fails? What if the component **before** it fails? What if the component **after** it fails?

---

## 1. Location Service Fails

**Symptom**: Driver positions go stale. Matching uses wrong locations.

**What if Location Service crashes?**
- Redis TTL (10s) auto-expires driver entries → drivers appear offline
- Active trips: rider loses live tracking
- Fix: Rider app falls back to polling ETA endpoint every 5s
- Driver reconnects to Location Service (auto-reconnect in app)

**What if Redis (location store) fails?**
- All drivers disappear from matching pool → no rides can be matched
- Prevention: Redis Sentinel / Redis Cluster with replicas
- Fallback: Route location writes to PostgreSQL temporarily (slower but functional)
- Recovery: Redis restores from replica; drivers re-register on next heartbeat (within 3s)

**What if driver app crashes mid-trip?**
- Last known location cached in Redis (10s TTL)
- Trip continues; rider sees last known position
- Driver reconnects → location resumes
- If driver offline >60s → Trip Service flags trip for manual review

---

## 2. Matching Service Fails

**Symptom**: Riders request rides but never get assigned a driver.

**What if Matching Service crashes?**
- Ride requests queue in Kafka (topic: `ride-requests`)
- Standby Matching Service instance picks up from Kafka offset
- Max delay = restart time (~30s) + queue drain
- Idempotency: trip_id in each message → duplicate processing safe (upsert)

**What if driver doesn't respond to offer (timeout)?**
- 10s timeout → move to next driver
- After 3 drivers decline → notify rider "searching..." (extend radius)
- After 5 min no match → "No drivers available" + suggest retry

**What if offer sent but driver accepts after trip assigned elsewhere?**
- Trip already in `DRIVER_ASSIGNED` state → reject late accept
- Return 409 Conflict to driver app → driver app shows "trip no longer available"

---

## 3. Trip Service / PostgreSQL Fails

**Symptom**: Trip state can't be read or written. Payments may duplicate.

**What if PostgreSQL primary fails?**
- Automatic failover to read replica (promoted to primary) via Patroni/pgBouncer
- RTO: ~30s, RPO: last WAL segment (~1s of data)
- In-flight trips: state preserved in Kafka events (replay to rebuild)

**What if trip state update fails mid-trip?**
- Kafka event for state change published first (outbox pattern)
- DB write retried; if still fails → dead-letter queue → manual resolution
- Idempotency key on all state transitions prevents double-transition

**What if payment fails at trip completion?**
- Trip marked `COMPLETED` in DB regardless
- Payment retried async (up to 3x with exponential backoff)
- If all retries fail → driver still paid (Uber absorbs risk), rider charged later or banned
- Separate `payment_status` column decoupled from `trip_status`

---

## 4. Surge Pricing Service Fails

**Symptom**: Surge multiplier not updated. Riders undercharged or always paying surge.

**What if Surge Service crashes?**
- Last computed multiplier stays in Redis (doesn't expire — safe default)
- New trips priced at stale multiplier (acceptable for <5 min outage)
- Fallback: default multiplier = 1.0 (no surge) — favors riders

**What if Redis surge cache is wiped?**
- Surge Service recomputes from Kafka (demand/supply events from last 5 min)
- Cold start: ~30s to rebuild
- Default to 1.0 during cold start

---

## 5. Notification Service Fails

**Symptom**: Drivers don't receive trip offers. Riders don't get updates.

**What if push notification (FCM/APNs) fails?**
- Retry 3x with backoff
- Fallback: SMS via Twilio for critical events (trip assigned, driver arrived)
- Driver also polls `/trips/pending-offers` every 5s as safety net

**What if WebSocket connection drops (rider)?**
- Client auto-reconnects with exponential backoff
- On reconnect: fetch current trip state via REST (`GET /trips/{id}`)
- No state lost — DB is source of truth

---

## 6. Geospatial / Sharding Failures

**What if a Redis geo-shard goes down?**
- Drivers in that geographic region appear unavailable
- Matching Service detects empty result → expands radius to adjacent shards
- Redis Cluster automatically reroutes to replica shard

**What if geohash boundary causes matching miss?**
- Driver 100m away but in different geohash cell → not returned in radius query
- Fix: Query radius always slightly larger than actual radius needed (buffer 10%)
- GEORADIUSBYMEMBER handles this correctly (true circle, not cell-based)

---

## 7. Cascading Failure Scenario

**Scenario**: Location Service overwhelmed at peak hour (NYE, stadium event)

```
High location write volume
    → Redis CPU spikes
    → Location queries slow
    → Matching Service waits longer per request
    → Matching Service queue backs up
    → Ride requests timeout
    → Riders retry → more requests
    → Full cascade failure
```

**Prevention**:
1. **Rate limit** driver location updates: 1 update/3s (already designed)
2. **Circuit breaker** in Matching Service: if Location Service P99 > 500ms → use cached snapshot (5s old)
3. **Geohash-based sharding** of Redis: peak events don't overload single node
4. **Load shedding**: during extreme load, return "High demand, try again in 2 min" for new requests
5. **Pre-warming**: for known events (concerts, sports), pre-position drivers via incentives

---

## Idempotency Patterns Used

| Operation | Idempotency Key | Method |
|-----------|----------------|--------|
| Trip request | `rider_id + timestamp bucket (1min)` | Dedupe in Redis for 5min |
| Driver location update | `driver_id` | Upsert in Redis |
| Payment charge | `trip_id` | Stripe idempotency key |
| State transition | `trip_id + from_state + to_state` | DB unique constraint |
| Push notification | `trip_id + event_type` | Dedupe before send |
