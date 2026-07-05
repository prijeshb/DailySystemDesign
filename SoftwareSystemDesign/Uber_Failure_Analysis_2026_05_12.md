# Ride-Sharing System — Failure Analysis
**Date:** 2026-05-12 | **System:** Uber/Lyft-style Ride-Sharing

> **Philosophy:** Assume everything will fail. Design for graceful degradation, not just happy-path reliability. Ask: *"What breaks if this component goes down, and what is the user experience?"*

---

## Failure Map Overview

```
Client Apps
    │
API Gateway ─────────────── [F1] Gateway down
    │
    ├── Trip Service ──────── [F2] Trip Service crash
    │       │
    │       └── Trip DB ───── [F3] DB overload / shard failure
    │
    ├── Location Service ──── [F4] Location writes failing
    │       │
    │       └── Redis Geo ─── [F5] Redis cluster down
    │
    ├── Dispatch Service ──── [F6] Matching engine down
    │
    ├── WebSocket Hub ──────── [F7] WS server crash mid-ride
    │
    ├── Kafka ──────────────── [F8] Kafka partition lag / broker down
    │
    ├── Payment Service ────── [F9] Payment failure
    │
    └── Notification Svc ──── [F10] Notification delivery failure

External Dependencies
    ├── Maps/ETA API ───────── [F11] Google Maps / OSRM down
    ├── Payment Gateway ────── [F12] Stripe / Braintree down
    └── Push Provider ──────── [F13] APNS / FCM down
```

---

## F1: API Gateway / Load Balancer Failure

### Scenario
Primary API gateway instance crashes or becomes network-partitioned.

### Impact
- **All clients:** 100% of requests fail — riders can't request rides, drivers can't send heartbeats
- **In-progress rides:** Not directly affected (WebSocket connections held by WS Hub, not gateway) but location updates stop
- **Business:** Complete outage

### Detection
- Health check probes from monitoring (Datadog/Prometheus) fail
- Client-side error rate spikes to 100% within seconds

### Prevention & Mitigation
1. **Multi-instance deployment:** 3+ gateway instances across AZs (Active-Active)
2. **DNS failover:** Route 53 health checks → reroute within 30s
3. **Circuit breaker at client:** Mobile SDK retries with exponential backoff (1s, 2s, 4s, max 30s)
4. **Global Load Balancing:** Anycast routing (Cloudflare) — traffic automatically shifts to nearest healthy PoP

### Recovery
- Auto-scaling group spins up replacement within 60–90s
- In-progress rides reconnect via WebSocket heartbeat and reconnection logic

### Residual Risk
Brief (30–90s) hard outage for new requests. Acceptable given multi-AZ setup.

---

## F2: Trip Service Crash

### Scenario
The Trip Service pod/instance crashes after receiving a ride request but before committing the trip to DB.

### Impact
- Rider gets a timeout / error, but no trip record was created → retry is safe
- If crash after DB write but before Kafka publish → **inconsistency risk**

### The Dangerous Sub-case: Write to DB succeeds, Kafka publish fails
```
Trip Service
  1. INSERT trip (status=REQUESTED) → ✅ committed to DB
  2. Publish to Kafka "trip-events"  → ❌ crash before publish

Result: Trip exists in DB (status=REQUESTED forever), but Dispatch never gets triggered.
Rider sees "Finding driver..." indefinitely.
```

### Prevention
**Transactional Outbox Pattern:**
```sql
-- In same DB transaction:
BEGIN;
  INSERT INTO trips (...) VALUES (...);
  INSERT INTO outbox (topic, payload) VALUES ('trip-events', '{"trip_id": ...}');
COMMIT;
```
A separate **Outbox Relay** process continuously polls `outbox` table and publishes to Kafka, then marks as published. This decouples the DB commit from Kafka publish — if the app crashes, the relay still publishes when it recovers.

### Additional Mitigations
1. **Idempotent retries:** Trip creation is idempotent — `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`; rider SDK includes a UUID per request
2. **Multiple instances:** k8s runs 5+ replicas; if one crashes, others handle requests
3. **Trip timeout job:** Cron job every 60s sets trips stuck in REQUESTED for > 5 min to CANCELLED

---

## F3: Trip Database Failure (Primary Shard Down)

### Scenario
PostgreSQL primary node for one geographic shard fails.

### Impact
- All trip writes and reads for cities on that shard fail
- Rides in that shard are stuck — state changes (start, complete) can't be committed

### Prevention & Mitigation

**Replica Promotion (Automatic Failover):**
- Use **Patroni** (PostgreSQL HA) with automatic leader election via etcd/Consul
- Replica promotion in 15–30 seconds
- Application uses connection pooler (PgBouncer) that points to current primary

**Read Replicas for Non-Critical Reads:**
```
Write path: always → Primary
Read path:
  - Current trip status → Primary (needs strong consistency)
  - Historical trips / receipts → Read Replica (stale OK)
  - Analytics queries → Read Replica (dedicated)
```

**Multi-AZ Synchronous Replica:**
- At minimum one synchronous replica in different AZ → RPO = 0 (no data loss)
- Additional async replicas for reads/analytics

**Graceful Degradation During Failover (30s window):**
- Trip writes queue in Redis (short TTL) and are flushed post-recovery
- Drivers can still send location updates (Redis, unaffected)
- Existing rides degrade to "best-effort" state — client shows last known status

### Recovery
- Patroni elects new primary in <30s
- PgBouncer reconnects automatically
- Outbox relay catches up on any missed Kafka publishes

---

## F4: Location Service Failure

### Scenario
Location Service instances crash — driver heartbeats can't be processed.

### Impact
- **Driver location index:** Stale — drivers appear to be where they were before the outage
- **Active rides:** Riders don't see driver moving; ETA incorrect
- **Matching:** Dispatch uses stale geo data → may offer rides to drivers who have moved

### The Domino Effect
```
Location Service down (5 minutes)
  → Redis Geo not updated
  → Dispatch queries stale driver positions
  → Driver offered who is actually 20km away (was moved pre-outage)
  → Driver declines (long ETA) → matching cascade delays
  → Rider waits 5+ minutes → cancels → surge
```

### Prevention
1. **Multiple instances + k8s readiness probes:** Request only routed to healthy pods
2. **Client-side retry with jitter:** Driver SDK retries failed heartbeat within 10s
3. **Kafka buffer for location updates:** Instead of direct write, driver SDK publishes to Kafka; Location Service consumes. If service restarts, it replays unprocessed messages from Kafka.

**Kafka-buffered Location Architecture (preferred for scale):**
```
Driver App → Kafka [location-raw] → Location Service → Redis Geo
```
If Location Service crashes:
- Driver updates accumulate in Kafka (retained for 1 hour)
- On restart, Location Service processes backlog → Redis restored within seconds
- Dispatch can pause matching until index is fresh, or use ETA-based fallback

4. **Staleness TTL:** Each driver entry in Redis has TTL of 30s. If no update received, driver auto-removed from available pool → Dispatch won't offer stale drivers.

### Staleness Detection
```python
# Location Service on each update:
redis.geoadd("drivers:available", lng, lat, driver_id)
redis.set(f"driver:{driver_id}:last_seen", time.time(), ex=30)

# Dispatch before offering:
last_seen = redis.get(f"driver:{driver_id}:last_seen")
if not last_seen or (time.time() - float(last_seen)) > 20:
    skip_driver()  # too stale
```

---

## F5: Redis Cluster Failure (Geo Index Down)

### Scenario
Redis cluster (holding driver locations) loses quorum or corrupts data.

### Impact
- **Matching completely broken:** Dispatch can't find nearby drivers
- **Active ride tracking:** Location streaming stops
- **New requests:** All go unmatched

### This is a Critical Single Point of Failure.

### Mitigation: Multi-Layer Defense

**1. Redis Sentinel / Redis Cluster with Replication:**
- 3-node Redis Cluster (each with 1 primary + 1 replica)
- Automatic failover on primary loss via Raft consensus
- Replica lag < 100ms → minimal data loss

**2. Standby Redis in separate AZ:**
- Hot standby receives asynchronous replication
- On primary cluster failure: DNS switch to standby within 60s

**3. Graceful Degradation: Fallback Geo Index:**
```
Normally: Redis Geo → fast O(1) lookup
Fallback:  PostgreSQL PostGIS driver table
           - updated by async flush every 30s
           - slower (50ms vs 1ms) but correct
```
```sql
-- PostGIS fallback query:
SELECT driver_id, ST_Distance(
    location::geography,
    ST_MakePoint($lng, $lat)::geography
) as distance_m
FROM drivers
WHERE status = 'AVAILABLE'
  AND updated_at > NOW() - INTERVAL '30 seconds'
ORDER BY distance_m ASC
LIMIT 20;
```

**4. Re-warm on Recovery:**
- On Redis restart, Location Service replays last 60s of Kafka [location-raw] topic to rebuild the geo index
- Drivers actively sending heartbeats re-appear within 30s naturally

---

## F6: Dispatch Service Failure (Matching Engine Down)

### Scenario
All Dispatch Service instances crash. No new rides can be matched.

### Impact
- New trip requests pile up in Kafka [trip-events] — unconsumed
- Riders see "Finding your driver..." forever
- No new matches, but in-progress rides continue normally (matching is done)

### Prevention
1. **Stateless design:** Dispatch is stateless — all state in Redis + Kafka. Any instance can process any trip → easy horizontal scaling
2. **Auto-scaling:** k8s HPA scales Dispatch on queue depth metric (Kafka consumer lag > 1000 → add instances)
3. **Kafka consumer group:** If one consumer crashes, Kafka reassigns its partition to another consumer within 30s

### Recovery
- New Dispatch pod starts, joins consumer group, resumes from last committed Kafka offset
- Trips waiting in Kafka get processed in order — match happens, just with added latency (30–60s delay)

### Idempotency of Matching:
Dispatch must be idempotent — a trip can be dispatched multiple times if the consumer restarts:
```python
def process_trip_requested(event):
    trip = db.get_trip(event.trip_id)
    if trip.status != "REQUESTED":
        return  # already matched, skip (idempotent)
    ...
```

---

## F7: WebSocket Server Crash (Mid-Ride)

### Scenario
A WebSocket server holding 10,000 active rider connections crashes. Riders lose their real-time location feed during an active trip.

### Impact
- Riders on active rides see driver location freeze
- No new location updates until client reconnects
- Client apps should auto-reconnect

### Mitigation

**Client-Side Auto-Reconnect:**
```javascript
// Mobile SDK:
function connectWebSocket(trip_id) {
    ws = new WebSocket(`wss://ws.uber.com/trips/${trip_id}`);
    ws.onclose = () => {
        setTimeout(() => connectWebSocket(trip_id), 2000);  // reconnect in 2s
    };
}
```

**Stateless WebSocket Servers (Connection State Externalized):**
- No in-memory session state → reconnect can land on any server
- Redis stores: `trip:{trip_id}:ws_server` (which server currently holds the connection)
- On reconnect, new server registers itself in Redis, previous server's subscription auto-expires

**Connection Drain on Shutdown:**
- k8s `preStop` hook sends SIGTERM → WS server stops accepting new connections
- Existing connections receive a "reconnect" signal (WebSocket close code 1001)
- Clients reconnect to a healthy server (load balancer removes unhealthy instance from pool)

---

## F8: Kafka Broker Failure

### Scenario
One or more Kafka brokers go down.

### Impact Depends on Replication Factor:
```
Replication factor = 3, min.insync.replicas = 2

1 broker down:  → No impact (2 replicas still in sync) ✅
2 brokers down: → Partition leader election begins; brief pause (seconds)
3 brokers down: → All writes fail; consumers halt
```

### Configuration Best Practices
```
# Producer config (guaranteed delivery):
acks = all           # wait for all ISR replicas
retries = 10
retry.backoff.ms = 200
enable.idempotence = true  # exactly-once producer semantics

# Consumer config:
enable.auto.commit = false   # manual commit after processing
isolation.level = read_committed
```

### Mitigation
1. **Replication factor = 3** across 3 AZs → can survive 1 full AZ loss
2. **Producer retries with idempotence:** Message not lost; duplicate protection
3. **Consumer idempotency:** All consumers check current state before acting (F6 pattern)
4. **Separate clusters per criticality:**
   - **Critical cluster:** location-stream, trip-events (highest replication, lowest lag SLA)
   - **Standard cluster:** notifications, analytics (can tolerate delay)

---

## F9: Payment Service / Charge Failure

### Scenario A: Payment succeeds but acknowledgment is lost
Trip is completed, charge goes through at Stripe, but Payment Service crashes before recording the result.

**Risk:** Double-charging on retry.

**Solution: Idempotency Keys**
```python
# Payment Service:
idempotency_key = f"trip-{trip_id}-charge-v1"
response = stripe.PaymentIntent.create(
    amount=fare_cents,
    currency="usd",
    payment_method=rider.payment_method_id,
    idempotency_key=idempotency_key  # Stripe deduplicates on this
)
```
Stripe guarantees same key → same result returned → no double charge.

### Scenario B: Payment genuinely fails (card declined)

```
Trip completed → Payment fails → What happens?
```

**Flow:**
1. Payment Service records `payment_status = FAILED`
2. Retry 3 times with exponential backoff (immediate, 1 hour, 24 hours)
3. After all retries fail: email rider with payment failure notice
4. Trip marked `payment_status = OUTSTANDING_BALANCE`
5. Rider blocked from requesting new rides until resolved
6. Support ticket auto-created

**Driver still gets paid** (platform absorbs risk of collection):
- Driver payment from a separate settlement pool, not directly tied to rider charge

### Scenario C: Payment Service itself is down at trip completion

```
Trip complete event → Payment Service down → Event sits in Kafka
```

**Solution:** Payment Service consumes from Kafka (async). Trip completion event is durable in Kafka. When Payment Service recovers, it processes queued events. Rider is charged, gets receipt — just delayed by minutes.

Payment must be idempotent:
```python
def process_trip_completed(event):
    existing = db.get_payment(trip_id=event.trip_id)
    if existing and existing.status == "CHARGED":
        return  # already charged, skip
    charge_rider(event.trip_id, event.fare)
```

---

## F10: Driver App Crashes Mid-Ride

### Scenario
Driver's app crashes while the rider is in the car. Driver can't tap "End Trip."

### Impact
- Trip status stays IN_PROGRESS forever
- Rider can't be charged
- Driver can't get paid

### Mitigations
1. **Trip duration timeout:** If trip duration > 4 hours (or estimated_duration × 3), auto-complete the trip using GPS breadcrumbs
2. **Rider "End Trip" fallback:** Rider can also trigger trip end from their app (driver confirmation not strictly required if driver is unresponsive for 5+ minutes)
3. **Driver reconnect:** App reconnects on restart; trips in IN_PROGRESS state are recovered from server on app launch
4. **Breadcrumb-based fare:** If app crashes, fare is calculated from stored GPS breadcrumbs (Cassandra), not from in-app data

---

## F11: Maps / ETA API Failure (Google Maps)

### Scenario
Google Maps Distance Matrix / Directions API returns errors or times out.

### Impact
- ETA estimates become unavailable
- Route polyline not computed
- Fare estimates fail (if distance-based)

### Mitigation: Defense in Depth
```
Primary:   Google Maps Platform API
Secondary: HERE Maps API         (hot standby, circuit breaker switches)
Tertiary:  OSRM (self-hosted)    (open-source routing, in k8s cluster)
Fallback:  Haversine formula     (straight-line distance × 1.3 road factor)
```

Circuit breaker pattern:
```python
@circuit_breaker(failure_threshold=5, recovery_timeout=30)
def get_eta_google(origin, dest):
    return google_maps_client.directions(origin, dest)

def get_eta(origin, dest):
    try:
        return get_eta_google(origin, dest)
    except CircuitOpenError:
        return get_eta_here(origin, dest)  # failover
    except Exception:
        return get_eta_haversine(origin, dest)  # final fallback
```

---

## F12: Network Partition (Split Brain)

### Scenario
Network partition splits Dispatch Service instances into two groups. Both groups can see different subsets of drivers and may attempt to match the same driver to two trips.

### Risk: One driver assigned two trips simultaneously.

### Prevention
**Distributed Lock via Redis (Redlock algorithm):**
```python
# Before sending offer to driver:
lock_key = f"driver_lock:{driver_id}"
lock_acquired = redis.set(lock_key, trip_id, nx=True, ex=30)
# nx=True: only set if not exists (atomic)
# ex=30: auto-expire after 30s (handles crash)

if not lock_acquired:
    skip_this_driver()  # another dispatch instance has it
```

**Database optimistic lock as final guard:**
```sql
-- Final assignment (trip service):
UPDATE trips SET driver_id = $1, status = 'MATCHED'
WHERE trip_id = $2 AND status = 'REQUESTED';
-- Only one UPDATE succeeds; second gets rows_affected = 0
```

**Two-phase commit** not used (too slow) — instead, DB constraint is the authoritative tiebreaker.

---

## Summary: Failure Severity Matrix

| Failure | Severity | Detection Time | Recovery Time | Data Loss? |
|---------|----------|---------------|--------------|------------|
| API Gateway down | P0 | <10s | 30–90s | None |
| Trip DB primary fails | P0 | <30s | 15–30s | None (sync replica) |
| Redis Geo down | P0 | <10s | 30–60s | Location data (ephemeral) |
| Dispatch Service crash | P1 | <30s | 30–60s | None (Kafka durable) |
| Location Service crash | P1 | <10s | <30s | None (Kafka replay) |
| WS Server crash | P1 | <5s | 2–5s | None (client reconnects) |
| Kafka broker (1 of 3) | P2 | <60s | Automatic | None |
| Payment Service crash | P2 | <30s | Minutes | None (Kafka durable) |
| Maps API down | P2 | <5s | Seconds (failover) | None |
| Driver app crash | P3 | ~5 min | Driver restart | None (breadcrumbs) |

---

## Chaos Engineering Recommendations (Netflix-style)

Run these drills to verify your mitigations actually work:

1. **Kill Redis primary** → verify Sentinel promotes replica in <30s, matching continues
2. **Kill random Dispatch pod** → verify Kafka reassigns partition, no trips lost
3. **Kill all Location Service pods** → verify location-raw Kafka backlog processes on restart
4. **Simulate network partition** → verify Redlock prevents double-booking
5. **Inject Maps API timeout** → verify circuit breaker switches to OSRM
6. **Kill WS server** → verify rider app reconnects within 5s and location stream resumes
7. **Fail payment charge** → verify idempotency key prevents double-charge on retry

---

*Files in this series:*
- `Uber_System_Design_2026_05_12.md` — full architecture and design
- `Uber_Failure_Analysis_2026_05_12.md` ← this file
- `Uber_Interview_QA_2026_05_12.md` — interview questions and model answers
