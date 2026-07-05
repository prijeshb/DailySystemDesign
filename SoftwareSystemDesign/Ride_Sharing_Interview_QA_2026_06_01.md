# Ride-Sharing System — Interview Q&A
*Date: 2026-06-01*

---

## Opening Questions (Scoping)

**Q: Design a ride-sharing system like Uber.**

Good first moves:
- "Should I cover all features or focus on the core booking + matching flow?"
- "What's the scale? DAU, rides/day, geographic regions?"
- "Real-time tracking required?"

Typical answer: ~10M DAU, 5M rides/day, global, yes real-time.

**Q: What are the core entities?**
Rider, Driver, Trip, Location (ephemeral), Fare, Vehicle.
Start with these 5 and justify each.

---

## Deep-Dive: Location

**Q: How do you store and query driver locations efficiently?**
Redis GEOADD/GEORADIUS. Drivers push every 3s. TTL 10s auto-expires offline drivers.

**Q: Why not just use a SQL database with lat/lng columns?**
SQL queries like `WHERE lat BETWEEN x1 AND x2 AND lng BETWEEN y1 AND y2` are slow at scale and don't handle the spherical distance correctly. Even with PostGIS, in-memory Redis is 10-100x faster for point-in-radius queries on frequently-updated data.

**Q: What's a geohash and why use it?**
Encodes lat/lng as a short string. Nearby locations share a prefix. Allows querying a region by prefix match. Redis uses it internally for GEO commands. Also useful for sharding (shard by geohash prefix = geographic partitioning).

**Q: What if two drivers are on the border of a geohash cell?**
GEORADIUSBYMEMBER uses true circle (Haversine distance), not cell membership. The cell is just for indexing — the query result is accurate regardless of cell boundaries.

---

## Deep-Dive: Matching

**Q: How do you match a rider to a driver?**
Query nearby available drivers from Redis → score by distance + rating + acceptance rate → send sequential offers with 10s timeout.

**Q: Why sequential and not parallel offers to multiple drivers?**
Parallel causes race conditions. Multiple drivers accept the same trip — you need distributed locks or last-write-wins, both messy. Sequential is simpler, slightly slower worst-case, but cleaner.

**Follow-up: What's the latency tradeoff of sequential offers?**
Worst case: 3 drivers × 10s timeout = 30s before rider knows no driver. Mitigation: expand radius in parallel to next tier while waiting.

**Q: How do you handle a driver accepting after the trip is assigned?**
Trip status is `DRIVER_ASSIGNED` → reject with 409. Driver app shows "trip no longer available." Idempotent state machine prevents double-assignment.

**Q: What if no drivers are available at all?**
Return "no drivers available" after N attempts. Optionally: put rider in a waitlist queue (Kafka), notify when driver becomes available nearby.

---

## Deep-Dive: Surge Pricing

**Q: How does surge pricing work?**
Compute `demand / supply` ratio per geohash cell every 30s. If ratio > threshold → multiplier = 1 + k*(ratio - 1), capped at 3x. Store in Redis, serve on fare estimate.

**Q: What's the right cell granularity?**
Geohash level 6 ≈ 1.2km × 0.6km. Smaller = more accurate but noisier (1 driver leaving tanks supply for that cell). Larger = smooth but misses local spikes. Level 6 is Uber's approximate granularity.

**Q: What if the surge service crashes?**
Use last cached value in Redis (no TTL → stale but not missing). Fallback = 1.0x (no surge). Acceptable for short outages. Alert on Surge Service health.

---

## Deep-Dive: Real-Time Tracking

**Q: How does the rider see the driver moving on the map?**
Driver app → WebSocket → Location Service → Kafka → Trip Service → WebSocket → Rider app.

**Q: Why WebSocket over polling?**
Polling every 3s wastes bandwidth and adds latency. WebSocket keeps persistent connection, pushes updates instantly. For mobile, battery trade-off exists but push wins at this scale.

**Q: What happens if the WebSocket connection drops?**
Client reconnects with exponential backoff. On reconnect, fetches current trip state via REST. DB is source of truth — no state lost.

**Q: How do you scale WebSocket connections?**
WebSocket servers are stateful (connection pinned to server). Use a sticky load balancer (consistent hashing by session). Horizontally scale WebSocket servers; use Redis Pub/Sub to fan out location updates across servers.

---

## Deep-Dive: Payments

**Q: How do you prevent double charging?**
Idempotency key = trip_id passed to Stripe. Stripe deduplicates on their end. Store payment_status separately from trip_status. Retry async, not inline.

**Q: What if payment fails after trip completes?**
Driver is still paid (platform absorbs). Rider flagged for retry. After 3 failures → account restricted → prompted to update payment method.

**Q: How do you handle refunds for cancelled trips?**
Cancellation fee depends on state (before vs after driver arrival). Refund issued via Stripe Refund API with original charge_id. Stored in `refunds` table for audit.

---

## Scale & Capacity

**Q: How many location updates per second?**
- 1M active drivers, update every 3s → ~333K writes/sec
- Redis cluster with 10 shards → 33K writes/shard → manageable

**Q: How do you shard the trips table?**
By rider_id → rider queries their trip history on one shard. Alternatively by trip_id (uniform) but then rider history requires scatter-gather.

**Q: How do you handle a city with a major event (10x normal demand)?**
Pre-scale (auto-scaling based on event calendar). Redis sharding means geographic hot spots don't take down unrelated regions. Surge pricing incentivizes supply. Load shedding for new requests if needed.

---

## Follow-Up / Hard Questions

**Q: How would you add carpooling (multiple riders same car)?**
Trip entity gets `rider_ids[]`. Matching considers detour cost. Route planning needed (TSP subset). Driver sees multi-stop route. Fare split at API level. State machine gets more complex (each rider can cancel independently).

**Q: How would you handle a driver going offline mid-trip?**
Location TTL expires (10s). Trip Service detects driver offline → alert rider, alert support. Options: reassign trip (complex — different driver, different ETA) or wait 2 min for reconnect. In practice: attempt reassign after 5 min.

**Q: How would you design the rating system to prevent manipulation?**
Ratings stored only after trip completes. Rider/driver can't see each other's rating until both submit (or 24h passes). Outlier detection: sudden rating drop → flag for review. Weight recent ratings higher. Minimum 10 trips before public rating shown.

**Q: Design the ETA prediction system.**
Inputs: pickup location, dropoff location, current traffic (Google Maps API / internal), time of day, day of week, weather. ML model trained on historical trips. Output: P50/P90 ETA. Served via separate ETA Service with Redis cache (same route + time bucket → cached result for 2 min).
