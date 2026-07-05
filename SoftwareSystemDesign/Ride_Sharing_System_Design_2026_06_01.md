# Ride-Sharing System Design (Uber/Lyft)
*Date: 2026-06-01*

---

## First Principles: Do We Need This?

**Problem**: People need transport from A→B without owning a vehicle. Taxis exist but are expensive, unreliable, not trackable. Private cars are idle 95% of the time.

**Why a platform?** Aggregates supply (idle cars) with demand (riders). Network effects: more drivers → faster pickups → more riders → more drivers.

**Core value**: Real-time matching + transparent pricing + reliable ETA.

---

## Entities

| Entity | Key Attributes |
|--------|---------------|
| Rider | id, name, phone, payment_method, rating |
| Driver | id, name, vehicle, license, rating, status (available/busy/offline) |
| Trip | id, rider_id, driver_id, pickup, dropoff, status, fare, timestamps |
| Location | driver_id, lat, lng, timestamp |
| Fare | trip_id, base, distance_rate, time_rate, surge_multiplier, total |

---

## Actions / API

```
POST /trips/request          → rider requests ride (pickup, dropoff)
GET  /trips/{id}             → trip status + driver location
POST /trips/{id}/cancel      → cancel trip
POST /trips/{id}/complete    → driver marks complete
GET  /drivers/nearby         → find available drivers near location
PATCH /drivers/location      → driver updates location (every 3s)
GET  /trips/{id}/fare        → fare estimate + breakdown
POST /trips/{id}/rate        → rate driver/rider
```

---

## Data Flow

```
Driver App → Location Service (every 3s) → Location Store (Redis GeoSet)
                                                    ↓
Rider requests ride → Matching Service ← [Driver pool query: nearby + available]
                            ↓
                    Assign best driver → Notify driver (push/WebSocket)
                            ↓
                    Driver accepts → Trip created in DB
                            ↓
                    Trip ongoing → Location updates streamed to rider via WebSocket
                            ↓
                    Dropoff → Fare calculated → Payment charged → Trip closed
```

---

## High-Level Design

```
[Rider App] ──WebSocket──┐
[Driver App] ──WebSocket─┤
                         ↓
                  [API Gateway / Load Balancer]
                    /          |          \
          [Trip Svc]    [Location Svc]  [Auth Svc]
              |               |
         [Matching Svc]  [Redis GeoSet]
              |
         [Notification Svc] → FCM/APNs
              |
         [Fare/Surge Svc]
              |
         [Payment Svc] → Stripe/Braintree
              |
      [PostgreSQL]    [Kafka]    [S3 (receipts)]
```

---

## Low-Level Design

### 1. Location Service
- Drivers send `PATCH /drivers/location` every 3 seconds
- Store in **Redis GEOADD** (sorted set with geohash score)
- Key: `drivers:available` → value: `driver_id` → score: geohash
- TTL: 10s per entry (if driver stops sending → auto-expires = offline)
- Query: `GEORADIUSBYMEMBER drivers:available <lat> <lng> 5km ASC COUNT 20`

**Why Redis over PostGIS?**
- Redis: in-memory, sub-ms geo queries, auto-expiry ✓
- PostGIS: durable, complex queries, but slower for high-frequency writes ✗
- **Tradeoff**: Redis loses data on crash (but location is ephemeral — OK)

### 2. Matching Service
Algorithm:
```
1. Query Redis: drivers within 5km radius, status=available
2. Score each driver:
   score = w1*(distance) + w2*(driver_rating) + w3*(acceptance_rate)
3. Send offer to top driver → wait 10s for accept
4. If no accept → next driver (cascade)
5. If no driver in 5km → expand to 10km
```

**Why not broadcast to all?** Race condition — multiple drivers accept same trip. Sequential offer avoids conflict without distributed locks.

### 3. Surge Pricing
```
surge_multiplier = f(demand / supply) in geohash cell

if (requests_last_5min / available_drivers) > threshold:
    multiplier = 1.0 + k * (demand/supply - 1)
    cap at 3.0x (policy)
```
- Geohash cells: ~1km² granularity (geohash level 6)
- Computed every 30s by Surge Service, cached in Redis

**Tradeoff**: Smaller cells = more accurate surge but noisier (1 driver covers whole cell)

### 4. Trip State Machine
```
REQUESTED → DRIVER_ASSIGNED → DRIVER_EN_ROUTE → ARRIVED → IN_PROGRESS → COMPLETED
                                                                        → CANCELLED (any state)
```
Stored in PostgreSQL with `status` column + timestamps.
Published to Kafka on every transition → downstream consumers (analytics, notifications, billing).

### 5. Real-Time Location Streaming (Rider sees driver moving)
- Driver app → WebSocket connection to Location Service
- Location Service publishes to Kafka topic `driver-locations`
- Trip Service subscribes, filters by active trip's driver_id
- Pushes update to rider's WebSocket connection

**Tradeoff**: WebSocket vs SSE vs polling
- WebSocket: bidirectional, lower overhead ✓ (chosen)
- SSE: server→client only, simpler
- Polling: simple but 3s delay + wasteful

### 6. Database Schema (PostgreSQL)

```sql
CREATE TABLE trips (
  id UUID PRIMARY KEY,
  rider_id UUID NOT NULL,
  driver_id UUID,
  status VARCHAR(20) NOT NULL,
  pickup_lat DOUBLE PRECISION,
  pickup_lng DOUBLE PRECISION,
  dropoff_lat DOUBLE PRECISION,
  dropoff_lng DOUBLE PRECISION,
  fare_total DECIMAL(10,2),
  surge_multiplier DECIMAL(3,2) DEFAULT 1.0,
  requested_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

CREATE INDEX idx_trips_rider ON trips(rider_id);
CREATE INDEX idx_trips_driver ON trips(driver_id);
CREATE INDEX idx_trips_status ON trips(status) WHERE status NOT IN ('COMPLETED','CANCELLED');
```

### 7. Sharding Strategy
- **Trips table**: shard by `rider_id` (rider queries their trips → same shard)
- **Driver table**: shard by `driver_id`
- **Location**: Redis cluster, shard by geohash prefix (geographic sharding)

---

## Key Tradeoffs Summary

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Location store | Redis GeoSet | PostGIS | Speed > durability for ephemeral data |
| Matching | Sequential offer | Broadcast | Avoid race conditions |
| Real-time | WebSocket | Polling | Lower latency, less bandwidth |
| Surge granularity | Geohash-6 (~1km²) | City-level | Balance accuracy vs noise |
| Trip DB | PostgreSQL | Cassandra | ACID for financial records |
| Async events | Kafka | RabbitMQ | Durability + replay capability |
