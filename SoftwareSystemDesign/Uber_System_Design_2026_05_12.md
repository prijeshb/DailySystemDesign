# Ride-Sharing System Design (Uber / Lyft)
**Date:** 2026-05-12 | **Difficulty:** Hard | **Category:** Real-time + Geospatial

---

## 1. Problem Framing & Scope

Design a ride-sharing platform where riders request trips, the system matches them to nearby drivers, tracks the ride in real time, and handles payment on completion.

### Functional Requirements
- Rider can request a ride by specifying pickup and drop-off location
- System finds and matches a nearby available driver
- Driver can accept or decline the match
- Both rider and driver see each other's real-time location during the trip
- System calculates fare and charges rider on trip completion
- Ride history, ratings, and receipts are stored

### Non-Functional Requirements
- **Matching latency:** < 2 seconds to surface a match
- **Location update throughput:** millions of driver pings per second globally
- **High availability:** 99.99% for matching and ride tracking
- **Consistency:** trip state must never be ambiguous (no double-booking)
- **Scale:** 5 million concurrent drivers, 3 million active rides at peak
- **Durability:** all trip/payment records must never be lost

### Out of Scope
- Surge pricing algorithm internals
- Driver onboarding / background checks
- Carpooling (UberPool) multi-passenger matching

---

## 2. Entities

```
User (Rider)
├── user_id (UUID)
├── name, email, phone
├── payment_method_id
└── rating (float)

Driver
├── driver_id (UUID)
├── user_id (linked)
├── vehicle: { make, model, plate, capacity }
├── license_info
├── current_status: OFFLINE | AVAILABLE | EN_ROUTE | ON_TRIP
└── rating (float)

Location (ephemeral, high-write)
├── driver_id
├── lat, lng
├── heading, speed
└── timestamp

Trip
├── trip_id (UUID)
├── rider_id, driver_id
├── status: REQUESTED | MATCHED | DRIVER_EN_ROUTE | IN_PROGRESS | COMPLETED | CANCELLED
├── pickup: { lat, lng, address }
├── dropoff: { lat, lng, address }
├── route_polyline
├── fare_estimate, fare_actual
├── requested_at, started_at, ended_at
└── payment_status: PENDING | CHARGED | FAILED | REFUNDED

Payment
├── payment_id (UUID)
├── trip_id
├── amount, currency
├── method: { card_last4, provider }
└── status, processed_at

Rating
├── rating_id
├── trip_id
├── rater_id, ratee_id
├── score (1–5), comment
└── created_at
```

---

## 3. Core Actions & APIs

### Rider Actions
```
POST /trips/request
  Body: { pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type }
  Returns: { trip_id, estimated_fare, estimated_eta, status: "REQUESTED" }

GET /trips/{trip_id}/status
  Returns: { status, driver: { name, rating, vehicle, lat, lng }, eta_seconds }

DELETE /trips/{trip_id}
  → Cancel ride (free if driver not yet matched)

POST /trips/{trip_id}/rate
  Body: { score, comment }
```

### Driver Actions
```
POST /drivers/location
  Body: { lat, lng, heading, speed }
  → High-frequency heartbeat (every 4 seconds)

GET /drivers/match-offer
  Returns: { trip_id, pickup_distance_m, pickup_eta_s, fare_estimate }
  → Long-poll or WebSocket push

POST /drivers/match-offer/{trip_id}/accept
POST /drivers/match-offer/{trip_id}/decline

POST /trips/{trip_id}/start      → Driver picked up rider
POST /trips/{trip_id}/complete   → Driver ended trip
```

### Internal APIs
```
POST /internal/dispatch           → Matching engine call
POST /internal/fare/calculate     → Fare service
POST /internal/payment/charge     → Payment service
```

---

## 4. Data Flow

### Ride Request Flow
```
Rider App
  │  POST /trips/request
  ▼
API Gateway → Trip Service
  │  1. Create trip record (status=REQUESTED) in Trip DB
  │  2. Publish TripRequested event → Kafka [trip-events]
  ▼
Dispatch Service (consumes TripRequested)
  │  3. Query Location Index (geospatial) for drivers within 5km radius
  │  4. Filter: status=AVAILABLE, vehicle type match, not blacklisted
  │  5. Rank by: proximity, rating, acceptance rate
  │  6. Send match offer to top driver (via WebSocket/push)
  ▼
Driver App receives offer → accepts (within 15s timeout)
  │  POST /drivers/match-offer/{trip_id}/accept
  ▼
Dispatch Service
  │  7. Update trip status → MATCHED (optimistic lock / CAS)
  │  8. Update driver status → EN_ROUTE
  │  9. Publish TripMatched event → Kafka
  ▼
Trip Service / Notification Service
  │  10. Push notification to rider: "Driver found – John, 3 min away"
  │  11. Begin streaming driver location to rider via WebSocket
```

### Location Update Flow
```
Driver App (every 4s)
  │  POST /drivers/location  { lat, lng, heading, speed }
  ▼
Location Service
  │  1. Write to Redis Geo (GEOADD drivers:available <lng> <lat> <driver_id>)
  │  2. If driver is ON_TRIP → also publish to Kafka [location-stream]
  ▼
Location Stream Consumer
  │  3. Fan-out driver location to rider's WebSocket connection
  │  4. Persist to TimeSeries DB (trip breadcrumbs for fare/dispute)
```

### Trip Completion & Payment Flow
```
Driver taps "End Trip"
  │  POST /trips/{trip_id}/complete
  ▼
Trip Service
  │  1. Compute distance from breadcrumb store
  │  2. Call Fare Service → final fare
  │  3. Update trip status → COMPLETED
  │  4. Publish TripCompleted event → Kafka
  ▼
Payment Service (consumes TripCompleted)
  │  5. Charge rider's payment method via Stripe/Braintree
  │  6. Update payment record
  │  7. Publish PaymentProcessed event
  ▼
Notification Service
  │  8. Send receipt to rider (email + push)
  │  9. Prompt both rider and driver for rating
```

---

## 5. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                            │
│     Rider iOS/Android App          Driver iOS/Android App       │
└───────────────┬──────────────────────────────┬──────────────────┘
                │ HTTPS / WebSocket            │ HTTPS / WebSocket
┌───────────────▼──────────────────────────────▼──────────────────┐
│                    API GATEWAY / LOAD BALANCER                  │
│          (Auth, Rate Limiting, SSL Termination, Routing)        │
└──┬─────────┬──────────┬───────────┬──────────┬──────────────────┘
   │         │          │           │          │
   ▼         ▼          ▼           ▼          ▼
 Trip      Location   Driver     User/Auth   Notification
Service    Service    Service     Service     Service
   │         │          │
   │         ▼          │
   │    ┌─────────┐     │
   │    │  Redis  │     │
   │    │  Geo +  │◄────┘ (driver status + geo index)
   │    │  Cache  │
   │    └─────────┘
   │
   ▼
┌────────────────────┐
│   Dispatch Service │ (matching engine)
│   (stateless,      │
│    horizontally    │
│    scaled)         │
└────────────────────┘
   │
   ▼
┌───────────────────────────────────────────┐
│              KAFKA (Event Bus)            │
│  Topics: trip-events, location-stream,    │
│          payment-events, notifications    │
└───┬──────────────┬────────────────────────┘
    ▼              ▼
Payment         Analytics /
Service         Data Warehouse
    │               (Flink / Spark)
    ▼
Stripe / Braintree
(external)

┌───────────────────────────────────────────┐
│              DATA STORES                  │
│                                           │
│  Trip DB:      PostgreSQL (sharded)       │
│  User/Driver:  PostgreSQL                 │
│  Location:     Redis Cluster (Geo)        │
│  Breadcrumbs:  Cassandra / InfluxDB       │
│  Ratings:      PostgreSQL                 │
│  Session:      Redis                      │
└───────────────────────────────────────────┘
```

---

## 6. Low-Level Design

### 6.1 Geospatial Driver Index (Most Critical Component)

**Problem:** Given a pickup location, find the N nearest available drivers in real time, with millions of drivers updating location every 4 seconds.

**Solution: Redis Geo + H3 Hexagonal Indexing (Uber's actual approach)**

**Redis Geo (simpler path):**
```
# Driver heartbeat
GEOADD drivers:available <lng> <lat> <driver_id>
EXPIRE driver:{driver_id}:presence 30  # auto-expire if heartbeat stops

# Find nearby drivers
GEORADIUS drivers:available <lng> <lat> 5 km ASC COUNT 20
```

**H3 Hexagonal Grid (production path, Uber's S2/H3):**
- Earth divided into hexagonal cells at multiple resolutions
- Resolution 9 hex ≈ 0.1 km² (city block sized)
- Driver's hex cell is computed from lat/lng on heartbeat
- Matching: query rider's cell + 6 adjacent cells at resolution 9
- If < threshold results, expand to resolution 8 (wider)

```python
import h3

def get_search_cells(lat, lng, resolution=9):
    center_cell = h3.geo_to_h3(lat, lng, resolution)
    neighbors = h3.k_ring(center_cell, 1)  # center + 6 neighbors
    return neighbors

# Store in Redis: SADD cell:{hex_id} driver_id
# Lookup: SUNION cell:{hex1} cell:{hex2} ... cell:{hex7}
```

**Trade-off: Redis Geo vs H3**
| Aspect | Redis GEORADIUS | H3 Grid |
|--------|----------------|---------|
| Simplicity | ✅ Simple | ❌ Custom code |
| Scale | Single shard limited | ✅ Partition by cell |
| Precision | ✅ Exact radius | Hex approximation |
| Query pattern | Circle radius | ✅ Neighbor lookup = O(1) |
| Real-world usage | Small scale | Uber, Lyft production |

### 6.2 Trip State Machine

```
                        ┌─── timeout (15s) ──► CANCELLED
REQUESTED ──► MATCHED ──┤
                        └─► DRIVER_EN_ROUTE ──► IN_PROGRESS ──► COMPLETED
                                    │                  │
                              driver cancel       rider cancel ──► CANCELLED
                                    │
                                CANCELLED
```

**State transitions are stored in PostgreSQL with optimistic locking:**
```sql
UPDATE trips
SET status = 'MATCHED', driver_id = $1, updated_at = now()
WHERE trip_id = $2
  AND status = 'REQUESTED'   -- guards against double-assign
  AND updated_at = $3;       -- optimistic lock version check
```
If rows_affected = 0 → another process already changed state → abort and notify dispatch.

### 6.3 Dispatch (Matching) Algorithm

```python
def dispatch(trip_id, pickup_lat, pickup_lng, vehicle_type):
    candidates = geo_index.find_nearby_drivers(
        lat=pickup_lat, lng=pickup_lng,
        radius_km=5, vehicle_type=vehicle_type,
        limit=20
    )
    
    ranked = rank_drivers(candidates, pickup_lat, pickup_lng)
    
    for driver in ranked:
        lock_key = f"driver_lock:{driver.id}"
        acquired = redis.set(lock_key, trip_id, nx=True, ex=30)
        if not acquired:
            continue  # driver already being offered another trip
        
        offer_sent = push_offer_to_driver(driver.id, trip_id)
        response = wait_for_response(driver.id, trip_id, timeout=15)
        
        if response == "ACCEPT":
            assign_driver(trip_id, driver.id)
            redis.delete(lock_key)
            return SUCCESS
        else:
            redis.delete(lock_key)  # free driver for other trips
            continue
    
    return NO_DRIVER_FOUND

def rank_drivers(candidates, pickup_lat, pickup_lng):
    scored = []
    for d in candidates:
        eta = estimate_eta(d.lat, d.lng, pickup_lat, pickup_lng)
        score = (
            0.5 * (1 / (eta + 1)) +     # proximity (higher = better)
            0.3 * d.rating / 5 +          # driver quality
            0.2 * d.acceptance_rate        # reliability
        )
        scored.append((score, d))
    return [d for _, d in sorted(scored, reverse=True)]
```

**Trade-off: Sequential vs Parallel Offers**
| Approach | Pros | Cons |
|----------|------|------|
| Sequential (one at a time) | No double-booking | Slower if first driver declines |
| Parallel broadcast | Fast match | Must handle race conditions; bad UX (driver feels pressured) |
| Hybrid (Uber's model) | Offer top N, first-accept wins | Complex dedup, slight chance of no acceptance |

Uber uses a **soft parallel** model: top 3 drivers get the offer, first to accept wins, rest get a cancel signal.

### 6.4 Real-Time Location Streaming

```
Driver App ──► Location Service ──► Redis Geo (available drivers index)
                    │
                    ├─► Kafka [location-stream] (if driver ON_TRIP)
                    │
                    └─► WebSocket Hub ──► Rider App
```

**WebSocket Fan-out Architecture:**
- Each WebSocket server holds in-memory map: `trip_id → [rider_connection]`
- Location stream consumer reads from Kafka topic partitioned by `driver_id`
- Consumer looks up active trip for driver, pushes lat/lng to rider's WS server via pub/sub (Redis Pub/Sub or internal RPC)

```
Kafka Consumer (driver_id=D123)
  → finds trip_id=T456 for D123
  → looks up: which WS server holds T456's rider?
  → Redis: GET trip:{T456}:ws_server → "ws-server-7"
  → Publishes to Redis channel: PUBLISH ws-server-7:trip:{T456} {lat, lng, heading}

WS Server 7
  → subscribed to its own Redis channel
  → receives message → pushes to rider's open WebSocket
```

### 6.5 Fare Calculation

```
fare = base_fare
     + (price_per_minute × trip_duration_minutes)
     + (price_per_km × distance_km)
     + surge_multiplier
     + applicable_tolls
     - discounts / promo_codes
     + booking_fee
```

Distance computed from **breadcrumb polyline** (GPS track stored in Cassandra/InfluxDB) rather than straight-line — handles detours, traffic. Fallback: Google Maps Distance Matrix API.

### 6.6 Database Sharding Strategy

**Trip DB (PostgreSQL):**
- Shard by `city_id` (geographic shard) — most queries are local
- Hot shard risk for NYC/London: further shard by `trip_id % N`
- Completed trips older than 90 days → archive to S3 / data warehouse

**Driver Location (Redis Cluster):**
- Shard by geographic region (continent → country → city)
- Each Redis cluster serves one metro area
- Cross-region queries not needed (rider and driver in same city)

---

## 7. Key Trade-offs

| Decision | Choice Made | Why | What You Give Up |
|----------|-------------|-----|-----------------|
| Location store | Redis Geo + H3 | Sub-millisecond geo queries, auto-expiry of stale drivers | No persistence (ok — location is ephemeral) |
| Matching | Dispatch service (separate) | Can scale independently, swap algorithm | Extra network hop vs monolith |
| Trip state DB | PostgreSQL with row-level lock | Strong consistency, no double-booking | Lower write throughput vs NoSQL |
| Location updates | WebSocket (persistent) | Low latency, no polling overhead | Complex connection management at scale |
| Breadcrumbs | Cassandra | High write throughput, time-series friendly | No ad-hoc joins |
| Event bus | Kafka | Durable, replayable, fan-out | Operational complexity |
| Driver offer | Sequential-first with soft parallel | Balances speed and UX | Slightly complex dedup logic |

---

## 8. Real-World References

- **Uber Engineering Blog:** "Uber's Fulfillment Platform" — describes the Dispatch v2 architecture with H3 hexagonal indexing and ETA service
- **Uber H3 library:** Open-sourced geospatial indexing library (github.com/uber/h3)
- **Lyft Engineering:** "Real-time Geospatial Data at Lyft" — describes their Redis-based driver index with 400ms P99 location staleness target
- **Uber's "Ringpop":** Consistent hashing ring for routing WebSocket connections across a fleet of servers
- **Netflix Zuul:** API gateway pattern used at Uber for WebSocket proxying
- **Kafka at Uber:** Uber processes 1 trillion+ Kafka messages per day across all services

---

## 9. Capacity Estimation

```
Drivers (global active peak):        5,000,000
Location update frequency:           every 4s
Location writes/sec:                 5,000,000 / 4 = 1,250,000 writes/sec

Active rides at peak:                3,000,000
Location fan-out per active ride:    1 update/4s to 1 rider
Fan-out events/sec:                  750,000 /sec

Trip requests/day (global):          25,000,000
Trip writes/sec:                     25M / 86400 ≈ 290 writes/sec (very manageable)

Storage (trips, 1 year):
  1 trip ≈ 2KB → 25M × 365 × 2KB ≈ 18 TB/year

Storage (breadcrumbs, 30-day retention):
  1 GPS point ≈ 50 bytes, every 4s, avg trip 20min
  = 300 points × 50B = 15KB per trip
  25M trips/day × 15KB = 375 GB/day → ~11 TB/month
```

---

*Files in this series:*
- `Uber_System_Design_2026_05_12.md` ← this file
- `Uber_Failure_Analysis_2026_05_12.md` — failure modes and mitigations
- `Uber_Interview_QA_2026_05_12.md` — interview questions and model answers
