# Food Delivery System Design (DoorDash / UberEats / Swiggy)
**Date:** 2026-06-04

---

## 0. First Principles — Do We Need This?

**Problem:** Hungry person + restaurant far away = no food.  
**Why not call the restaurant?** Limited reach, no tracking, cash-only, no aggregation.  
**Core value:** Aggregate supply (restaurants), match with demand (users), orchestrate fulfillment (drivers) in real time.

Three distinct marketplaces in one system:
1. **Demand side** — customers browsing, ordering
2. **Supply side** — restaurants accepting, preparing
3. **Fulfillment side** — drivers picking up, delivering

Each has different latency, consistency, and availability requirements.

---

## 1. Entities

| Entity | Key Attributes |
|--------|---------------|
| **User** | id, location, payment_methods, preferences |
| **Restaurant** | id, geolocation, cuisine_tags, operating_hours, prep_time_avg |
| **MenuItem** | id, restaurant_id, name, price, availability, modifiers |
| **Cart** | user_id, items[], restaurant_id, subtotal |
| **Order** | id, user_id, restaurant_id, driver_id, items[], status, timestamps |
| **Driver** | id, current_location, status (available/busy/offline), vehicle_type |
| **Delivery** | order_id, driver_id, pickup_time, drop_time, route_polyline |
| **Payment** | order_id, amount, method, status, idempotency_key |
| **Review** | order_id, user_id, target (restaurant/driver), rating, text |

---

## 2. Core Actions & Estimated Scale

| Action | RPS (peak) | Notes |
|--------|-----------|-------|
| Browse restaurants | 50K | Heavy read, cacheable |
| Search menu | 20K | Elasticsearch |
| Place order | 5K | Write-heavy, critical path |
| Update driver location | 500K | 1M drivers × 1 ping/2s |
| Track order status | 100K | WebSocket/SSE |
| Payment process | 5K | External PSP call |

---

## 3. Data Flow

```
User App
  │
  ├─[Browse]──► API Gateway ──► Restaurant Service ──► Geo Index (PostGIS/Geohash)
  │                                                  └─► Redis Cache (restaurant list by zone)
  │
  ├─[Order]──► Order Service ──► DB (write order, status=PLACED)
  │                           ──► Payment Service ──► PSP (Stripe/Razorpay)
  │                           ──► Kafka: order.placed
  │                                   │
  │                           Restaurant Service consumes ──► Push notification to restaurant
  │
  ├─[Accept]─ Restaurant App ──► Order Service (status=ACCEPTED, prep_time=N)
  │                           ──► Kafka: order.accepted
  │                                   │
  │                           Driver Matching Service consumes
  │                               └──► Find nearest available driver
  │                               └──► Assign (status=DRIVER_ASSIGNED)
  │
  ├─[Pickup]─ Driver App ──► Order Service (status=PICKED_UP)
  │         ─ Location ──► Location Service ──► Redis (driver geo)
  │                                         ──► Kafka: location.updated
  │                                                 │
  │                                         Tracking Service ──► WebSocket push to user
  │
  └─[Deliver]─ Driver App ──► Order Service (status=DELIVERED)
                           ──► Payment Service (capture/settle)
                           ──► Notification Service
```

---

## 4. High-Level Design

```
                          ┌─────────────────┐
   Mobile/Web ────────────►  API Gateway    │
                          │  (Auth, Rate    │
                          │   Limiting)     │
                          └────────┬────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
   ┌─────▼──────┐          ┌───────▼──────┐         ┌───────▼──────┐
   │ Restaurant │          │ Order        │         │ Driver       │
   │ Service    │          │ Service      │         │ Service      │
   └─────┬──────┘          └───────┬──────┘         └───────┬──────┘
         │                         │                         │
   ┌─────▼──────┐          ┌───────▼──────┐         ┌───────▼──────┐
   │ Postgres   │          │ Postgres     │         │ Location     │
   │ (catalog)  │          │ (orders)     │         │ Service      │
   │ +Redis     │          │              │         │ (Redis Geo)  │
   └────────────┘          └───────┬──────┘         └───────┬──────┘
                                   │                         │
                          ┌────────▼─────────────────────────▼──────┐
                          │              Kafka                        │
                          │  (order.placed, order.accepted,           │
                          │   location.updated, order.delivered)      │
                          └───┬───────────┬───────────┬──────────────┘
                              │           │           │
                    ┌─────────▼───┐  ┌────▼───────┐  ┌▼──────────────┐
                    │ Driver      │  │ Tracking   │  │ Notification  │
                    │ Matching    │  │ Service    │  │ Service       │
                    │ Service     │  │ (WS/SSE)   │  │ (FCM/APNs)    │
                    └─────────────┘  └────────────┘  └───────────────┘
                                                              │
                                                     ┌────────▼───────┐
                                                     │ Payment Service│
                                                     │ (Stripe/PSP)   │
                                                     └────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Restaurant Discovery (Geo Search)

**Do we need custom geo-index?** Yes — "show me restaurants within 5km" is the core query.

**Options:**
| Approach | How | Trade-off |
|----------|-----|-----------|
| **Geohash** | Encode lat/lng → string prefix. Nearby = same prefix | Fast prefix scan in Redis/DB. Edge case: border cells need neighbor lookup |
| **QuadTree** | Recursive spatial partition | Better for non-uniform density, complex to update |
| **PostGIS** | `ST_DWithin(restaurant.geom, user.geom, 5000)` | Simple, battle-tested. Slower at extreme scale |
| **S2 Geometry** (Google) | Hilbert curve cell IDs | Used by Uber, very efficient. Complex impl |

**Choice:** PostGIS for correctness + Redis Geohash cache for hot zones.

```sql
-- Restaurant search query
SELECT r.*, ST_Distance(r.geom, ST_MakePoint($lng, $lat)::geography) AS dist
FROM restaurants r
WHERE r.is_active = true
  AND ST_DWithin(r.geom, ST_MakePoint($lng, $lat)::geography, $radius_meters)
ORDER BY dist
LIMIT 20;
```

**Cache strategy:** Cache restaurant list by geohash cell (5-char = ~5km×5km). TTL=60s. Restaurant updates invalidate relevant cells.

**Trade-off:** Stale restaurant data (60s) vs DB hit on every request. For restaurants, 60s staleness is acceptable. For menu availability, TTL=10s or use pub/sub invalidation.

---

### 5.2 Driver Matching

**First principles:** Assign the driver who minimizes: `pickup_time + prep_time_remaining`.

**Real-time driver location:**
```
Driver App → WebSocket → Location Service → Redis GEOADD drivers:available <lng> <lat> <driver_id>
```

**Matching algorithm:**
```
1. Order accepted → publish order.accepted{order_id, restaurant_geoloc, est_ready_time}
2. Driver Matching Service:
   a. GEORADIUS drivers:available <restaurant_lng> <restaurant_lat> 3km ASC COUNT 10
   b. Filter: driver.status == AVAILABLE
   c. Score = distance_to_restaurant + max(0, restaurant.prep_time - drive_time)
   d. Offer to top driver → 30s timeout → if declined, next driver
3. On accept: SET driver.status = BUSY, ZREM from available set
```

**Why not global optimizer?** Too complex, too slow. Greedy nearest-driver works well in practice (proven by Uber/DoorDash).

---

### 5.3 Order State Machine

```
CART ──[place_order]──► PLACED ──[restaurant_accepts]──► ACCEPTED
                                                          │
                                              [driver_assigned]
                                                          │
                                                    DRIVER_ASSIGNED
                                                          │
                                              [driver_picks_up]
                                                          │
                                                    PICKED_UP
                                                          │
                                              [driver_delivers]
                                                          │
                                                    DELIVERED ──► [user_confirms/timeout] ──► COMPLETED
                                                    
Any state ──[cancel]──► CANCELLED (before PICKED_UP only)
Any state ──[fail]───► FAILED
```

**Storage:** Each transition = append-only event in `order_events` table + update `orders.status`.

```sql
CREATE TABLE orders (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  restaurant_id UUID NOT NULL,
  driver_id UUID,
  status VARCHAR(20) NOT NULL,
  total_amount DECIMAL(10,2),
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

CREATE TABLE order_events (
  id UUID PRIMARY KEY,
  order_id UUID REFERENCES orders(id),
  event_type VARCHAR(50),
  actor_id UUID,
  actor_type VARCHAR(20), -- user/restaurant/driver/system
  payload JSONB,
  created_at TIMESTAMPTZ
);
```

---

### 5.4 Real-time Tracking

**Do we need WebSocket?** Yes — polling every 5s creates 100K×12 = 1.2M RPS just for tracking. WebSocket maintains persistent connection.

```
Driver App ──[every 3s]──► Location Service (WebSocket)
                                │
                           Redis: GEOADD + publish to channel order:<order_id>
                                │
                           Tracking Service (subscribed to Redis pub/sub)
                                │
                           Push to User WebSocket connection
```

**Scale WebSocket servers:** Sticky sessions (consistent hash on order_id/user_id to WebSocket server). Redis pub/sub decouples producers from consumers.

**Fallback:** If WebSocket drops → SSE → Long polling (graceful degradation).

---

### 5.5 Payment

**Idempotency is critical.** Network failures can cause double-charges.

```
Client ──► Order Service
             │
          Generate idempotency_key = SHA256(order_id + user_id + amount)
             │
          Payment Service:
            1. Check: idempotency_key exists in DB? Return cached result.
            2. No? → Call PSP with idempotency_key header
            3. PSP confirms → Write to DB (order_id, idempotency_key, status, psp_ref)
            4. Publish payment.completed event
```

**Why capture on delivery?** Authorize at order placement → capture at delivery. Prevents charging for cancelled/failed orders.

---

### 5.6 Menu Cache Design

```
Restaurant updates menu
        │
        ▼
Menu Service writes to Postgres
        │
        ├──► Invalidate Redis key: menu:{restaurant_id}
        └──► Publish to Kafka: menu.updated{restaurant_id}
                │
          CDN edge cache invalidation (for web)
```

**Read path:** Redis first → miss → Postgres → write-through to Redis (TTL=300s).

**Trade-off:** Stale menu vs always-fresh.
- 5min TTL = user might order unavailable item → handle with "item unavailable" error at order placement (re-check from DB at order time, not from cache).

---

## 6. Key Trade-offs Summary

| Decision | Choice | Why | Cost |
|----------|--------|-----|------|
| Geo search | PostGIS + Redis Geohash cache | Correctness + speed | Stale 60s restaurant list |
| Tracking | WebSocket + Redis pub/sub | Low latency push | Complex infra, sticky routing |
| Driver matching | Greedy nearest | Simple, fast, good enough | Not globally optimal |
| Payment timing | Auth-on-order, capture-on-deliver | No charge for failed orders | Auth holds may expire |
| Menu cache | Write-through, TTL=5min | Read performance | Item might be stale |
| Order storage | Append-only events | Full audit trail, debugging | Storage cost, complex queries |
| Driver location | Redis GEOADD | O(log N) geo queries | In-memory cost for 1M drivers |

---

## 7. Capacity Estimates

```
Users: 50M active, 5M daily orders
Peak: 5x = 25K orders/min = ~420 orders/sec

Driver locations: 1M drivers × 1 update/3s = 333K writes/sec to Redis
Tracking reads: 5M active orders × 1 push/3s = ~1.7M pushes/sec (WebSocket, not HTTP)

Storage:
- Orders: 5M/day × 365 × 500B = ~900GB/year
- Location history: 333K/s × 100B = 33MB/s = ~1TB/day (keep 24h only = 1TB rolling)
- Menu catalog: 500K restaurants × 100 items × 1KB = 50GB (fits in Redis)
```

---

## 8. Real-World References

- **DoorDash Engineering Blog:** [Geo Search](https://doordash.engineering/2022/09/14/building-a-service-mesh-for-doordash/) — uses S2 geometry, Cassandra for driver locations
- **Uber Eats:** GraphQL BFF pattern for menu aggregation
- **Swiggy:** [Hyperlocal delivery](https://bytes.swiggy.com/) — Redis GEORADIUS, Kafka for order events
- **Stripe:** Idempotency keys pattern for payment retries
