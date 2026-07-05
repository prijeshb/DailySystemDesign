# Google Maps — Navigation & Mapping System
**Date:** 2026-06-10  
**Difficulty:** Hard  
**Real-world refs:** Google Maps, Apple Maps, HERE Maps, OpenStreetMap, Mapbox, Ola Maps

---

## 0. First Principles — Do We Even Need This?

**The problem:** A user at (lat, lng) wants to get to a destination. They need: the fastest route, turn-by-turn instructions, real-time traffic, and an accurate ETA.

**Why not just store routes in a database?**
- Earth's road network = ~300M nodes, ~700M edges. Routes are dynamic (traffic, incidents). You can't precompute every pair (300M² = impossible).
- Routes must be computed on-demand from a graph, not fetched from a table.

**Why not just use straight-line distance?**
- Roads curve, have speed limits, turn restrictions, one-ways. Straight-line (Haversine) is useless for navigation. You need a road network graph.

**Why is ETA hard?**
- Speed on a road segment varies by time-of-day, day-of-week, weather, incidents. Static speed limits give poor ETAs. Real Google Maps uses historical + live traffic ML models.

**Core constraints:**
1. **Scale:** 1B users/day, ~50M concurrent route requests
2. **Accuracy:** Route must be driveable. ETA within ±5 minutes 90% of the time
3. **Freshness:** Traffic data updates ≤ 30s lag
4. **Map updates:** New roads, closures propagate within minutes
5. **Offline support:** Users must navigate with cached tiles when network is gone

---

## 1. Scope & Requirements

### Functional
- Geocoding: address → (lat, lng)
- Reverse geocoding: (lat, lng) → address
- Route calculation: A to B, optionally via waypoints, multiple modes (drive, walk, transit, bike)
- Real-time traffic overlay
- Turn-by-turn navigation (step-by-step instructions)
- Map tile rendering (visual map)
- Place search (restaurants, ATMs near me)
- ETA calculation with traffic
- Incident reporting (user-contributed: accidents, closures)

### Non-Functional
- Route computation < 500ms p99 for cross-country queries
- Map tiles load < 200ms (cached)
- Location update from driver → reflected in traffic layer < 30s
- 99.99% uptime for routing
- Tiles served from CDN edge globally

### Out of Scope
- Street View, satellite imagery pipeline
- Indoor navigation
- Public transit GTFS parsing (treat as given data source)
- 3D building rendering

---

## 2. Entities & Actions

### Entities

```
Node          id, lat, lng, type (intersection | endpoint)
Edge          id, from_node, to_node, road_name, length_m, 
              speed_limit_kmh, road_type, direction (oneway/both),
              turn_restrictions: []
Road          collection of edges forming a named road
Place         id, name, lat, lng, category, address
Tile          z/x/y, rendered image, last_updated
TrafficSegment  edge_id, current_speed, historical_speed[hour][dow], incident_flag
Route         origin, destination, waypoints, steps[], total_distance, eta
UserLocation  user_id, lat, lng, heading, speed, timestamp
```

### Actions

```
Geocode(address)                    → (lat, lng)
ReverseGeocode(lat, lng)            → address
GetRoute(origin, dest, mode, time)  → Route
GetTile(z, x, y)                   → image (PNG/WebP)
SearchNearby(lat, lng, category)    → []Place
UpdateLocation(user_id, lat, lng)   → void  [background, from navigation sessions]
ReportIncident(lat, lng, type)      → void
```

---

## 3. Data Flow

```
User opens app
  → Client renders map tiles (CDN, cached)
  → Client geocodes destination (Geocoding Service)
  → Client requests route (Routing Service)
      → Routing fetches current traffic weights (Traffic Service)
      → Routing runs Contraction Hierarchies query on road graph
      → Returns polyline + turn steps + ETA
  → Client begins navigation
      → Client sends location updates every 3-5s → Location Ingestion Service
          → Kafka topic: location_updates
          → Traffic Aggregator consumes → updates edge speeds
          → Speed updates → written to Traffic DB
      → Reroute triggered if user deviates > 50m from route

Background:
  Map data pipeline: OSM / proprietary survey → Graph Builder → Spatial DB → precompute CH shortcuts
  Tile rendering pipeline: Map data → Tile Renderer (Mapnik) → Tile Store (S3) → CDN
```

---

## 4. High-Level Design

```
                         ┌──────────────┐
  Client ──────────────► │  API Gateway │
                         └──────┬───────┘
           ┌────────────────────┼────────────────────┐
           ▼                    ▼                    ▼
  ┌──────────────┐    ┌──────────────────┐   ┌────────────────┐
  │  Geocoding   │    │  Routing Service │   │  Tile Service  │
  │  Service     │    │  (CH algorithm)  │   │  (CDN-backed)  │
  └──────┬───────┘    └────────┬─────────┘   └───────┬────────┘
         │                     │                      │
         ▼                     ▼                      ▼
  ┌────────────┐    ┌──────────────────┐     ┌───────────────┐
  │ Geocoding  │    │  Road Graph      │     │  Tile Store   │
  │ Index (ES) │    │  (In-memory CH)  │     │  (S3 + CDN)   │
  └────────────┘    │  + Traffic Layer │     └───────────────┘
                    └────────┬─────────┘
                             │ reads edge weights
                    ┌────────▼─────────┐
                    │  Traffic Service  │
                    └────────┬──────────┘
                             │
                    ┌────────▼──────────┐
  Location Updates  │  Kafka            │◄── Location Ingestion
  from nav sessions └────────┬──────────┘
                             │
                    ┌────────▼──────────┐
                    │ Traffic Aggregator│
                    │ (Flink)           │
                    └────────┬──────────┘
                             │
                    ┌────────▼──────────┐
                    │  Traffic DB       │
                    │  (Cassandra:      │
                    │  edge_id → speed) │
                    └───────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Road Graph & Contraction Hierarchies

**Why not plain Dijkstra?**
- Road graph: 300M nodes, 700M edges
- Plain Dijkstra on full graph: ~2-5 seconds per cross-country query
- Users expect < 500ms → need precomputed shortcuts

**Contraction Hierarchies (CH):**
```
Offline preprocessing (run once, takes hours):
  1. Rank all nodes by "importance" (how often they appear in shortest paths)
  2. Contract least important nodes: for each node u being contracted,
     if shortest path A → u → B exists and no shorter path exists:
       add shortcut edge A → B directly
  3. Result: ~10x more edges but now a two-phase bidirectional query runs in ~5ms

Online query:
  1. Upward search from source (only visit higher-ranked neighbors)
  2. Upward search from destination (same)
  3. Meeting point = shortest path
  Complexity: ~10K nodes visited vs 300M in full Dijkstra
```

**Traffic-aware routing:**
```
Each edge has weight = travel_time_seconds
  travel_time = (length_m / 1000) / current_speed_kmh * 3600

current_speed_kmh = f(historical_speed[hour_of_day][day_of_week], 
                      live_speed_from_probes,
                      incident_flag)

CH precomputed shortcuts use base speeds.
At query time: penalty overlay applied on top of shortcuts for heavy traffic.
```

**Trade-off:** CH requires re-preprocessing when road network changes (new road, closure). Full rebuild = hours. Incremental updates (LCH — Live CH) exist but complex. In practice: rebuild nightly, apply live overlay for traffic-only changes during day.

### 5.2 Map Tiles

**Tile coordinate system:**
```
Zoom level z: world divided into 2^z × 2^z tiles
  z=0: 1 tile (whole world)
  z=15: 32768 × 32768 = ~1B tiles (street level)
  z=18: house-level

Tile ID: (z, x, y)
  x = floor((lng + 180) / 360 × 2^z)
  y = floor((1 - ln(tan(lat×π/180) + 1/cos(lat×π/180)) / π) / 2 × 2^z)
```

**Tile rendering pipeline:**
```
Map data (PostGIS / OSM) 
  → Tile Renderer (Mapnik/MapboxGL)
  → Rendered PNG/WebP tiles
  → S3 (origin store, all zoom levels)
  → CloudFront CDN (edge cached by (z,x,y))

Client requests tile (z=15, x=23456, y=12345):
  1. CDN hit? → return tile (~1ms)
  2. CDN miss → S3 → CDN populates → return (~50ms)
  3. S3 miss (very new road) → render on-demand → store → return (~200ms)
```

**Storage estimate:**
```
z=0..14: pre-rendered, ~500GB total (zoom 0-14 not many unique tiles)
z=15..18: on-demand or pre-rendered for popular regions
Total pre-rendered: ~10TB
Active CDN cache: ~1TB (only commonly accessed regions/zoom levels)
```

**Trade-off:** Vector tiles (serve data, render on client) vs Raster tiles (pre-rendered PNG).
- Vector: smaller transfer, client-side style customization, smooth zoom. Used by modern Google Maps.
- Raster: simpler server, works on weak devices, predictable quality.
Modern approach: vector tiles for mobile/desktop, raster fallback for older clients.

### 5.3 Geocoding & Reverse Geocoding

```
Address → (lat, lng):
  Structured: "123 MG Road, Bengaluru" → parse components → index lookup
  Unstructured: "pizza near terminal 1" → NLP parse → place search + ranking

Implementation:
  1. Address Index: Elasticsearch with geo_point field
     - Indexed fields: street, city, state, country, pincode, lat, lng
     - Query: multi-match + function_score (boost exact match > partial)
  2. Fuzzy matching: ES handles typos, abbreviations
  3. Normalization: "Rd" → "Road", "St" → "Street" preprocessing

Reverse geocoding (lat, lng) → address:
  PostGIS query:
    SELECT name, address
    FROM places
    WHERE ST_DWithin(geom, ST_MakePoint(lng,lat)::geography, 100)  -- 100m radius
    ORDER BY ST_Distance(geom, ST_MakePoint(lng,lat)) 
    LIMIT 1;
  
  Cached aggressively: same lat/lng requested many times (POI, landmark)
  Cache key: round to 4 decimal places (11m precision)
```

### 5.4 Real-Time Traffic

```
Data sources:
  1. Probe data: active navigation users send GPS every 3-5s
  2. Sensor loops: government highway sensors (some cities)
  3. Incident reports: user-reported + camera AI detection

Location Ingestion Service:
  Client → gRPC (binary, efficient) → Location Ingestion Service
        → Kafka topic: location_updates (keyed by edge_id)

Traffic Aggregator (Flink):
  Input: location_updates stream
  Per edge, 1-minute tumbling window:
    - Collect all (user_id, speed, heading) probes on that edge
    - Median speed of probes with matching heading
    - Apply smoothing: new_speed = 0.7 × current_speed + 0.3 × probe_median
  Output: edge_id → current_speed → write to Cassandra

Schema (Cassandra — write-heavy, known access pattern):
  CREATE TABLE traffic (
    edge_id    BIGINT,
    updated_at TIMESTAMP,
    speed_kmh  FLOAT,
    confidence INT,       -- number of probes contributing
    PRIMARY KEY (edge_id)
  );

Routing Service reads traffic:
  - Bulk fetch needed edges before query (< 5ms from Cassandra at low latency)
  - Or: pre-load entire traffic table into routing service memory (10M edges × 8 bytes = 80MB — fits)
  - Traffic cache in routing service: 30s TTL
```

**Trade-off of caching traffic in routing service memory:**
- Pro: Eliminates per-query Cassandra reads, faster routing
- Con: Stale up to 30s (acceptable: traffic changes don't invalidate 30s-old routes meaningfully)
- Con: Large memory footprint per routing server instance → use routing service with ≥ 512MB heap

### 5.5 ETA Calculation

```
Naive ETA:
  sum(edge_length / current_speed) for all edges on route

Real ETA (ML approach, used by Google):
  Features:
    - time_of_day, day_of_week, month
    - historical travel time for this route at this time
    - current traffic on each segment
    - weather (rain → +20% travel time)
    - event detection (stadium near route)
  
  Model: Gradient boosting (GBDT) trained on billions of historical trips
  Output: P50 ETA, P90 ETA (show P50, use P90 for "might take longer" warning)
  
  For interview: simple model is fine:
    ETA = Σ (edge_length / congested_speed) + stop_penalty × num_signals
    congested_speed = speed_limit × congestion_factor[hour_of_day]
```

### 5.6 Place Search (Nearby)

```
User: "coffee near me" at (lat=12.97, lng=77.59)
  1. Parse intent: category=coffee_shop
  2. Geo query: 
     SELECT * FROM places 
     WHERE category='coffee_shop' 
       AND ST_DWithin(geom, ST_MakePoint(77.59, 12.97), 2000)  -- 2km
     ORDER BY rating DESC, ST_Distance(...) ASC
     LIMIT 20;
  3. Cache: GEORADIUS from Redis (places indexed in Redis GEO by category)
     GEORADIUS places:coffee_shop 77.59 12.97 2 km ASC COUNT 20

Indexing for search:
  - Elasticsearch for text search + geo: handles "coffee near me" + "Starbucks Brigade Road"
  - Redis GEO for purely proximity queries (no text)
  - PostGIS for complex spatial queries (polygon containment, route-aware search)
```

---

## 6. Capacity Estimates

```
Users: 1B DAU, 50M concurrent during peak

Location updates:
  20M active navigators × 1 update/4s = 5M updates/sec
  Each update = 50 bytes → 250MB/s ingest
  Kafka: 250MB/s easily handled by 5-10 brokers

Routing requests:
  50M concurrent × avg 1 route/30min = ~28K routes/sec
  Each CH query: ~5ms → 28K × 0.005s = 140 server-seconds/sec
  At 1 CPU/server → ~140 routing server cores

Tile requests:
  50M concurrent × ~10 tiles loaded/s (as user pans) = 500M tile requests/sec
  CDN handles this; origin hit rate < 1% (tiles change slowly)
  CDN bandwidth: 500M × 15KB avg = 7.5 TB/s (distributed across CDN edge)

Traffic DB (Cassandra):
  ~300M road segments globally
  Write: 5M location updates → ~5M edge speed updates/sec (fan-out from probe to edge)
  Actually: 10M "active" edges at any time
  Read: routing service pre-loads all — mostly bulk reads, not per-query
```

---

## 7. Key Trade-off Decisions

| Decision | Choice | Why | Trade-off |
|----------|--------|-----|-----------|
| Routing algorithm | Contraction Hierarchies | <5ms cross-country query | Preprocessing takes hours; rebuild on road changes |
| Traffic storage | Cassandra (edge_id → speed) | Write-heavy, known access pattern | No complex queries; can't JOIN with road data |
| Tile storage | S3 + CDN | Infinite scale, globally distributed | Stale tiles if map data changes (TTL = 24h for most tiles) |
| Geocoding | Elasticsearch | Fuzzy matching, fast text search | Not a primary DB; must sync from source of truth |
| Location ingest | Kafka + Flink | Decouple ingest from processing, replay | Adds latency (~5-30s end to end for traffic update) |
| Road graph | In-memory on routing servers | µs access for CH queries | Memory-heavy (~50GB per server); horizontal scaling needed |
| Tile format | Vector tiles | Small size, client renders at zoom | Requires capable client; older devices need raster fallback |

---

## 8. Scalability Patterns

**Routing service sharding:**
```
Option A: Stateless (each server holds full graph in memory)
  - All CH query servers hold same 50GB graph
  - Route any query to any server
  - Trade-off: 50GB RAM per routing instance; graph update = rolling restart

Option B: Geographic sharding
  - Region-based partitioning: India routing server only has India graph
  - Cross-region queries: route through multiple servers, stitch results
  - Trade-off: Cross-region (Delhi → London) needs handoff logic
```
**Used in practice:** Hierarchical approach — continental graph (coarse) for long-haul + regional graph (detailed) for local legs.

**Traffic hotspot handling:**
```
A highway with 10M probes/hour → one Kafka partition gets overloaded
Fix: partition by geographic cell (H3/S2 cell_id), not edge_id
  → even distribution, no single hotspot
```
