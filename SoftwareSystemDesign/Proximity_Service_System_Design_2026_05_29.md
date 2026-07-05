# Proximity Service System Design
*Date: 2026-05-29 | Reference: Yelp, Google Places, Foursquare*

---

## First Principles Check

**Do we need a proximity service?**  
Yes — location queries ("restaurants near me") can't be answered efficiently with standard DB indexes. A plain `SELECT WHERE city = X` works at small scale but breaks at millions of businesses across unbounded geo space. We need spatial indexing.

**Core problem:** Given a user's lat/lng and a radius, return nearby businesses fast, at scale.

---

## 1. Entities

| Entity | Key Attributes |
|--------|---------------|
| Business | id, name, lat, lng, category, rating, open_hours, metadata |
| User | id, location (ephemeral), preferences |
| Review | id, business_id, user_id, rating, text, created_at |
| Search | user_id, lat, lng, radius, category, timestamp |

**What lives where:**
- Business location data → geospatial index
- Business metadata (name, hours, photos) → document store / relational DB
- Reviews → relational DB with business_id index
- User location → never stored permanently (privacy); only used per-request

---

## 2. Actions (APIs)

```
GET /search?lat=37.7&lng=-122.4&radius=500&category=restaurant&limit=20
→ List<BusinessSummary>

GET /businesses/{id}
→ BusinessDetail (name, hours, photos, rating)

POST /businesses/{id}/reviews
→ Create review

GET /businesses/{id}/reviews?page=1
→ Paginated reviews
```

**Scale targets (clarify in interview):**
- 500M businesses globally
- 100M DAU, peak 10K QPS search
- Search latency p99 < 100ms
- Business updates: ~1K/sec (add/edit/close)

---

## 3. Data Flow

```
User App
   │
   ▼
API Gateway (auth, rate limit)
   │
   ├──► Search Service ──► Geospatial Index (read)
   │         │
   │         └──► Business DB (fetch metadata for results)
   │
   ├──► Business Service ──► Business DB (CRUD)
   │         │
   │         └──► Index Update Queue ──► Geospatial Index (write)
   │
   └──► Review Service ──► Review DB
```

**Write path:** Business update → Kafka → async index update (eventual consistency OK for location data)  
**Read path:** Search Service → geospatial index (location filter) → Business DB (enrich with metadata) → return

---

## 4. High-Level Design

```
┌─────────────────────────────────────────────┐
│                  Clients                    │
└───────────────────┬─────────────────────────┘
                    │
            ┌───────▼────────┐
            │  API Gateway   │ (auth, rate limit, routing)
            └──┬──────┬──────┘
               │      │
    ┌──────────▼┐    ┌▼──────────────┐
    │  Search   │    │  Business     │
    │  Service  │    │  Service      │
    └──────┬────┘    └──────┬────────┘
           │                │
    ┌──────▼──────┐  ┌──────▼────────┐
    │ Geospatial  │  │  Business DB  │  (PostgreSQL + PostGIS
    │   Index     │  │  + Cache      │   or MySQL sharded)
    │ (Quadtree / │  └───────────────┘
    │  Geohash)   │
    └─────────────┘
           │
    ┌──────▼──────┐
    │  Location   │  (Redis cluster — hot area cache)
    │   Cache     │
    └─────────────┘
```

---

## 5. Low-Level Design

### 5.1 Geospatial Indexing — The Core Problem

**Option A: Geohash**
- Encode lat/lng into a base-32 string (e.g., `9q8yy`)
- Each character narrows the bounding box
- Length 6 ≈ 1.2km × 0.6km precision

```
Geohash length → Approx cell size
4  → 39km × 20km
5  → 4.9km × 4.9km
6  → 1.2km × 0.6km   ← use for 500m radius queries
7  → 153m × 153m
```

**Search algorithm:**
1. Compute geohash of user location at length 6
2. Get 8 neighboring cells (to avoid edge-of-cell misses)
3. Query index for `geohash LIKE 'prefix%'` in those 9 cells
4. Filter by exact distance (haversine formula)
5. Rank by distance + rating

**Trade-off:** Simple to implement, but cells are rectangular — edge queries need neighbor expansion. Uneven density (NYC vs Montana) means same-length geohash covers wildly different populations.

---

**Option B: Quadtree**
- Recursively subdivide space into 4 quadrants
- Leaf nodes: ≤ N businesses (e.g., 100)
- Dense areas get deeper trees (smaller cells)
- Sparse areas stay shallow

```
World
├── NW quadrant
│   ├── NW-NW (leaf: 23 businesses)
│   ├── NW-NE (split further — dense city)
│   │   ├── ... (depth 12+)
```

**Trade-off:** Adapts to density — great for urban areas. But harder to shard and update than geohash. Rebuilding the tree on business updates is expensive.

**Real-world choice:** Most systems (Yelp, Uber) use **geohash** for its simplicity and Redis compatibility. Quadtrees are better when density variance is extreme.

---

**Option C: PostGIS / S2 Geometry**
- PostgreSQL + PostGIS: native `ST_DWithin(point, center, radius)` with GiST index
- Google S2: hierarchical spherical geometry, used by Google Maps
- Trade-off: Correct sphere geometry (no edge distortion) but heavier operationally

**Recommendation for interview:** Use **Geohash + Redis** for search, **PostgreSQL with PostGIS** as source of truth. Geohash goes in Redis (hot reads), PostGIS handles precise queries and writes.

---

### 5.2 Data Storage

**Business Table (PostgreSQL, sharded by business_id):**
```sql
CREATE TABLE businesses (
  id          BIGINT PRIMARY KEY,
  name        VARCHAR(255),
  lat         DOUBLE PRECISION,
  lng         DOUBLE PRECISION,
  geohash6    CHAR(6),       -- pre-computed, indexed
  category    VARCHAR(50),
  is_active   BOOLEAN,
  updated_at  TIMESTAMP
);

CREATE INDEX idx_geohash ON businesses(geohash6) WHERE is_active = TRUE;
```

**Redis Geospatial (for hot queries):**
```
GEOADD businesses:restaurant <lng> <lat> <business_id>
GEORADIUS businesses:restaurant <lng> <lat> 500 m ASC COUNT 20
```

Redis `GEORADIUS` uses geohash internally — O(N+log(M)) where N is results, M is total. Very fast for hot categories in dense areas.

---

### 5.3 Caching Strategy

**What to cache:**
- Business metadata (name, rating, hours) → Redis/Memcached, TTL 10min
- Search results by geohash cell + category → Redis, TTL 60s
- "Popular nearby" static tiles → CDN-cacheable if query is standard

**Cache-aside pattern:**
```
Search Service:
1. Check Redis for geohash6 + category
2. HIT → return cached business IDs → enrich from business metadata cache
3. MISS → query geospatial DB → store in Redis → return
```

**Trade-off:** Cached results may show a business that just closed. Acceptable for Yelp (eventual consistency), unacceptable for emergency services. TTL is the knob.

---

### 5.4 Sharding

**Business DB:** Shard by `business_id` (consistent hashing). Simple, even distribution.  
**Geospatial index:** Shard by geohash prefix (geographic sharding).

```
Geohash prefix → Shard
9q (California)    → Shard-A
dr (New York)      → Shard-B  
u2 (London)        → Shard-C
```

**Problem with geo sharding:** Hot spots. NYC shard gets 100x more traffic than Montana shard.

**Solution:** Sub-shard hot cells. Shard-B (NYC) → Shard-B1 (Manhattan), Shard-B2 (Brooklyn), etc. Detect via traffic monitoring, migrate proactively.

---

### 5.5 Ranking

Simple: `score = w1 * (1/distance) + w2 * rating + w3 * review_count`

Advanced (post-MVP):
- Personalization: user history, clicked categories
- Boost open businesses
- Paid placement (separate auction layer)
- ML re-ranking over top-100 candidates

---

## 6. Key Trade-offs

| Decision | Option A | Option B | Why |
|----------|----------|----------|-----|
| Geo index | Geohash | Quadtree | Geohash simpler, Redis-native |
| Source of truth | PostgreSQL + PostGIS | Custom geospatial store | Proven, good tooling |
| Search cache | Redis GEORADIUS | No cache | 60s stale OK, 10x perf gain |
| Consistency | Eventual (async index update) | Strong | Location changes aren't critical |
| Shard key | Geo prefix | business_id | Locality = better cache hits |

---

## 7. Scale Estimations

```
500M businesses × 200 bytes = 100 GB (fits in memory with sharding)
10K QPS search × 50ms avg = manageable with 20 search service nodes
Peak: 10K QPS × 20 results × 100 bytes = 20 MB/s response bandwidth
Redis GEORADIUS: handles 50K QPS per node → 5 nodes for peak
```

---

*Files: System Design | Failure Analysis | Interview Q&A*
