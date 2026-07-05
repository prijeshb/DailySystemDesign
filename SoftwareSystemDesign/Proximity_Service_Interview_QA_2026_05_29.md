# Proximity Service — Interview Q&A
*Date: 2026-05-29*

---

## Opening Questions (Scoping)

**Q: Walk me through how you'd design Yelp.**

Strong answer structure:
1. Clarify scale (users, businesses, QPS, radius range)
2. Define entities and APIs first
3. Jump to the core problem: spatial indexing
4. Present geohash vs quadtree trade-off before picking
5. Show failure awareness: "what if Redis is down during peak?"

---

## Core Concepts

**Q: Why can't you just use a SQL `WHERE lat BETWEEN x AND y AND lng BETWEEN a AND b`?**

A: You can, with a 2D index, and it actually works for small scale. But:
- A bounding box isn't a circle — you return ~27% extra records outside the actual radius
- Standard B-tree indexes can only efficiently filter on one dimension at a time
- At 500M records across millions of queries, you need purpose-built geospatial indexing
- PostGIS GiST indexes or geohash solve this; raw lat/lng range scans don't scale

---

**Q: Explain geohashing and its limitations.**

A: Geohash encodes lat/lng into a base-32 string. Adjacent strings share prefixes = geographic proximity. Useful because you can store it as a VARCHAR and query by prefix.

Limitations:
1. **Boundary problem:** Two points inches apart can have completely different geohashes if they're on a cell boundary. Fix: always query all 8 neighbors.
2. **Uneven cell size:** At high latitudes, cells shrink in east-west direction — same precision geohash covers less area near poles.
3. **Wraparound:** The 180th meridian (international date line) isn't handled — geohash "jumps" there.

---

**Q: Geohash vs Quadtree — when would you pick each?**

| | Geohash | Quadtree |
|-|---------|----------|
| Density adaptation | No — fixed cell size | Yes — splits dense areas deeper |
| Redis support | Native (GEOADD) | Custom implementation |
| Shard key | Natural (prefix) | Complex |
| Update cost | O(1) update | Potential tree rebalance |
| **Pick when** | Uniform-ish density, need Redis | Extreme density variance |

For Yelp: Geohash. For a mapping system with extreme rural/urban variance: Quadtree or S2.

---

**Q: How does Redis GEORADIUS work internally?**

A: It uses geohash encoding internally:
1. Encode center point to geohash
2. Calculate which geohash cells overlap the radius
3. Scan those cells in a sorted set (score = geohash integer)
4. Return members within radius (post-filters with exact distance)

Time complexity: O(N + log(M)) where N = results, M = total members in search area.

---

## Scaling & Sharding

**Q: How would you shard 500M businesses?**

Two strategies:

**By business_id (hash sharding):**
- Even distribution, simple routing
- Con: a "find businesses in NYC" query hits all shards

**By geographic region (range sharding on geohash prefix):**
- "find near NYC" hits only NYC shard → better cache locality
- Con: uneven load (NYC shard 100x busier than Montana shard)

**Best answer:** Hash shard the business metadata DB (frequent point lookups). Geographic shard the geospatial index (range queries). Sub-shard hot geographic cells when detected.

---

**Q: A user in Manhattan is doing 10K searches/second on one geohash cell. How do you handle it?**

A: Layered approach:
1. **Redis cache** for that geohash+category combo — one DB query serves thousands of requests
2. **CDN caching** for truly static queries (e.g., "restaurants in Times Square" — changes slowly)
3. **Read replicas** on the geospatial DB for the hot shard
4. **Rate limiting** per user/IP to prevent abuse
5. Long-term: shard the hot cell into sub-cells

---

## Failure Scenarios

**Q: Redis goes down. What happens?**

A: Cache miss on all search requests → they hit the geospatial DB directly → DB overwhelmed.

Mitigation:
1. Redis Cluster: 1 node down doesn't kill the cluster
2. Circuit breaker: if DB error rate spikes, fast-fail search requests (503 is better than cascade)
3. Request coalescing: lock on cache key, only one goroutine fetches from DB, others wait
4. Fallback: serve slightly stale results from a backup cache with longer TTL

---

**Q: A business updates its location. How do you ensure the geospatial index reflects this?**

A: 
1. Write new location to Business DB (synchronous, source of truth)
2. Publish update event to Kafka via **outbox pattern** (same DB transaction)
3. Index Updater consumer reads event:
   - DELETE old geohash entry
   - INSERT new geohash entry  
   - Must be idempotent (UPSERT by business_id)
4. If Kafka is down, outbox retries until delivered
5. Eventual consistency window: typically <1s in normal operation

---

**Q: How do you handle a business that's on the boundary of a geohash cell?**

A: Always query 9 cells (current + 8 neighbors). Then post-filter with exact haversine distance to eliminate false positives from the corners. The extra 8 queries are cheap in Redis (pipelined).

---

## Design Deep-Dives

**Q: How would you make search results personalized?**

A:
1. Return top-100 candidates from geospatial index (by distance)
2. Pass to ranking service with user context (history, preferences, time of day)
3. ML model scores each candidate: `f(distance, rating, category_match, user_affinity)`
4. Return top-20 re-ranked results

**Trade-off:** Personalization requires user history storage + ML infra. Start with distance + rating. Add personalization when you have enough data and the latency budget.

---

**Q: How would you handle search for "coffee shops open right now"?**

A: Two approaches:

**Approach A: Filter in DB**
- Store `open_hours` as structured data per day
- Post-filter results: `is_open_now(business.hours, current_time)`
- Pro: accurate. Con: can't cache (time-sensitive)

**Approach B: Pre-compute open status**
- Every minute, run job to compute `is_open` flag per business
- Store in Redis: `business:123:is_open = true`
- Cache search results with 60s TTL (max 60s stale on open/closed status)
- Pro: fast. Con: up to 60s lag on open/close transitions

For Yelp: Approach B is fine. For food delivery (DoorDash), Approach A with fresh data matters more.

---

**Q: How would you add a "trending nearby" feature?**

A: 
1. **Event stream:** Every business view/click → Kafka topic
2. **Stream processor (Flink):** Sliding window count per geohash cell per hour
3. **Trending store:** Redis sorted set per geohash cell: `ZADD trending:9q8y <count> <business_id>`
4. **API:** `GET /trending?geohash=9q8y` → `ZREVRANGE` top 10

**Challenge:** Geohash cells are fixed size — trending in a large cell might be dominated by a popular mall. Consider using a multi-resolution approach (query cell + parent cell + grandparent cell for sparse areas).

---

## Follow-up / Curveball Questions

**Q: How is this different from designing Uber's driver location tracking?**

A: Key difference is **write pattern**:
- Yelp: businesses rarely move (low write rate). Optimize for reads.
- Uber: drivers update location every 4 seconds (high write rate). Need a write-optimized spatial store.

For Uber: use Redis GEOADD with TTL per driver (auto-expire inactive drivers), dedicated write-heavy infrastructure, and don't cache driver locations (too stale too fast).

---

**Q: What's your database schema for storing business hours efficiently?**

```sql
-- Option A: JSONB (flexible)
open_hours JSONB  -- {"Mon": ["09:00","21:00"], "Tue": ...}

-- Option B: Separate table (queryable)
CREATE TABLE business_hours (
  business_id BIGINT,
  day_of_week SMALLINT,  -- 0=Sun, 6=Sat
  open_time   TIME,
  close_time  TIME,
  PRIMARY KEY (business_id, day_of_week)
);
```

Option A: simpler, good for display. Option B: allows DB-level "open now" queries.

---

**Q: How would you prevent spam businesses (fake locations)?**

A:
1. **Validation:** lat/lng must be within valid range; geohash must match claimed city
2. **Review moderation:** New businesses require verification (phone call, Google Maps cross-check)
3. **Anomaly detection:** Business that moves >100km in 1 hour → flag for review
4. **User reports:** "Business not here" signals → threshold → auto-flag → human review

---

## Self-Assessment Checklist

Before finishing your answer, verify you covered:
- [ ] Geospatial indexing choice with justification
- [ ] Geohash boundary problem solution (9-cell query)
- [ ] Cache layer and TTL reasoning
- [ ] What happens when each major component fails
- [ ] How writes propagate to the index (eventual consistency)
- [ ] How you'd scale a hot geographic shard
- [ ] Personalization trade-off (if prompted)
