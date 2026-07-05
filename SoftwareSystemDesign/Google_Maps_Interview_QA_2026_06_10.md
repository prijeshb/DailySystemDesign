# Google Maps — Interview Q&A
**Date:** 2026-06-10

---

## How Interviewers Open This Question

- "Design a simplified version of Google Maps."
- "Design a navigation system. Focus on routing."
- "How would you design a system to find the shortest route between two points at scale?"
- "Design Google Maps — start with whatever you think is most important."

**Your opening move:** Clarify scope.
> "Google Maps is huge. Should I cover all of: routing, map tiles, geocoding, real-time traffic? Or focus on one area like routing with traffic?"

Interviewer will usually say "start high-level, then go deep where you find it interesting." Always go deep on routing — it's the core.

---

## Concept 1: Road Graph & Routing

**Q: Why can't you just use Dijkstra on the full road graph?**

A: The road network has 300M nodes and 700M edges. Plain Dijkstra explores too many nodes — cross-country queries take 2-5 seconds. Users expect < 500ms. We need Contraction Hierarchies (CH): precompute shortcuts offline so that at query time, a bidirectional search finds the path visiting only ~10K nodes instead of 300M. Query time drops to ~5ms.

**Q: What is Contraction Hierarchies? Can you explain it simply?**

A: Two ideas: (1) Rank every intersection by "importance" — how often it appears on shortest paths. A highway interchange is important; a dead-end residential street is not. (2) Contract unimportant nodes: if the shortest path A → u → B exists (via u) and u is being removed, add a shortcut edge A → B directly. After preprocessing, a query runs upward on both sides (source and destination), only visiting more important neighbors. The two upward searches meet at the most important node(s) on the path.

**Q: How do you handle real-time traffic if CH shortcuts are precomputed for static speeds?**

A: CH precomputation uses base speeds (historical by time-of-day). At query time, apply a penalty overlay — multiply edge weights for congested segments. The shortcut structure is still valid; you just reweight the edges it skips through. Full re-preprocessing only happens when the road network itself changes (new road, permanent closure) — done nightly. Traffic weight updates happen every 30 seconds via the overlay.

**Follow-up: What's the trade-off of this overlay approach vs recomputing CH with live traffic?**

A: The overlay introduces approximation — the shortcuts were contracted assuming base speeds, but the actual shortest path through those shortcuts may not be optimal when traffic weights change significantly. In practice, this error is small (<5% route quality) for moderate congestion. For extreme cases (flooded highway, total closure), incidents are modeled as edge removal (weight = ∞), not just speed reduction, which CH handles correctly.

---

## Concept 2: Map Tiles

**Q: How does Google Maps serve map tiles to billions of users?**

A: The world is divided into a tile pyramid at different zoom levels. Zoom 0 = 1 tile (whole earth). Zoom 15 = 32,768 × 32,768 tiles (street level). Each tile = 256×256 or 512×512 pixel image (or vector data). 

Pipeline: Map data → Tile Renderer → S3 (origin store) → CDN edge nodes globally. Clients request tiles by (z, x, y) coordinate. CDN cache-hit rate >99% for popular areas. Stale tiles (after map update) invalidated via cache-control max-age and version suffixes.

**Q: Vector tiles vs raster tiles — what's the difference?**

A: Raster = pre-rendered PNG/WebP images. Simple to serve, works everywhere. Problem: each zoom level needs separate images; style changes require full re-render.

Vector = send raw geometry (roads, polygons, labels as coordinates). Client renders using WebGL. Advantages: smooth zoom (render at any zoom level), smaller transfer size, client-side style customization (dark mode, custom colors). Trade-off: requires powerful client (modern phone); older/low-end devices need raster fallback.

**Follow-up: How do you handle map data updates (new road, renamed street)?**

A: Two paths: (1) Daily batch: re-render affected tiles, push to S3, invalidate CDN for those (z,x,y) keys. (2) Real-time: vector tiles allow client-side overlay updates without full re-render. For routing, incident overlays (road closed, construction) take effect immediately via the traffic overlay layer — no tile re-render needed.

---

## Concept 3: Real-Time Traffic

**Q: How do you collect real-time traffic data?**

A: Primary source = "probe data" from active navigation users. Every 3-5 seconds, client sends (lat, lng, speed, heading). With 1B users, ~20M active navigators at peak = 5M location updates/second. This tells us actual travel speeds on every road segment right now. Secondary: government road sensors (sparse), incident reports (users + AI cameras).

**Q: Why use Kafka for location updates? Why not write directly to Cassandra?**

A: Several reasons: (1) Decoupling — ingestion service doesn't need to know about Cassandra schema. (2) Backpressure — if Cassandra is slow, Kafka buffers without dropping updates. (3) Fan-out — same location stream used for traffic aggregation AND for live tracking (share-my-trip) AND for analytics. Kafka lets multiple consumers independently process the same stream. (4) Replay — if Flink aggregator has a bug, replay the Kafka topic to recompute.

**Follow-up: How do you aggregate 5M location updates/sec into edge-level speeds?**

A: Flink streaming job. For each road edge, collect all probes whose location is on that edge (map-matching: snap GPS to nearest road edge). In a 1-minute tumbling window, compute median speed of all probes on that edge. Apply exponential smoothing: `new_speed = 0.7 × previous + 0.3 × observed_median`. Write to Cassandra. Routing servers cache traffic data in memory (30s TTL) to avoid per-query DB reads.

---

## Concept 4: Geocoding

**Q: How does geocoding work at scale?**

A: Geocoding = address → (lat, lng). Build an index: take all addresses from map data (OSM, government datasets), normalize (expand abbreviations, standardize format), store in Elasticsearch with geo_point field. On query: multi-match search across street, city, state fields + ranking by relevance + geographic bias (results near user's current location ranked higher). ES handles fuzzy matching for typos.

**Q: How does reverse geocoding work?**

A: (lat, lng) → nearest address. PostGIS: `ST_DWithin(geom, point, 100m)` + order by distance, limit 1. Cached heavily in Redis: round coordinates to 4 decimal places (~11m precision), use as cache key. Cache-hit rate is very high since same point is reverse-geocoded many times (POI, landmarks).

---

## Concept 5: ETA

**Q: How accurate is ETA and how do you compute it?**

A: Naive: sum(edge_length / current_speed). Real: ML model trained on billions of historical trips. Features include time of day, day of week, current traffic, weather, events near route. Output: P50 (median expected) and P90 (90th percentile) ETAs. UI shows P50 with a range hint.

**Q: Why show P90 at all?**

A: User trust. If you always show P50 and the user arrives late 50% of the time, they distrust the app. Showing "usually X, might take up to Y" sets accurate expectations. Airlines learned this: they pad flight times to show "on time" more often.

---

## Scaling / Design Follow-ups

**Q: How do you shard the routing service?**

A: Options: (1) Replicate full graph to all routing servers (stateless — any server handles any query). Simple but memory-heavy (~50GB per instance). (2) Geographically shard — India routing server only. Cross-region queries need inter-server calls. In practice, use hierarchical: regional graph for local detail + continental graph for long-haul, stitched at region boundaries.

**Q: How does Google Maps handle the route while you're driving (rerouting)?**

A: Client tracks GPS position every second. Compares to expected position on route. If deviation > 50m for 5+ seconds (not GPS noise) → trigger re-route request. Re-route = new routing query from current position. Prefetch likely next routes in background (if user might miss a turn, compute backup route before the miss happens).

**Q: What if 10 million people all search for routes at the same time (New Year's Eve)?**

A: (1) Rate limiting at API Gateway with queue + 202 Accepted response. (2) Result caching: if many users request same A→B route at same time, cache result for 60s. (3) Pre-scale based on event calendar. (4) Client-side jitter on retries to avoid thundering herd. (5) Degraded mode: offer last-cached route from Redis if routing service overloaded.

**Q: How do you handle a major road closure discovered in real-time?**

A: (1) Incident reporting: first few users who hit the closure report it (or AI camera detects stopped traffic). (2) Incident service marks edge weight = ∞ (impassable) in traffic overlay. (3) Routing servers read updated overlay on next traffic refresh (30s). (4) Users currently navigating that route get rerouted automatically on next navigation refresh. (5) Map tile update: closure marker rendered and pushed to CDN. Full chain from first report to rerouted users: ~2-5 minutes.

---

## Trade-off Questions (Interviewers Love These)

**Q: SQL or NoSQL for map data (nodes, edges)?**

A: Graph data (nodes, edges with spatial attributes) → PostGIS (PostgreSQL + spatial extension). Supports `ST_DWithin`, `ST_Distance`, R-tree spatial indexes. The road graph is preloaded into memory for CH — PostGIS is just the authoritative store, not on the hot query path. Traffic (edge_id → speed, write-heavy, simple key access) → Cassandra. Geocoding index → Elasticsearch.

**Q: Should you use a dedicated graph database (Neo4j) for the road network?**

A: No, for routing. CH requires precomputed shortcuts — graph DBs don't support this. CH is a custom algorithm run on a custom in-memory graph structure. PostGIS stores the raw data; the routing engine loads it into its own memory-optimized structure. Graph DBs are good for social networks (traversals of arbitrary depth) but not for shortest-path routing at this scale.

**Q: What's the cost of serving 500M tile requests/second?**

A: Tiles served from CDN edge — effectively free per-request (CDN pricing is bandwidth, not per-request). CDN bandwidth: 500M × 15KB = 7.5 TB/s. At $0.01/GB CDN bandwidth = $75,000/hour = significant cost. In practice, CDN negotiates bulk pricing. S3 origin hits are <1% of total = 5M requests/s to origin — aggressive caching keeps this manageable. Real optimization: vector tiles are 3-10x smaller than raster → 3-10x CDN cost reduction.
