# System Design Concepts Index
> Living reference. Updated as new concepts appear in daily designs.
> Use this before generating a new system design to pick the right tool for the job.

---

## HOW TO USE
- Each concept has: **What it is · When to use · When NOT to · Trade-offs · Real-world**
- Cross-referenced with systems where it appeared

---

## INDEX

1. [Geo Search](#1-geo-search)
2. [Database Choice: SQL vs NoSQL](#2-database-choice-sql-vs-nosql)
3. [Schema Design: Polymorphic vs Separate Tables](#3-schema-design-polymorphic-vs-separate-tables)
4. [Idempotency](#4-idempotency)
5. [Real-time Communication: WebSocket vs Polling vs SSE](#5-real-time-communication)
6. [Driver / Resource Matching](#6-driver--resource-matching)
7. [Routing Engine (OSRM vs Google Maps)](#7-routing-engine)
8. [Caching Patterns](#8-caching-patterns)
9. [Event-Driven Architecture (Kafka + Outbox)](#9-event-driven-architecture)
10. [State Machines for Orders/Workflows](#10-state-machines)
11. [Redis Data Structures Cheatsheet](#11-redis-data-structures-cheatsheet)
12. [OLTP vs OLAP Split](#12-oltp-vs-olap-split)
13. [TOCTOU — Atomic Check and Act](#13-toctou--atomic-check-and-act)
14. [Lambda vs Kappa Architecture](#14-lambda-vs-kappa-architecture)
15. [Time Windowing (Tumbling, Sliding, Session)](#15-time-windowing)
16. [Count-Min Sketch / Top-K Heavy Hitters](#16-count-min-sketch--top-k)
17. [Stream Processing Delivery Guarantees](#17-stream-processing-delivery-guarantees)
18. [HLS / Adaptive Bitrate Streaming](#18-hls--adaptive-bitrate-streaming)
19. [HyperLogLog — Approximate Unique Counting](#19-hyperloglog)
20. [CDN Pull vs Push](#20-cdn-pull-vs-push)
21. [Virtual Waiting Queue](#21-virtual-waiting-queue)
22. [Seat / Resource Hold Pattern](#22-seat--resource-hold-pattern)
23. [Order Book Data Structure](#23-order-book-data-structure)
24. [Total Order Sequencer](#24-total-order-sequencer)
25. [Write-Ahead Log (WAL)](#25-write-ahead-log-wal)
26. [Circuit Breaker — Market Halt (Financial)](#26-circuit-breaker--market-halt-financial)
27. [Contraction Hierarchies — Fast Graph Routing](#27-contraction-hierarchies--fast-graph-routing)
28. [Map Tile Serving (Z/X/Y Tile Pyramid)](#28-map-tile-serving)
29. [Database Replication & Replica Lag](#29-database-replication--replica-lag)
30. [Read-Your-Own-Writes (RYOW)](#30-read-your-own-writes-ryow)
31. [Cache Update Strategies for High-Write Systems](#31-cache-update-strategies-for-high-write-systems)
32. [Circuit Breaker — Microservices Pattern](#32-circuit-breaker--microservices-pattern)
32. [SFU vs MCU vs P2P (Video Conferencing Topology)](#32-sfu-vs-mcu-vs-p2p)
33. [WebRTC: ICE / STUN / TURN](#33-webrtc-ice--stun--turn)
34. [Simulcast + Congestion Control (TWCC/REMB)](#34-simulcast--congestion-control)
35. [Packet Loss Recovery: NACK / FEC / PLC](#35-packet-loss-recovery)
36. [Consistent Hashing + Virtual Nodes](#36-consistent-hashing--virtual-nodes)
37. [LSM Tree (Log-Structured Merge Tree)](#37-lsm-tree)
38. [Quorum Reads/Writes (R+W>N)](#38-quorum-readswrites)
39. [Vector Clocks](#39-vector-clocks)
40. [Gossip Protocol + Φ Failure Detector](#40-gossip-protocol)
41. [Merkle Tree Anti-Entropy](#41-merkle-tree-anti-entropy)
42. [Hinted Handoff](#42-hinted-handoff)
43. [Anycast Routing (BGP-based)](#43-anycast-routing)
44. [Multi-Tier CDN Cache Hierarchy](#44-multi-tier-cdn-cache-hierarchy)
45. [Request Coalescing (Thundering Herd Fix)](#45-request-coalescing)
46. [stale-while-revalidate / stale-if-error](#46-stale-while-revalidate--stale-if-error)
47. [Cache Key Normalization](#47-cache-key-normalization)
48. [Origin Shield (CDN Mid-Tier Cache)](#48-origin-shield)
49. [Inverted Index (Elasticsearch Internals)](#49-inverted-index)
50. [BM25 Scoring / TF-IDF](#50-bm25-scoring)
51. [Faceted Search / ES Aggregations](#51-faceted-search)
52. [Search Relevance Tuning (function_score, Boosts)](#52-search-relevance-tuning)
53. [Index Alias Pattern (Zero-Downtime Re-index)](#53-index-alias-pattern)
54. [Redis Sorted Set for Leaderboards](#54-redis-sorted-set-for-leaderboards)
55. [Windowed Leaderboard (TTL-based Key Rotation)](#55-windowed-leaderboard)
56. [Score Idempotency — ZADD GT/LT](#56-score-idempotency--zadd-gtlt)
57. [Feature Store (Online vs Offline)](#57-feature-store)
58. [Rule Engine Pattern](#58-rule-engine-pattern)
59. [Fail-Open vs Fail-Closed vs Fail-Safe](#59-fail-open-vs-fail-closed-vs-fail-safe)
60. [Champion/Challenger ML Testing](#60-championchallenger-ml-testing)
61. [Graph-Based Fraud Ring Detection](#61-graph-based-fraud-ring-detection)
62. [Service Discovery (Consul / Kubernetes)](#62-service-discovery)
63. [JWT vs API Key vs Token Introspection](#63-jwt-vs-api-key-vs-token-introspection)
64. [Request Pipeline (Middleware Chain)](#64-request-pipeline-middleware-chain)
65. [Bulkhead Pattern (Thread Pool Isolation)](#65-bulkhead-pattern)
66. [Erasure Coding (Reed-Solomon)](#66-erasure-coding)
67. [Content-Addressed Storage](#67-content-addressed-storage)
68. [Multipart Upload Protocol](#68-multipart-upload-protocol)
69. [Pre-Signed URLs (HMAC Auth)](#69-pre-signed-urls)
70. [Tiered Log Storage (Hot/Warm/Cold)](#70-tiered-log-storage)
71. [Sampling Strategies (Head-based vs Tail-based)](#71-sampling-strategies)
72. [Distributed Tracing (TraceContext / OpenTelemetry)](#72-distributed-tracing)
73. [Dead Man's Switch (Watchdog Pattern)](#73-dead-mans-switch)
74. [Chunked Audio Streaming + Gapless Playback](#74-chunked-audio-streaming--gapless-playback)
75. [Remote Playback Protocol (Device Command Pattern)](#75-remote-playback-protocol)
76. [Materialized Path — Comment Trees](#76-materialized-path--comment-trees)
77. [Hybrid Fan-out (Push/Pull)](#77-hybrid-fan-out-pushpull)
78. [Score Decay / Time-weighted Ranking (Hot Algorithm)](#78-score-decay--time-weighted-ranking)
79. [RTMP Ingest + Stream Key Auth + Reconnect](#79-rtmp-ingest--stream-key-auth--reconnect)
80. [Transcoding Pipeline — Parallel Quality Ladders](#80-transcoding-pipeline--parallel-quality-ladders)
81. [LL-HLS vs Standard HLS (Latency vs Cost)](#81-ll-hls-vs-standard-hls)
82. [Live Chat Fanout — Redis Pub/Sub Sharding](#82-live-chat-fanout--redis-pubsub-sharding)
83. [Two-Tower Recommendation Model](#83-two-tower-recommendation-model)
84. [Candidate Retrieval + Re-ranking Pipeline](#84-candidate-retrieval--re-ranking-pipeline)
85. [Completion Rate as Implicit Feedback Signal](#85-completion-rate-as-implicit-feedback-signal)
86. [Short-Form Video: Progressive MP4 vs HLS](#86-short-form-video-progressive-mp4-vs-hls)
87. [Multi-Tenant Data Isolation](#87-multi-tenant-data-isolation)
88. [Connection Registry Pattern (WebSocket Routing)](#88-connection-registry-pattern)
89. [Notification Priority Tiers + Bulkhead Queues](#89-notification-priority-tiers--bulkhead-queues)
90. [FCM/APNS Token Lifecycle Management](#90-fcmapns-token-lifecycle-management)
91. [Provider Failover Chain (Notification Channels)](#91-provider-failover-chain)
92. [Digest Batching Pattern](#92-digest-batching-pattern)
93. [Email Delivery Stack (SMTP/SPF/DKIM/DMARC)](#93-email-delivery-stack)
94. [Email Thread Grouping (In-Reply-To / References)](#94-email-thread-grouping)
95. [Bayesian Spam Filter](#95-bayesian-spam-filter)
96. [Snowflake ID Structure (Timestamp + Machine + Sequence)](#96-snowflake-id-structure)
97. [Clock Skew Handling in Distributed Systems](#97-clock-skew-handling)
98. [UUID v4 vs UUID v7](#98-uuid-v4-vs-uuid-v7)
99. [Ticket Server Pattern (Flickr-style)](#99-ticket-server-pattern)
100. [MQTT Protocol (QoS, Retained, LWT)](#100-mqtt-protocol)
101. [Device Shadow Pattern (Desired vs Reported State)](#101-device-shadow-pattern)
102. [Time-Series DB: Downsampling + Retention Policies](#102-time-series-db-downsampling)
103. [Edge Aggregation + Jittered Reconnect Backoff](#103-edge-aggregation--jittered-reconnect-backoff)
104. [Base62/Base58 Encoding for Short IDs](#104-base62base58-encoding-for-short-ids)
105. [Content Expiry Pattern (Lazy Gate + Active Cleanup)](#105-content-expiry-pattern-lazy-gate--active-cleanup)
106. [Perceptual Hashing (pHash / PDQ / PhotoDNA)](#106-perceptual-hashing)
107. [Tiered Moderation Pipeline (Hash → ML → Human)](#107-tiered-moderation-pipeline)
108. [Human-in-the-Loop Review Queue](#108-human-in-the-loop-review-queue)
109. [Precision vs Recall Per-Category Threshold Tuning](#109-precision-vs-recall-threshold-tuning)
110. [Behavioral Graph Detection (CIB / Bot Farms)](#110-behavioral-graph-detection)
111. [Content-Defined Chunking (Rabin Fingerprinting)](#111-content-defined-chunking-rabin-fingerprinting)
112. [Sync Cursor / Change Feed Pattern](#112-sync-cursor--change-feed-pattern)
113. [Offline-First Sync with Conflict Resolution](#113-offline-first-sync-with-conflict-resolution)
114. [LAN Sync (Peer-to-Peer within Local Network)](#114-lan-sync)
115. [Elo Rating System (Peer-Sourced Ranking)](#115-elo-rating-system)
116. [Bloom Filter for Seen-State Tracking](#116-bloom-filter-for-seen-state-tracking)
117. [Thumbnail-First Loading (Progressive Photo Pipeline)](#117-thumbnail-first-loading)
118. [Git Content-Addressed DAG](#118-git-content-addressed-dag)
119. [Packfile + Delta Compression](#119-packfile--delta-compression)
120. [Fork Copy-on-Write (Shared Object Store)](#120-fork-copy-on-write)
121. [Pre-computed Pack for Clone Offload](#121-pre-computed-pack-for-clone-offload)
122. [Merge Strategy Trade-offs (Merge / Squash / Rebase)](#122-merge-strategy-trade-offs)
123. [Soft Delete + Delayed Hard Delete (Recovery Window)](#123-soft-delete--delayed-hard-delete)

---

## 1. Geo Search

**What it is:** Finding entities (restaurants, drivers, venues) within a radius of a point.

### Options

| Approach | How | Pros | Cons | Use When |
|----------|-----|------|------|----------|
| **PostGIS** | `ST_DWithin(geom, point, radius)` with GiST R-Tree index | Simple, battle-tested, full SQL | Single node, slower at extreme scale | Default choice, up to ~10M rows |
| **Geohash** | Encode lat/lng → string prefix. Nearby = same prefix | Redis-native, fast prefix scan | Border cell problem (check 8 neighbors) | Redis-based geo, moderate scale |
| **QuadTree** | Recursive spatial partition | Great for non-uniform density | Complex to update concurrently | Custom geo service, dense urban data |
| **S2 Geometry** | Hilbert curve cell IDs on sphere | Minimal distortion globally, multi-level | Complex impl, no built-in Redis support | Global scale (Uber H3, Google Maps) |

### Key Rule
> PostGIS + Redis Geohash cache covers 95% of interview scenarios.
> Mention S2/QuadTree to show depth.

### Border Cell Problem (Geohash)
```
│  cell_8  │  cell_9  │
          ↑ user here — prefix "8" but restaurant 100m right has prefix "9"
```
Fix: always query the 8 neighboring cells in addition to user's cell.

### Appeared In
- Food Delivery (restaurant discovery, driver location)
- Proximity Service, Ride Sharing, Uber

---

## 2. Database Choice: SQL vs NoSQL

### Decision Framework (First Principles)

Ask these questions:
1. Do I need aggregations (`AVG`, `GROUP BY`, `JOIN`)? → SQL
2. Do I need FK constraints / uniqueness across entities? → SQL
3. Is my schema truly variable per record? → NoSQL (MongoDB)
4. Is it write-heavy, append-only, never joined? → Cassandra / MongoDB
5. Do I need full-text search? → Elasticsearch (separate, not primary DB)

### Comparison Table

| DB | Best For | Avoid When |
|----|----------|------------|
| **Postgres** | Transactional data, aggregations, structured schema, constraints | Extreme write throughput (>100K writes/s) |
| **MongoDB** | Variable schema per document, self-contained documents, CMS/catalog | Need aggregations, FK constraints, cross-doc consistency |
| **Cassandra** | Append-only, time-series, massive write throughput, known query patterns | Complex queries, joins, ad-hoc analytics |
| **Redis** | Cache, session, geo, counters, rate limiting, pub/sub | Persistence-critical data (use as layer, not source of truth) |
| **Elasticsearch** | Full-text search, log analysis, faceted search | Primary data store, transactions |
| **BigQuery/Redshift** | Analytics, aggregations over billions of rows | Live transactional queries |

### MongoDB: When It Genuinely Wins
1. **Variable schema** — Product catalog (phone vs shirt vs book have different fields)
2. **Self-contained documents + high writes** — Activity/event logs, never joined
3. **CMS/Blog** — Whole page = one document fetch, structure varies per content type

### MongoDB: Common Wrong Reasons
- "Flexible schema" — if your schema is stable, this doesn't apply
- "NoSQL is faster" — Postgres with proper indexes is faster for structured queries
- "We might need to scale" — Postgres scales very far before you need to shard

### Appeared In
- Food Delivery (reviews → Postgres; activity log → MongoDB/Cassandra)

---

## 3. Schema Design: Polymorphic vs Separate Tables

**The question:** One `reviews` table for restaurants AND drivers, or two separate tables?

### Polymorphic (Single Table)
```sql
reviews (id, order_id, user_id, target_id, target_type, rating, text)
-- target_type = 'restaurant' | 'driver'
```
**Pros:** Simple, one place to look  
**Cons:**
- No FK enforcement (`target_id` points to two different tables)
- Every query needs `WHERE target_type = ...` — easy to miss
- Schema can't evolve independently per type
- Index efficiency lower (mixed data in same leaf nodes)

### Separate Tables ✓ Better
```sql
restaurant_reviews (id, order_id, user_id, restaurant_id, food_rating, packaging_rating, text)
driver_reviews     (id, order_id, user_id, driver_id, friendliness_rating, on_time_rating, text)
```
**Pros:**
- Real FK constraints
- Indexes only contain relevant rows
- Each table evolves independently (restaurant gets `cuisine_rating`, driver gets `on_time_rating`)
- Queries are cleaner

### Rule
> Use separate tables when: entities have different attributes, evolve independently, or are queried independently.
> Use polymorphic only when: schema is truly identical and always queried together.

### Appeared In
- Food Delivery (restaurant reviews vs driver reviews)

---

## 4. Idempotency

**What it is:** Same operation, executed multiple times = same result as executing once.

**Why needed:** Network failures cause retries. Without idempotency, retries = duplicate charges, duplicate orders, duplicate notifications.

### Key Rule: Client Generates the Key
```
Client generates UUID before first attempt → sends with every retry
Server checks: key seen before? → return cached result
               key new?         → execute + store result
```

**Why NOT server-generated:**
- Requires extra round trip to get key
- If that call fails, problem moves one level up, not solved

**Why NOT SHA256(order_id + user_id + amount):**
- `order_id` doesn't exist before order is created (chicken-and-egg)
- Can't generate server-side hash before the first attempt

### Implementation
```
Client:  key = UUID()  [stored locally, per intent]
Request: POST /orders { idempotency_key: key, ...payload }

Server:
  1. SELECT result FROM idempotency_keys WHERE key = $1
  2. Found?  → return cached result (no re-execution)
  3. Not found? → execute → INSERT (key, result) → return result
```

### PSP Integration (Stripe)
```
Stripe-Idempotency-Key: <your_key>   ← Stripe deduplicates on their end too
```

### Edge Case: Two Genuine Requests
Two different user taps = two different UUIDs = two separate charges. Correct.

### Appeared In
- Food Delivery (payment), Payment System, Flash Sale

---

## 5. Real-time Communication

| Method | How | Pros | Cons | Use When |
|--------|-----|------|------|----------|
| **Polling** | Client requests every N seconds | Simple | Wastes RPS, high latency | Simple status checks, low frequency |
| **Long Polling** | Client holds request open until update | Simpler than WS | Server holds connections, still HTTP overhead | Fallback when WS not available |
| **SSE** | Server pushes over HTTP, one direction | Simple, HTTP/2 native, auto-reconnect | Server→client only | Notifications, feed updates |
| **WebSocket** | Persistent bidirectional TCP connection | Low latency, bi-directional | Complex (sticky routing, reconnect logic) | Real-time tracking, chat, live collaboration |

### Why Not Polling for Tracking
```
5M active orders × poll every 5s = 1M RPS just for tracking
WebSocket = server pushes only on update → fraction of that load
```

### WebSocket Scaling Pattern
```
Driver App → Location Service (WS) → Redis pub/sub channel: order:{id}
                                                   ↓
                                    Tracking Service (subscribed)
                                                   ↓
                                    Push to User WebSocket
```
Sticky sessions (consistent hash on user_id/order_id) so user always hits same WS server.

### Graceful Degradation
```
WebSocket → SSE → Long Polling
```
Client tries best available, falls back on failure.

### Appeared In
- Food Delivery (order tracking), Chat System, Collab Doc Editor, Ride Sharing

---

## 6. Driver / Resource Matching

**Problem:** Given a request (order, ride), assign the best available resource (driver).

### Two-Phase Approach
```
Phase 1 (cheap, Redis GEORADIUS):
  → Get 10 nearest available drivers within radius
  → Filter by heading (dot product > 0 = moving toward destination)

Phase 2 (accurate, OSRM batch):
  → Get road ETA for top 3-5 candidates
  → Score = road_ETA + max(0, prep_time - road_ETA)
  → Assign lowest score
```

### Heading Filter (Velocity Vector)
```python
dx = restaurant.lng - driver.lng    # direction to target
dy = restaurant.lat - driver.lat

vx = speed × cos(heading)           # driver velocity
vy = speed × sin(heading)

dot = dx*vx + dy*vy
# dot > 0 → toward target ✓
# dot < 0 → away from target ✗
```

### Why Nearest ≠ Best
A driver 2km away heading toward restaurant beats driver 0.5km away heading away — must factor in velocity/direction.

### Scoring Formula
```
score = road_ETA_to_pickup + max(0, wait_time_at_pickup - road_ETA)
```
Penalizes both: arriving late (food cold) and arriving very early (driver waits idle).

### Offer Flow
```
Send offer to driver → 30s timeout (TTL in Redis)
  Accepted → mark BUSY, ZREM from available pool
  Declined / timeout → next candidate
```

### Driver Status as Distributed Lock
```
SET driver:{id}:status BUSY NX EX 3600
-- NX = only if not exists → atomic, prevents double-assignment
```

### Appeared In
- Food Delivery, Ride Sharing, Uber

---

## 7. Routing Engine

**What it is:** Given two coordinates, return real road distance + ETA.

### OSRM vs Google Maps

| | OSRM (self-hosted) | Google Maps API |
|--|-----|-----|
| Cost | Free (infra only) | ~$5/1000 requests |
| Latency | ~5ms | ~50-100ms |
| Traffic | Basic | Real-time |
| Setup | Complex (preprocess map data) | Just API key |
| Scale | Unlimited (your infra) | Pay per call |

### At What Scale Does OSRM Pay Off?
```
420 orders/sec × 5 driver candidates = 2,100 routing calls/sec
Google Maps = ~$900/day
OSRM = infra cost only
```
Above ~100K calls/day → OSRM wins economically.

### OSRM Table Endpoint (Batch)
```
POST /table/v1/driving/
  sources: [driver1, driver2, driver3]
  destinations: [restaurant]
→ 3×1 duration matrix in ONE call (not 3 separate calls)
```

### How OSRM Works Internally
- Road network = graph (nodes=intersections, edges=road segments with speed)
- **Contraction Hierarchies**: pre-process once (hours), shortcut important routes
- Query time: ~1-5ms even on full planet data

### Appeared In
- Food Delivery (driver ETA), Ride Sharing, Proximity Service

---

## 8. Caching Patterns

### Write Strategies

| Pattern | How | Pros | Cons | Use When |
|---------|-----|------|------|----------|
| **Cache-aside** | App checks cache → miss → DB → populate cache | Simple, only cache what's needed | Cache miss = slow first read | General reads |
| **Write-through** | Write to cache + DB simultaneously | Cache always fresh | Write latency (two writes) | Read-heavy, consistency needed |
| **Write-behind** | Write to cache → async write to DB | Fast writes | Data loss risk if cache dies | Write-heavy, loss-tolerant |

### Invalidation Strategies

| Strategy | How | Use When |
|----------|-----|----------|
| **TTL** | Key expires after N seconds | Tolerable staleness (menus, restaurant list) |
| **Event-driven** | Write event → invalidate specific key | Low staleness tolerance (item availability) |
| **Cache stampede prevention** | Only one request rebuilds cache (mutex/lock) | High traffic, expensive DB query |

### TTL Guidelines (Food Delivery)
```
Restaurant list (by zone):  60s   — tolerable if user sees slightly stale list
Menu items:                 300s  — but re-check availability at order time from DB
Driver location:            N/A   — always real-time, never cache
Order status:               N/A   — always real-time via WebSocket
```

### Rule
> Never cache data that must be strongly consistent at time of write (payments, inventory locks).
> Always re-check from DB at action time even if UI showed cached data.

### Appeared In
- Food Delivery, Distributed Cache, Hotel Booking, Flash Sale

---

## 9. Event-Driven Architecture (Kafka + Outbox)

### Why Kafka
- Decouples services (Restaurant Service doesn't need to call Driver Service directly)
- Durable: messages survive service crashes
- Replay: reprocess events if consumer crashes
- Fan-out: one event → multiple consumers (matching + notification + analytics)

### Outbox Pattern (Prevents Lost Events)
**Problem:** Service writes to DB + publishes to Kafka — what if crash happens between?

```
-- WRONG: two separate operations, can fail between them
INSERT INTO orders ...
PUBLISH to Kafka         ← crash here → order created, event never sent
```

```
-- CORRECT: Outbox pattern
BEGIN TRANSACTION;
  INSERT INTO orders (id, status, ...)
  INSERT INTO outbox (order_id, event, published=false)
COMMIT;

-- Separate Outbox Publisher process:
SELECT * FROM outbox WHERE published=false
FOR EACH: publish to Kafka → UPDATE published=true
```

Crash anywhere → transaction rolls back OR outbox catches up on recovery.

### Consumer Idempotency
Kafka delivers at-least-once. Consumers must handle duplicate events:
```
Before processing: check if event_id already processed
If yes → skip
If no  → process → mark as processed
```

### Appeared In
- Food Delivery, Notification System, Payment System, Message Queue

---

## 10. State Machines

**When to use:** Any multi-step workflow with defined transitions and rules (orders, payments, bookings).

### Order State Machine (Food Delivery)
```
CART → PLACED → ACCEPTED → DRIVER_ASSIGNED → PICKED_UP → DELIVERED → COMPLETED
                                                                    ↗
Any state → CANCELLED (business rules define which transitions are valid)
Any state → FAILED
```

### Implementation
```sql
-- Source of truth: orders.status
-- Audit trail: order_events (append-only)

UPDATE orders SET status='ACCEPTED' 
WHERE id=$1 AND status='PLACED'   -- ← optimistic locking, prevents invalid transitions
RETURNING *;

INSERT INTO order_events (order_id, event_type, actor_id, payload, created_at)
VALUES ($1, 'ORDER_ACCEPTED', $2, $3, now());
```

### Why Append-Only Events
- Full audit trail for debugging and disputes
- Can replay events to reconstruct state
- Basis for event sourcing if needed

### Appeared In
- Food Delivery, Hotel Booking, Payment System, Flash Sale

---

## 11. Redis Data Structures Cheatsheet

| Structure | Command | Use Case |
|-----------|---------|----------|
| **String** | `SET/GET` | Session tokens, counters, driver status, idempotency locks |
| **Hash** | `HSET/HGET` | Object fields (user profile, config) |
| **Sorted Set (ZSET)** | `ZADD/ZRANGE/ZRANGEBYSCORE` | Leaderboards, rate limiting (sliding window), priority queues |
| **GEO** (ZSET internally) | `GEOADD/GEORADIUS` | Driver locations, restaurant proximity |
| **List** | `LPUSH/RPOP` | Simple queues, recent activity |
| **Set** | `SADD/SISMEMBER` | Unique visitors, tag sets |
| **Pub/Sub** | `PUBLISH/SUBSCRIBE` | Real-time fanout (tracking, notifications) |
| **Stream** | `XADD/XREAD` | Persistent message queue (Kafka-lite) |

### GEO Commands (Most Used in System Design)
```
GEOADD drivers:available 72.87 19.07 "driver:101"   -- add/update position
GEORADIUS drivers:available 72.88 19.07 3 km ASC COUNT 10 WITHDIST WITHCOORD
GEODIST drivers:available "driver:101" "driver:202" km
GEOPOS drivers:available "driver:101"               -- get position
ZREM drivers:available "driver:101"                 -- remove (driver goes busy)
```

### TTL Patterns
```
SET driver:101:offer "order:456" EX 30    -- offer expires in 30s, auto-cleanup
SET key value NX EX 60                   -- set only if not exists + TTL (distributed lock)
```

---

## 12. OLTP vs OLAP Split

**Rule:** Never run analytics queries on your production OLTP database.

```
User places order     → Postgres (OLTP) — low latency, transactional
"Top restaurants this week" dashboard → BigQuery/Redshift (OLAP) — complex aggregations
```

### Pipeline
```
Postgres (OLTP)
    ↓ CDC (Change Data Capture) via Debezium
Kafka
    ↓ Stream processor (Flink/Spark)
BigQuery / Redshift (OLAP)
    ↓
Analytics dashboards (Metabase, Looker)
```

### Elasticsearch for Search
```
Postgres writes → sync to Elasticsearch
User searches "pizza near me" → Elasticsearch (full-text + geo)
Result: restaurant IDs → fetch full data from Postgres/Redis
```

Elasticsearch is a search index, not a primary DB. Never write to it first.

### Appeared In
- Food Delivery, News Aggregator, Instagram, Twitter Feed

---

---

## 13. TOCTOU — Atomic Check and Act

**What it is:** Time Of Check To Time Of Use. A race condition where the state changes between when you check a condition and when you act on it.

**Real-world bug:** Paytm wallet — balance was ₹0 but transactions went through because check and deduct were two separate operations. Concurrent requests both passed the check before either deducted.

### The Bug Pattern

```
Thread 1:  READ balance = 0 → check: 0 >= 0? YES → WRITE balance = -100
Thread 2:  READ balance = 0 → check: 0 >= 0? YES → WRITE balance = -100
Result: balance = -100  ✗  (or -200 if both writes land)
```

Gap between READ and WRITE is the vulnerability. Any concurrent request in that gap sees stale state.

### Where It Appears

| System | Shared Resource | Bug |
|--------|----------------|-----|
| Wallet (Paytm) | Balance | Negative balance |
| Ticket booking | Seat inventory | Overselling |
| Flash sale | Item stock | Sell more than available |
| Hotel booking | Room availability | Double booking |
| Rate limiter | Request counter | Exceed limit |

### Fix 1: Atomic UPDATE with WHERE guard ✓ Best for most cases

```sql
UPDATE wallets
SET balance = balance - 100
WHERE user_id = $1
  AND balance >= 100;    -- check + deduct in one atomic statement

-- rows_affected = 0 → insufficient funds (decline)
-- rows_affected = 1 → success
```

Single DB statement. No gap. Row locked during update.

### Fix 2: DB CHECK Constraint (Last Line of Defense)

```sql
ALTER TABLE wallets
ADD CONSTRAINT balance_non_negative CHECK (balance >= 0);
-- DB rejects any write that makes balance negative, even if app logic has a bug
```

### Fix 3: SELECT FOR UPDATE (Pessimistic Lock)

```sql
BEGIN;
SELECT balance FROM wallets WHERE user_id = $1 FOR UPDATE;  -- locks row
-- no other transaction can read/write this row until COMMIT
UPDATE wallets SET balance = balance - 100 WHERE user_id = $1;
COMMIT;
```

**Use when:** Multiple fields need to be checked together before acting.  
**Cost:** Serializes concurrent requests on same row — lower throughput.

### Fix 4: Optimistic Locking (version number)

```sql
-- Read with version
SELECT balance, version FROM wallets WHERE user_id = $1;
-- got: balance=500, version=7

-- Update only if nobody changed it since our read
UPDATE wallets
SET balance = balance - 100, version = version + 1
WHERE user_id = $1 AND version = 7;

-- 0 rows = conflict → retry
-- 1 row = success
```

**Use when:** Contention is low. No DB lock held — better throughput. Retry on conflict.

### Fix 5: Never Read From Replica for Financial Checks

```
Replica lag = stale balance = both threads see ₹500, both deduct ₹500

Rule: ALL balance checks → primary DB only
      Replicas → history, statements, analytics only
```

### Defense in Depth (All Four Together)

```
Layer 1: Atomic UPDATE WHERE balance >= amount     ← app logic
Layer 2: CHECK constraint balance >= 0             ← DB safety net
Layer 3: All balance reads from primary only       ← no stale reads
Layer 4: Idempotency key per transaction           ← no duplicate deductions
```

Any one layer reduces the bug. All four make it impossible.

### Rule
> Whenever you see: READ → check → WRITE as separate steps on shared mutable state, ask: what happens if two requests do this simultaneously?
> Fix: collapse check + write into one atomic operation (SQL WHERE clause, Redis SETNX, DB constraint).

### Appeared In
- Food Delivery (payment, inventory), Flash Sale, Hotel Booking, Rate Limiter, Payment System

---

## 14. Lambda vs Kappa Architecture

**The problem:** You need both low-latency results (dashboard) and accurate historical results (billing). These have conflicting requirements — fast vs exact.

### Lambda Architecture

```
Raw Events → [Speed Layer (Flink/Spark Streaming)] → approximate, <1min latency
           → [Batch Layer (Spark on S3)]           → exact, hours latency
           → [Serving Layer]                        → merges both views
```

| Layer | How | Latency | Accuracy | Use For |
|-------|-----|---------|----------|---------|
| Speed | Stream processor, windowed aggregation | <1min | ~99% (late events, restarts) | Dashboard, real-time alerts |
| Batch | Reprocess all raw events nightly | Hours | 100% | Billing, reconciliation |
| Serving | Queries speed for recency, batch for history | — | — | All user queries |

**Pros:** Billing-grade accuracy + real-time responsiveness  
**Cons:** Two codebases (stream + batch), operational complexity, results can temporarily diverge

### Kappa Architecture

```
Raw Events → [Single Stream Processor] → results
           → [Kafka long retention]    → source of truth for replay
```

**Reprocessing:** Bug found? Create new consumer group, replay from Kafka offset 0. New output replaces old.

**Pros:** One codebase, simpler  
**Cons:** Requires long Kafka retention (weeks/months = expensive), replay takes hours

### Decision Rule
```
Need billing-grade exact counts AND real-time?   → Lambda
Corrections acceptable within hours via replay?  → Kappa
Simple aggregation, no billing requirement?      → Kappa
```

### Appeared In
- Ad Click Aggregator (Lambda: stream for dashboard, batch for billing)

---

## 15. Time Windowing

**What it is:** Grouping a stream of events into time-based buckets for aggregation.

### Window Types

| Type | Definition | Example | Use When |
|------|-----------|---------|---------|
| **Tumbling** | Fixed, non-overlapping buckets | [10:00-10:01), [10:01-10:02) | "Clicks per minute" — non-overlapping periods |
| **Sliding** | Fixed size, moves by slide interval | Every 1min: [10:00-10:05), [10:01-10:06) | "Clicks in last 5 min" — rolling window |
| **Session** | Gap-based: window ends after N seconds of inactivity | User activity bursts | User session analytics |
| **Global** | All events, no time bound | Aggregate since start | Counters, total counts |

### Tumbling vs Sliding — Key Difference
```
Tumbling (5min):
  [10:00─────10:05) [10:05─────10:10)   ← no overlap

Sliding (5min window, 1min slide):
  [10:00─────10:05)
       [10:01─────10:06)
            [10:02─────10:07)           ← overlap, more expensive
```
Sliding windows are more expensive: each event may appear in multiple windows.

### Late Events & Watermarks
```
Event time:      10:00:30
Processing time: 10:01:05  (35s late due to network)

Without watermark:
  Window [10:00, 10:01) already closed → event dropped

With watermark (30s allowed lateness):
  Window stays open until processing_time = 10:01:30
  → event arrives at 10:01:05 → still accepted ✓

After watermark passes:
  Late events → side output (separate stream) → batch correction
```

### Storage Pattern (Pre-computed tumbling → query-time sliding)
```
Store: 1-minute tumbling aggregates in ClickHouse
Query: "last 5 min" = SUM(count) WHERE window_start >= now()-5min

Advantage: one storage format, any window size queryable at read time
Cost: slightly slower query vs pre-computed sliding window
```

### Appeared In
- Ad Click Aggregator (tumbling 1m windows, query-time sliding)

---

## 16. Count-Min Sketch / Top-K

**The problem:** Track frequencies of millions of distinct items (ad_ids) and find top-K efficiently. Exact counting requires O(N) memory — prohibitive for 10M ad_ids.

### Count-Min Sketch

```
2D array: d rows × w columns
d = number of hash functions
w = width (controls error rate)

On event (ad_id):
  For i in 0..d:
    col = hash_i(ad_id) % w
    table[i][col] += 1

Estimate frequency(ad_id):
  return min(table[i][hash_i(ad_id) % w] for i in 0..d)
```

**Properties:**
- **Never undercounts** (only inflated by collisions)
- **May overcount** by ε × total_events (ε = 1/w)
- **Space:** O(d × w) regardless of distinct elements
- **Update/Query:** O(d) — typically d=5, so O(5) = effectively O(1)

**Parameters:**
```
Error ε = e/w     (e = Euler's number ≈ 2.72)
Prob of exceeding error = (1/2)^d

For 1% error with 99% confidence:
  w = e/0.01 ≈ 272
  d = log(1/0.01) ≈ 7
```

### Top-K Pattern (Sketch + Redis Sorted Set)

```
Flink, on each click:
  1. Update Count-Min Sketch for ad_id
  2. Estimated freq = min(sketch cells for ad_id)
  3. If freq > current_kth_largest:
       ZINCRBY redis:topk:{window} 1 {ad_id}
       Trim sorted set to K elements (ZREMRANGEBYRANK 0 -(K+1))

Query top-K:
  ZREVRANGE redis:topk:{window} 0 K-1 WITHSCORES
```

### Exact Top-K (for billing)
```sql
-- ClickHouse, nightly
SELECT ad_id, SUM(count) as total
FROM click_counts
WHERE window_start >= today() - INTERVAL 1 DAY
GROUP BY ad_id
ORDER BY total DESC
LIMIT 10
```

### Trade-off Summary

| Approach | Accuracy | Latency | Memory | Use When |
|----------|----------|---------|--------|---------|
| Count-Min + Redis | ~99% | O(1) | O(d×w) | Real-time dashboard top-K |
| Exact SQL | 100% | Seconds | N/A | Billing, reports |
| HashMap | 100% | O(N) | Real-time, low cardinality only |

### Appeared In
- Ad Click Aggregator (real-time top-K ads by click count)

---

## 17. Stream Processing Delivery Guarantees

**Three levels:**
| Guarantee | Meaning | How | Use When |
|-----------|---------|-----|---------|
| **At-most-once** | Message may be lost, never duplicated | Fire-and-forget, no ACK | Metrics where loss is tolerable |
| **At-least-once** | Message delivered ≥1 times, may duplicate | Retry on failure, consumer deduplicates | Most systems; simpler than exactly-once |
| **Exactly-once** | Delivered exactly once | Transactional writes + offset commit in same transaction | Billing, financial ledger |

**Exactly-once in Kafka:**
```
Producer: enable.idempotence=true + transactional.id
Consumer: read → process → write to DB + commit offset in same DB transaction
          If crash: replayed message finds DB already updated (idempotent write)
```

**Rule:** Exactly-once is expensive. Use at-least-once + idempotent consumers for most cases.

### Appeared In
- Ad Click Aggregator, Food Delivery, Payment System

---

## 18. HLS / Adaptive Bitrate Streaming

**What it is:** Video split into small segments (~2-10s). Client switches quality based on bandwidth.

```
Source Video → Transcoder → Multiple qualities: 1080p, 720p, 480p, 360p
Each quality → split into 2s segments → stored on CDN

.m3u8 playlist:
  #EXT-X-STREAM-INF:BANDWIDTH=5000000  → 1080p playlist URL
  #EXT-X-STREAM-INF:BANDWIDTH=1500000  → 720p playlist URL
  #EXT-X-STREAM-INF:BANDWIDTH=500000   → 360p playlist URL

Client downloads .m3u8 → picks bitrate based on measured bandwidth
        → downloads segments → buffers 3-5 segments ahead
        → switches quality per segment based on real-time bandwidth
```

**ABR algorithm:** If download rate of last segment > current bitrate threshold → upgrade quality.

### Appeared In
- Live Streaming, YouTube

---

## 19. HyperLogLog — Approximate Unique Counting

**Problem:** Count distinct users across billions of events. Exact counting = O(N) memory.

**HyperLogLog:** Probabilistic. Counts distinct elements with ~0.81% error using only 12KB memory regardless of cardinality.

```
Redis: PFADD uv:page:home user_123
       PFCOUNT uv:page:home  → ~100M (±0.81%)
       PFMERGE total uv:page:home uv:page:feed  → union
```

**Use when:** Need distinct count at scale; 1% error is acceptable.
**Don't use when:** Need exact count (e.g., billing for API calls per user).

### Appeared In
- Ad Click Aggregator (unique users per ad), Instagram (unique viewers)

---

## 20. CDN Pull vs Push

| | Pull (lazy) | Push (eager) |
|--|---|---|
| **How** | Edge fetches from origin on first request, caches | You upload content to all edge nodes upfront |
| **Best for** | Unpredictable access patterns, long-tail content | Known popular content, large files (video) |
| **Cache miss** | First user in a region gets origin latency | No cache miss — content pre-distributed |
| **Storage cost** | Only popular content cached | All content pushed regardless of popularity |

**Rule:** Pull for dynamic/unpredictable. Push for known-popular large assets (video segments, software releases).

### Appeared In
- Live Streaming, YouTube, Instagram

---

## 21. Virtual Waiting Queue

**What it is:** Instead of hammering the server with concurrent requests, users take a numbered position in a virtual queue. They wait their turn before accessing the system.

```
User hits sale page → assigned queue position (Redis INCR queue:event:{id}:counter)
User polls /queue/status → returns position + estimated wait
When user's turn: issued a time-limited access token (JWT, 5 min TTL)
Token presented at checkout → bypass queue, proceed to hold seat

Queue advancement: every N seconds, admit next M users (rate-limited admission)
```

**Why not just rate limit?**
- Rate limit = some users get through, rest get 429 errors
- Queue = everyone has fair shot, orderly access, no thundering herd

**Redis pattern:**
```
INCR  queue:event:123:position        → user's position
GET   queue:event:123:serving         → current serving position
User's wait = (position - serving) / admission_rate
```

### Appeared In
- Ticketmaster (flash sale seat selection)

---

## 22. Seat / Resource Hold Pattern

**What it is:** Temporarily reserve a resource (seat, room, ticket) while user completes payment. Prevents double-sell without permanently blocking the resource.

```
User selects seat → HOLD (10 min TTL, stored in Redis)
User pays → on payment success: HOLD → SOLD in DB (atomic UPDATE)
Hold expires → seat returns to available pool automatically (TTL)

Redis key: hold:event:{id}:seat:{id} → account_id  EX 600
```

**Critical:** At hold time, check must be atomic:
```
SET hold:event:123:seat:3A "user:456" NX EX 600
-- NX = only if not exists → prevents two users holding same seat
-- Returns OK (got hold) or nil (already held by someone else)
```

**Trade-off:** Hold duration. Too short (2 min) → user can't complete payment → frustration. Too long (30 min) → speculative holds block genuine buyers.

### Appeared In
- Ticketmaster, Hotel Booking

---

## 23. Order Book Data Structure

**What it is:** An in-memory data structure holding all open buy and sell orders for a single trading instrument, organized by price.

```
Bid side (BUY orders) — descending price (best = highest bid)
  ₹2850 → [Order_A: 500, Order_B: 200]    ← best bid
  ₹2848 → [Order_C: 1000]
  ₹2845 → [Order_D: 300]

Ask side (SELL orders) — ascending price (best = lowest ask)
  ₹2851 → [Order_E: 400, Order_F: 150]    ← best ask
  ₹2853 → [Order_G: 600]
  ₹2856 → [Order_H: 200]

Spread = best ask - best bid = 2851 - 2850 = ₹1
```

### Data Structure Choice

| Structure | Top price | Insert | Cancel by ID | Use |
|-----------|-----------|--------|-------------|-----|
| **Heap** | O(1) | O(log N) | O(N) — must scan | Bad: O(N) cancel |
| **TreeMap<Price, Deque<Order>>** | O(1) | O(log P) | O(1) with HashMap | ✓ Best |
| **Sorted Array** | O(1) | O(N) | O(N) | Bad: O(N) insert |

**Combined structure:**
```
bids: TreeMap<Price, Deque<Order>>   ← sorted descending
asks: TreeMap<Price, Deque<Order>>   ← sorted ascending
order_map: HashMap<OrderId, Order>   ← O(1) lookup for cancel
```

**P = distinct price levels** (typically 100-1000, not total orders).
Insert = O(log P), not O(log N total orders).

**Memory:** 100K open orders × 200 bytes = 20MB per instrument. 500 instruments = 10GB → fits comfortably in RAM.

### Matching (Price-Time Priority)
1. Best price first (lowest ask vs highest bid)
2. Ties at same price → FIFO (time priority = order in deque)

### Trade-off
> In-memory = µs matching latency. But state lost on crash → must pair with WAL for recovery.

### Appeared In
- Stock Exchange

---

## 24. Total Order Sequencer

**What it is:** A single component that assigns monotonically increasing sequence numbers to all incoming orders, ensuring the matching engine sees events in a deterministic total order.

**Why needed:**
```
Gateway 1 receives: BUY AAPL 100 @ 150   (t=10:00:00.000001)
Gateway 2 receives: SELL AAPL 100 @ 150  (t=10:00:00.000002)

Without sequencer: Matching Engine might receive them in either order depending on network.
With sequencer:    seq_no=1001 assigned to BUY, seq_no=1002 to SELL. Always deterministic.
```

**Implementation:**
```
Single-threaded process:
  Read order from network buffer
  Assign seq_no = atomic_increment(counter)
  Write (seq_no, timestamp, order) to WAL (append-only)
  Forward to Matching Engine

Throughput: ~2M seq/sec (pure memory, sequential write)
NSE peak: ~500K orders/sec → 4× headroom
```

**Recovery:** Replay WAL from seq_no 0 → rebuild identical state. Deterministic.

**High availability:** Primary + hot standby. Standby replicates WAL in real-time. On primary failure → standby promotes, continues from last replicated seq_no.

**Sharding:** Per instrument group. RELIANCE orders only need total ordering with other RELIANCE orders — not with TCS orders. Multiple sequencers, each handling a subset of instruments.

### Trade-off
> Single-threaded = simple total ordering but potential bottleneck. Mitigated by: (1) it does very little per message, (2) shard by instrument group.

### Appeared In
- Stock Exchange

---

## 25. Write-Ahead Log (WAL)

**What it is:** Before executing any operation, write it to a sequential append-only log. On crash, replay the log to restore state.

**Rule:** Persist first, execute second. Never the reverse.

```
t=0ms:   Order received
t=0.1ms: Written to WAL (fsync to disk)  ← durability point
t=0.2ms: Order enters matching engine
t=0.5ms: Trade executed
t=1ms:   ACK sent to client

Crash at t=0.4ms (after WAL write):
  Recovery: replay WAL → re-run matching → trade re-generated (idempotent)
  
Crash at t=0.05ms (before WAL write):
  Order never durably recorded → treated as never received
  Client retries → same idempotency key → processed fresh
```

**WAL format (binary, append-only):**
```
[seq_no: 8 bytes][timestamp: 8 bytes][payload_len: 4 bytes][payload: N bytes]
```

**WAL vs Database:**
| | WAL | DB Write |
|--|---|---|
| Operation | Sequential append | Random read/write (B-tree) |
| Latency | ~0.1ms (SSD sequential) | ~1ms (random I/O) |
| Use | Durability before execution | Queryable persistent state |

**Pair WAL with:** Periodic checkpoints (snapshot of current state). On recovery, load last checkpoint + replay only WAL entries after checkpoint. Avoids replaying entire history.

### Appeared In
- Stock Exchange (order durability), also implicit in Postgres, Kafka internals

---

## 26. Circuit Breaker — Market Halt (Financial)

> Note: Different from the microservices circuit breaker pattern (retry/fallback). This is a market safety mechanism.

**What it is:** Automatic trading halt triggered when price moves beyond a threshold, preventing cascade failures (flash crashes).

```
Reference price: previous close = ₹2800

Price rises/falls during session:
  ±5%  (₹2660 - ₹2940) → 2-minute pause, auto-resume
  ±10% (₹2520 - ₹3080) → 15-minute halt
  ±20% (₹2240 - ₹3360) → halt for rest of day

Market-wide (index level):
  Nifty drops 10% → all equities halted 45 minutes
  Nifty drops 15% → halted rest of day
```

**Implementation:**
```python
# After each trade execution in Matching Engine:
pct_change = abs(last_price - reference_price) / reference_price * 100
if pct_change >= CIRCUIT_LIMIT_PERCENT:
    instrument.status = HALTED
    broadcast_halt(instrument, duration_minutes)
    reject all new orders for this instrument
    schedule_resume(now + duration)
```

**Why it works:**
- Pause gives market participants time to assess if the move is rational or error-driven
- Prevents stop-loss cascade (each price drop triggers more stop-loss sells → more drops)
- Gives exchange time to detect fat-finger errors or rogue algo before catastrophic fill

**Real examples:**
- Flash Crash 2010: Dow -1000 pts in minutes. Circuit breakers now mandatory on US exchanges.
- NSE adopted dynamic price bands post-2012.

**Trade-off:** Circuit breakers protect against cascades but can trap traders who need to hedge — legitimate selling also halted. Balance between stability and market efficiency.

### Appeared In
- Stock Exchange
### Trade-off Summary

| Approach | Accuracy | Latency | Memory | Use When |
|----------|----------|---------|--------|---------|
| Count-Min + Redis | ~99% | O(1) | O(d×w) | Real-time dashboard top-K |
| Exact SQL | 100% | Seconds | N/A | Billing, reports |
| HashMap | 100% | O(1) amortized | O(N distinct) | Low cardinality only |

### Appeared In
- Ad Click Aggregator (real-time top-K ads by click count)

---

## 23. Order Book Data Structure

**What it is:** In-memory structure holding all open buy/sell orders for one instrument, organized by price level.

```
Bid side (BUY) — descending (best bid = highest):
  ₹2850 → [Order_A:500, Order_B:200]   ← best bid
  ₹2848 → [Order_C:1000]

Ask side (SELL) — ascending (best ask = lowest):
  ₹2851 → [Order_E:400, Order_F:150]   ← best ask
  ₹2853 → [Order_G:600]

Spread = best ask - best bid = ₹1
```

### Data Structure Choice

| Structure | Top price | Insert | Cancel by ID | Verdict |
|-----------|-----------|--------|-------------|---------|
| Heap | O(1) | O(log N) | O(N) — must scan | Bad: O(N) cancel |
| **TreeMap<Price, Deque<Order>>** | O(1) | O(log P) | O(1) via HashMap | ✓ Use this |
| Sorted Array | O(1) | O(N) | O(N) | Bad |

P = distinct price levels (typically 100-1000, not total orders).

**Combined:**
```
bids: TreeMap<Price, Deque<Order>>   descending
asks: TreeMap<Price, Deque<Order>>   ascending
order_map: HashMap<OrderId, Order>   O(1) cancel
```

**Memory:** 100K open orders × 200 bytes = 20MB/instrument. 500 instruments = 10GB — fits in RAM.

**Trade-off:** In-memory = µs latency. State lost on crash → must pair with WAL.

### Appeared In
- Stock Exchange

---

## 24. Total Order Sequencer

**What it is:** Single component assigning monotonically increasing sequence numbers to all orders before they reach the matching engine.

**Why needed:** Two gateways receive orders simultaneously. Without global seq_no, matching engine sees non-deterministic input → different trades depending on arrival order → legally unacceptable.

```
Gateway 1 ──┐
Gateway 2 ──┼──► Sequencer (single-threaded)
Gateway 3 ──┘        │ assigns seq_no = ++counter
                     │ writes (seq_no, order) to WAL
                     ↓
              Matching Engine (reads in seq_no order)
```

**Throughput:** ~2M seq/sec (pure memory + sequential write). NSE peak ~500K/sec → 4× headroom.

**HA:** Primary + hot standby. Standby replicates WAL in real-time. Failover in ~500ms.

**Sharding:** Per instrument group — RELIANCE orders only need total ordering with other RELIANCE orders.

**Trade-off:** Single-threaded is simple but single point of failure. Mitigated by hot standby and the fact it does very little per message.

### Appeared In
- Stock Exchange

---

## 25. Write-Ahead Log (WAL)

**What it is:** Append-only sequential log written before any operation executes. On crash, replay to restore state.

**Rule:** Persist first, execute second. Never the reverse.

```
t=0.0ms: Order received
t=0.1ms: Written to WAL (fsync)   ← durability point
t=0.2ms: Enters matching engine
t=0.5ms: Trade executes

Crash at t=0.4ms (after WAL): replay WAL → re-run matching → same trade (idempotent)
Crash at t=0.05ms (before WAL): order never recorded → client retries with same ID → deduplicated
```

**WAL format (binary, append-only):**
```
[seq_no: 8B][timestamp: 8B][payload_len: 4B][payload: NB]
```

| | WAL | DB Write |
|--|---|---|
| Operation | Sequential append | Random I/O (B-tree) |
| Latency | ~0.1ms | ~1ms |
| Queryable | No | Yes |

**Pair with checkpoints:** snapshot state periodically. Recovery = load last checkpoint + replay WAL since that point. Avoids replaying entire history.

### Appeared In
- Stock Exchange (order durability). Also internal to Postgres, Kafka.

---

## 26. Circuit Breaker — Market Halt

> Different from microservices circuit breaker (retry/fallback). This is a financial market safety mechanism.

**What it is:** Automatic trading halt when price moves beyond threshold, preventing flash crash cascades.

```
Reference price: previous close = ₹2800

±5%  → 2-minute pause, auto-resume
±10% → 15-minute halt
±20% → halt for rest of day

Market-wide (Nifty index):
±10% → halt all equities 45 minutes
±15% → halt rest of day
```

**Implementation (Matching Engine, after each trade):**
```python
pct_change = abs(last_price - ref_price) / ref_price * 100
if pct_change >= LIMIT:
    instrument.status = HALTED
    reject all incoming orders
    broadcast halt via market data feed
    schedule_resume(now + duration)
```

**Why cascade happens without circuit breaker:**
```
Price drops 5% → stop-loss orders trigger → more SELL orders
→ price drops further → more stop-losses → ...
Flash Crash 2010: Dow -1000 pts in 20 minutes from one $4.1B sell order
```

**Trade-off:** Prevents cascade but traps traders who need to hedge legitimate exposure. Balance: use graduated halts (short pause first) rather than immediate long suspension.

### Appeared In
- Stock Exchange

---

## 27. Contraction Hierarchies — Fast Graph Routing

**What it is:** A preprocessing technique on a road graph that adds shortcut edges to enable sub-10ms shortest-path queries on graphs with 300M+ nodes.

**Why needed:** Plain Dijkstra on full road network = 2-5 seconds per cross-country query. Unacceptable for a navigation product.

### How It Works

```
Offline preprocessing (run once, takes hours):
  1. Rank all nodes by "importance":
       importance(u) = shortcut_count × edge_difference × (queries through u)
       Dead-end streets: low importance. Highway interchanges: high importance.
  
  2. Contract nodes in order of importance (least important first):
       Contracting node u:
         For each pair (v, w) where v→u→w is a path:
           If v→u→w is the ONLY shortest path from v to w:
             Add shortcut edge v→w with weight = d(v,u) + d(u,w)
       Remove u from graph.
  
  3. Result: augmented graph with ~2x more edges but CH structure intact.

Online query (bidirectional upward Dijkstra):
  Forward search from source:   only visit neighbors with higher rank
  Backward search from dest:    only visit neighbors with higher rank
  Meeting point = shortest path node
  
  Visited: ~10K nodes (vs 300M in plain Dijkstra)
  Latency: ~5ms cross-country
```

### Traffic Integration

```
CH precomputed with base speeds (historical by hour-of-day).
At query time: apply live traffic as weight multiplier on affected edges.
Shortcut weights are NOT recomputed — slight approximation.
Error is small (<5%) for moderate congestion.
For severe events (road closure): set weight = ∞ → CH naturally avoids.
```

### When to Rebuild CH

| Event | Response |
|-------|----------|
| Road network change (new road, permanent closure) | Full CH rebuild (nightly) |
| Traffic congestion | Live weight overlay — no rebuild |
| Incident (accident, temporary closure) | Edge weight = ∞ in overlay — no rebuild |

### Trade-off Summary

| Approach | Preprocessing | Query Time | Traffic Support | Use When |
|----------|--------------|------------|-----------------|---------|
| Plain Dijkstra | None | 2-5s | Dynamic edge weights | Toy examples, small graphs |
| A* | None | 500ms-2s | Dynamic | Simple maps (<1M nodes) |
| **Contraction Hierarchies** | Hours | ~5ms | Overlay | Production navigation at scale |
| OSRM (CH variant) | Hours | ~1-5ms | Limited | Self-hosted routing API |

### Appeared In
- Google Maps (routing engine)

---

## 28. Map Tile Serving

**What it is:** Divide the world into a grid of square image/vector tiles at multiple zoom levels. Serve each tile independently via CDN.

### Tile Coordinate System

```
Zoom level z: world divided into 2^z × 2^z grid
  z=0: 1 tile (whole earth, ~40M km/tile)
  z=10: 1024×1024 tiles (~40km/tile — city level)
  z=15: 32K×32K tiles (~1.2km/tile — street level)
  z=18: 262K×262K tiles (~150m/tile — building level)

Tile address: (z, x, y) — unique ID for any map square at any zoom

Conversion (lng, lat → tile x, y at zoom z):
  x = floor((lng + 180) / 360 × 2^z)
  y = floor((1 - ln(tan(lat×π/180) + 1/cos(lat×π/180)) / π) / 2 × 2^z)
```

### Serving Pipeline

```
Map data (PostGIS/OSM) 
  → Tile Renderer (Mapnik for raster, Tegola for vector)
  → S3 (origin, all tiles pre-rendered for z=0..14)
  → CDN edge nodes (cache by (z,x,y) key)
  → Client

Request flow:
  Client → CDN edge → hit? return tile (~1ms)
                    → miss? → S3 → return + populate CDN (~50ms)
                    → not in S3? → render on-demand → S3 → CDN (~200ms)
```

### Raster vs Vector Tiles

| | Raster (PNG/WebP) | Vector (PBF/MVT) |
|--|---|---|
| **What's served** | Pre-rendered image | Raw geometry + labels |
| **Rendering** | Server-side | Client-side (WebGL) |
| **Zoom** | Blurry between zoom levels | Smooth (render at any zoom) |
| **Size** | ~15KB/tile | ~3-5KB/tile |
| **Customization** | Fixed style (re-render to change) | Client applies any style |
| **Device requirement** | Any | Modern browser/phone |
| **Use** | Fallback, older clients | Modern Google Maps, Mapbox |

### Cache Invalidation

```
Tile update trigger: map data changes (new road, renamed street)
  1. Identify affected (z,x,y) tiles from changed geometry
  2. Re-render → upload new tiles to S3 with new version suffix
  3. Invalidate CDN for affected tiles (CDN purge API)
  4. Client tile URL includes version param → forces re-fetch
  
Frequency: most tiles stable for days/weeks.
           High-change areas (construction zones) updated daily.
```

### Storage Estimate

```
z=0..14: ~500GB (sparse at high zoom, limited unique tiles)
z=15: ~10TB for global coverage
z=16-18: on-demand only (too many tiles to pre-render all)
CDN active cache: ~1-2TB (only popular/urban tiles hot-cached)
```

### Trade-off
> Pre-render all tiles at all zoom levels: predictable latency, high storage cost, stale risk.
> On-demand rendering: fresh always, unpredictable latency, compute cost on cache miss.
> **Hybrid:** Pre-render z=0-15 globally; on-demand for z=16-18 (user zooms in rarely and to specific areas).

### Appeared In
- Google Maps (tile serving pipeline)

---

## 29. Database Replication & Replica Lag

**What it is:** Every write on the primary is recorded in the WAL (Write-Ahead Log) — a sequential, append-only change log. Replicas tail this log and apply changes to their own data files.

### How It Works

```
Primary:
  1. Client writes → Postgres appends to WAL:
       [LSN 0/3A1F20] UPDATE seats id=seat_3A status=HELD
  2. Applies change to its own data files
  3. Streams WAL entry to replicas

Replica:
  1. Receives WAL entry
  2. Applies same change to its own data files
  3. Acknowledges receipt (LSN confirmed)

Lag = primary's current LSN − replica's last acknowledged LSN
```

### Sync vs Async Replication

| Mode | How | Lag | Risk | Use When |
|------|-----|-----|------|----------|
| **Synchronous** | Primary waits for replica ACK before returning success | 0 (always current) | Primary stalls if replica dies | Financial data, zero-tolerance for loss |
| **Asynchronous** (default) | Primary returns immediately, streams in background | 10ms–1s normally, spikes under load | Replica can be behind | Most production systems — performance wins |

### What Causes Lag to Spike
- Network congestion between primary and replica
- Replica under heavy read load (slow to apply WAL entries)
- Large transaction on primary (replica applies whole txn at once — appears as sudden jump)
- Replica restart — must catch up from last acknowledged LSN

### Measuring Lag
```sql
-- On primary: how far behind is each replica?
SELECT client_addr,
       pg_current_wal_lsn() - sent_lsn    AS send_lag_bytes,
       sent_lsn - replay_lsn              AS replay_lag_bytes
FROM pg_stat_replication;
```
Also available as seconds-lag metric in CloudWatch (RDS), Datadog.

### The Core Rule
> Replica reads for *display* are fine — staleness is a UX issue.
> Replica reads for *decisions* (hold checks, balance checks, inventory locks) are bugs — lag causes wrong outcomes.
> Always read from primary before any write-path decision.

### Appeared In
- Ticketmaster (seat availability), Food Delivery (balance check), Flash Sale (inventory), Rate Limiter

---

## 30. Read-Your-Own-Writes (RYOW)

**The problem:** User writes to primary. Immediately reads. Read is routed to replica. Replica hasn't caught up yet. User sees stale data — their own write appears to not have happened.

```
User pays → order status = CONFIRMED on primary
User refreshes page → read goes to replica (500ms behind)
Replica shows: order status = PENDING
User sees: "payment processing" when it already confirmed
```

### Option A: Sticky Primary Read After Write (simplest)

```
After any write → set session flag: read_from_primary=true, TTL=5s
Next request: middleware checks flag → routes to primary → clears flag
All other requests → replicas as normal
```

Pro: simple, no client changes needed.  
Con: primary gets extra read traffic for 5s after every write. Bounded but adds up at scale.

### Option B: LSN-Based Wait (precise)

```
Write response includes: { data: ..., lsn: "0/3A1F20" }
Client sends on next read: X-Read-After-LSN: 0/3A1F20

Server:
  SELECT pg_wal_replay_wait('0/3A1F20', timeout_ms=2000)
  -- blocks until replica has applied up to that LSN, then reads

Fallback: if replica doesn't catch up in 2s → fall back to primary
```

Pro: reads from replica, waits only as long as actual lag. Zero primary load if replica is fast.  
Con: read latency = max(replica lag, 0). Client must store and forward LSN.  
Postgres 14+ has `pg_wal_replay_wait()` natively.

### Option C: Proxy Session Pinning (transparent)

```
PgBouncer / ProxySQL / RDS Proxy tracks LSN per session:
  User writes → proxy notes: session S has seen LSN X
  User reads  → proxy routes to first replica at or past LSN X
              → none ready? → routes to primary

No application code change.
```

Pro: application is unaware, works across all writes automatically.  
Con: requires proxy layer with session-aware routing support.

### Which to Use

| | Option A | Option B | Option C |
|--|--|--|--|
| Complexity | Low | Medium | Low (if proxy exists) |
| Primary load | Small increase (5s window) | Near zero | Near zero |
| Precision | Blunt (always primary for TTL) | Exact (wait for your LSN) | Exact |
| Code change needed | Yes (middleware) | Yes (client + server) | No |

For interviews: describe A as "simple and works," B as "precise — only waits as long as actual lag," C as "transparent at proxy layer."

### Appeared In
- Ticketmaster (order confirmation), Food Delivery (order status), Any system with read replicas

---

## 31. Cache Update Strategies for High-Write Systems

**Extends concept #8.** Specifically: how cache stays fresh when writes are frequent and the cache is on the hot path.

### Three Mechanisms

**TTL (Passive)**
```
Cache entry expires after N seconds → next read rebuilds from DB
```
Pro: zero coupling between write and cache. Simple.  
Con: thundering herd — if many keys expire simultaneously (set at same time during onsale), all miss at once → DB spike.  
Fix: jitter TTL — `TTL = base + random(0, jitter)` — spreads expirations.

**Write-Through (Active, Synchronous)**
```
Write to DB → immediately write to cache in same request handler
```
Pro: cache always fresh after write, ~1ms update.  
Con: adds cache write to the hot path — under contention (holds/sec), every hold now does DB write + Redis write. If Redis is slow, hold latency suffers. Crash between DB write and cache write → stale until TTL.

**CDC → Kafka → Cache Consumer (Async, Decoupled)**
```
DB write → WAL → Debezium → Kafka topic: seats.changes
                                  ↓
                          Cache Updater Service
                          → Redis HSET (batched pipeline)
                          → SSE push to browsers
```
Pro: hold path is clean (DB write only). Cache updater is separate, can batch 1000 changes into one Redis pipeline. Falls behind under pressure without slowing holds.  
Con: ~100-200ms lag from write to cache update. Consumer must be idempotent (Kafka at-least-once).

### Hot Event Rule

```
Normal events  → write-through fine (low write rate, simplicity wins)
Hot events     → CDC only (keep hold path clean, async cache update)
Always         → TTL as backstop (self-heals if CDC lags or dies)
```

**Never use write-through on the hold path during peak load.** It couples two operations that don't need to be coupled and adds latency to your most critical path.

### Thundering Herd on Cache Miss

```
10,000 users → cache TTL expires simultaneously
10,000 requests → cache miss → 10,000 DB reads at once
DB: overwhelmed
```

Fix — Cache mutex (dog-pile prevention):
```
On cache miss:
  SET rebuild_lock:{key} NX EX 2    -- only one request rebuilds
  Got lock? → fetch from DB → populate cache → release lock
  No lock?  → wait 50ms → retry read (another request is rebuilding)
```
Or: serve stale while rebuilding (if TTL + stale-while-revalidate configured).

### Appeared In
- Ticketmaster (seat map, hot events), Food Delivery (menu cache), Flash Sale (inventory), Distributed Cache

---

## 32. SFU vs MCU vs P2P

**The problem:** How do N participants share audio/video in a group call?

### P2P (Mesh)
```
N=4: A sends to B, C, D. B sends to A, C, D. Total links = N(N-1)/2
Each client uploads (N-1) streams simultaneously

N=4  → 3 uploads per client
N=10 → 9 uploads per client (kills mobile/cellular)
```
**Use:** 1:1 calls only. N>3 is impractical.

### MCU (Multipoint Control Unit)
```
Each client sends 1 stream to MCU
MCU decodes all streams → composites video grid → re-encodes → sends 1 stream to each
Client: 1 upload, 1 download (minimal bandwidth)
Server: decode × N + encode × N per frame = O(N) CPU
```
**Use:** Legacy SIP systems, very low-bandwidth environments. Extremely expensive at scale.

### SFU (Selective Forwarding Unit) ✓ Industry Standard
```
Each client sends 1 stream to SFU
SFU forwards RTP packets selectively (no decode/encode)
Client downloads: only active speaker streams (top 3-5)
```
**Server CPU:** Packet routing only. O(1) per packet regardless of content.  
**Client bandwidth:** 1 upload + ~3 downloads (via simulcast layer selection)

### Trade-off Summary

| | P2P | MCU | SFU |
|--|---|---|---|
| Client upload | O(N) | 1 stream | 1 stream |
| Server CPU | None | O(N) decode+encode | O(bandwidth) routing |
| Recording | Hard | Easy (server has composite) | Requires forking |
| E2E Encryption | Easy | Impossible (server decodes) | Possible (insertable streams) |
| Use at scale | No | No | Yes |

### Appeared In
- Video Conferencing (core architecture)

---

## 33. WebRTC: ICE / STUN / TURN

**The problem:** Two peers behind NAT need to establish UDP media connections. NAT blocks unsolicited inbound packets.

### STUN (Session Traversal Utilities for NAT)
```
Client → STUN server: "what's my public IP:port?"
STUN server → Client: "your reflexive address is 1.2.3.4:54321"
Client uses this in SDP offer: ice candidate = 1.2.3.4:54321

Works for: Full cone, restricted cone NAT (most home routers)
Fails for: Symmetric NAT (enterprise/CGNAT) — different external port per destination
```

### TURN (Traversal Using Relays around NAT)
```
When STUN fails → all media relayed through TURN server
Client ──► TURN server ◄── Other peer
Both sides connect to TURN (not each other directly)

~15% of users need TURN (symmetric NAT, firewalls blocking UDP)
Cost: TURN relay = full media bandwidth passing through it (expensive)
TURN over TCP port 443: last resort for ultra-restrictive firewalls
```

### ICE (Interactive Connectivity Establishment)
```
Collects all candidates:
  1. Host candidates    (local IP)
  2. Server-reflexive   (STUN-derived public IP)
  3. Relay candidates   (TURN server address)

Tests all in priority order (local > STUN > TURN)
Picks best working path
Handles failover: path breaks → renegotiates to next best
```

### Rule
> Default: STUN. Budget for 15% TURN users — they consume full media bandwidth.
> Geo-distribute TURN servers to minimize relay latency for TURN users.

### Appeared In
- Video Conferencing (NAT traversal for media plane)

---

## 34. Simulcast + Congestion Control

**The problem:** Participants have vastly different bandwidths (100Mbps fiber vs. 3G mobile). One video quality doesn't fit all.

### Simulcast
```
Publisher encodes 3 parallel quality layers simultaneously:
  Layer H: 1080p @ 2.5Mbps
  Layer M: 480p  @ 600Kbps
  Layer L: 180p  @ 150Kbps

SFU selects which layer to forward to each subscriber.
No re-encoding on server — just forward different RTP stream.
```

**Key:** Publisher always sends all 3. SFU switches without renegotiation.  
**Cost:** Publisher uses ~3× bandwidth (2.5 + 0.6 + 0.15 ≈ 3.25Mbps total upload).

### Bandwidth Estimation: REMB vs TWCC

| | REMB | TWCC |
|--|---|---|
| Direction | Receiver estimates, tells sender | Sender observes based on receiver reports |
| Accuracy | Lower (one estimate per interval) | Higher (per-packet timestamps) |
| Standard | Legacy | Modern (recommended) |

```
TWCC flow:
  Receiver sends feedback: "packet #1001 arrived at t=100ms, #1002 at t=101ms, #1003 LOST"
  Sender runs GoogCC algorithm:
    - Delay gradient (are packets arriving faster or slower?) → congestion signal
    - Loss rate → capacity signal
    - Combine → bandwidth estimate
    - Pick simulcast layer matching estimate
```

### Hysteresis (Prevents Layer Thrashing)
```
Don't switch layers on every fluctuation:
  Upgrade: bandwidth must exceed threshold for 2 consecutive seconds
  Downgrade: bandwidth below threshold for 1 second (faster to react)
```

### Appeared In
- Video Conferencing (adaptive quality per subscriber)

---

## 35. Packet Loss Recovery: NACK / FEC / PLC

**The problem:** UDP is unreliable. Packets get lost. How do you recover or hide it?

### NACK (Negative Acknowledgement) — Reactive
```
Receiver detects missing RTP seq=1042 (gap in sequence)
Sends RTCP NACK: "I'm missing seq=1042, please retransmit"
Sender retransmits packet (from send buffer, 200-500ms retention)

Works for: <5% loss, low-latency connections (RTT < 100ms)
Fails for: high loss (retransmit also gets lost), high RTT (retransmit arrives too late)
```

### FEC (Forward Error Correction) — Proactive
```
Sender adds redundant packets alongside originals:
  Every 5 RTP packets → 1 FEC repair packet (XOR of the 5)
  If any 1 of 5 is lost → receiver reconstructs from FEC

Cost: +20% bandwidth overhead
Works for: random loss up to ~20%
Does NOT help: 2+ packets from same FEC group lost (only 1 can be recovered)
```

### PLC (Packet Loss Concealment) — Audio Only
```
OPUS codec: if packet missing, generates synthetic audio from surrounding frames
Transparent for <10% loss
For 10-20% loss: slight robotic quality
Above 20%: audible degradation → rely on layer downgrade

No extra bandwidth. Entirely client-side.
```

### Layered Recovery Strategy
```
0-5% loss:   NACK alone (low overhead, low latency)
5-20% loss:  FEC + NACK (FEC for random loss, NACK for burst loss)
>20% loss:   Layer downgrade (lower bitrate = more error resilient) + FEC
Audio always: PLC as base layer regardless of other mechanisms
```

### Rule
> NACK for interactive (latency-sensitive). FEC for broadcast/streaming (latency-tolerant). PLC always for audio.
> At >20% sustained loss: no recovery mechanism helps — must reduce bitrate.

### Appeared In
- Video Conferencing (media quality under network degradation)

---

## 32. Circuit Breaker — Microservices Pattern

> Different from #26 (financial market halt). This is a fault-tolerance pattern for service dependencies.

**What it is:** A state machine wrapping calls to an external dependency. When the dependency starts failing, the circuit "opens" — calls fail fast without hitting the dependency. Periodically probes for recovery. Prevents cascade failures where one slow service takes down everything upstream.

### The Problem Without It

```
Service A calls Service B (Redis, Payment, Queue, etc.)
Service B is slow/down → A's threads block waiting for timeout (30s each)
500 concurrent users → 500 threads blocked → A's thread pool exhausted → A dies
Service C calls A → A is dead → C dies
→ Cascade failure from one downstream dependency
```

### State Machine

```
         failure threshold          probe succeeds
CLOSED ────────────────────► OPEN ──────────────────► HALF-OPEN
  ▲                            │                           │
  │                            │ all calls fail fast       │ allow 1 probe call
  │                            ▼                           │
  └────────────────────────────── probe fails ─────────────┘
         reset threshold
```

**CLOSED** (normal): calls pass through, failures counted.  
**OPEN** (tripped): all calls fail immediately (no network call made). Returns fallback.  
**HALF-OPEN** (recovery probe): allow one call through. Success → CLOSED. Failure → OPEN again.

### Implementation

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60s, probe_interval=10s):
        self.state = CLOSED
        self.failure_count = 0
        self.last_failure_time = None

    def call(self, fn, fallback):
        if self.state == OPEN:
            if now() - self.last_failure_time > self.probe_interval:
                self.state = HALF_OPEN   # try one probe
            else:
                return fallback()        # fail fast, no network call

        try:
            result = fn()                # actual call
            self.on_success()
            return result
        except Exception:
            self.on_failure()
            return fallback()

    def on_success(self):
        self.failure_count = 0
        self.state = CLOSED

    def on_failure(self):
        self.failure_count += 1
        self.last_failure_time = now()
        if self.failure_count >= self.failure_threshold:
            self.state = OPEN
```

### Applied to Waiting Queue + Redis Failure

```
Queue Service wraps Redis calls in circuit breaker:

CLOSED (normal):
  ZADD queue:{event_id} → Redis ✓
  ZPOPMIN → drain tokens ✓

Redis primary fails → Sentinel starts election (30s):
  5 consecutive failures → circuit OPEN
  All queue operations: fail fast, return fallback immediately

Fallback options (in order of preference):
  1. Rate limiting at API Gateway (500 req/sec global cap)
  2. Serve pre-buffered tokens from local memory
  3. Reject with 503 + Retry-After: 30

Circuit HALF-OPEN after probe_interval:
  One ZADD attempt → succeeds (Redis back) → CLOSED, normal operation
```

### Fallback Design — the Hard Part

Circuit breaker without a fallback just converts "slow failure" to "fast failure." The fallback determines system behavior during the outage:

| Fallback | Behavior | Use When |
|----------|----------|----------|
| **Cached response** | Return last known good value | Read-heavy, stale ok (seat map, event list) |
| **Degraded mode** | Skip non-critical step (queue check), proceed with rate limit | Queue service, notification service |
| **Queue locally** | Buffer writes in memory, replay when circuit closes | Low write volume, short outage expected |
| **Hard reject (503)** | Return error immediately | No safe degraded mode exists (payment) |

### Key Numbers to Tune

```
failure_threshold:  5 failures     (too low = flapping; too high = slow to trip)
timeout:            60s            (how long before probing — match expected recovery time)
probe_interval:     10s            (how often to check if dependency recovered)
call_timeout:       500ms          (per-call timeout before counting as failure)
```

### Circuit Breaker vs Retry

These are complementary, not alternatives:

```
Retry: transient failures (network blip, single timeout) → retry with backoff
Circuit Breaker: sustained failures (service down) → stop retrying, fail fast

Without circuit breaker:
  Retry storms: 500 users × 3 retries × 30s timeout = 45,000 thread-seconds held
With circuit breaker:
  After 5 failures → open → all 500 users get instant fallback → thread pool free
```

Rule: retry for transient, circuit break for sustained.

### Real-world
- Netflix Hystrix (original), Resilience4j (Java), Polly (.NET)
- AWS SDK has built-in retry + circuit breaking
- Redis client libraries (Lettuce, ioredis) support circuit breaker config

### Appeared In
- Ticketmaster (Redis queue failover), Food Delivery (payment service), Notification System, Any system with external dependency

---

## 36. Consistent Hashing + Virtual Nodes

**What it is:** Map keys and nodes to a hash ring [0, 2^128). A key is owned by the first node clockwise from its hash position. Virtual nodes (vnodes) assign each physical node multiple ring positions.

```
Ring:  0 ─── t1(A) ─── t2(B) ─── t3(C) ─── t4(A) ─── 2^128
              │              │              │
           Node A         Node B         Node C

key hash → lands at position P → walk clockwise → first N nodes = replicas
```

**Why V=150 virtual nodes per physical node:**
- Even distribution: each node handles ~1/N of keys across 150 non-contiguous segments
- Node failure: 150 small key ranges absorbed by different neighbors (no hotspot)
- Node addition: takes a few vnodes from each existing node (even rebalance)
- Heterogeneous hardware: larger nodes get more vnodes assigned

**When to use:** Any distributed storage system that must partition data across nodes and handle dynamic topology (Cassandra, DynamoDB, Redis Cluster).

**When NOT to use:** If data can fit on one node + replicas. Adds operational complexity for no gain.

**Trade-off:** More vnodes = better balance but more ring metadata (150 nodes × 150 vnodes = 22,500 entries). V=150 is the standard sweet spot.

### Appeared In
- Distributed KV Store (2026-06-12), Distributed Cache

---

## 37. LSM Tree

**What it is:** Log-Structured Merge Tree. Write-optimized storage engine: append to WAL → MemTable (in-memory) → periodic flush to immutable SSTable files on disk → background compaction merges SSTables.

```
Write path:
  PUT(k,v) → WAL (sequential, fsync) → MemTable (sorted skip-list)
  MemTable full → flush to L0 SSTable (immutable, sorted)

Read path:
  MemTable → L0 SSTables → L1 SSTables → ...
  Bloom filter per SSTable → O(1) check: "can this file have key K?"
  Binary search within SSTable → O(log N)

Compaction (background):
  Merge adjacent SSTables → remove duplicates + tombstones → fewer files
  Levels: L0 (newest) → L1 → L2 (oldest, most compacted)
```

**Write amplification vs Read amplification:**
- LSM: writes fast (sequential), reads slower (check multiple files) → write amplification during compaction
- B-Tree: reads fast (one tree traversal), writes slower (random I/O, update in place)

**When to use:** Write-heavy KV stores (Cassandra, RocksDB, LevelDB, InfluxDB).

**When NOT to use:** Read-heavy, low write workloads → B-Tree (Postgres, MySQL) wins on reads.

**Trade-off summary:**
| | LSM Tree | B-Tree |
|--|---|---|
| Writes | Fast (sequential) | Slower (random I/O) |
| Reads | Slower (multi-file, mitigated by bloom filter) | Fast |
| Space | Needs compaction to reclaim | More compact |
| Crash recovery | WAL replay | WAL + page recovery |

### Appeared In
- Distributed KV Store (2026-06-12)

---

## 38. Quorum Reads/Writes

**What it is:** In a system with N replicas, require W ACKs for a write and R responses for a read. Key rule: **R + W > N** guarantees at least one replica in every read/write overlap → always reads fresh data.

```
N=3, W=2, R=2:
  Write quorum: {A, B}  (2 of 3)
  Read quorum:  {B, C}  (2 of 3)
  Overlap: B → B always has latest write ✓

Common configs:
  R=2, W=2 → balanced, strong consistency, tolerates 1 failure
  R=1, W=1 → max availability, eventual consistency, max throughput
  R=1, W=3 → write all, fast reads (rarely used)
  R=3, W=1 → all replicas must agree to read (paranoid consistency)
```

**Tunable consistency:** Allow application to choose per-request. DynamoDB: `ConsistentRead=true` → R=N (all replicas). Default → R=1.

**When R+W ≤ N:** Eventual consistency. Reads may return stale data. Acceptable for user sessions, shopping carts; not acceptable for financial balances.

**Trade-off:** Higher W → more durable, higher write latency. Higher R → more consistent, higher read latency. Sweet spot: R=W=ceil(N/2)+1.

### Appeared In
- Distributed KV Store (2026-06-12)

---

## 39. Vector Clocks

**What it is:** A map `{node_id → logical_counter}` attached to each version of a key. Used to detect concurrent writes (conflicts) and track causal order without relying on wall-clock time.

```
Rules:
  On write at Node A: increment A's counter
    {} → {A:1} → {A:2}
  
  On write at B after reading A's version {A:2}:
    {A:2} → {A:2, B:1}  (causal successor — B "knows about" A's 2 writes)
  
  Conflict (neither dominates):
    {A:3} vs {B:1}
    A:3 is not ≥ B:1 in all components → concurrent → CONFLICT
    
  Dominance (A's version supersedes B's):
    {A:3, B:1} vs {A:2, B:1}
    First dominates: every component ≥ second → A:3 is newer
```

**Conflict resolution options:**
1. **LWW (Last-Write-Wins):** use timestamp → simple, risks losing concurrent writes on clock skew
2. **Sibling return:** return both versions, client resolves (Amazon's shopping cart approach)
3. **CRDT:** data structure with auto-merge (counters: sum, sets: union)

**When to use:** Distributed stores where concurrent writes are possible and data loss is unacceptable (shopping cart, collaborative editing).

**When NOT to use:** If LWW is acceptable (user profile fields where re-entry is trivial).

### Appeared In
- Distributed KV Store (2026-06-12), Collab Doc Editor (OT/CRDT variant)

---

## 40. Gossip Protocol

**What it is:** Epidemic-style information propagation. Each node periodically selects K random peers and exchanges its view of cluster state. Converges in O(log N) rounds.

```
Every T=1s, Node A:
  1. Pick 3 random peers (B, C, D)
  2. Send: my_heartbeat_table = {A:seq_100, B:seq_95, C:seq_88, ...}
  3. Each peer: take max(my_table, received_table) per node

Failure detection:
  Node X's heartbeat stops → after Φ threshold → SUSPECT → DOWN
  
  Φ-accrual detector (better than fixed timeout):
    Tracks inter-arrival times of heartbeats → models as distribution
    Φ = -log(probability_X_is_still_alive)
    Φ > 8 → mark DOWN (very high confidence, not just slow)
    Avoids false positives from transient network delays
```

**Propagation time:** O(log N) gossip rounds. With 100 nodes and T=1s: failure detected in ~7 seconds globally.

**When to use:** Cluster membership, failure detection in leaderless systems (Cassandra, DynamoDB, Consul).

**When NOT to use:** When you need strong consistency of membership (use Raft-based consensus like etcd/Zookeeper instead).

**Trade-off:** Fast convergence, decentralized, no SPOF. But: probabilistic (not guaranteed), can have brief inconsistent views during partitions.

### Appeared In
- Distributed KV Store (2026-06-12)

---

## 41. Merkle Tree Anti-Entropy

**What it is:** A hash tree where leaves = hash(data for a key range), parents = hash(children). Two nodes compare roots: equal → identical data; different → drill down to find exactly which ranges diverge.

```
              root
             /    \
          h(L)   h(R)
          / \    / \
       h1  h2  h3  h4    ← hash of each key range [0-25%), [25-50%), etc.

Comparison protocol:
  A.root ≠ B.root → compare left subtrees
  A.h(L) ≠ B.h(L) → compare h1 and h2
  A.h1 ≠ B.h1 → range [0-25%) has diverged
  Send only those keys → repair
  
Cost: 10M keys = 100GB diff without Merkle
      Merkle: O(log N) comparisons → ~50KB metadata → targeted repair
```

**When to use:** Background anti-entropy repair after network partitions, node recovery in distributed storage.

**When NOT to use:** For small datasets where full comparison is cheap.

**Trade-off:** Merkle tree must be kept updated as data changes (incremental hashing). Initial build is O(N). After that, updates are O(log N) per write if tree is maintained lazily.

### Appeared In
- Distributed KV Store (2026-06-12)

---

## 42. Hinted Handoff

**What it is:** When a replica is temporarily unavailable and a write would miss it, the coordinator stores the write locally with a "hint" — a note saying "deliver this to Node X when it recovers."

```
Node B is DOWN. Write quorum W=2, N=3.
Coordinator: write to A ✓, write  Delivers hint to Node B → Node B applies write → hint deleted
```

**When to use:** Dynamo-style systems where availability > consistency. Allows W=2 writes even when a replica is temporarily unreachable.

**When NOT to use:** Strict consistency systems (bank accounts). A hinted write is not a confirmed replica write.

**Trade-off:** Improves write availability. But hints stored locally can pile up if node is down for a long time → limit hint storage window (default 3h in Cassandra). After expiry → rely on read repair or anti-entropy.

### Appeared In
- Distributed KV Store (2026-06-12)

---

## 54. Redis Sorted Set for Leaderboards

**What it is:** A Redis data structure mapping members to floating-point scores, kept sorted by score. Backed by a skip list (range queries) + hash table (O(1) member lookup). Supports O(log N) insert/update/rank-query.

### Core Commands

| Command | What it does | Complexity |
|---------|-------------|------------|
| `ZADD key [GT\|LT\|NX] score member` | Insert or update score | O(log N) |
| `ZREVRANK key member` | 0-indexed rank from top (highest score = 0) | O(log N) |
| `ZSCORE key member` | Get raw score | O(1) |
| `ZREVRANGE key start stop [WITHSCORES]` | Top-K range | O(log N + K) |
| `ZMSCORE key m1 m2 ...` | Batch score fetch | O(N members) |
| `ZCOUNT key min max` | Count members in score range | O(log N) |
| `ZINCRBY key delta member` | Increment score (additive leaderboard) | O(log N) |

### When to Use
- Any ranking problem: game scores, reputation points, trending content, search result caching
- Read-heavy rank queries (ZREVRANK is O(log N) vs O(N) SQL window function)
- 50K+ writes/sec where SQL index contention is the bottleneck

### When NOT to Use
- Leaderboards requiring complex multi-attribute ranking (e.g., rank by wins, then by KDR) — Redis sorts by single numeric score only
- Data that must survive Redis restart without persistence configured

### Capacity
- Each member: ~64 bytes overhead. 10M members ≈ 640MB RAM. Comfortable on a 16GB instance.
- Hard limit: 2^32 members per sorted set (~4 billion).

### Trade-offs

| Aspect | Detail |
|--------|--------|
| Latency | Microseconds in-process; <1ms from app server |
| Memory | All in-RAM — more expensive than disk-based DB |
| Persistence | AOF / RDB snapshotting; async replication. Not zero-loss without WAIT |
| Sharding | Cross-shard rank queries require O(N_shards) parallel ZCOUNT + merge |

### Appeared In
- Real-time Leaderboard (2026-06-15)

---

## 55. Windowed Leaderboard (TTL-based Key Rotation)

**What it is:** Pattern for maintaining separate leaderboards per time window (daily, weekly) without cron-based resets. Each window gets a unique key derived from the current date; Redis TTL auto-expires old keys.

```
Key schema:
  lb:{game}:daily:{YYYYMMDD}    → expires after 2 days
  lb:{game}:weekly:{YYYY_WW}    → expires after 14 days
  lb:{game}:alltime             → no expiry

Score write:
  today = datetime.utcnow().strftime('%Y%m%d')
  ZADD lb:fortnite:daily:{today} GT score player_id
  EXPIRE lb:fortnite:daily:{today} 172800   # 2 days = 86400 * 2
```

### Why TTL > Explicit Reset

| Approach | Pros | Cons |
|----------|------|------|
| TTL key per day | Zero downtime, no cron, history preserved briefly | Key proliferation (1K games × 365 = 365K keys; fine) |
| DEL + recreate at midnight | Simple mental model | Downtime window, thundering herd on first read of empty key |
| Sorted set per window, manual reset | Fine-grained control | Operational complexity, risk of forgetting reset |

### Clock Skew Risk
If game servers are in different timezones or have clock drift, score might land in wrong day's key. Fix: all Score Services use UTC and NTP-synced clocks. Alternatively, pass the window key explicitly from a central "window coordinator" service.

### When to Use
- Any periodic leaderboard (daily challenge, weekly tournament, seasonal rankings)
- When you want historical window data briefly available (e.g., "yesterday's leaderboard")

### Appeared In
- Real-time Leaderboard (2026-06-15)

---

## 56. Score Idempotency — ZADD GT/LT

**What it is:** Making score submissions safe to retry by ensuring duplicate or out-of-order submissions don't corrupt the leaderboard. Two mechanisms: Redis `ZADD GT/LT` flags and application-layer idempotency keys.

### ZADD Flags

| Flag | Behavior | Use When |
|------|---------|----------|
| `GT` | Update only if new score > current | Best-score leaderboard (player's lifetime best) |
| `LT` | Update only if new score < current | Fastest-time leaderboard (lower is better) |
| `NX` | Only add if member not yet present | First-submission-wins |
| `XX` | Only update existing members | Never add new members (maintenance mode) |
| _(none)_ | Always overwrite | Last-submission-wins (most recent match score) |

```redis
# Game: highest score ever
ZADD lb:game:alltime GT 9500 player123     # → 9500
ZADD lb:game:alltime GT 8000 player123     # → no-op, still 9500
ZADD lb:game:alltime GT 9500 player123     # → no-op (retry of same value = safe)

# Game: speedrun, fastest time
ZADD lb:game:speedrun LT 92.3 player123   # → 92.3s
ZADD lb:game:speedrun LT 95.1 player123   # → no-op, 95.1 > 92.3 (slower)

# Additive XP leaderboard (no GT/LT — use ZINCRBY)
ZINCRBY lb:game:xp 500 player123          # add 500 XP
```

### Application-Layer Idempotency Key

For additive scores, ZADD GT won't help (ZINCRBY is not idempotent). Use idempotency keys:

```python
idem_key = f"idem:{match_id}:{player_id}"
result = redis.set(idem_key, 1, nx=True, ex=86400)  # SET NX (only if not exists)
if result is None:
    return "already processed"  # retry detected
r
---

## 57. Feature Store

**What it is:** A centralized system that stores, serves, and manages ML features — both for real-time inference (online store) and model training (offline store).

### Two-Tier Architecture

```
Write path (async):
  Transaction events → Flink → Feature computation
    → Online Store  (Redis):    velocity, account stats   ← <2ms read, real-time path
    → Offline Store (BigQuery): historical snapshots      ← for training

Read path:
  Real-time inference:  Online Store (Redis HGET) — 1-2ms
  Model training:       Offline Store with point-in-time query
```

### Point-in-Time Correctness (Critical for Training)

```
Wrong (future leakage):
  Transaction at T=10:00 → fetch features at T=10:01 → model sees outcome-correlated features
  → overfit → production performance collapses

Correct:
  Transaction at T=10:00 → fetch features as of T=09:59:59
  Feature Store: stores (feature, value, event_time) → can serve "as of any past timestamp"
  
Feast/Tecton: frameworks that enforce this automatically
```

### Trade-offs

| | Without Feature Store | With Feature Store |
|--|---|---|
| Consistency | Training features ≠ inference features (training-serving skew) | Same feature computation code for both |
| Reuse | Each model reimplements same features | Shared feature definitions |
| Latency | Ad-hoc Redis lookups | Pre-computed, standardized fetches |
| Complexity | Low | Medium (need to maintain store) |

**Training-serving skew** is the #1 cause of ML model underperforming in production vs. offline metrics. Feature Store prevents it.

### When to Use
- Multiple ML models sharing the same features (fraud, recommendation, credit scoring)
- Features require expensive computation (sliding window aggregations, graph traversal)
- Need point-in-time correct training data

### When NOT to Use
- Single model, simple features (just pass raw request fields)
- Low traffic (<100 RPS) — overhead not justified
- Latency >10ms is acceptable — can compute features on the fly

### Appeared In
- Fraud Detection System (2026-06-16)

---

## 58. Rule Engine Pattern

**What it is:** A system that evaluates a set of configurable if-then rules against an input, returning an action. Rules are data (stored in DB/config), not code — editable by non-engineers without deployment.

### Structure

```python
Rule:
  id:          unique identifier
  priority:    evaluation order (lower = evaluated first)
  condition:   boolean expression over input features
  action:      DENY | APPROVE | REVIEW | FLAG
  reason:      human-readable string for audit

Evaluation:
  Sort rules by priority
  For each rule: if condition(features) is True → return action
  If no rule matches → proceed to fallback (ML scorer, default action)
```

### Short-Circuit Evaluation
```
Hard DENY rules (priority=1): check first, fail fast — no need to evaluate 50 other rules
Soft rules (priority=3+): only if no hard rule triggered

Performance: 50 rules × O(1) evaluation = ~1ms total
```

### Storage Pattern
```
Rules in Postgres (source of truth, audit log)
→ cache in Redis (hash: rule_id → rule_json)
→ gateway loads to in-memory on startup, refreshes every 60s via Redis pub/sub

Update:
  Ops team edits rule in UI → Postgres write → publish to Redis pub/sub
  All gateways receive update within 60s — no code deploy, no restart
```

### Pros vs Cons

| Aspect | Rules | ML |
|--------|-------|-----|
| Explainability | Perfect (exact condition) | Approximate (SHAP) |
| Adaptability | Manual update (hours) | Auto-retrain (24h cycle) |
| New fraud pattern | Blind until rule written | Adapts from data |
| False positive risk | Exact (no noise) | Requires threshold tuning |
| Audit trail | Built-in (rule_id in decision) | "Score was 78" |

**Rule:** Use rules for known, deterministic patterns. Use ML for nuanced, combination patterns.

### When to Use
- Compliance requirements (must be auditable, explainable)
- Known fraud patterns that are deterministic (stolen card block list, velocity hard limits)
- Non-engineers need to adjust policy (risk team, compliance team)

### When NOT to Use
- Complex non-linear patterns (amount × account_age × merchant × device combination)
- Rapidly evolving attack patterns where manual rule writing is too slow

### Appeared In
- Fraud Detection System (2026-06-16)

---

## 59. Fail-Open vs Fail-Closed vs Fail-Safe

**The question:** When a critical system dependency is unavailable, what should the system do?

### Three Modes

| Mode | Behavior | Use When |
|------|----------|----------|
| **Fail-Closed** | Reject all requests. "When in doubt, deny." | Zero tolerance for bad outcomes (nuclear plant, medical device). Revenue loss acceptable. |
| **Fail-Open** | Accept all requests. "When in doubt, allow." | Bad outcome of blocking >> bad outcome of allowing. Internal tool auth, cache layer. |
| **Fail-Safe** | Apply minimal rules, allow with degraded confidence. | Revenue + safety both matter. Most production systems. |

### Fraud Detection Fail-Safe

```
Normal: full enrichment + rules + ML → precise decision
Fail-safe (fraud system degraded):
  1. Check local in-memory block list (stolen cards, refreshed every 60s)
  2. Apply only hard velocity rule (50 tx/hr instead of 20 — more permissive)
  3. Approve with flag: "degraded_mode = true"
  4. Queue for retrospective scan when system recovers

Result: catches ~80% of fraud (major stolen cards), approves rest
        vs fail-closed: no revenue blocked
        vs fail-open: still blocks known-bad cards
```

### Retrospective Scan (Fail-Safe Companion)

```
When system recovers:
  SELECT * FROM transactions WHERE degraded_mode = true AND approved = true
  Re-run full fraud analysis with fresh models
  Flag any transactions now scoring > 70 → initiate chargeback
  
This is the safety net that makes fail-safe viable:
  You accept that fraud gets through temporarily
  But you have a plan to detect and remediate it post-hoc
```

### Payment System Fail-Closed Example
```
Payment processor can't reach bank authorization → fail-closed (decline transaction)
Why: the failure is in the authorization path itself
     Approving without bank auth = financial risk, not just user experience risk
```

### Circuit Breaker + Fail-Safe
```
On dependency failure:
  Circuit OPENS → all calls fail fast (no 30s timeout per call)
  Fallback code (fail-safe) runs instead
  Circuit probes every 10s → on recovery: CLOSED → normal path

The combination is: circuit breaker catches the failure, fail-safe determines the behavior during failure
```

### Appeared In
- Fraud Detection System (failure analysis, 2026-06-16)
- Ticketmaster (queue service Redis failure)

---

## 60. Champion/Challenger ML Testing

**What it is:** Running two ML models simultaneously — current production model (champion) and new candidate (challenger) — comparing on real traffic before replacing champion.

### Why Needed
ML models can look great on offline test sets but underperform in production:
- Training data distribution ≠ production data (concept drift)
- Feature computation in training ≠ in serving (training-serving skew)
- Edge cases not in test set

### Testing Strategy

```
Stage 1: Shadow mode (0% challenger decisions, 100% logging)
  All transactions: champion decides, challenger also scores (but decision ignored)
  Compare: challenger_score vs champion_score distribution
  Check: do they agree on clear cases? Disagree on gray zone (30-70)?
  Duration: 24-48h to see daily patterns

Stage 2: Canary (5% challenger, 95% champion)
  5% of traffic: challenger makes the actual decision
  Monitor: DENY rate, FPR (from support tickets), TPR (from cases)
  Compare vs champion cohort
  Duration: 7 days (capture weekly patterns, Monday ≠ Saturday fraud patterns)

Stage 3: Promote or rollback
  If AUC improves AND FPR within acceptable range → champion = challenger
  If FPR spikes → rollback: challenger_traffic_pct = 0
```

### Metrics Comparison

```
Champion (last 7 days):
  AUC: 0.94
  FPR: 0.018%
  TPR: 91.2%
  Score P50: 12  P95: 68

Challenger (5% canary, last 7 days):
  AUC: 0.96 ↑
  FPR: 0.015% ↑
  TPR: 93.1% ↑
  Score P50: 10  P95: 65
→ Promote challenger (all metrics better)
```

### Hot Reload (Zero-Downtime Promotion)

```
TF Serving model versioning:
  /models/fraud_model/versions/41/  ← champion
  /models/fraud_model/versions/42/  ← challenger

model_config.proto:
  model_version_policy { specific { versions: [41, 42] }}

Traffic split: proxy routes X% to v42, (100-X)% to v41
Promotion: update alias "fraud_model_prod" → v42 → TF Serving hot-reloads
No restart. No downtime. Rollback: alias → v41 (same, <30s)
```

### Applies To
Any ML system that affects revenue or safety:
- Fraud detection (deny rate = revenue loss if FPR spikes)
- Ad ranking (revenue per impression)
- Search ranking (click-through rate)
- Recommendation (engagement)

### Appeared In
- Fraud Detection System (2026-06-16)

---

## 61. Graph-Based Fraud Ring Detection

**What it is:** Modeling accounts, cards, devices, and identifiers as a graph to detect coordinated fraud that individual transaction checks miss.

### Why Individual Transaction Checks Fail

```
Account A makes 5 small, spread-out transactions → below velocity threshold → APPROVE
Account B does same → APPROVE
Account C does same → APPROVE

But: A, B, C all share device D1, and B, C share phone P1
→ Fraud ring: all three are mule accounts controlled by same attacker
→ Individual analysis: each looks clean
→ Graph analysis: connected → flag all
```

### Graph Model

```
Nodes:
  Account   (id, created_at, kyc_level)
  Card      (id, bin, issuer)
  Device    (fingerprint, os, ip_history)
  Phone     (e164_hash)     ← hashed for privacy
  Email     (sha256_hash)   ← hashed

Edges:
  (Account)-[:USES_CARD]->(Card)
  (Account)-[:USES_DEVICE]->(Device)
  (Account)-[:REGISTERED_WITH]->(Phone)
  (Account)-[:EMAIL]->(Email)
  (Account)-[:SENT_TO]->(Account)   ← money transfer graph
```

### Detection Patterns

```
1. Shared Device (device fingerprint collision):
   MATCH (a1:Account)-[:USES_DEVICE]->(d:Device)<-[:USES_DEVICE]-(a2:Account)
   WHERE a1 <> a2 AND d.account_count > 5
   → Device used by 5+ accounts = likely mule account farm

2. Fraud Ring Expansion (2-hop):
   MATCH (fraud:Account {status: "confirmed_fraud"})-[:USES*1..2]-(suspect:Account)
   RETURN DISTINCT suspect.id
   → Flag all accounts within 2 hops of confirmed fraud

3. Money Mule Detection:
   MATCH p = (src:Account)-[:SENT_TO*2..4]->(dst:Account)
   WHERE src.confirmed_fraud = true
   RETURN nodes(p)  ← chain of accounts layering stolen money

4. Velocity on Graph:
   MATCH (a:Account)-[:USES_DEVICE]->(d:Device)
   WHERE d.tx_count_24h > 50
   → Device transacting at extreme rate (bot)
```

### When to Run

```
Async (not real-time): graph traversal 50-200ms, too slow for <100ms budget
Triggers:
  - Transaction REVIEW → graph check before human analyst sees it
  - Case resolved as fraud → expand to find ring members
  - Nightly batch: scan all high-risk accounts for new graph connections

Real-time alternative (partial):
  At REVIEW time: Redis SET check — is device_id known to be associated with fraud?
  Maintained separately: device_fraud_association:{device_id} → last seen with fraud account
  O(1) lookup, real-time safe
```

### Graph DB Choice

| DB | Traversal | Scale | Use When |
|----|-----------|-------|---------|
| **Neo4j** | Cypher queries, fast traversal | 100M nodes, 1B edges | Dedicated fraud graph, complex traversal |
| **Amazon Neptune** | Gremlin/SPARQL, managed | Same scale, managed | Cloud-native, less ops overhead |
| **PostgreSQL recursive CTE** | `WITH RECURSIVE`, moderate depth | <10M nodes | Simple 2-3 hop traversal, small scale |
| **Kafka Streams + in-memory** | Custom logic | Limited by RAM | Real-time simple graph (device→account mapping) |

### Trade-offs

| Aspect | Detail |
|--------|--------|
| Latency | 50-200ms traversal → async only |
| Privacy | Must hash PII (phone, email) before storing as graph nodes |
| Graph update lag | ~1s behind transactions (Kafka consumer) |
| Complex queries | 6-hop traversals can OOM Neo4j on dense graphs → limit to 3 hops |

### R
---

## 62. Service Discovery

**What it is:** A mechanism for services to find each other's network addresses without hardcoded IPs. Services register on startup (with IP, port, health check URL) and deregister on shutdown. Clients query the registry to get current healthy instances.

### Two Models

| | Client-side discovery | Server-side discovery |
|--|---|---|
| **How** | Client queries registry, picks instance, connects directly | Client talks to LB, LB queries registry and routes | 
| **Examples** | Netflix Ribbon + Eureka | AWS ALB + ECS, Kubernetes Service | 
| **Pros** | Client has full control, can implement custom LB logic | Client is simple (no discovery logic) |
| **Cons** | Client complexity; language-specific SDK required | LB is indirection; extra hop |

### Consul (Most Common Self-Hosted)

```
Service startup:
  PUT /v1/agent/service/register {
    "Name": "order-svc",
    "Address": "10.0.1.45",
    "Port": 8080,
    "Check": { "HTTP": "http://10.0.1.45:8080/health", "Interval": "10s" }
  }

Query (by gateway):
  GET /v1/health/service/order-svc?passing=true
  → [{ "Service": { "Address": "10.0.1.45", "Port": 8080 } }, ...]

Watch (streaming updates):
  Consul watch with ?wait=60s&index=<last_seen_index>
  → Returns immediately when state changes (long-poll)
  → Gateway updates in-memory pool within 1s of change
```

### Kubernetes (Cloud-Native)

```
Service: a stable DNS name (order-svc.default.svc.cluster.local) + ClusterIP
Endpoints: Kubernetes maintains the list of healthy pod IPs behind a Service
kube-proxy: programs iptables rules → VIP routes to pod IPs

Gateway (in K8s): call order-svc:8080 → kube-proxy routes to healthy pod
Pod death: K8s updates Endpoints within ~5s → kube-proxy updates iptables
```

### Failure: Registry Down

```
Gateway: serve from local in-memory cache (30s TTL)
+ independent gateway health checks (removes dead instances even without registry)
Result: can survive registry outage for 30s without routing errors
```

### When to Use
- Any microservices system where instances scale dynamically
- When IPs are ephemeral (containers, spot instances)

### When NOT to Use
- Monolith (only one instance, hardcode is fine)
- Fixed infrastructure (bare metal, static IPs) — DNS is simpler

### Trade-offs

| Aspect | Detail |
|--------|--------|
| Freshness | Watch-based: ~1s delay. Poll-based: TTL delay (5-30s) |
| Health check lag | Failed instance still in registry for 1-2 health-check intervals (~10-30s) |
| Gateway local cache | Reduces load on registry; adds TTL-worth of staleness |

### Appeared In
- API Gateway (2026-06-17)

---

## 63. JWT vs API Key vs Token Introspection

**The question:** How should clients authenticate with an API gateway?

### JWT (JSON Web Token) — Self-Contained

```
Structure: Header.Payload.Signature (base64url encoded)

Header: { "alg": "RS256", "typ": "JWT" }
Payload: { "sub": "user_123", "roles": ["user"], "exp": 1750000000, "jti": "tok_abc" }
Signature: RSA_sign(header + "." + payload, private_key)

Gateway verify:
  1. Decode header → get alg
  2. Verify signature with Auth Service's PUBLIC key (stored in gateway memory)
  3. Check exp: token.exp > now()  (plus leeway for clock skew)
  4. Extract sub, roles
  
Cost: ~0.5ms CPU, zero network calls
```

**Revocation problem:** JWT is stateless. Once issued, valid until exp.

| Revocation solution | How | Cost | Use when |
|---------------------|-----|------|---------|
| Short TTL (15 min) | Let it expire | 0 | Default — user sessions |
| jti blocklist (Redis) | Gateway checks SET jti:{id} | +1ms Redis GET/req | Admin tokens, password reset |
| Token introspection | Call Auth Service | +10-50ms | Instant revoke required |

### API Key — Server-to-Server

```
Client: Authorization: ApiKey sk_live_abc123xyz

Gateway:
  key_hash = SHA256(raw_key)          ← store hash, not raw key
  info = Redis.GET("api_keys:" + key_hash)
  → { user_id, plan, rate_limit_tier, scopes, created_at }
  
If not in Redis → lookup DB → cache 60s
If not in DB → 401

Revocation: invalidate Redis key + DB soft-delete
            Gateway cache: stale for up to 60s (acceptable for most cases)
            Immediate: flush Redis key (ops action)
```

### Token Introspection — Opaque Tokens

```
Client sends: Authorization: Bearer <opaque_string>
Gateway: POST /introspect → Auth Service
Auth Service: lookup token in DB → return active/inactive + user metadata

Cost: 10-50ms network call on EVERY request
Use only for: admin dashboards, internal tools, systems where instant revoke is critical
NOT for: high-traffic public APIs (100K RPS × 50ms = unsustainable Auth Service load)
```

### Decision Matrix

| Use Case | Mechanism | Why |
|----------|----------|-----|
| Mobile/browser user | JWT RS256, 15-min TTL | Fast, no Auth Service call |
| Third-party API | API Key | Server identity, scoped permissions |
| Admin panel | JWT + jti blocklist | Can be revoked on logout |
| Internal service-to-service | mTLS (service identity) | No user context needed |
| High-security (banking auth) | Short JWT + refresh token rotation | Fresh token each session |

### Appeared In
- API Gateway (2026-06-17)

---

## 64. Request Pipeline (Middleware Chain)

**What it is:** A sequential chain of processing steps applied to every request. Each step (middleware) does one thing: extract auth, rate limit, log, transform. Middleware can short-circuit (return response early) or pass to the next step.

```
Request → [M1: Parse] → [M2: Auth] → [M3: RateLimit] → [M4: CircuitBreaker] → [M5: Forward] → Response
                                         ↑ 429 here                              ↑ 503 here
```

### Why Middleware, Not One Big Handler

- **Single Responsibility:** each middleware has one job → easy to test, replace, reorder
- **Reuse:** auth middleware shared across all routes, not reimplemented per route
- **Configurability:** different routes use different middleware subsets (public route: skip auth middleware)
- **Observability:** wrap the chain in a timing middleware → measure each step's latency

### Implementation Pattern

```python
class Middleware:
    def handle(self, request, next_handler) -> Response:
        # pre-processing
        result = next_handler(request)  # call next middleware
        # post-processing
        return result

# Chain:
pipeline = [
    ParseMiddleware(),
    AuthMiddleware(jwt_public_key),
    RateLimitMiddleware(redis, policies),
    CircuitBreakerMiddleware(states),
    ForwardMiddleware(service_registry),
    LoggingMiddleware(logger),      # wraps entire chain (outermost)
]

# Execute:
def execute(request, middlewares):
    if not middlewares:
        return Response(404)
    head, *tail = middlewares
    return head.handle(request, lambda req: execute(req, tail))
```

### Order Matters

```
CORRECT ORDER:
1. Parse (always first — need parsed request to do anything)
2. Auth (before rate limit — rate limit key is user_id, need auth first)
3. Rate Limit (before upstream call — avoid forwarding and then rate limiting)
4. Circuit Breaker (before forward — fail fast without calling upstream)
5. Forward (the actual call)
6. Response Transform (after forward — modify response)
7. Logging (outermost — capture total latency including all steps)

WRONG: Rate Limit before Auth → rate limit key is IP (weaker) instead of user_id
WRONG: Logging before Forward → log doesn't capture upstream latency
```

### Short-Circuit

```
Any middleware can return early without calling next():
  AuthMiddleware: invalid JWT → return 401, stop chain
  RateLimitMiddleware: exceeded → return 429, stop chain
  CircuitBreakerMiddleware: OPEN → return 503, stop chain
  
Benefit: upstream is never called for invalid requests → saves upstream CPU
```

### Real-World

- **Nginx:** filter modules (rewrite, auth, proxy) are ordered middleware
- **Express.js:** `app.use(middleware)` — middleware chain
- **Kong:** "plugins" are middleware (auth plugin, rate-limit plugin, log plugin)
- **Netflix Zuul:** "filters" (pre-filters, route-filters, post-filters)

### Trade-offs

| Aspect | Detail |
|--------|--------|
| Latency | Each middleware adds overhead. Keep hot path thin. Async where possible. |
| Error propagation | If middleware M3 panics, does M7 (logging) still run? Need try/finally wrapping. |
| Config | Different routes need different middleware subsets → config-driven middleware selection |
| Testing | Each middleware unit-testable in isolation (pass mock `next` function) |

### Appeared In
- API Gateway (2026-06-17)

---

## 65. Bulkhead Pattern (Thread Pool Isolation)

**What it is:** Named after ship bulkheads (watertight compartments). Isolate resources (thread pools, connection pools, semaphores) by caller or dependency so one failing dependency can't consume all resources and take down everything.

### The Problem Without Bulkheads

```
Service A has one thread pool: 200 threads, shared by all callers.

Payment Service → Auth Service: calls take 30s (Auth Service slow)
→ 200 threads consumed by Payment calls, waiting 30s each
→ All other callers (User Service, Order Service, Gateway) → 0 threads available
→ They all fail too, even though Auth Service was only called by Payment

One bad caller starves everyone.
```

### Implementation

```
Auth Service: separate thread pools per caller type

Pool 1: "external" (API Gateway calls)      → 200 threads
Pool 2: "payment-svc"                       → 50 threads
Pool 3: "internal-admin"                    → 20 threads

Payment Service slow → exhausts Pool 2 (50 threads)
API Gateway calls → unaffected, Pool 1 has 200 threads
```

**Key:** Differentiate callers via `X-Caller-Service` header or mTLS certificate (service identity).

```
Auth Service handler:
  caller = request.header("X-Caller-Service") || "external"
  pool = thread_pools[caller]  // get the right pool
  pool.submit(handle_request)
  if pool.queue_full:
      return 503  // reject immediately, don't queue indefinitely
```

### Semaphore-Based (Lighter Than Thread Pool)

```
Instead of separate thread pools:
  payment_svc_semaphore = Semaphore(50)   # max 50 concurrent requests from payment-svc
  
  if not payment_svc_semaphore.try_acquire(timeout=100ms):
      return 503  # bulkhead full
  try:
      result = handle_request()
  finally:
      payment_svc_semaphore.release()
```

Lower overhead than thread pools. Good for async/non-blocking servers.

### Combined with Circuit Breaker

```
Circuit Breaker: detects when a dependency is failing → stops calling it
Bulkhead: limits how much damage a slow dependency does when the breaker hasn't tripped yet

Together:
  1. Dependency starts slowing → bulkhead absorbs (50 threads consumed, others protected)
  2. Failure count exceeds threshold → circuit breaker opens → 0 threads consumed
  3. Both mechanisms: fast self-protection even during brief degradation
```

### Trade-offs

| Aspect | Detail |
|--------|--------|
| Resource waste | Each pool pre-allocated, even when idle → higher baseline memory/threads |
| Pool sizing | Requires capacity planning per caller: under-provision = too many 503s; over-provision = wasted resources |
| Observability | Monitor pool utilization per bulkhead: alert at >80% sustained usage → resize |
| Async systems | Thread pools less meaningful; use semaphores or rate-limit per caller instead |

### When to Use
- Service called by multiple upstream services with different traffic patterns
- High-stakes dependency (Auth, Payment) that shouldn't be monopolized by one caller
- Any shared resource (DB connections, external API quota) that can be exhausted

### When NOT to Use
- Single caller (no isolation benefit)
- Highly async system (Go goroutines, Node.js event loop): thread pools less relevant; use semaphores

### Real-World
- Netflix Hystrix (original bulkhead implementation, now archived)
- Resilience4j (Java): `Bulkhead.of("payment", BulkheadConfig.custom().maxConcurrent(50).build())`
- AWS Lambda: concurrency limits per function = bulkhead at infrastructure level

### Appeared In
- API Gateway failure analysis — Auth Service isolation (2026-06-17)
- Fraud Detection (Failure Analysis)

---

## 66. Erasure Coding (Reed-Solomon)

**What it is:** An error-correction scheme that splits data into N data shards and adds M parity shards. Any N shards from the N+M total can reconstruct the full data. Tolerates up to M simultaneous shard losses.

**RS(6,3) example:**
```
64MB chunk → split into 6 data shards (10.67MB each)
Compute 3 parity shards (XOR-based Reed-Solomon field arithmetic)
Total: 9 shards × 10.67MB = 96MB for 64MB data (1.5× overhead)

Recovery: any 6 of 9 shards → full data
Tolerates: up to 3 simultaneous shard failures
```

**Why not just 3× replication?**
| | 3× Replication | EC 6+3 |
|--|---|---|
| Storage overhead | 3× (1PB → 3PB) | 1.5× (1PB → 1.5PB) |
| Tolerated failures | 2 (any) | 3 (any) |
| Read path | Fetch 1 shard | Fetch 6 shards + decode |
| Hot read latency | Lowest (single copy) | Higher (6-fetch + decode) |
| Write latency | Wait for 3 ACKs | Wait for 6 data ACKs |

**Rule:** EC for cold/warm storage (cost matters, latency less critical). Replication for hot data (lowest latency read). Facebook uses 3× replication for hot photos, 10+4 EC for cold archive.

**Trade-off:**
- Adds: CPU for encode/decode (fast on modern hardware with SIMD, ~100ms for 64MB)
- Subtracts: 2× storage cost vs replication
- Degraded read: must fetch 6 shards + decode. With replication, just fetch another replica.

### Appeared In
- S3 Object Storage (2026-06-18)

---

## 67. Content-Addressed Storage

**What it is:** Objects are named by their content's cryptographic hash: `chunk_id = SHA256(chunk_bytes)`. The address IS the content fingerprint.

```
chunk_id = SHA256(bytes) → immutable, globally unique ID
Store: /chunks/{chunk_id} → bytes

Read: fetch chunk by ID → verify SHA256(bytes) == chunk_id → data integrity guaranteed
```

**Benefits:**
1. **Automatic deduplication:** Two files with identical 64MB block → same chunk_id → stored once
2. **Self-verifying:** Recompute hash on every read. Mismatch = corruption detected.
3. **Immutability:** Can't update in-place — content change → new hash → new object. Enables safe caching (ID never refers to different content).

**Costs:**
- Can't update in place: any change = new chunk. Fine for object storage, problematic for databases.
- Large file = many chunks. Metadata overhead per chunk.
- SHA256 collision = theoretically two different files map to same chunk_id. SHA256: not yet broken; treat as impossible for practical purposes.

**Real-world:** Git commits are SHA1-addressed. Cassandra uses MD5 for partition tokens. IPFS uses content hashing for distributed web. Facebook Haystack uses XOR-based checksums per needle.

**Trade-off:** Content addressing enables deduplication and integrity checks at the cost of immutability. Every "update" creates garbage (old chunks) that need garbage collection.

### Appeared In
- S3 Object Storage (2026-06-18)

---

## 68. Multipart Upload Protocol

**What it is:** Protocol for uploading large objects (>5GB) as independent chunks that can be uploaded in parallel and resumed on failure.

```
1. Initiate → upload_id
2. Upload parts (parallel):
     PUT /bucket/key?partNumber=N&uploadId=X → ETag_N
3. Complete → merge parts, commit metadata, visible as single object
   (or) Abort → delete uploaded parts
```

**Why needed:**
- HTTP PUT has no native resume-on-failure
- 100GB file over flaky connection would restart from 0 on any error
- Multipart: only failed part re-uploads

**Rules (S3-compatible):**
- Min part size: 5MB (except last part)
- Max part size: 5GB
- Max parts: 10,000
- Max object size via multipart: 5TB

**Parallelism:** Client splits file into N parts, uploads M in parallel → throughput = M × single-stream bandwidth. 10 parallel × 125MB/s = 1.25GB/s upload.

**Server-side:** Each part is stored as chunks independently. `CompleteMultipartUpload` assembles the ordered part list into a single logical object in metadata (no byte-level copying needed — the part's chunks are already on storage nodes).

**Cleanup hazard:** Incomplete uploads accumulate orphaned parts. Always set lifecycle rule to abort incomplete uploads after N days.

**Trade-off:** Complex client code vs simple single-PUT. But for >1GB files, multipart is mandatory for reliability.

### Appeared In
- S3 Object Storage (2026-06-18)

---

## 69. Pre-Signed URLs (Stateless HMAC Auth)

**What it is:** A time-limited URL embedding a cryptographic proof that the URL's issuer authorized the specific operation (GET/PUT) on a specific object for a specific duration.

```
Generation (server-side):
  canonical = "{method}\n{bucket}\n{key}\n{expires}\n{content_type}"
  signature = HMAC_SHA256(secret_key, canonical)
  url = "https://storage.example.com/{bucket}/{key}"
       + "?X-Expires={unix_ts}&X-Signature={hex(sig)}"

Validation (per request, no DB lookup):
  1. Check X-Expires > now() — not expired
  2. Recompute canonical_string from request params
  3. Recompute HMAC with server's secret key
  4. Constant-time compare: if match → authorized
```

**Use cases:**
- Upload directly from browser to storage (skip API server for large uploads)
- Share private object with third party for limited time
- CDN pulls from private bucket (CDN uses pre-signed URL as origin auth)

**Trade-offs:**

| Aspect | Stateless (HMAC) | Stateful (DB lookup) |
|--------|-----------------|---------------------|
| Validation cost | ~0.5ms CPU | ~1ms + DB/Redis |
| Revocation | Cannot revoke early (wait for expiry) | Immediate revocation |
| Scale | Infinite (no state) | Bounded by DB capacity |
| Implementation | Simple | Requires distributed state |

**Revocation workaround:** Track `jti` (JWT-style unique URL ID) in Redis SET with TTL = URL expiry. On access: `SISMEMBER revoked_urls {jti}`. Adds 1ms but enables revocation. Only add for high-sensitivity URLs.

**Security:** HMAC prevents forging (secret key on server only). Can't craft a valid URL without the secret. Append IP binding to canonical string to make URL non-transferable.

### Appeared In
- S3 Object Storage (2026-06-18)

---

## 70. Tiered Log Storage (Hot/Warm/Cold)

**What it is:** Store log data across multiple storage tiers with decreasing cost and increasing latency, based on recency.

```
Tier     Storage          Latency    Cost/GB/mo   Retention
Hot      Elasticsearch    <1s        $0.15-0.25   7 days
Warm     S3 + Athena      10-60s     $0.023       30 days
Cold     S3 Glacier       4-12h      $0.004       1 year
```

**Cost example (500 services, 6.5TB/day compressed):**
```
All in ES:    365d × 6.5TB × $0.15 = ~$355K/month
With tiering:
  ES hot 7d:    45TB × $0.15  = $6,750
  S3 warm 30d: 195TB × $0.023 = $4,485
  Glacier 1yr:  2.4PB × $0.004 = $9,830
  Total: ~$21K/month  ← 94% savings
```

**Implementation:** ES ILM moves indices from hot→warm→delete automatically. S3 lifecycle policy transitions to Glacier after N days. Athena queries S3 Parquet partitioned by `year/month/day/service`.

**Trade-off:** Queries spanning tiers have unpredictable latency. User must know "historical search = minutes, not seconds." Route by time range before querying.

### Appeared In
- Distributed Logging (2026-06-19)

---

## 71. Sampling Strategies (Head-based vs Tail-based)

**What it is:** The decision to keep or drop a log/trace event, to control storage cost.

### Head-based
```
Decision at request ENTRY (before outcome known):
  Random 10% → propagate "sampled=true" in traceparent header

Pros: Simple, no buffering
Cons: 10% of errors may be dropped (outcome unknown at decision time)
```

### Tail-based
```
Decision AFTER trace completes (all spans collected):
  Has error span?   → KEEP 100%
  P99 latency high? → KEEP 100%
  Otherwise         → KEEP 1%

Pros: Always captures errors + outliers
Cons: Requires stateful 30s buffer (Flink tumbling window)
     At 2.5M spans/sec × 30s × 500B = 37.5GB buffer
```

### Adaptive (Datadog-style)
```
Per-service rate target: max 100 traces/sec
Errors: always 100% regardless
Dynamic rate: lower if ingestion exceeds target
```

**Rule:** Tail-based for production debugging (never miss an error). Head-based for non-critical telemetry (performance).

### Appeared In
- Distributed Logging (2026-06-19)

---

## 72. Distributed Tracing (TraceContext / OpenTelemetry)

**What it is:** Follow one request across N services by propagating a trace_id through all calls.

### W3C TraceContext
```
HTTP header: traceparent: 00-{trace_id(128bit)}-{parent_span_id(64bit)}-{flags}
Example: traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01

flags: 01 = sampled, 00 = not sampled
```

### Trace Data Model
```
Trace = tree of Spans, all sharing same trace_id

Span = {trace_id, span_id, parent_span_id, service, operation,
        start_time, end_time, status, attributes}

Spans assembled by: collect all spans with same trace_id, build tree via parent_span_id
```

### Propagation Rule
```
On inbound: extract trace_id, parent_span_id from header
Create own span_id
All logs in this request: { trace_id, span_id }  ← correlation key
On outbound call: set traceparent header with trace_id + my span_id as parent
```

**Failure mode:** Any service that doesn't propagate breaks the trace at that hop.
**Fix:** Enforce propagation in framework middleware, not developer discretion.

**Storage:** Cassandra `PRIMARY KEY (trace_id, span_id)` — trace_id lookup = partition scan = fast. No full-text needed.

**Trade-off:** ~5-10KB overhead per request. At scale, must sample (see #71). 100% trace capture = impractical at 2.5M req/sec.

### Appeared In
- Distributed Logging (2026-06-19)

---

## 73. Dead Man's Switch (Watchdog Pattern)

**What it is:** The ABSENCE of an expected signal triggers an alert. Monitors the monitor.

**Problem:** If your alert engine crashes, silence looks identical to "everything is fine." Standard alerting only fires on bad events — it can't detect its own failure.

```
Normal: Alert Engine → heartbeat signal every 30min → external watchdog ✓
Failure: Signal stops → watchdog sees 35min gap → pages on-call
```

### Implementation Options

**Heartbeat via external SaaS:**
```
Alert Engine: HTTP POST to https://deadmanssnitch.com/{id} every 30min
Service: if no check-in within window → page
```

**TTL key in Redis:**
```
Alert Engine: SET watchdog:alert-engine "alive" EX 60 (every 30s)
Watchdog: GET key → nil means key expired → agent not running → alert
```

**Absence-of-event alert:**
```
Rule: error_count{service="payment"} == 0 for 10min during business hours
→ Either nothing is failing (good) or service stopped emitting (bad)
→ Combine with service health check to distinguish
```

**Where it applies:**
- Batch jobs: "nightly report not generated by 6am" → alert
- Data pipelines: "no events from service X in 5 min" → dead producer?
- Payment processors: "0 payments in 15 min during peak hours" → pipeline down?

**Trade-off:** False positives during planned maintenance. Solution: maintenance window API that suppresses dead man's alerts during known downtime.

**Rule:** Any critical async/continuous process should have a dead man's switch. Silence is not the same as success.

### Appeared In
- Distributed Logging (2026-06-19). Pattern applicable to any scheduled/continuous process.

---

## 74. Chunked Audio Streaming + Gapless Playback

**What it is:** Serving audio as on-demand byte ranges (HTTP Range requests) with client-side pre-fetch to achieve zero-gap transitions between songs.

### Why Not Serve Full Files

```
Full file download: user must wait 30s to download 3MB before playback starts
Chunked (Range requests):
  Client: GET /audio/{id} Range: bytes=0-163839  (10s @ 128kbps)
  Server: 206 Partial Content → 160KB → playback starts immediately
  Client: while playing chunk 1, pre-fetch chunk 2 (bytes=163840-327679)
  → Continuous playback, <200ms startup latency
```

### HTTP Range Request Pattern

```
song_size = duration_ms / 1000 × (bitrate_bps / 8)

Seek to position T seconds:
  byte_offset = T × (bitrate_bps / 8)
  GET /audio/{id} Range: bytes={byte_offset}-{byte_offset + chunk_size}

Benefits:
  - Seek without re-downloading from start
  - Resume after network interruption from last byte
  - CDN caches byte ranges independently
```

### Gapless Playback Implementation

```
Track A duration: 3:47 (227s)
At T = 212s (15s before end):
  Client pre-fetches: first 2 chunks of Track B (0-20s)
  Track B buffered in secondary audio decoder

At T = 227s:
  Audio decoder crossfades: A ends → B starts from buffered data
  Zero silence gap

Abandon pre-fetch on skip:
  User skips at T = 200s → cancel Track B pre-fetch
  → fetch Track C instead (new queue head)

Cost: ~15% extra bandwidth from abandoned pre-fetches (skip rate ~20% of plays)
Acceptable tradeoff for seamless UX
```

### Adaptive Quality Switching

```
Normal: pre-fetch at user's selected quality (320kbps)
Network degrades mid-song:
  Download rate monitoring: if chunk arrives <30% of expected rate:
    → Switch next chunk request to lower quality (128kbps)
  Network recovers:
    → Ramp back up after 3 consecutive chunks complete on time

Per-chunk quality decisions (no renegotiation needed, just different Range request URL)
```

### Trade-offs

| Approach | Startup Latency | Bandwidth | Seek Speed | Use When |
|----------|----------------|-----------|------------|---------|
| Full download | High (30s) | 1× | O(1) after download | N/A (not viable) |
| **Chunked Range** | Low (<200ms) | 1× + ~15% pre-fetch | O(1) | All streaming |
| HLS segments (pre-split) | Low | 1× | Requires segment metadata | Video (not ideal for audio) |

**Rule:** Use HTTP Range requests for audio. Pre-split segments (HLS) add server-side complexity with no benefit for audio. Server stores one file per quality; client drives all chunking logic.

### Appeared In
- Spotify Music Streaming (2026-06-21)

---

## 75. Remote Playback Protocol

**What it is:** A pattern where one device (Controller) sends commands to another device (Speaker) through a server relay, without P2P media transfer. The Speaker handles all media streaming independently.

### Why Server Relay (Not P2P)

```
P2P option:
  Phone → NAT traversal (ICE/STUN/TURN) → Desktop
  Problems:
    - Corporate firewalls block P2P
    - TURN relay needed for ~15% of users (expensive media relay)
    - Complex connection setup (5-10s vs <1s)

Server relay option:
  Phone → HTTPS command → Presence Service → WebSocket → Desktop
  Desktop → independently fetches audio from CDN
  
Commands are tiny (100 bytes): negligible server load
Media never passes through server (Desktop←→CDN direct)
→ Best of both: simple, reliable, no media relay cost
```

### Architecture

```
Presence Service:
  Maintains WebSocket connection per active device
  Redis: SET device:{user_id}:{device_id} {ws_conn_id} EX 3600
  
Controller (Phone) sends command:
  POST /me/player/command:
    { action: PLAY, track_id: "abc", target_device: "desktop_xyz" }
  
Presence Service:
  Lookup WebSocket for desktop_xyz → forward command
  
Speaker (Desktop) receives command:
  Issues own audio request to CDN
  Reports back state: { playing: true, track_id: "abc", position_ms: 0 }
  
Controller polls /me/player (every 5s):
  Gets: { device: desktop_xyz, playing: true, track: "abc", position: 4200 }
  Updates UI accordingly
```

### Commands Supported

| Command | Payload |
|---------|---------|
| PLAY | track_id, context (playlist/album) |
| PAUSE | — |
| SEEK | position_ms |
| SKIP_NEXT / SKIP_PREV | — |
| SET_VOLUME | volume (0-100) |
| QUEUE_ADD | track_id, position |
| TRANSFER_PLAYBACK | new_device_id (moves Speaker role) |

### Failure Handling

```
Speaker WebSocket disconnects (desktop sleeps):
  Presence Service: removes device from available list
  Controller: "Device unavailable" — offer other available devices

Command delivery failure:
  Retry once after 200ms
  If fails: return error to Controller: "Could not reach device"

Multiple controllers try simultaneously:
  Last command wins (server serializes via WebSocket send order)
  State reported back to all controllers within 1s
```

### Trade-offs

| | Server Relay | P2P |
|--|---|---|
| Setup time | <1s (WS already open) | 5-10s (ICE gathering) |
| Firewall compatibility | 100% (HTTPS/WSS) | ~85% (STUN), ~100% with TURN |
| Server load | Minimal (tiny commands) | Zero (if P2P succeeds) |
| Media path | Client←→CDN direct | Peer←→Peer or TURN relay |
| Reliability | High (deterministic) | Variable (NAT-dependent) |

**Rule:** Use server relay for command/control (low bandwidth, simplicity wins). Only use P2P for media when latency is critical and cost matters (video conferencing). For Spotify Connect-style remote playback: relay is strictly better.

### Real-World
- Spotify Connect, Apple AirPlay 2 (control plane), Chromecast (cast protocol uses relay for control, device fetches media independently)

### Appeared In
- Spotify Music Streaming (2026-06-21)

---

## 76. Materialized Path — Comment Trees

**What it is:** Store the full ancestor path as a string on each tree node, enabling single-query subtree retrieval and depth-first sorted traversal.

```sql
comments (
  id         UUID,
  parent_id  UUID NULL,
  path       TEXT,   -- e.g. "0001.0004.0009"
  depth      INT
)

-- Insert top-level: path = lpad(seq, 4, '0')      → "0012"
-- Insert reply:     path = parent.path + "." + lpad(seq, 4, '0')  → "0012.0003"

-- Fetch entire thread in display order (one query):
SELECT * FROM comments WHERE post_id = $1 ORDER BY path ASC;
```

Path is lexicographically sortable → depth-first traversal order matches display order.

### Trade-offs

| | Materialized Path | Adjacency List | Nested Sets |
|--|---|---|---|
| Fetch subtree | One range scan | Recursive CTE | One range query |
| Insert | Requires path generation | Simple | Rebalances entire set |
| Move node | Update all children's path | Update parent_id | Rebalance |
| Sort within siblings | Encoded in path string | Requires separate sort | No |

**When to use:** Comments (never moved, need fast tree retrieval, sibling sort needed).
**When NOT to use:** Organizational hierarchies where nodes move frequently (cascade update expensive).

### Appeared In
- Reddit (comment trees, 2026-06-22)

---

## 77. Hybrid Fan-out (Push/Pull)

**What it is:** Push new content to currently online users' feed caches; let offline users pull on demand. Avoids writing to millions of subscribers per post.

```
On new post in large community (42M subscribers):
  Online users (active last 30 min, ~500K):
    → ZADD user:{uid}:feed {score} {post_id}  (push)
  Offline users:
    → Pull on login: query top posts from subscribed communities, merge + rank

Threshold: community < 1K online subscribers → always pull
           community > 100K online → push to online set only
```

### Trade-offs

| | Push-only | Pull-only | Hybrid |
|--|---|---|---|
| Feed freshness (online) | Instant | On load | Instant for online |
| Write cost per post | O(all subscribers) | Zero | O(online subscribers) |
| Fan-out storm risk | High | None | Low |
| DB load at feed load | Zero | High | Low |

**Key insight:** Most subscribers aren't online. Pushing to them wastes writes. Pull cost per user is bounded by subscription count.

### Appeared In
- Reddit (subreddit feed, 2026-06-22)
- Twitter Feed (celebrity tweets)

---

## 78. Score Decay / Time-weighted Ranking (Hot Algorithm)

**What it is:** A ranking function combining content quality (votes) with recency so older content naturally sinks without explicit TTL or expiry.

**Reddit's formula:**
```python
import math
EPOCH = datetime(2005, 12, 8, 7, 46, 43)

def hot_score(ups, downs, created_at):
    score = ups - downs
    order = math.log10(max(abs(score), 1))
    sign = 1 if score > 0 else (-1 if score < 0 else 0)
    age_seconds = (created_at - EPOCH).total_seconds()
    return sign * order + age_seconds / 45000
```

**Intuition:** Every 45,000 seconds (~12.5h) of age = 1 unit of log10(votes). A new post with 1 vote equals a 12.5h-old post with 10 votes.

**Why log10?** Linear score → old viral posts win forever. Log = diminishing returns on virality, recency can catch up.

### Other Approaches

| Formula | Used By | Behavior |
|---------|---------|---------|
| `score / (age + 2)^1.5` | Hacker News | Steeper decay |
| `score × e^(-λt)` | Generic | Smooth continuous |
| Redis ZADD + TTL
---

## 79. RTMP Ingest + Stream Key Auth + Reconnect

**What:** RTMP (Real-Time Messaging Protocol) is the industry-standard TCP-based protocol for streaming video from encoder (OBS) to ingest server. Runs on port 1935.

**How it works:**
```
Streamer OBS → rtmp://ingest.example.com/live/{stream_key}
Ingest server:
  1. TCP handshake (RTMP connect)
  2. Validate stream_key: GET /auth?key={hash(stream_key)} → channel_id
  3. Open Stream record; begin receiving H.264+AAC chunks
  4. Forward raw video to Kafka for transcoding
```

**Stream Key:**
- Long random token (UUID v4 + HMAC-SHA256 signature)
- Stored as bcrypt hash in DB; never in plaintext
- Rotate on compromise; different key per destination (multi-stream)

**Reconnect logic:**
```
Connection drop detected → mark stream `reconnecting` + 60s grace timer
Store in Redis: stream_session:{stream_key} → {stream_id, last_seq, channel_id} TTL=120s
On reconnect: any ingest node checks Redis → resumes from last_seq
If grace period expires → mark stream offline → notify followers
```

**Pros:**
- TCP reliability: no packet loss (unlike UDP-based WebRTC)
- Supported by all major streaming tools (OBS, Streamlabs, vMix)
- Simple server implementation

**Cons:**
- 1–2s protocol overhead added to latency
- TCP head-of-line blocking: one lost packet stalls all data
- Not browser-native (requires plugin or separate app)

**Real world:** Twitch, YouTube Live, Facebook Live all use RTMP ingest. Twitch moved some interactive features to WebRTC delivery but kept RTMP ingest.

### Appeared In
- Live Streaming (Twitch) (2026-06-23)

---

## 80. Transcoding Pipeline — Parallel Quality Ladders

**What:** Convert one high-quality source video stream into multiple lower-quality variants simultaneously, producing segments fast enough to keep pace with real-time ingest.

**GOP alignment (critical):**
```
GOP = Group of Pictures: I-frame + following P/B frames
Segment boundary must align with I-frame (keyframe)
Without alignment: seeking artifacts, ABR switches cause glitch
OBS: configure keyframe interval = segment duration (e.g., 2s)
```

**Pipeline per stream:**
```
Raw video (e.g., 1080p60 6 Mbps)
    │ Kafka message: raw_segment (2s GOP)
    ├──► FFmpeg Worker A → 1080p30 6 Mbps  → S3
    ├──► FFmpeg Worker B → 720p30  3 Mbps  → S3
    ├──► FFmpeg Worker C → 480p30  1.5 Mbps→ S3
    └──► FFmpeg Worker D → 360p30  0.8 Mbps→ S3
Each worker: ~2 vCPU, ~2 GB RAM, ~500ms to transcode 2s segment
```

**Scaling:**
- 100K streams × 4 qualities = 400K workers
- Kubernetes HPA on Kafka consumer lag metric
- Use spot/preemptible instances (can handle 1–2 worker restart gaps)

**Idempotency:**
- Segment S3 key = `{stream_id}/{quality}/{seq:08d}.ts`
- S3 PUT is idempotent → safe to retry on worker crash

**Graceful degradation (quality shedding):**
```
If consumer lag > 5 segments:
  Step 1: disable 1080p (halves CPU demand)
  Step 2: increase segment duration 2s → 6s (fewer FFmpeg invocations)
  Step 3: prioritize top-N streams by viewer_count
```

**Pros:** Parallel per-quality workers → fault isolation; one quality failure doesn't affect others
**Cons:** CPU-intensive; 400K containers for 100K streams is operationally complex

**Real world:** Mux, AWS MediaConvert, Twitch all use per-GOP parallel transcode workers.

### Appeared In
- Live Streaming (Twitch) (2026-06-23)

---

## 81. LL-HLS vs Standard HLS

**What:** Low-Latency HLS (Apple spec, 2019) reduces live latency from 15–30s to 3–5s while remaining backward-compatible with HLS clients.

**Standard HLS:**
```
Segment duration: 6–8s (must wait for full segment before publishing)
Playlist: lists last 3 complete segments
Client polls playlist every 6–8s
Latency: 3 segments + network = ~18–25s
```

**LL-HLS mechanisms:**
```
1. Partial Segments (EXT-X-PART):
   Publish 200ms "parts" within a 2s segment
   Client can start playing partial segment while rest encodes
   Latency: 1–2 partial segments + network ≈ 0.5–1s improvement

2. Playlist Delta Updates:
   Client sends _HLS_msn= and _HLS_part= params
   Server only returns new entries (not full playlist)
   Reduces payload size 10×

3. Blocking Playlist Reload:
   Client sends request for "next sequence not yet published"
   Server holds request until that sequence ready → pushes immediately
   No polling delay; server-push semantics over HTTP/2
```

**Latency vs Cost tradeoff:**
| Mode | Latency | Segment Duration | CDN Requests/min/viewer |
|------|---------|-----------------|-------------------------|
| Standard HLS | 15–30s | 6–8s | 10 |
| LL-HLS | 3–5s | 2s | 30 |
| WebRTC | <1s | N/A (RTP) | Continuous UDP |

**When to use LL-HLS vs WebRTC:**
- LL-HLS: works with existing CDN, scales to millions, 3–5s acceptable (Twitch general viewing)
- WebRTC: <1s needed, small audience (<10K), interactive features (co-streaming, Q&A)

**Pros:** Reuses CDN infrastructure; backward compatible; HTTP/2 server push
**Cons:** 3× CDN request rate → cost; requires HTTP/2 support on CDN

**Real world:** Twitch uses LL-HLS for most streams; YouTube Live uses DASH-CMAF.

### Appeared In
- Live Streaming (Twitch) (2026-06-23)
- YouTube (2026-05-13) — referenced HLS/ABR (#18)

---

## 82. Live Chat Fanout — Redis Pub/Sub Sharding

**What:** Distributing chat messages from one sender to thousands of concurrent WebSocket connections in <100ms.

**Architecture:**
```
Viewer sends message
    → Chat API (HTTP) → rate check → Cassandra write → Kafka publish
    → Fanout Worker (Kafka consumer, per channel)
    → Redis PUBLISH channel:{channel_id} {message_json}
    → All WS servers subscribed to that Redis channel
    → Each WS server fans to its connected viewers
```

**Scale math (50K viewer channel):**
```
50K viewers → ~50 WS servers (1K viewers each)
Each WS server: SUBSCRIBE channel:{channel_id}
On PUBLISH: 50 Redis deliveries → each WS server sends 1K messages
Total: 50K WS sends per chat message

At 10 msgs/sec: 500K WS sends/sec (manageable per server ~10K/sec)
At 100 msgs/sec: 5M WS sends/sec → must scale WS servers
```

**Sharding Redis for top channels:**
```
Redis single-threaded PUBLISH saturates at ~100K commands/sec
Shard: redis_node = hash(channel_id) % N_shards
Top channels (>100K viewers): dedicated Redis instance
Detection: if viewer_count > 100K → reassign to premium shard
```

**Alternative: tiered fanout**
```
Tier 1 (≤1K viewers): direct Redis Pub/Sub
Tier 2 (1K–100K): Redis Pub/Sub + WS server pool (50 servers)
Tier 3 (>100K): Kafka → region-split fanout (separate Kafka consumers per region)
  Each region maintains local Redis + WS cluster
  Cross-region sync via Kafka topic replication
```

**Pros:** Sub-100ms delivery; stateless WS servers; Redis handles subscription state
**Cons:** Redis single-threaded limit; message loss on Redis failover (~30s gap with Sentinel)

**Real world:** Twitch re:Invent 2019 talk describes this exact architecture. Discord uses similar but with Elixir/Phoenix Channels for WS layer (better per-process concurrency model).

### Appeared In
- Live Streaming (Twitch) (2026-06-23)

---

## 83. Two-Tower Recommendation Model

**What it is:** A neural network with two separate "towers" — one encoding users, one encoding items (videos). At query time, the user embedding is matched against pre-computed item embeddings using approximate nearest neighbor (ANN) search to retrieve top-K candidates.

```
User Tower:                    Item Tower:
  inputs: watch history,         inputs: video features,
          liked hashtags,                 caption/hashtags,
          demographic,                    visual/audio CNN,
          recency-weighted                engagement stats,
          past embeddings                 freshness decay
      ↓                              ↓
  128-dim embedding             128-dim embedding
      ↓                              ↓
          dot_product(u, v) → similarity score
          
FAISS ANN search: given u, find top-500 v with highest similarity
```

**Training:** Contrastive learning. Positive pair = (user, completed video). Negative pair = (user, video shown but skipped).

**Trade-offs:**

| Aspect | Detail |
|--------|--------|
| Scalability | Pre-computed item index → billion-scale retrieval in <5ms |
| Cold start | New videos: embeddings from content features only (no engagement yet) |
| Freshness | Item embeddings update at retrain frequency (daily) |
| Accuracy | Approximate (embedding space); feeds into a re-ranking model |

**When to use:** First-stage retrieval in any recommendation system at scale where ranking over all items is infeasible.

**Real world:** TikTok, YouTube, Instagram, Spotify, Pinterest all use Two-Tower at retrieval stage.

### Appeared In
- TikTok Short Video (2026-06-24)

---

## 84. Candidate Retrieval + Re-ranking Pipeline

**What it is:** A multi-stage recommendation pipeline separating cheap approximate retrieval (generate N candidates from billions) from expensive accurate ranking (rank top N precisely).

```
Stage 1: Candidate Retrieval (offline/near-offline, ~1000 candidates)
  - Two-Tower ANN:   personalized (user embedding vs item FAISS index)
  - Trending pool:   top 1000 videos in region/time window
  - Follow feed:     new videos from followed accounts
  - Hashtag match:   videos from user's engaged hashtags
  → Merge → ~1000 candidates → store in Redis EX 1800

Stage 2: Re-ranking (online, per request, <50ms)
  → Fetch candidates from Redis (~5ms)
  → Lightweight scoring model (gradient boosted tree or shallow NN)
  → Features: p(complete), p(like), p(share) + real-time context
  → Sort, return top 20

Stage 3: Diversity + Business Rules
  → Max 3 consecutive same-creator
  → 10% exploration slots
  → Filter: seen, blocked, age-restricted
```

**Why separate stages?**
- Retrieval at billion-scale: can't run ranking model over all items per request
- Ranking at 1000 items: feasible in <50ms; accuracy worth it

**Trade-offs:**

| Aspect | Detail |
|--------|--------|
| Latency | Pre-computed = fast; ranking in <50ms |
| Staleness | Candidates can be 30 min stale (TTL 1800s) |
| Error amplification | Bad retrieval → bad ranking (garbage in, garbage out) |
| Complexity | Two separate models to train, deploy, monitor |

**Real world:** Industry standard at Google, Meta, TikTok, Netflix. Called "recall → rank → rerank" in ML literature.

### Appeared In
- TikTok Short Video (2026-06-24)

---

## 85. Completion Rate as Implicit Feedback Signal

**What it is:** Using the fraction of video watched (watch_duration / video_duration) as a training signal, more honest than explicit feedback like likes.

```
Client sends events every 2s: { video_id, user_id, watch_duration_ms }
On exit: { total_watched_ms, action: "SKIP" | "COMPLETE" }

completion_rate = total_watched_ms / video_duration_ms

Buckets:
  0–25%:   strong negative
  25–75%:  neutral
  75–100%: strong positive
  >100%:   replay = very strong positive

Signal weighting in ranking model:
  score = 3.0 × p(complete>75%) + 2.5 × p(share) + 1.5 × p(like) − 2.0 × p(skip<25%)
```

**Why completion > likes:**
- Likes require deliberate action → friction → undercounts true interest
- 90% completion without like = real interest; like with 10% completion = noise
- Completion is passive → harder to game → more honest signal

**Trade-offs:**

| Aspect | Detail |
|--------|--------|
| Honesty | Passive = less gameable than likes |
| Client complexity | Requires streaming heartbeat events during playback |
| Normalization | 95% on 5s ≠ 95% on 5min; normalize or bucket by video length |
| Data loss | Network drops → watch events lost → incomplete training data |

**When to use:** Any video/audio recommendation. Also: article scroll depth, podcast listen time.

### Appeared In
- TikTok Short Video (2026-06-24)

---

## 86. Short-Form Video: Progressive MP4 vs HLS

**What it is:** Choosing between single-file progressive download and segmented HLS streaming based on video length and viewer behavior.

```
Progressive MP4:
  Single file → client requests byte range starting at 0
  Time to first frame: ~200ms (CDN latency + initial buffer)
  Requests: 1 total

HLS:
  1. Fetch master .m3u8 manifest
  2. Fetch quality playlist .m3u8
  3. Fetch first 2s segment
  Time to first frame: 3 round trips × 50ms + segment download = 300–500ms
```

**Decision rule:**
```
Video ≤ 60s  → Progressive MP4 (no seek needed, faster start, simpler)
Video > 60s  → HLS 2s segments (ABR, seek, partial download)
Live stream  → LL-HLS or WebRTC
```

**Trade-offs:**

| | Progressive MP4 | HLS |
|--|---|---|
| Start latency | ~200ms | ~400ms |
| Adaptive bitrate | No (single quality) | Yes (per-segment) |
| Seek | Byte-range (approximate) | Segment-precise |
| CDN complexity | Low (1 file) | High (manifest + N segments) |
| Storage | Lower | Higher (many small files) |

**Hybrid (TikTok approach):** Serve 720p progressive for ≤60s. On poor network → client switches to 480p URL mid-video (app logic, not ABR protocol). Longer videos use HLS.

**Real world:** Netflix (2015) showed progressive download starts 200ms faster than HLS for clips under 90s. TikTok uses progressive MP4 for most content (confirmed via network traffic analysis).

### Appeared In
- TikTok Short Video (2026-06-24)

---

## 87. Multi-Tenant Data Isolation

**What it is:** Pattern for ensuring one tenant (customer/workspace/organization) cannot access another tenant's data in a shared infrastructure system.

### Three Isolation Models

| Model | How | Isolation Level | Cost | Use When |
|-------|-----|----------------|------|---------|
| **Silo** | Separate DB per tenant | Complete | High (N DBs, N connections) | Enterprise, regulated industries, top-tier customers |
| **Bridge** | Shared DB, separate schema per tenant | Medium | Medium | Mid-market, moderate volume |
| **Pool** | Shared DB, shared tables, tenant_id column | Lowest | Low | SaaS, SMB, most startups |

### Pool Model (Most Common in SaaS)

```sql
-- Every table has workspace_id
SELECT * FROM channels WHERE workspace_id = $1 AND id = $2;

-- APPLICATION RULE: workspace_id must NEVER be derived from user input alone.
-- Always extract from the authenticated JWT:
--   workspace_id = jwt.claims.workspace_id  ← server-side, tamper-proof
--   Never: workspace_id = request.body.workspace_id  ← injectable
```

**Defense layers:**
1. JWT scoped to workspace — token contains workspace_id, cannot be modified by client
2. Middleware guard — every route handler calls `assert_workspace(request, resource)` before returning
3. DB-level RLS (PostgreSQL Row Security Policies) — enforces workspace_id at DB layer as last resort
4. Index prefix — every indexed table has `(workspace_id, ...)` as prefix

### Elasticsearch: Index-per-Tenant

```
Option A: single index, filter by workspace_id on every query
  Risk: query bug → cross-tenant data leak

Option B: index per tenant (preferred for sensitive data)
  slack_messages_{workspace_id}
  Risk: index proliferation → fix via ILM archival policy
```

### Trade-offs

| Concern | Silo | Pool |
|---------|------|------|
| Data leakage risk | None | Bug in WHERE clause → cross-tenant |
| Noisy neighbor | None | One large tenant can affect others |
| Schema migration | Per-tenant rollout (slow) | Single migration (fast) |
| Cost at 500K tenants | Prohibitive | Cheap |

**Rule:** Default to pool model. Offer silo as premium tier for enterprise/regulated. Strict middleware + JWT enforcement compensates.

### Appeared In
- Slack (2026-06-25), Any SaaS multi-tenant platform

---

## 88. Connection Registry Pattern (WebSocket Routing)

**What it is:** Shared registry (Redis) mapping user_id → WebSocket server, so any backend service can route to the right server without direct service-to-service connections.

**Problem without it:** Fan-out Service wants to push to User A. User A is connected to WS-Server-3. How does it know?

```
On connect:
  WS-Server-3: SET ws:conn:{user_id} "ws-server-3" EX 90

Heartbeat (every 60s):
  WS-Server-3: EXPIRE ws:conn:{user_id} 90

On graceful disconnect:
  WS-Server-3: DEL ws:conn:{user_id}

On crash:
  Key auto-expires after 90s

Fan-out lookup:
  server = GET ws:conn:{user_id}
  nil     → offline → queue in offline_queue:{user_id}
  found   → publish to ws:{server}:inbox → WS server delivers
```

### Scaling

```
5M concurrent users × 50 bytes/key = 250MB Redis → trivial
23K msg/sec × 15 members avg = 345K GET/sec → well within Redis 1M ops/sec
```

### Multi-Device

```
ws:conn:{user_id}:devices → HASH { "mobile-uuid": "ws-3", "desktop-uuid": "ws-7" }
Fan-out: HGETALL → deliver to ALL devices
```

### Trade-off

| Approach | Pro | Con |
|----------|-----|-----|
| Redis registry | O(1) lookup, auto-TTL | Redis failure breaks routing |
| Kafka broadcast to all WS servers | No registry dependency | Each server must filter irrelevant messages |

**Redis failure fallback:** broadcast to all WS server inboxes (Kafka), each server delivers only to its own connected users. Correct but less efficient.

### Appeared In
- Slack (2026-06-25), Chat System, any WebSocket-based real-time system

---

## 89. Notification Priority Tiers + Bulkhead Queues

**What it is:** Separating notifications into priority tiers with isolated worker pools so high-priority events (OTP, fraud alert) are never delayed by bulk traffic (marketing emails).

### Three-Tier Pattern

```
Critical Queue (SQS FIFO):
  OTP, fraud alert, payment failure
  Workers: 50 dedicated, reserved capacity, never auto-scale down
  Retries: 5 attempts, 30s backoff, then DLQ + PagerDuty alert

Normal Queue (SQS Standard):
  New message, follower, comment
  Workers: 100, auto-scale 10-200 based on queue depth
  Retries: 10 attempts, exponential backoff

Bulk Queue (SQS Standard, rate-limited):
  Marketing, digest, weekly report
  Workers: 20, max throughput capped at 1K/sec (budget + provider limit)
  Retries: 3 attempts, then drop (not worth retrying stale marketing)
```

### Why Not One Queue with Priority Field?

Single queue with priority = unbounded starvation risk:
- 100M bulk messages queued
- 1 OTP message arrives
- Priority queue must scan past or park 100M messages → O(1) dequeue is a lie under load

Separate queues = O(1) always for critical, guaranteed isolation.

### Trade-offs

| Approach | Pro | Con |
|----------|-----|-----|
| Single queue + priority | Simple, one place to monitor | Starvation of low-priority, no isolation |
| Separate queues | True isolation, guaranteed critical latency | More infra, per-queue alarms/dashboards |
| Kafka topics (one per priority) | Same isolation + replay | More ops, consumer group complexity |

**Rule:** Use separate queues when latency SLAs differ by >10×. OTP must arrive in <5s; bulk can wait hours.

### Real-world
- Uber notification service: 3 tiers (P0=critical, P1=time-sensitive, P2=bulk)
- Airbnb "Merlot": priority lanes with dedicated thread pools per tier

### Appeared In
- Notification System (2026-06-26)

---

## 90. FCM/APNS Token Lifecycle Management

**What it is:** Device push tokens are volatile — they change on reinstall, OS upgrade, or after 270 days of inactivity. Stale tokens waste API calls and may cause provider throttling.

### Token States

```
ACTIVE:    Registered, recently seen (last_seen < 30 days)
STALE:     Not seen for 30-90 days — may still work
EXPIRED:   >90 days OR provider returned NotRegistered/Unregistered
```

### Update Flow

```
App startup → FCM/APNS SDK returns token
  → POST /devices/register { token, platform, device_id }
  → UPSERT devices ON CONFLICT (user_id, device_id) DO UPDATE SET token, updated_at

Why upsert on device_id (not token)?
  Token rotates; device is stable.
  Same device can have multiple token rotations.
```

### Cleanup on Delivery Failure

```python
response = fcm.send(token, payload)

if response.error == "NotRegistered":        # FCM: token invalid
    delete_device_token(user_id, device_id)
    return

if response.error == "Unregistered" and apns:  # APNS: 410 + timestamp
    if apns_timestamp > device.registered_at:   # token was rotated after we registered it
        delete_device_token(user_id, device_id)
```

### Background Cleanup (Proactive)

```sql
-- Nightly job: mark tokens stale
UPDATE devices SET status = 'stale'
WHERE last_seen_at < now() - INTERVAL '90 days'
  AND status = 'active';

-- Purge expired tokens (provider confirmed invalid or >180 days unseen)
DELETE FROM devices WHERE status = 'expired'
  OR last_seen_at < now() - INTERVAL '180 days';
```

### Trade-offs

| Action | Pro | Con |
|--------|-----|-----|
| Delete immediately on NotRegistered | No wasted future calls | If FCM error was transient, lose valid token |
| Mark stale, retry once after 24h | Safer | One extra failed call per token |
| Aggressive cleanup | Lean token store | Miss token rotations in offline users |

**Rule:** Delete on `NotRegistered` immediately (FCM is authoritative). Mark stale for 90+ day inactive users (safer than delete — user may just be offline).

### Appeared In
- Notification System (2026-06-26)

---

## 91. Provider Failover Chain

**What it is:** A prioritized sequence of delivery providers per channel, with circuit breakers, so that a single provider failure doesn't cause notification loss.

### Pattern

```
Email failover chain:
  Primary:   AWS SES       ($0.10/1000, lowest cost)
  Secondary: SendGrid      ($0.20/1000, faster support)
  Tertiary:  Mailgun       ($0.25/1000)
  Last:      DLQ → retry when primary recovers

SMS failover chain:
  Primary:   Twilio        ($0.0079/SMS, highest deliverability)
  Secondary: AWS SNS SMS   ($0.00645/SMS, different carrier routes)
  Tertiary:  downgrade → push notification with same content

Push failover:
  FCM (Android): no alternative — Apple controls APNs for iOS exclusively
  If FCM down: queue with TTL, retry on recovery
  If delay > TTL (5min for time-sensitive): deliver as in-app on next open
```

### Implementation with Circuit Breaker

```python
class ProviderChain:
    def __init__(self, providers):
        self.providers = providers  # ordered list
        self.breakers = {p: CircuitBreaker() for p in providers}

    def send(self, message):
        for provider in self.providers:
            if self.breakers[provider].state == OPEN:
                continue  # skip tripped provider
            try:
                result = self.breakers[provider].call(
                    lambda: provider.send(message),
                    fallback=None
                )
                if result:
                    return result  # success, stop chain
            except Exception:
                pass  # try next provider
        
        # All providers failed
        enqueue_for_retry(message, delay=300)
        return None
```

### Cost vs Reliability Trade-off

| Config | Reliability | Cost | Use When |
|--------|-------------|------|----------|
| Single provider | Low (SPOF) | Lowest | Dev/test only |
| Primary + secondary | High | +20-50% overhead for failover volume | Production standard |
| Three providers | Very high | +50-100% for failover volume | Critical (OTP, fraud) only |

**Rule:** Failover providers are cheap in practice — they only receive traffic during primary outage (rare). Overhead is mostly in maintaining API keys and integration code, not cost.

### Appeared In
- Notification System (2026-06-26)

---

## 92. Digest Batching Pattern

**What it is:** Aggregate N individual notifications into a single delivery (email/push) to reduce volume, provider cost, and user annoyance.

### When to Use

```
Without digest:
  User has 20 followers in 30 minutes → 20 separate emails
  User unsubscribes → lost forever

With digest:
  All 20 follower notifications → one "You have 20 new followers" email
  Volume: 20× reduction → 95% lower cost for social notification type
```

### Implementation

```sql
-- Pending digest table
pending_digest (
    id          UUID,
    user_id     UUID,
    notif_id    UUID,   -- FK to notification_events
    type        VARCHAR,
    queued_at   TIMESTAMPTZ,
    deliver_after TIMESTAMPTZ   -- when DND ends or next digest window
)

-- Hourly digest job
SELECT user_id, array_agg(notif_id ORDER BY queued_at) as notif_ids
FROM pending_digest
WHERE deliver_after <= now()
GROUP BY user_id
HAVING COUNT(*) >= 1;

FOR each (user_id, notif_ids):
    render_digest_email(user_id, notif_ids)  -- "You have 5 new followers + 3 likes"
    send_email(user_id, digest_email)
    DELETE FROM pending_digest WHERE id = ANY(notif_ids)
```

### Digest Template Strategy

```
1 notification:  "John followed you"
2-4:             "John, Alice, and 2 others followed you"
5+:              "You have 8 new followers this hour"
Mixed types:     "5 new followers · 3 likes · 1 comment"
```

### Trade-offs

| Config | User Experience | Cost Reduction | Complexity |
|--------|----------------|----------------|------------|
| No digest (immediate) | Real-time, may feel spammy | Baseline | Low |
| Hourly digest | Up to 60min delay for social events | 70-80% | Medium |
| Daily digest | Summary only, low friction | 95% | Low |
| Smart digest (N threshold) | Digest when ≥5 pending, else immediate | 50% | High |

**LinkedIn data:** Digest mode reduced email send volume 80%, reduced unsubscribes 30%, no meaningful engagement drop.

**Rule:** Digest only non-time-sensitive notification types. OTP, fraud alert, payment: always immediate.

### Appeared In
- Notification System (2026-06-26)

---

## 93. Email Delivery Stack (SMTP / SPF / DKIM / DMARC)

**What it is:** The protocol and authentication layer governing how emails are sent, authenticated, and received across the internet.

### Protocol Stack

| Protocol | Port | Purpose |
|----------|------|---------|
| SMTP | 25 (server), 587 (submission) | Transfer emails between MTAs |
| IMAP | 993 (TLS) | Access mailbox from multiple devices, server-authoritative |
| POP3 | 995 (TLS) | Download + delete; device-local copy |
| HTTP/REST | 443 | Modern API (Gmail API) |

### Email Authentication

**SPF:** DNS TXT record authorizing which IPs can send for domain. Receiving MTA checks sender IP against list.
```
company.com TXT "v=spf1 ip4:192.0.2.0/24 ~all"
```

**DKIM:** Sender signs headers+body with private key. Public key in DNS. Receiving MTA verifies signature → proves no tampering in transit.

**DMARC:** Policy on what to do when SPF+DKIM fail. Adds alignment check (authenticated domain must match From header).
```
_dmarc.company.com TXT "v=DMARC1; p=reject; rua=mailto:reports@company.com"
p=none → report only | p=quarantine → spam | p=reject → bounce
```

### Why All Three

| Attack | SPF | DKIM | DMARC |
|--------|-----|------|-------|
| Unauthorized sending IP | ✓ | — | — |
| Message tampered in transit | — | ✓ | — |
| From address spoofed | — | — | ✓ (alignment) |

### Self-hosted vs Managed ESP

| | SES / SendGrid | Self-hosted SMTP |
|--|---|---|
| Cost | $0.10/1K | ~$0.001/1K (infra) |
| Deliverability | Managed | Your responsibility |
| Break-even | 10M emails/day | Above that → self-host |

**Rule:** Below 10M/day → SES/SendGrid. Above → self-hosted SMTP. At 5B/day, SES = $3M/month vs self-hosted = $55K/month.

### Appeared In
- Email System (2026-06-27)

---

## 94. Email Thread Grouping (In-Reply-To / References)

**What it is:** Algorithm assigning emails to conversation threads using RFC 5322 headers, without a central registry.

### Header Chain

```
Original:  Message-ID: <abc@co.com>
Reply:     In-Reply-To: <abc@co.com>  |  References: <abc@co.com>
Reply²:    In-Reply-To: <def@co.com>  |  References: <abc@co.com> <def@co.com>
```

### Resolution Algorithm (priority order)

```python
1. In-Reply-To → lookup message_id → return thread_id if found
2. References (last entry first) → same lookup
3. Subject normalization: "Re: Meeting" → "meeting"
   → find thread with same participants + normalized subject within 7 days
4. Create new thread
```

**Storage:** Cassandra table `email_by_message_id (message_id PK) → (email_id, thread_id)`

### Edge Cases

| Scenario | Result |
|----------|--------|
| `In-Reply-To` points to deleted email | Step 2 via References fallback |
| Two unrelated same-subject emails | False merge in Step 3 — scope to same participants |
| Old client omits headers | New thread created (acceptable) |

### Trade-off
> Steps 1+2 (header-based): precise, no false positives, works globally
> Step 3 (subject-based): approximate, false merges — last resort only
> Thread merging retroactively is expensive (update all thread_id references)

### Appeared In
- Email System (2026-06-27)

---

## 95. Bayesian Spam Filter

**What it is:** Probabilistic classifier scoring spam probability from word frequencies learned from labeled corpora.

### Naive Bayes (Log Space)

```python
# Scoring (online)
spam_score = log(prior_spam)
ham_score  = log(prior_ham)

for token in tokenize(email.body + subject):
    spam_score += log(P(token | spam))   # pre-computed from training
    ham_score  += log(P(token | ham))

if spam_score - ham_score > threshold → SPAM
```

Log space: avoids float underflow from multiplying thousands of small probabilities.
Laplace smoothing: `P(token|spam) = (count + 1) / (total + vocab_size)` — prevents P=0 for unseen words.

### Comparison

| | Bayesian | Rule-based | Neural (BERT) |
|--|---------|-----------|--------------|
| Inference | ~10ms | <1ms | ~50ms |
| New vocabulary | Poor (Laplace helps) | No | Good |
| Explainable | Yes | Yes | No |
| Per-user adaptation | Yes (count deltas) | No | Expensive |

### Per-User Model
Global model misses user context ("pharma discount" = spam globally, not for a pharmacist).
```
score = 0.7 × global_score + 0.3 × user_model_score
User feedback (mark not-spam) → increment user ham_counts
```

### Staged Approach (Cost Optimization)
At 350K emails/sec, running BERT on all = ~$500K/month in GPU compute.
```
Stage 1: SPF/DKIM/DMARC + IP blocklist   → <1ms, catches obvious (score < 30 or > 70)
Stage 2: Bayesian classifier             → ~10ms, handles 90% of borderline
Stage 3: Neural (BERT)                   → ~50ms, only for score 30–70 (~5% of traffic)
```

### Trade-off
> Bayesian: cheap, explainable, requires periodic retraining → gets stale on adversarial spam.
> BERT: accurate, adapts, expensive → reserve for borderline cases only.

### Appeared In
- Email System (2026-06-27)

---

## 96. Snowflake ID Structure

**What it is:** A 64-bit integer ID that encodes timestamp + machine identity + sequence — generated locally with no coordination on the hot path.

### Bit Layout
```
| 0 | 41-bit ms timestamp | 10-bit machine ID | 12-bit sequence |
```

| Segment | Bits | Capacity |
|---------|------|----------|
| Sign | 1 | Always 0 (keeps ID positive) |
| Timestamp | 41 | 69 years from custom epoch |
| Machine ID | 10 | 1,024 unique nodes |
| Sequence | 12 | 4,096 IDs per ms per node |

### When to Use
- Any system needing globally unique, time-sortable 64-bit IDs at high throughput
- When B-tree index performance matters (sequential inserts are faster than random)
- When you want to embed creation time in the ID without a DB lookup

### When NOT to Use
- When you can't run an ID service (use UUID v7 instead)
- When IDs must be unpredictable / non-enumerable (add XOR scramble layer)
- When you need >1024 generator nodes (extend machine ID bits)

### Trade-offs
| Pro | Con |
|-----|-----|
| Zero coordination on hot path (~10ns) | Requires ZooKeeper or equivalent for machine ID assignment |
| Time-sortable → fast B-tree inserts | 69-year epoch (plan re-epoch by ~2083) |
| Decodable (timestamp + machine readable) | Predictable → enumerable by attackers |
| 64-bit → half the storage of UUID | Clock skew can cause ID generation pause |

### Real-World
- Twitter Snowflake (original, 2010)
- Instagram (pl/pgsql per shard, no service)
- Discord (42ms + 10 worker + 12 seq + 1 process)
- Sonyflake (16-bit machine, 8-bit seq, 10ms units)

### Appeared In
- Distributed ID Generator (2026-06-28)
- Referenced in: Twitter Feed, Chat System, Payment System, URL Shortener

---

## 97. Clock Skew Handling in Distributed Systems

**What it is:** Techniques to handle NTP-induced clock corrections that can cause a node's clock to jump backward, breaking monotonic timestamp assumptions.

### Why It Matters
Snowflake and any timestamp-based ID generator assumes `current_time >= last_used_time`. NTP can violate this by stepping the clock backward.

### Handling Options

| Strategy | Mechanism | When to Use |
|----------|-----------|-------------|
| **Spin-wait** | Loop until `now > last_ms` | Small corrections (<200ms); adds latency but safe |
| **Refuse + alert** | Raise exception, page on-call | Safety-critical systems; brief unavailability acceptable |
| **Use last_ms** | Treat backward clock as same ms | Only safe if correction < 1ms and sequence not exhausted |
| **Monotonic clock** | Use `CLOCK_MONOTONIC` (CPU TSC) | Never goes backward; decoupled from wall time |

### EC2 / Cloud Specifics
- EC2 uses `chrony` with gradual slew (max ±500ppm) — no abrupt step
- Exception: VM migration / paused instance → clock can jump
- Detection: check instance metadata for migration events; pause generation on detection

### Trade-off
> Spin-wait: safe, adds latency up to correction size. Monotonic clock: safe, zero latency — but doesn't track wall time drift (IDs may not reflect true creation time if system was paused).

### Appeared In
- Distributed ID Generator (2026-06-28)

---

## 98. UUID v4 vs UUID v7

**What it is:** Two variants of the 128-bit UUID standard with fundamentally different performance profiles.

### Comparison

| Property | UUID v4 | UUID v7 |
|----------|---------|---------|
| Structure | 122 random bits | 48ms timestamp + 74 random bits |
| Sort order | Random | Time-sortable (monotonic) |
| B-tree insert pattern | Random → page splits | Sequential → minimal splits |
| Coordination needed | None | None |
| Storage | 128 bits (16 bytes) | 128 bits (16 bytes) |
| Standard | RFC 4122 | RFC 9562 (2024) |
| Index fragmentation | High at >10M rows | Low |

### UUID v4 B-tree Problem
Random UUIDs cause ~50% of inserts to land in the "middle" of the B-tree → page splits → write amplification → 3–5× slower inserts vs sequential at 100M+ rows.

### When to Use UUID v7 over Snowflake
- Zero infrastructure tolerance (no ID service to run)
- Multi-language / multi-framework environment (UUID is universal)
- 128-bit storage acceptable
- Don't need machine ID embedded in the ID

### Trade-off
> UUID v7 vs Snowflake: UUID v7 wins on simplicity (no service), Snowflake wins on storage (64-bit) and decodability.

### Appeared In
- Distributed ID Generator (2026-06-28)

---

## 99. Ticket Server Pattern

**What it is:** A centralized MySQL/Postgres server whose sole job is generating unique IDs via `AUTO_INCREMENT`, used by all other services.

### How It Works (Flickr, 2010)
```sql
-- Two servers in active-active, offset step
Server 1: auto_increment_increment=2, auto_increment_offset=1  → 1,3,5,7...
Server 2: auto_increment_increment=2, auto_increment_offset=2  → 2,4,6,8...
```
Each service batch-fetches a range of IDs from whichever server is available.

### When to Use
- Low-to-medium throughput (<1M IDs/sec)
- You already have MySQL/Postgres in your stack
- Simple ops: no ZooKeeper, no Snowflake service

### When NOT to Use
- High throughput (>1M/sec): DB becomes bottleneck
- Need time-embedded IDs (ticket server gives no timestamp)
- Need geo-distributed generation (latency to central server)

### Trade-off
| Pro | Con |
|-----|-----|
| Simple: just a DB table | Single point of failure (even with 2 servers) |
| No extra infrastructure | Network hop on every ID (~1ms) |
| Familiar tooling | No timestamp in ID |
| Batch fetch reduces load | Adding more servers requires resharding step offsets |

### Real-World
- Flickr photo IDs (original usage)
- Some older e-commerce platforms for order IDs

### Appeared In
- Distributed ID Generator (2026-06-28)

---

## 100. MQTT Protocol

**What it is:** A lightweight pub/sub messaging protocol designed for constrained devices over unreliable networks. Binary protocol with 2-byte fixed header (vs ~800 bytes for HTTP).

### QoS Levels

| Level | Name | Guarantee | Packets | Use When |
|-------|------|-----------|---------|---------|
| **QoS 0** | At-most-once | May lose | 1 (PUBLISH) | Metrics where loss is tolerable |
| **QoS 1** | At-least-once | No loss, may duplicate | 2 (PUBLISH + PUBACK) | Telemetry — duplicates are idempotent |
| **QoS 2** | Exactly-once | No loss, no duplicate | 4 (PUBLISH + PUBREC + PUBREL + PUBCOMP) | Commands — execute exactly once (lock, reset) |

### Retained Messages
```
PUBLISH topic="$shadow/delta/{id}" retain=true payload={delta}
→ Next subscriber to that topic immediately receives the LAST retained message.
Use: device shadow delta, config — device gets desired state on reconnect without polling.
```

### Last Will and Testament (LWT)
```
Device connects with:
  will_topic: "status/{device_id}"
  will_payload: "offline"
  will_retain: true

If device disconnects ungracefully (TCP broken, no DISCONNECT packet):
  Broker auto-publishes LWT → Status Service marks device offline
```

### Persistent Sessions
```
clean_session=false → broker retains subscriptions + undelivered QoS 1/2 messages
On reconnect (within expiry): broker delivers queued messages → device catches up
```

### When NOT to Use MQTT
- Server-to-server (use Kafka/gRPC — better tooling, observability)
- High-bandwidth data (video, large files) — overhead per message amortizes poorly
- Request-reply semantics (use gRPC) — MQTT is fire-and-forget pub/sub

### Trade-offs
| Pro | Con |
|-----|-----|
| 2-byte header → efficient for small payloads | No native request-reply (must build on top) |
| QoS levels built-in | QoS 2 = 4 round trips (avoid except for critical commands) |
| Retained messages for state sync | No message schema validation at protocol level |
| Persistent sessions for offline delivery | Broker is stateful → harder to scale horizontally |

### Real-World
- AWS IoT Core, Azure IoT Hub, Google Cloud IoT — all use MQTT at the edge
- EMQX: 100M+ MQTT connections per cluster
- Home Assistant: primary device integration protocol

### Appeared In
- IoT Data Pipeline (2026-06-29)

---

## 101. Device Shadow Pattern

**What it is:** A persistent, cloud-side representation of a device's current and desired state. Enables bidirectional state sync even when devices are intermittently offline.

### Structure
```json
{
  "desired":  { "mode": "cool", "setpoint": 22 },
  "reported": { "mode": "idle", "temperature": 24.5 },
  "delta":    { "mode": "cool", "setpoint": 22 },
  "version":  8,
  "timestamp": 1751155200
}
```

### Sync Flow
```
1. User changes setting → Shadow Service updates desired, increments version
2. Shadow publishes delta to $shadow/delta/{device_id} (retained=true)
3. Device online: receives delta → applies → publishes new reported state
4. Device offline: retained delta persists → delivered on reconnect
5. reported == desired → delta = {} → shadow reconciled
```

### Optimistic Locking (Concurrent Updates)
```sql
UPDATE device_shadows
SET desired = desired || $new_desired::jsonb, version = version + 1
WHERE device_id = $1 AND version = $expected_version;
-- 0 rows → 409 Conflict → client re-reads and retries
```

### Storage
- **Redis:** hot path, <1ms reads → `HSET shadow:{device_id} desired ... reported ... version ...`
- **Postgres:** durable source of truth, versioned history

### Trade-off
| Pro | Con |
|-----|-----|
| Devices work offline — catch up on reconnect | Extra complexity vs simple HTTP commands |
| Decouples "set intention" from "execution" | Delta computation must handle partial JSON updates |
| Version history for audit | Redis miss → Postgres fallback (100ms cold read) |

### Appeared In
- IoT Data Pipeline (2026-06-29)

---

## 102. Time-Series DB: Downsampling + Retention Policies

**What it is:** Automatically reducing time-series data granularity as it ages to keep storage manageable while preserving long-term trend visibility.

### Retention Policy Ladder
```
raw_7d:     full resolution, 7-day TTL   → ~86 GB (10:1 TSDB compression)
hourly_90d: 1h aggregates, 90-day TTL   → 216 GB
daily_2yr:  1d aggregates, 2-year TTL   → 73 GB
S3 archive: daily Parquet → cold, query via Athena
```

### Downsampling Query (InfluxDB Continuous Query)
```sql
SELECT MEAN(value) AS mean, MAX(value) AS max, MIN(value) AS min
INTO hourly_90d.telemetry
FROM raw_7d.telemetry
GROUP BY time(1h), device_id, sensor_type
-- Runs automatically; no scheduler needed
```

### Cardinality Rule
```
Tags (indexed): device_id, sensor_type, owner_id — LOW cardinality OK
Fields: value, unit — high cardinality OK here (not indexed)

NEVER put high-cardinality data (request_id, session_id) in tags.
→ Each unique tag combination = one in-memory series → OOM
```

### TSDB Comparison

| | InfluxDB | TimescaleDB | Cassandra |
|--|---------|------------|-----------|
| Downsampling | Built-in CQ | Postgres scheduled job | Separate Spark job |
| SQL | Flux (custom) | Full SQL | CQL (limited) |
| Multi-region | Enterprise only | via Patroni | Native |
| Sweet spot | IoT / DevOps metrics | SQL-familiar teams | >10B writes/day |

### Trade-off
> Raw retention longer = full forensic capability, proportionally higher cost.
> Downsampled only = storage efficient, can't reconstruct individual events after TTL.
> Decision: match raw retention to "how long do we need to debug individual incidents" — typically 7-30 days.

### Appeared In
- IoT Data Pipeline (2026-06-29)

---

## 103. Edge Aggregation + Jittered Reconnect Backoff

### Edge Aggregation Pattern
**What it is:** Pre-aggregate or filter data on a local gateway before sending to cloud, reducing bandwidth and message count.

```
Without edge agg: 1 reading/sec → 86,400 messages/day/device
With edge agg:    1 average per 10s → 8,640 messages/day/device → 10× reduction
```

**When to aggregate at edge:**
- Network is metered (cellular IoT) — every byte costs money
- Cloud ingest cost scales per-message (AWS IoT Core = $1/million messages)
- Local latency needed (smart home switch: responds in <10ms locally, no cloud round trip)

**When NOT to aggregate:**
- Need individual events for spike detection (averaging hides anomalies)
- Regulatory requirement for raw readings (medical IoT, smart meters)

### Jittered Reconnect Backoff
**What it is:** Adding randomness to retry delays to spread reconnection load after a shared outage.

**Problem:** 500K devices all retry after identical 2s backoff → 500K simultaneous TCP connections → server crash loop.

**Full Jitter (AWS recommendation):**
```python
def reconnect_delay(attempt, base=1.0, cap=60.0):
    return random.uniform(0, min(cap, base * 2**attempt))
# attempt=0 → [0,1]s, attempt=5 → [0,32]s, attempt=6+ → [0,60]s
```

### Trade-off
| Approach | Avg Reconnect | Herd Risk | Use When |
|----------|--------------|-----------|---------|
| Fixed retry | Fast | Extreme | Never at IoT scale |
| Exponential (no jitter) | Medium | Medium | <100 clients |
| **Full jitter** | Slower avg | None | IoT fleets, mobile apps |
| Equal jitter (half-jitter) | Medium | Very low | When minimum delay required |

### Appeared In
- IoT Data Pipeline (2026-06-29)

---

## 104. Base62/Base58 Encoding for Short IDs

**What it is:** Encode large integers into compact URL-safe strings using alphanumeric characters.

```
Base62 alphabet: A-Z a-z 0-9 (62 chars)
Base58: Base62 minus 0, O, I, l (58 chars) — avoids visual confusion

6 chars → 62^6 = 56.8 billion unique IDs
8 chars → 62^8 = 218 trillion unique IDs
```

### Why Not Base64?
Base64 uses `+` and `/` which require percent-encoding in URLs. Base62 is URL-safe natively.

### ID Space vs URL Length

| Chars | Base62 IDs | At 1M creates/day |
|-------|-----------|------------------|
| 6 | 56.8B | 155,000 years to exhaust |
| 7 | 3.5T | 9.6M years |

6 chars sufficient for any Pastebin/URL-shortener at realistic scale.

### Birthday Collision Risk (random-only generation)
```
P(collision after n IDs) ≈ n² / (2 × space)
At 100M IDs in 56.8B space: P ≈ 0.0088% — retry handles this
At 1B IDs in 56.8B space: P ≈ 0.88% — switch to pre-generated pool
```

### Pre-Generated Key Pool Pattern
```
1. Background job: random base62 strings → deduplicate → store in keys_available
2. On create: SELECT key FOR UPDATE SKIP LOCKED (atomic claim)
3. Refill when pool < 100K keys
```
**Pros:** Zero collision, deterministic length.  
**Cons:** Extra service, needs periodic refill job.

### Trade-off
> Pre-generated pool: safe, requires background job.
> On-the-fly random + retry: simpler but risky at >1B IDs.
> Snowflake → base62: longer URLs (10-11 chars), no extra service.

### Appeared In
- Pastebin (2026-06-30), URL Shortener

---

## 105. Content Expiry Pattern (Lazy Gate + Active Cleanup)

**What it is:** Two-tier expiry — serve correct behavior immediately (lazy gate at read time) + reclaim storage periodically (active cleanup job).

### Two Tiers

**Tier 1 — Lazy Gate (read path)**
```
On GET /content/{id}:
  Redis TTL auto-evicts cache at expiry
  Cache miss → DB lookup → if expires_at < now() → return 404
  User never sees expired content regardless of cleanup timing
```

**Tier 2 — Active Cleanup (storage)**
```sql
-- Runs every 5min (pg_cron or external cron):
DELETE FROM pastes WHERE expires_at < now() LIMIT 5000;
-- S3: lifecycle rule (prefix → Glacier 30d → delete 180d)
```

### Why Both?
- Lazy only: rows accumulate indefinitely → table bloat → slow scans
- Active only: race condition — missed cleanup window → expired content served
- Together: correct UX + bounded storage growth

### Trade-off
> Cleanup frequency vs DB write load. Every 5min + LIMIT 5000 = safe.
> Never rely solely on Redis TTL — Redis is a cache; DB is source of truth for expiry enforcement.

### Appeared In
- Pastebin (2026-06-30), URL Shortener


---

## 108. X3DH — Async Key Exchange for E2E Encryption

**What it is:** Extended Triple Diffie-Hellman. Lets two parties establish a shared secret without being online simultaneously. Foundation of the Signal Protocol (WhatsApp, Signal).

### How It Works
```
Bob pre-uploads to Key Distribution Center:
  identity_key:     Ed25519 public key (permanent)
  signed_prekey:    X25519, signed by identity_key (rotated weekly)
  one_time_prekeys: batch of X25519 keys (used once, popped on use)

Alice (online, Bob offline) fetches Bob's keys:
  DH1 = DH(Alice.identity_key,  Bob.signed_prekey)
  DH2 = DH(Alice.ephemeral_key, Bob.identity_key)
  DH3 = DH(Alice.ephemeral_key, Bob.signed_prekey)
  DH4 = DH(Alice.ephemeral_key, Bob.one_time_prekey)  [if available]
  shared_secret = KDF(DH1 || DH2 || DH3 || DH4)
```

### Key Properties
- **Async**: Bob doesn't need to be online
- **Mutual auth**: DH1+DH2 bind both parties' long-term identity keys
- **Forward secrecy per session**: one-time prekey (DH4) ensures unique starting secret each conversation

### Fallback
One-time prekeys exhausted → omit DH4 → slightly weaker but still secure.
Server alerts device → app uploads fresh batch silently in background.

### Trade-offs
| | X3DH | Simpler symmetric | Online-only ECDH |
|--|--|--|--|
| Async key agreement | ✓ | ✗ | ✗ |
| Forward secrecy | Per session | ✗ | Depends |
| Complexity | High | Low | Medium |

### Appeared In
- WhatsApp (2026-07-01)

---

## 109. Double Ratchet — Per-Message Forward Secrecy

**What it is:** Key derivation protocol that advances the session key with every message, providing forward secrecy and break-in recovery. Used after X3DH.

### Two Ratchets
```
Symmetric (chain key):
  chain_key_n+1 = KDF(chain_key_n)
  message_key_n = KDF(chain_key_n, 0x01)  → encrypt msg n, then DELETE key

DH ratchet (root key):
  Each party embeds new ephemeral DH key in their messages
  On receiving new DH key: new_root = KDF(root, DH(own_key, their_key))
  → Resets chain key → self-heals after compromise
```

### Properties
- **Forward secrecy**: message_key deleted after use → past messages irretrievable
- **Break-in recovery**: DH ratchet advances → future messages safe even after current state compromise
- **Out-of-order**: store up to 1000 skipped message keys for late-arriving packets

### Real-World
Signal, WhatsApp, Wire, Google RCS Messages

### Appeared In
- WhatsApp (2026-07-01)

---

## 110. SenderKey — Efficient Group E2E Encryption

**What it is:** A shared symmetric key distributed once to all group members, enabling O(1) encrypt per group message regardless of N.

### Problem Without SenderKey
```
N-member group → N separate encryptions per message → O(N) CPU + bandwidth
```

### Solution
```
Setup (once per new member):
  Alice generates SenderKey (AES-256 + chain_key)
  Distributes to each member M via 1:1 E2E session

Per message:
  encrypt(plaintext, SenderKey) → 1 ciphertext
  Server fans out same ciphertext to all N members
  Cost: O(1) encrypt, O(N) delivery of identical blob
```

### Member Removal → Key Rotation
```
Removed member has old SenderKey → rotate for forward secrecy
Remaining N-1 members receive new SenderKey
WhatsApp throttles: max 1 rotation/min per group (expensive op)
```

### Trade-offs
| | SenderKey | Pairwise Sessions |
|--|--|--|
| Encrypt per msg | O(1) | O(N) |
| Member removal | Must rotate all keys | No rotation needed |
| Best for | Groups >10 | 1:1 only |

### Appeared In
- WhatsApp group messaging (2026-07-01)

---

## 111. Offline Message Queue (Device-Targeted, TTL-Based)

**What it is:** Server-side durable queue for encrypted messages awaiting offline recipients, keyed per device with automatic TTL expiry.

### Design
```sql
CREATE TABLE offline_messages (
  user_id     BIGINT,
  device_id   UUID,
  created_at  TIMESTAMP,
  message_id  UUID,
  payload     BLOB,   -- server cannot read (E2E)
  PRIMARY KEY ((user_id, device_id), created_at, message_id)
) WITH default_time_to_live = 2592000;  -- 30 days
```

### Flow
```
Offline path:  write offline_messages + fire FCM/APNS push
Online return: WebSocket handshake sends last_received_id
               Server delivers all newer messages → client ACKs → delete rows
Multi-device:  separate queue per device; deleted per-device on ACK
```

### Trade-offs
| | Upside | Downside |
|--|--|--|
| 30-day TTL | Covers most offline scenarios; auto-cleanup | Messages lost if offline >30 days |
| Per-device queue | Multi-device correct | More storage than per-user |
| FCM push with preview | Better UX | Leaks content in notification |
| FCM no-preview | E2E-pure | Worse UX |

### Appeared In
- WhatsApp (2026-07-01)

---

## 112. Multi-Device Session Management (Linked Devices)

**What it is:** Scheme allowing multiple physical devices under one identity to each receive messages independently without sharing a private key.

### WhatsApp's Approach
```
Each device has its own keypair.
Linking: phone signs + publishes new device's public key to server.

Message to Bob (phone + laptop):
  Alice fetches both Bob device public keys
  Encrypts twice (once per device key)
  Server routes each ciphertext to the correct device

History: NOT synced retroactively (forward secrecy — old ratchet state gone)
         From link date forward: all messages delivered to all devices
```

### Trade-offs
| Approach | Security | Complexity | UX |
|----------|----------|------------|---|
| Multi-copy per device (WhatsApp) | Full E2E | O(devices) ciphertexts | Seamless |
| Shared device key | Weaker (1 compromise = all) | Simple | Seamless |
| Phone-as-proxy (old WhatsApp Web) | Good | Simple | Breaks when phone offline |

### Appeared In
- WhatsApp (2026-07-01)

---

## 113. Delivery Receipt Pattern (Sent / Delivered / Read)

**What it is:** Three-state acknowledgment propagated back to sender as visual confirmation of message progress.

### States
```
✓  gray  = server received (sent)
✓✓ gray  = recipient device received (delivered)
✓✓ blue  = recipient opened chat (read)
```

### Implementation
```
Sent:      Chat Service ACKs write to client → single ✓
Delivered: Recipient WebSocket ACKs → POST /receipt {message_id, 'delivered'}
           Server: INSERT message_status(message_id, recipient_id, 'delivered')
           Notify sender via their WebSocket
Read:      Recipient opens chat → POST /receipt {chat_id, last_read_message_id, 'read'}
           Server marks all messages up to last_read as 'read' for this user
```

### Idempotency (Out-of-Order Receipts)
```python
# Monotonic transitions only — prevent status regression
status_rank = {'sent': 0, 'delivered': 1, 'read': 2}
UPDATE message_status SET status = new_status
WHERE message_id = ? AND status_rank[status] < status_rank[new_status]
```

### Group Scale
```
1000-member group → up to 1000 delivery receipts per message
Batch receipts: collect 500ms → bulk insert
Pre-aggregate: delivered_count, read_count on messages row for fast sender query
```

### Privacy Trade-off
> Disabling read receipts is reciprocal: you can't see others' read receipts either.
> Prevents surveillance while maintaining fairness.

### Appeared In
- WhatsApp (2026-07-01)
- Chat System (2026-05-11)

---

## 106. Perceptual Hashing

**What it is:** A hash of image/video content that is similar for visually similar content (unlike cryptographic hashes). Slight crops, color shifts, or compressions yield the same or nearby hash.

| Type | How | Robustness | Use Case |
|------|-----|------------|----------|
| **pHash** | DCT of downscaled image | Moderate — survives resize/JPEG | General image dedup |
| **PDQ (Meta)** | More robust DCT variant | High — survives crops, overlays | CSAM hash-sharing |
| **PhotoDNA (MS)** | Proprietary, NCMEC-integrated | High | Industry CSAM detection |
| **Video TMK** | Per-frame + temporal | Video-specific | YouTube Content ID |

### When to Use
- Known-violation matching (CSAM, terrorism, piracy)
- Copyright content ID
- Deduplication of near-identical content

### When NOT to Use
- Novel content (not in DB) — need ML for that
- Text moderation (use NLP, not image hash)

### Trade-off
Hash DB only catches *known* violations. Adversaries who slightly modify images can (sometimes) evade pHash but not PDQ. Dual-hash requirement (pHash AND PDQ) raises the bar.

### Appeared In
- Content Moderation (2026-07-02)

---

## 107. Tiered Moderation Pipeline

**What it is:** A cascading decision architecture: cheap/fast checks first, expensive/accurate checks only when needed.

```
Tier 1: Hash check       (< 5ms, deterministic, known violations)
Tier 2: ML classifier    (500ms, probabilistic, novel content)
Tier 3: Human review     (minutes–hours, authoritative, edge cases)
```

### When to Use
- Content moderation at scale
- Any classification problem where cost varies by confidence level
- Fraud detection (rule engine → ML → manual review)

### Trade-off
- More tiers = more latency but lower human review cost
- Tier 2 threshold tuning is the key design variable

### Appeared In
- Content Moderation (2026-07-02)
- Fraud Detection (2026-06-16)

---

## 108. Human-in-the-Loop Review Queue

**What it is:** Work queue where ML-uncertain items are routed to human reviewers, with skills-based routing, priority queues, and SLA enforcement.

### Design Elements
- Priority queues by severity (P0=CSAM, P1=violence, P2=spam)
- Skills-based routing (language, category expertise)
- Time-boxed tasks: auto-escalate if not reviewed within SLA
- Overturn rate tracking per moderator (quality signal)

### Trade-off
- Adding humans improves accuracy but adds cost ($15–$25/hr per reviewer)
- Queue depth is the capacity constraint; ML threshold tuning controls inflow
- Moderator burnout is a real operational risk (PTSD from reviewing harmful content) — covered in duty-of-care policies

### Appeared In
- Content Moderation (2026-07-02)

---

## 109. Precision vs Recall Threshold Tuning

**What it is:** Per-category ML decision thresholds that trade off false positives (wrongly banning clean content) vs. false negatives (missing violations).

| Category | Threshold | Rationale |
|----------|-----------|-----------|
| CSAM | 0.999 (near-zero FP) | Legal zero-tolerance; false negative is catastrophic |
| Terrorism | 0.99 | High harm; accept some FP, catch all TP |
| Spam | 0.85 | Low harm; accept more FP; FN just means spam visible briefly |
| Nudity | 0.90 | Context-dependent (art vs. pornography) |

### Key Rule
> Thresholds are not one-size-fits-all. Model thresholds belong in a config database, not hard-coded. Product and policy teams own the thresholds; ML engineers own the model.

### Appeared In
- Content Moderation (2026-07-02)

---

## 110. Behavioral Graph Detection

**What it is:** Detecting coordinated inauthentic behavior (CIB) by modeling account activity as a graph and finding anomalous clusters.

### How
1. Build bipartite graph: accounts ↔ content they interact with
2. Find communities (Louvain, Label Propagation)
3. Flag clusters with: same content posted within 60s, accounts created in same batch, identical device fingerprints, coordinated like/share patterns

### When to Use
- Bot farm detection
- Election interference / coordinated manipulation
- Fake review networks (Amazon, Yelp)

### When NOT to Use
- Individual spam (overkill; single-account signals sufficient)

### Trade-off
- Graph community detection is expensive (O(n log n) for Louvain, but graph can be huge)
- Run offline (nightly batch), not real-time
- Real-time signals: velocity checks (N posts in M seconds from same IP) as lightweight proxy

### Appeared In
- Content Moderation (2026-07-02)

---

## 111. Content-Defined Chunking (Rabin Fingerprinting)

**What it is:** Splitting a file into variable-size blocks where boundaries are determined by the file's content (rolling hash), not by fixed offsets. Produces the same boundaries even after byte insertions — enabling true delta sync.

### Fixed-size vs. Content-Defined

| | Fixed-size (4MB) | Content-Defined (Rabin, avg 4MB) |
|--|---|---|
| Boundary | Every 4MB | Based on content rolling hash |
| After byte insert at start | ALL chunks shift → full re-upload | Only 1-2 boundary chunks change |
| Deduplication | Good for appends | Better for edits mid-file |
| Complexity | Simple | Moderate |

### How Rabin Fingerprinting Works

```
Sliding window of W=64 bytes moves over the file:
  At each position: window_hash = hash(bytes[i..i+W])
  Chunk boundary when: window_hash & MASK == PATTERN
  (larger MASK = smaller average chunk size)

Average chunk size = 4MB → MASK has ~22 zero bits
Min chunk: 1MB   Max chunk: 8MB
```

### Why It Matters for Sync

```
Fixed-size, insert 1 byte at position 0:
  ALL chunks shift by 1 → every hash changes → upload entire file

Rabin, insert 1 byte at position 0:
  Boundary shifts locally → 1-2 chunks near the insert change
  All other boundaries unchanged → only changed chunks upload
```

### Trade-off
- Pro: fewer uploads for edits within large files (especially text/code)
- Con: variable chunk sizes → harder to reason about storage, more metadata entries
- Real world: Dropbox uses content-defined chunking (tech blog, 2016)

### Appeared In
- Dropbox (2026-07-03) — delta sync optimization

---

## 112. Sync Cursor / Change Feed Pattern

**What it is:** A monotonically increasing integer assigned to every mutation. Clients store their last-seen cursor; on reconnect they request all changes since that cursor.

### Why Cursor, Not Timestamp

```
Timestamp-based: clock skew between client/server → miss or duplicate events
                 NTP adjustments → clocks go backward → miss events

Cursor-based:   server assigns IDs (not wall clock)
                monotonic, gapless, clock-independent
```

### Implementation

```sql
changes (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,  -- the cursor
  namespace_id BIGINT,
  file_id BIGINT,
  change_type ENUM('create','update','delete'),
  new_version_id BIGINT,
  changed_at TIMESTAMP
);
INDEX: (namespace_id, id)

-- Client sync query
SELECT * FROM changes
WHERE namespace_id = :ns AND id > :last_cursor
ORDER BY id ASC LIMIT 100;
```

### Trade-off
- Pro: simple, no clock dependency, easy replay from any point
- Con: global cursor doesn't scale to multi-region → use per-namespace cursors
- Watch out: AUTO_INCREMENT gaps on failed inserts → use SEQUENCE or Snowflake ID if strict monotonicity needed

### Appeared In
- Dropbox (2026-07-03) — device sync state tracking

---

## 113. Offline-First Sync with Conflict Resolution

**What it is:** Client works fully offline (local state is primary); sync is eventual — including handling conflicts when two clients diverge from the same base version.

### Key Principles

1. Client never blocks on network for reads (reads local state, syncs in background)
2. Client queues writes locally during offline period
3. On reconnect: flush queued writes → detect conflicts → resolve per strategy
4. Version tracking: every write carries `base_version` (what it was derived from); conflict = two writes share same `base_version`

### Conflict Resolution Strategies

| Strategy | How | Pro | Con |
|----------|-----|-----|-----|
| Last Write Wins | Server timestamp decides | Simple | Silently loses data |
| **Conflict Copy** | Keep both, create named duplicate | Zero data loss | User sees duplicates |
| CRDT merge | Data structure auto-merges | Invisible to user | Only works for text/counters |
| User-prompted | Ask user to choose | User control | Friction, not scalable |

**Dropbox:** Conflict Copy (works for any file type, including binary).  
**Google Docs:** CRDT / OT (text only, invisible to user).

### Trade-off
- Pro: excellent UX — app works offline seamlessly
- Con: conflict resolution adds complexity; user education needed ("what is a conflict copy?")

### Appeared In
- Dropbox (2026-07-03) — offline device support

---

## 114. LAN Sync

**What it is:** Devices on the same local network transfer file chunks directly to each other, bypassing cloud storage, for faster sync and reduced egress cost.

### Why

```
500MB file: B downloads from S3 via internet (10-50Mbps) = 80-400s
            B downloads from A on LAN (100-1000Mbps)    = 4-40s

Egress saved: 500MB × $0.09/GB ≈ $0.045 per transfer
At scale (10M DAU with large files): millions of dollars/year
```

### Implementation

```
Discovery: mDNS (Bonjour) multicast
  Each client announces: { device_id, chunk_hashes[], port }

Transfer:
  Device B needs chunk abc123 → checks LAN peers → found on Device A
  HTTP GET 192.168.1.5:54321/chunks/abc123
  B verifies: SHA-256(received_bytes) == abc123 → accept or discard

Fallback: No LAN peer → fetch from S3 (transparent to user)
```

### Security
- SHA-256 verification protects against corrupt or malicious LAN peers
- mDNS is LAN-local only (not internet-routable)
- Enterprise firewalls may block mDNS → fallback to S3 automatically

### Trade-off
- Pro: 5-20× faster local sync, saves CDN cost
- Con: P2P complexity, mDNS firewall issues in enterprise, not useful on mobile (changes networks constantly)

### Appeared In
- Dropbox (2026-07-03) — desktop client optimization

---

## 115. Elo Rating System (Peer-Sourced Ranking)

**What it is:** A self-calibrating score updated based on peer ratings. Being rated highly by high-Elo peers raises your score more than being rated by average peers.

### Formula
```
Δ = K × (actual - expected)
expected = 1 / (1 + 10^((rater_elo - rated_elo) / 400))
K = 32 (adjustment factor; higher K = more volatile)

New user: elo = 1400 (average start)
```

**Example:**
- High-Elo user (2000) swipes right on avg user (1400): avg user gains ~28 points (big boost — unexpected approval)
- Avg-Elo user (1400) swipes right on avg user (1400): avg user gains ~16 points (expected)

### When to Use
- Ranking items based on peer preference (profiles, content, players)
- When you want self-calibrating, peer-sourced quality signals
- When absolute scores matter less than relative ranking

### When NOT to Use
- When explicit criteria exist (rule-based ranking)
- When you need full explainability (Elo is opaque to users)
- When cold-start problem is critical (new users disadvantaged)

### Trade-offs

| Aspect | Trade-off |
|--------|-----------|
| Self-calibrating | No manual tuning needed, but can create stratification loops |
| Peer-sourced | Reflects real user preference, but biased by who gets exposure |
| Cold start | New users start at default — shown less → fewer signals → harder to break out |
| Gaming | High-Elo users could game by only swiping high-Elo → Fix: limit impact of very high/low elo raters |

**Cold start fix:** New user boost (7-day ×1.5 deck injection), force exposure to established users early.

**Echo chamber fix:** Inject 5-15% random candidates outside top Elo range in deck.

### Appeared In
- Tinder (profile ranking, deck generation) — 2026-07-04

---

## 116. Bloom Filter for Seen-State Tracking

**What it is:** A probabilistic data structure that answers "have I seen this element?" in O(1) with configurable false positive rate and no false negatives.

### How It Works
```
Bit array of size m, initialized to 0.
k hash functions.

On INSERT(x):
  For i in 0..k: bit_array[hash_i(x) % m] = 1

On QUERY(x):
  For i in 0..k: if bit_array[hash_i(x) % m] == 0 → return NOT_SEEN (certain)
  All bits set → return PROBABLY_SEEN (false positive possible)
```

**False positive rate:** ε ≈ (1 - e^(-kn/m))^k where n = elements inserted.

**Parameters for 0.1% error, 100K elements:**
- m ≈ 1.44 × n × log2(1/ε) ≈ 1.44 × 100K × log2(1000) ≈ 1.44M bits = ~180KB
- k = ln(2) × m/n ≈ 10 hash functions

### When to Use
- "Have I seen this URL?" (web crawlers)
- "Has this user swiped on this profile?" (Tinder)
- "Is this username taken?" (pre-check before DB query)
- "Is this cache key known to be missing?" (negative cache)

### When NOT to Use
- Need exact membership (financial deduplication)
- Need to DELETE elements (use Counting Bloom Filter instead)
- Very low cardinality (HashSet is simpler and exact)

### Trade-offs

| Approach | Space | Lookup | False Positive | Delete | Use When |
|----------|-------|--------|---------------|--------|---------|
| **Bloom Filter** | O(m) bits | O(k) ≈ O(1) | Yes (tunable) | No | Space-critical, tolerable FP |
| **HashSet** | O(n) | O(1) avg | No | Yes | Exact, low-to-mid cardinality |
| **Counting BF** | O(m × 4 bits) | O(k) | Yes | Yes | Need deletes + space efficiency |
| **Cuckoo Filter** | Similar to BF | O(1) | Yes | Yes | Modern alternative, better FP |

**Reconstruction:** Bloom filters only grow (no deletes). Rebuild periodically from source of truth (Cassandra, DB) to reset accumulated drift.

### Appeared In
- Tinder (already-swiped profile filter) — 2026-07-04
- Web Crawler (URL deduplication)

---

## 117. Thumbnail-First Loading (Progressive Photo Pipeline)

**What it is:** Serve smallest usable image first (for immediate display), load full resolution only on demand. Reduces CDN cost and perceived latency.

### Pattern
```
Upload trigger → generate 3 sizes:
  thumbnail: 400×400px WebP, ~20KB  (deck swipe card)
  medium:    800×800px WebP, ~80KB  (profile view)
  full:      1200×1200px WebP, ~200KB (zoom / download)

Serving:
  Swipe card: thumbnail URL pre-loaded in deck batch response
  Profile expand: medium URL loaded on tap
  Zoom: full URL loaded on pinch-to-zoom
  
Storage: S3 path: /photos/{user_id}/{photo_id}/{size}.webp
CDN: same CloudFront distribution, different path prefix per size
```

### Cost Impact

| Approach | Per-Swipe Data | 1B Swipes/day CDN |
|----------|---------------|-------------------|
| Full photo per swipe | 500KB | 500TB/day → $1.27M/month |
| Thumbnail first | 20KB | 20TB/day → $51K/month |
| **Savings** | **25×** | **96%** |

### Trade-offs

| Decision | Trade-off |
|----------|-----------|
| Thumbnail quality | Lower KB → visible blur on some devices. Find minimum acceptable via A/B test. |
| Pre-fetch depth | Pre-fetch 5 cards → wasted load if user swipes left; pre-fetch 2 → may show blank briefly |
| Format choice | WebP: 25-35% smaller than JPEG at same quality. AVIF: 50% smaller but limited support. |
| Client-side cache | Cache thumbnails in memory/disk → repeat views are free |

### Appeared In
- Tinder (swipe card photo loading) — 2026-07-04
- Instagram, YouTube (thumbnail loading patterns)

---

## 118. Git Content-Addressed DAG

**What it is:** Every git object (blob, tree, commit) is stored at an address = SHA-1(content). Objects form a Directed Acyclic Graph: commits point to trees, trees point to blobs and other trees, commits point to parent commits.

### Object Types
```
blob   = raw file bytes (leaf node)
tree   = directory: list of {mode, name, type, sha} entries
commit = tree_sha + parent_sha(s) + author + message
tag    = points to a commit with metadata
```

### Storage
```
S3 path: {namespace}/{sha[0:2]}/{sha[2:40]}
  sha[0:2] = 256 pseudo-random prefixes → avoids S3 hot-partition throttling
  
Content-addressed = immutable. Same content = same SHA.
Fork of 1000 repos sharing a file = 1 stored blob.
```

### Properties
| Property | Benefit | Cost |
|----------|---------|------|
| Content-addressed | Auto-deduplication across forks | SHA computation on every write |
| Immutable | Safe to cache anywhere indefinitely | Can't fix corruption in-place; must write new object |
| DAG (no cycles) | Easy traversal, reachability analysis | Merge commits have 2+ parents; tools must handle |
| SHA integrity | Detect corruption by recomputing SHA | SHA-1 theoretical collision risk (mitigated in git 2.29+) |

### When to Use This Pattern (Beyond Git)
- Any system needing content deduplication across many users/repos
- Immutable audit log where you can't tamper with history
- Distributed systems needing integrity verification without central authority

### Appeared In
- GitHub (2026-07-05) — core object storage model

---

## 119. Packfile + Delta Compression

**What it is:** Git bundles many objects into a single file (packfile) using delta compression — storing objects as deltas against similar objects rather than full copies.

### How It Works
```
Loose objects: individual files on disk (SHA → file)
  Efficient for writes, inefficient for serving (many small S3 GETs)

Packfile: bundle of objects with delta compression
  1. Find similar objects (same path across commits → similar content)
  2. Choose base object (often latest version)
  3. Store other versions as: base + delta (binary diff)
  4. Result: pack file (.pack) + index (.idx for random access)

Example:
  main.go v1: 10KB stored as-is (base)
  main.go v2: stored as delta (diff) = 200 bytes instead of 10KB
  → 50× compression for incremental changes
```

### Network Transfer (Smart HTTP)
```
Client sends: "have" sha list + "want" sha list
Server computes: missing objects = want - have
Server packs: only missing objects → sends single packfile stream

Benefit: only transmit what client needs. No redundant transfer.
Pre-computed pack: server pre-computes "full clone" pack nightly → CDN-served
  Clone = CDN pack download (fast) + git fetch for delta since pack (small)
```

### Trade-offs

| Approach | Write Speed | Read/Clone Speed | Storage | Use When |
|----------|-------------|------------------|---------|---------|
| Loose objects only | Fast (one write) | Slow (many small reads) | Higher (no delta) | Development mode, small repos |
| Pack all objects | Slow (repack operation) | Fast (single read, delta) | Low | Production serving |
| **Hybrid (loose + periodic repack)** | Fast writes | Good reads | Medium | Standard git: loose for new, periodic GC repack |

### Appeared In
- GitHub (2026-07-05) — efficient clone serving, object deduplication

---

## 120. Fork Copy-on-Write (Shared Object Store)

**What it is:** When a repository is forked, no objects are copied. The fork shares the parent's object store transparently. New objects are written to the fork's namespace only when content actually changes.

### Mechanics
```
Fork creation:
  1. New repo row (fork_parent_id = parent.id)
  2. Copy refs table (same SHAs, zero object copy)
  3. Fork's S3 namespace: empty

Object resolution (on git clone/fetch of fork):
  Lookup: s3://objects/{fork_id}/{sha[0:2]}/{sha[2:]}  → found? serve it
          Not found → s3://objects/{parent_id}/{sha[0:2]}/{sha[2:]} → serve from parent
  
First push to fork (new commit):
  New blobs (changed files): written to fork's namespace
  Unchanged blobs: still served from parent (transparent to client)
```

### Why It Matters at Scale
```
GitHub has ~100M repos, large fraction are forks.
Without CoW: fork of Linux kernel (3GB) = copy 3GB to new namespace = $0.07 per fork + slow
With CoW: fork = new row in DB + ref copy = milliseconds, near-zero cost

Storage savings: if 90% of forks never diverge significantly → 90% objects served from parent
```

### Edge Cases

| Scenario | Handling |
|----------|---------|
| Parent repo deleted | Pre-deletion: copy shared objects to each fork's namespace before deletion (async job) |
| N-deep fork chains | Object resolution: walk parent chain. Limit depth (5). Flatten on access (lazy copy to direct parent) |
| Circular forks | Impossible: fork_parent_id creates a DAG by definition; DB constraint prevents self-reference |
| Fork diverges heavily (most files changed) | Object resolution still correct; minor overhead of checking fork namespace first |

### Trade-offs
- Pro: near-zero cost fork creation, massive storage savings for similar repos
- Con: cross-namespace object reads (slightly slower), complexity in deletion flow

### Appeared In
- GitHub (2026-07-05) — fork creation, object storage optimization

---

## 121. Pre-computed Pack for Clone Offload

**What it is:** For large repositories, pre-compute a "full clone" packfile nightly and serve it via CDN. On-demand git clone = CDN pack download + small delta fetch for new commits.

### Problem
```
10GB monorepo. 1000 devs clone Monday morning.
On-demand pack: server reads all 10GB from S3, computes delta packfile, streams → O(N×clones) compute
Result: git server OOM under load
```

### Solution
```
Nightly pack job:
  git pack-objects --all --stdout > full-{date}.pack
  git index-pack → full-{date}.idx
  Upload to S3: s3://gh-packs/{repo_id}/full-2026-07-05.pack

Clone request:
  1. Client: git clone → server redirects to CDN URL for base pack
  2. Client downloads pack from CDN (no git server involved)
  3. Client: git fetch origin → git server sends delta (commits since pack date)
  4. Delta: typically small (last 24h of commits)

CDN hit rate: pack file changes once/day → very high (>99%) CDN cache hit rate
```

### Cost Impact
```
Without: 10GB × 1000 users × $0.09/GB = $900/Monday morning
With CDN pack:
  S3 → CDN: 10GB × 1 transfer = $0.90
  CDN → users: 10GB × 1000 users at $0.085/GB = $850
  But CDN caches until next nightly rebuild:
  First clone of day: CDN miss → $0.085/GB × 10GB = $0.85
  All subsequent clones: CDN hit → $0.00 (cached at edge)
  S3 transfer: 10GB × 1/day = $0.90/day

Net: ~$1/day vs $900/Monday (if no CDN)
```

### Trade-offs

| Approach | Clone Latency | Server Load | Storage Cost | Freshness |
|----------|--------------|-------------|--------------|-----------|
| On-demand pack | Variable (depends on server) | O(N) | None | Always current |
| **Pre-computed CDN pack** | Fast + consistent (CDN edge) | O(1) for server | One full pack/day | Up to 24h stale (delta covers gap) |
| Shallow clone | Very fast | Low | None | Always current (latest only) |

### Appeared In
- GitHub (2026-07-05) — clone optimization for large monorepos

---

## 122. Merge Strategy Trade-offs (Merge / Squash / Rebase)

**What it is:** Three approaches to integrating a feature branch into a main branch, each with different history characteristics.

### Three Strategies
```
1. Merge Commit:
   main:    A─B─C───────M
                       ╱
   feature: D─E─F─────╯
   M has two parents: C and F.
   History: fully preserved including all feature commits.

2. Squash Merge:
   main:    A─B─C─S
   S = single commit representing sum of D+E+F changes.
   History: feature branch history discarded.

3. Rebase Merge:
   main:    A─B─C─D'─E'─F'
   D',E',F' = replayed on top of C. New SHAs. Linear history.
   History: linear, as if feature was developed on latest main.
```

### When to Use Each

| Strategy | History | Git Bisect | Attribution | Revert | Use When |
|----------|---------|-----------|-------------|--------|---------|
| **Merge** | Branchy (DAG) | Works | Full (each commit) | One revert undoes all | OSS (preserve contributor commits), complex features |
| **Squash** | Linear | Easy | Lost (squashed to 1) | One revert undoes feature | Fast teams, small features, cleanup messy branch history |
| **Rebase** | Linear | Easy | Full (all commits, new SHA) | Must revert each commit | Linear history purists, monorepo teams |

### Key trade-off: Squash vs Rebase
```
Squash: loses intermediate history. Can't see "why did step 3 happen?"
Rebase: keeps all steps. But rewrites SHAs → invalidates previous review on those commits.

If PR review happened at E (sha=abc) and rebase produces E' (sha=xyz):
  Review comments reference sha=abc → now "outdated" in GitHub's view
  This is why GitHub shows "outdated" badge on rebased PR comments
```

### Appeared In
- GitHub (2026-07-05) — PR merge strategies

---

## 123. Soft Delete + Delayed Hard Delete (Recovery Window)

**What it is:** Mark data as deleted (soft delete) but retain it for a recovery window before permanent deletion. Protects against accidental or malicious deletions.

### Pattern
```
Soft delete:
  UPDATE repos SET deleted_at = now() WHERE id = $1
  Queries: WHERE deleted_at IS NULL (exclude soft-deleted)
  Deleted data: invisible to normal queries, but recoverable

Hard delete (after 30 days):
  SELECT id FROM repos WHERE deleted_at < now() - interval '30 days'
  For each: run deletion pipeline (objects, webhooks, DB rows)
  Delete S3 objects in batches (rate-limited to avoid S3 throttling)
```

### Recovery Window Design
```
30-day window for repos (GitHub standard)
  UI: Settings → Danger Zone → "Restore this repository" (visible for 30 days)
  
Shorter windows for other entities:
  Branch deletion: 90-day recovery via git reflog (local) or GitHub's branch restore
  Force push: 30-day ref history (GitHub stores all ref updates)
  PR close: reopen anytime (no deletion, just status change)
```

### Cascade Considerations
```
Repo deletion cascade:
  PRs, webhooks, deploy keys: soft-deleted with repo
  Objects (S3): NOT deleted immediately — GC after window
  Forks: NOT auto-deleted (they're independent repos)
  
What triggers hard delete:
  Scheduled job: daily scan for expired soft-deletes
  Rate-limited: max 1000 S3 deletes/sec (avoid API throttling)
  Order: webhooks → DB rows → S3 objects (most expensive last)
```

### Trade-offs

| Approach | Recovery | Storage Cost | Complexity |
|----------|---------|--------------|------------|
| Immediate hard delete | None | Lowest | Low |
| **Soft delete + window** | Yes (30 days) | Moderate (retain for window) | Medium |
| Soft delete forever | Full history | High | Low to query |
| Versioned S3 (object-level) | Per-file recovery | High (all versions stored) | Low (S3 handles it) |

### Rule
> Any destructive user action: soft delete first. Hard delete via background job after recovery window.
> Storage cost of 30-day retention is almost always worth avoiding one accidental deletion incident.

### Appeared In
- GitHub (2026-07-05) — repository deletion safety
- Dropbox (2026-07-03) — file deletion with version history

---

## 108. X3DH — Async Key Exchange for E2E Encryption

**What it is:** Extended Triple Diffie-Hellman. Lets two parties establish a shared secret without being online simultaneously. Foundation of the Signal Protocol (WhatsApp, Signal).

### How It Works
```
Bob pre-uploads to Key Distribution Center:
  identity_key:     Ed25519 public key (permanent)
  signed_prekey:    X25519, signed by identity_key (rotated weekly)
  one_time_prekeys: batch of X25519 keys (used once, popped on use)

Alice (online, Bob offline) fetches Bob's keys:
  DH1 = DH(Alice.identity_key,  Bob.signed_prekey)
  DH2 = DH(Alice.ephemeral_key, Bob.identity_key)
  DH3 = DH(Alice.ephemeral_key, Bob.signed_prekey)
  DH4 = DH(Alice.ephemeral_key, Bob.one_time_prekey)  [if available]
  shared_secret = KDF(DH1 || DH2 || DH3 || DH4)
```

### Key Properties
- **Async**: Bob doesn't need to be online
- **Mutual auth**: DH1+DH2 bind both parties' long-term identity keys
- **Forward secrecy per session**: one-time prekey (DH4) ensures unique starting secret each conversation

### Fallback
One-time prekeys exhausted → omit DH4 → slightly weaker but still secure.
Server alerts device → app uploads fresh batch silently.

### Trade-offs
| | X3DH | Simpler symmetric | Online-only ECDH |
|--|--|--|--|
| Async key agreement | ✓ | ✗ | ✗ |
| Forward secrecy per session | ✓ | ✗ | Depends |
| Complexity | High | Low | Medium |

### Appeared In
- WhatsApp (2026-07-05)

---

## 109. Double Ratchet — Per-Message Forward Secrecy

**What it is:** Key derivation protocol advancing the session key with every message, providing forward secrecy and break-in recovery. Used after X3DH.

### Two Ratchets
```
Symmetric (chain key):
  chain_key_n+1 = KDF(chain_key_n)
  message_key_n = KDF(chain_key_n, 0x01)  → encrypt msg n, then DELETE

DH ratchet (root key):
  Each party embeds new ephemeral DH key in their messages
  On receiving new DH key: new_root = KDF(root, DH(own_key, their_key))
  → Resets chain → self-heals even after current state compromise
```

### Properties
- **Forward secrecy**: message_key deleted after use → past messages irretrievable
- **Break-in recovery**: DH ratchet advances → future messages safe post-compromise
- **Out-of-order**: store up to 1000 skipped message keys for late-arriving packets

### Real-World
Signal, WhatsApp, Wire, Google RCS Messages

### Appeared In
- WhatsApp (2026-07-05)

---

## 110. SenderKey — Efficient Group E2E Encryption

**What it is:** A shared symmetric key distributed once to all group members, enabling O(1) encryption per group message regardless of group size N.

### Problem Without SenderKey
```
N-member group → N separate encryptions per message → O(N) CPU + bandwidth
```

### Solution
```
Setup (once per new member):
  Alice generates SenderKey (AES-256 + chain_key)
  Distributes to each member M via 1:1 E2E session

Per message:
  encrypt(plaintext, SenderKey) → 1 ciphertext
  Server fans out same ciphertext to all N members
  Cost: O(1) encrypt, O(N) delivery of identical blob
```

### Member Removal → Key Rotation
```
Removed member has old SenderKey → rotate for forward secrecy
Remaining N-1 members receive new SenderKey
WhatsApp throttles: max 1 rotation/min per group (expensive op)
```

### Trade-offs
| | SenderKey | Pairwise Sessions |
|--|--|--|
| Encrypt per msg | O(1) | O(N) |
| Member removal | Must rotate all keys | No rotation needed |
| Best for | Groups >10 | 1:1 only |

### Appeared In
- WhatsApp group messaging (2026-07-05)

---

## 111. Offline Message Queue (Device-Targeted, TTL-Based)

**What it is:** Server-side durable queue for encrypted messages awaiting offline recipients, keyed per device with automatic TTL expiry.

### Design
```sql
CREATE TABLE offline_messages (
  user_id     BIGINT,
  device_id   UUID,
  created_at  TIMESTAMP,
  message_id  UUID,
  payload     BLOB,   -- server cannot read (E2E ciphertext)
  PRIMARY KEY ((user_id, device_id), created_at, message_id)
) WITH default_time_to_live = 2592000;  -- 30 days
```

### Flow
```
Offline:   write offline_messages (TTL 30d) + fire FCM/APNS push
Online:    WebSocket handshake sends last_received_id
           Server delivers all newer messages → client ACKs → delete rows
Multi-dev: separate queue per device; deleted per-device on ACK
```

### Trade-offs
| Choice | Upside | Downside |
|--------|--------|----------|
| 30-day TTL | Covers most offline scenarios; auto-cleanup | Messages lost if offline >30 days |
| Per-device queue | Multi-device correct | More storage than per-user |
| FCM push with preview | Better UX | Leaks content in lock screen notification |
| FCM no-preview | E2E-pure | Worse UX ("You have a message") |

### Appeared In
- WhatsApp (2026-07-05)

---

## 112. Multi-Device Session Management (Linked Devices)

**What it is:** Scheme allowing multiple physical devices under one identity to each receive messages independently without sharing a private key.

### WhatsApp's Approach
```
Each device has its own keypair.
Linking: phone signs + publishes new device's public key to server.

Message to Bob (phone + laptop):
  Alice fetches both Bob's device public keys
  Encrypts message twice (once per device key)
  Server routes each ciphertext to correct device

History: NOT synced retroactively (forward secrecy — old ratchet state gone)
         From link date forward: all messages delivered to all devices
```

### Trade-offs
| Approach | Security | Complexity | UX |
|----------|----------|------------|---|
| Multi-copy per device | Full E2E per device | O(devices) ciphertexts | Seamless |
| Shared device key | Weaker (1 compromise = all) | Simple | Seamless |
| Phone-as-proxy (old WhatsApp Web) | Good | Simple | Breaks when phone offline |

### Appeared In
- WhatsApp (2026-07-05)

---

## 113. Delivery Receipt Pattern (Sent / Delivered / Read)

**What it is:** Three-state acknowledgment propagated back to sender as visual confirmation of message progress.

### States & Implementation
```
✓  gray  = server received (Chat Service ACKs write)
✓✓ gray  = recipient device received (POST /receipt {message_id, 'delivered'})
✓✓ blue  = recipient opened chat (POST /receipt {chat_id, last_read_message_id, 'read'})

Server: INSERT message_status(message_id, recipient_id, status)
Sender: notified via their WebSocket connection
```

### Idempotency (Out-of-Order Receipts)
```python
# Monotonic transitions only — prevent status regression
status_rank = {'sent': 0, 'delivered': 1, 'read': 2}
UPDATE message_status SET status = new_status
WHERE message_id = ? AND status_rank[status] < status_rank[new_status]
```

### Group Scale
```
1000-member group → up to 1000 delivery receipts per message
Optimization: batch receipts 500ms → bulk insert
Pre-aggregate delivered_count / read_count on messages row for fast sender query
```

### Privacy Trade-off
> Disabling read receipts is reciprocal: you lose ability to see others' read receipts too.
> Prevents surveillance while maintaining fairness.

### Appeared In
- WhatsApp (2026-07-05)
- Chat System (2026-05-11)
