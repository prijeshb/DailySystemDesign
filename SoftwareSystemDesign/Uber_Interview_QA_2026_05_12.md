# Ride-Sharing System — Interview Q&A
**Date:** 2026-05-12 | **System:** Uber/Lyft-style Ride-Sharing

> These Q&As mirror real interview patterns — how interviewers open, probe, and follow up. Study the *shape* of answers as much as the content.

---

## How Interviewers Structure This Problem

**Opening (5 min):** Scope clarification — what features, what scale?
**Design (25 min):** High-level → components → one deep-dive the interviewer chooses
**Probing (10 min):** "What if X fails?", "How would you scale Y?", "Why not Z?"
**Wrap-up (5 min):** Trade-offs, what you'd do differently, what you'd build next

The interviewer *will* interrupt. That's normal. They're testing how you handle ambiguity and redirects.

---

## Part 1: Scoping Questions (Interviewer Opens With These)

---

### Q1: "Before you design anything — what questions would you ask?"

**Strong answer pattern:**

> "I want to scope this correctly. A few things I'd clarify:
>
> On features — are we building the full platform (rider + driver + payment) or just the matching/dispatch piece? Should I include ride scheduling, carpooling, surge pricing?
>
> On scale — are we building for a single city, a country, or globally? Uber at peak has ~5 million concurrent drivers. Is that our target?
>
> On consistency requirements — for example, can the same driver be offered to two riders simultaneously? I'm assuming no — that's a strong consistency requirement for matching.
>
> On latency — what's the target for 'time to match'? For Uber it's under 2 seconds.
>
> I'll assume: global scale, ~5M active drivers, 3M concurrent rides peak, <2s match time, strong consistency on driver assignment."

**Why this works:** Shows you know what matters (scale drives architecture choices). Interviewer will narrow scope — that's fine, just confirm: "OK so let's focus on the core matching flow, real-time tracking, and basic payment. Got it."

---

### Q2: "What are the core entities in this system?"

**Expected answer:**
> "Four main entities: **User/Rider** (account, payment methods), **Driver** (vehicle info, status, real-time location), **Trip** (the core transactional record — pickup, dropoff, status state machine, fare), and **Location** (ephemeral high-frequency updates, separate from the trip record).
>
> The key insight is that Location is not really a traditional entity — it's a time-series stream. Storing it in the same database as trips would be a mistake because location writes are 1000x higher frequency."

**Follow-up the interviewer will ask:** *"Why separate location from trips?"*
> "Location is written every 4 seconds per driver — 5M drivers = 1.25M writes/second. Trip records are written maybe once per minute per ride. They have completely different throughput, retention, and consistency needs. Location is ephemeral and can be lost without catastrophe; trips must be durable. Mixing them in one DB would mean your trip DB has to handle 1M+ writes/sec — that's unnecessary and expensive."

---

## Part 2: Architecture Deep-Dive Questions

---

### Q3: "Walk me through what happens when a rider taps 'Request Ride'"

**This is the core flow question. Interviewers want to see you drive the end-to-end.**

> "Here's the flow:
>
> 1. Rider's app sends a POST to the Trip Service via the API Gateway: pickup location, dropoff location, vehicle type.
>
> 2. Trip Service creates a trip record in PostgreSQL with status REQUESTED, then publishes a TripRequested event to Kafka.
>
> 3. The Dispatch Service, which consumes this Kafka topic, queries the geospatial driver index — I'd use Redis Geo or H3 hexagonal cells — to find available drivers within 5km.
>
> 4. Dispatch ranks candidates by proximity, driver rating, and acceptance rate, then sends a push offer to the top driver.
>
> 5. If the driver accepts within 15 seconds, Dispatch updates the trip to MATCHED using an optimistic lock in the database. If they decline or timeout, we try the next driver.
>
> 6. The Trip Service pushes a notification to the rider: 'Driver found — arriving in 3 minutes.'
>
> 7. Driver's location now starts streaming to the rider via WebSocket."

**Interviewer follow-up:** *"Why use Kafka here instead of calling Dispatch directly?"*
> "Two reasons: reliability and decoupling. If I call Dispatch synchronously and it's slow or down, the rider's request fails. With Kafka, the trip is persisted durably before any matching begins. Dispatch processes it asynchronously — the rider gets 'searching for driver' UX while matching happens in the background. Also, Dispatch can crash and restart without losing the trip — it just picks up from the Kafka offset."

---

### Q4: "How do you store and query driver locations at scale?"

**This is the technical deep-dive most interviewers choose.**

> "This is the hardest scaling problem in this system. At 5M drivers updating every 4 seconds, we're talking 1.25 million writes per second.
>
> I'd use **Redis with Geo commands**. Redis GEOADD takes a longitude, latitude, and key, and stores it in a sorted set using Geohash encoding. GEORADIUS lets us query all drivers within, say, 5km of a point in O(N+log M) time. Writes are in-memory, extremely fast, and Redis can handle 100K+ ops/second per node.
>
> For production at Uber's scale, they actually use **H3 hexagonal indexing** — the earth is divided into hexagonal cells. A driver's heartbeat computes their H3 cell ID and stores the driver ID in a Redis Set for that cell. To find nearby drivers, you query the rider's cell plus its 6 immediate neighbors. This partitions the data naturally by geography, which means you can run a separate Redis cluster per metro area.
>
> One critical detail: every driver entry has a TTL of 30 seconds. If we don't hear from a driver, they vanish from the available pool automatically. This prevents matching a driver who went offline."

**Interviewer follow-up:** *"What if the Redis cluster goes down?"*
> "Then matching breaks — it's a critical dependency. So I'd have: 1) Redis Sentinel for automatic primary failover within 30 seconds, 2) a PostGIS table in PostgreSQL as a warm fallback — it gets async updates every 30 seconds and supports spatial queries, just 50ms instead of 1ms, and 3) on Redis restart, replay the last 60 seconds of the Kafka location stream to rebuild the index rapidly."

---

### Q5: "How do you prevent two drivers from being assigned the same trip, or one driver assigned two trips?"

**Tests your distributed systems knowledge.**

> "Two separate problems, both solvable.
>
> **One driver, two trips:** Before sending an offer, Dispatch acquires a distributed lock in Redis: `SET driver_lock:{driver_id} {trip_id} NX EX 30`. `NX` means only set if not exists — atomic. If another Dispatch instance already locked this driver, we skip them. The lock expires after 30 seconds automatically, so if the offering Dispatch crashes, the lock releases.
>
> **Two drivers assigned the same trip:** Even with the lock, I need a safety net. The final assignment uses an optimistic lock in PostgreSQL: `UPDATE trips SET driver_id = X, status = MATCHED WHERE trip_id = Y AND status = REQUESTED`. If two processes race, only the first UPDATE sees `status = REQUESTED` — the second finds `status = MATCHED` and does nothing. Rows affected = 0 tells us we lost the race.
>
> So Redis lock prevents most races at the application level; the database constraint is the authoritative final guard."

---

### Q6: "How do you stream the driver's location to the rider in real time?"

> "I'd use **WebSocket connections** — one persistent connection from the rider app to a WebSocket server. Polling (HTTP GET every 4s) would work but creates unnecessary overhead and adds latency.
>
> The flow: Driver sends a heartbeat to the Location Service every 4 seconds. If the driver is on an active trip, Location Service publishes to a Kafka topic partitioned by driver_id. A stream consumer on that partition looks up the active trip for this driver, then finds which WebSocket server holds the rider's connection (stored in Redis as `trip:{trip_id}:ws_server`), and publishes via Redis Pub/Sub. The WebSocket server forwards to the rider's open connection.
>
> The tricky part is when a WebSocket server crashes. The rider's app has reconnect logic — on disconnect, it retries with backoff. On reconnect, any WS server can serve them because all connection state is in Redis, not in-memory. The new server registers itself and starts receiving the location stream."

---

### Q7: "How does fare calculation work?"

> "Fare has three components: base fare, per-minute charge, and per-kilometer charge, plus surge multiplier and applicable fees.
>
> For distance, I don't use straight-line math — I use the actual route traveled, computed from the GPS breadcrumbs stored during the ride. These are written to Cassandra (or InfluxDB) every 4 seconds — a time-series store designed for high-write, time-ordered data. At trip end, I sum the distances between consecutive points.
>
> Why breadcrumbs instead of ETA-based estimate? Because the driver might take a detour, there might be traffic — the actual route is fairer to both parties. Uber also uses this as evidence in dispute resolution.
>
> If the breadcrumb store has gaps (driver lost GPS), we fall back to Google Maps Distance Matrix API between the known start and end coordinates."

**Interviewer follow-up:** *"What if the fare service is down when the trip ends?"*
> "Fare calculation is triggered by a TripCompleted event in Kafka. If the Fare Service is down, the event waits in Kafka. When the service recovers, it processes the backlog. The rider gets their receipt a few minutes late — not ideal but no data is lost. The service must be idempotent: check if fare already calculated before computing."

---

## Part 3: Failure & Edge Case Questions

---

### Q8: "What happens if the driver's app crashes mid-ride?"

> "The trip stays in IN_PROGRESS state. A few safeguards kick in:
>
> First, we have GPS breadcrumbs in Cassandra through the crash point, so we can compute how far the driver traveled.
>
> Second, we have a trip watchdog: a scheduled job checks for trips in IN_PROGRESS longer than `estimated_duration × 3`. If triggered, it auto-completes the trip using breadcrumb data.
>
> Third, the rider can also trigger an 'End Trip' from their app if the driver is unresponsive for 5+ minutes.
>
> When the driver's app restarts, it queries the current trip status from the server — if IN_PROGRESS, it resumes the tracking UI. The driver can then tap End Trip normally.
>
> Importantly, we never lose the fare because the breadcrumbs are server-side — the driver's app crash doesn't affect them."

---

### Q9: "What happens if there are no available drivers nearby?"

> "The system should fail fast and communicate clearly rather than making the rider wait indefinitely.
>
> After querying a 5km radius and finding nothing, the Dispatch Service tries a 10km radius. If still nothing, it marks the trip as NO_DRIVERS_AVAILABLE and notifies the rider immediately.
>
> The trip stays in REQUESTED state for up to 5 minutes — if a driver comes online in that window, they're immediately matched. After 5 minutes, the trip auto-cancels.
>
> On the UX side: we can show 'No drivers in your area right now — try again in a few minutes' with an estimated time until a driver will be nearby, derived from historical heat maps."

---

### Q10: "How would you handle surge pricing?"

> "Surge is a separate concern from dispatch. A Surge Service continuously monitors demand/supply ratio per geographic cell (using H3 cells again). When `pending_requests / available_drivers` in a cell exceeds a threshold, it publishes a surge multiplier for that cell.
>
> The Trip Service reads the surge multiplier at request time and includes it in the fare estimate shown to the rider. The multiplier is locked in when the rider confirms — so if surge changes during the ride, the rider's fare is based on what was shown at booking.
>
> The surge multiplier is stored in Redis (fast reads, TTL-based refresh) and updated every 60 seconds by the Surge Service. The Surge Service itself reads from the same driver availability index and Kafka trip-events topic, so it's a read-only consumer — no risk of affecting the critical write path."

---

## Part 4: Scale & Trade-off Questions

---

### Q11: "How would you scale the WebSocket layer to handle millions of concurrent connections?"

> "Each WebSocket server can hold ~50,000 concurrent connections on a single machine. For 3 million active rides, I need 60+ WS servers.
>
> The key challenge is routing: when a location update comes in for driver D on trip T, I need to reach the specific WS server holding the rider's connection. I do this via Redis: on connection, `SET trip:{trip_id}:ws_server {server_id}`. Consumers look up this key before publishing.
>
> For failover: if a WS server crashes, all its connections reconnect. Within a few seconds, the new connections land on healthy servers, they register in Redis, and the stream resumes. There's a brief gap in location updates — acceptable.
>
> For geographic scaling: I'd run separate WS clusters per region (US, EU, APAC). A rider in London connects to EU WS servers. Location stream consumers also run regionally — there's no reason to route a London driver's GPS update to a US server."

---

### Q12: "SQL vs NoSQL — how did you decide what to use where?"

> "I made this decision per access pattern:
>
> **PostgreSQL for Trips, Users, Drivers:** These need ACID transactions. 'Driver assigned to trip' must be strongly consistent — two rows atomically updated. Trip state transitions must be atomic. PostgreSQL with row-level locks and CAS updates gives me that. Write throughput is manageable — trips are hundreds per second, not millions.
>
> **Redis for location index and locks:** Sub-millisecond reads, in-memory, perfect for ephemeral data. Location data is inherently stale — 4 second refresh rate means I don't need durability. Redis losing its data on crash is recoverable from Kafka replay.
>
> **Cassandra for breadcrumbs:** Pure append-only time-series writes. Millions of GPS points per minute. Cassandra's write path is extremely efficient for this pattern. Queries are always `SELECT WHERE trip_id = X ORDER BY timestamp` — a perfect fit for Cassandra's primary key design.
>
> The anti-pattern would be trying to shove location updates into PostgreSQL or breadcrumbs into a key-value store without time-range query support."

---

### Q13: "The interviewer asks: 'This is a lot of services. Isn't this over-engineering?'"

> "Fair challenge. In a real company you'd start with a monolith — one service handling trips, dispatch, and location. I'd break them apart only when there's a specific scaling or operational reason.
>
> The key services I'd extract first, and why:
>
> **Location Service** — because the write throughput (1.25M/s) is orders of magnitude higher than everything else. Mixing it with Trip Service would require scaling your entire backend for location writes.
>
> **Dispatch Service** — because the matching algorithm is complex, changes frequently (ML improvements), and has very different scaling characteristics. It's CPU-intensive and you want to scale it independently.
>
> Everything else (payment, notifications, ratings) could reasonably live in a well-structured monolith in the early days. I separated them here to illustrate failure isolation — payment can go down without affecting matching."

---

## Part 5: Design Alternatives (Be Ready to Defend Your Choices)

---

### Q14: "Why WebSockets for location tracking instead of polling or Server-Sent Events?"

| Approach | Latency | Server Cost | Complexity | Best For |
|----------|---------|-------------|------------|----------|
| Short polling (every 4s) | 0–4s | Low (stateless) | Low | Low-scale systems |
| Long polling | 0–200ms | Medium | Medium | Medium scale |
| Server-Sent Events (SSE) | ~50ms | Medium | Low | Server→client only |
| WebSocket | ~50ms | High (stateful) | High | Bi-directional, real-time |

> "For a ride-sharing app, the connection is bi-directional (driver sends, rider receives) and must be low latency. SSE is unidirectional so driver can't use it for sending location. Short polling adds unnecessary latency and server load. WebSocket is the right choice here, despite connection management complexity, because it's the only one that gives us real-time bi-directional data efficiently at scale."

---

### Q15: "Could you use a CDN or edge network to improve performance?"

> "Interesting angle. Static assets (app images, maps tiles) — absolutely use a CDN.
>
> For dynamic API requests, a CDN doesn't help much — trip requests and location updates are unique to each user and highly personalized.
>
> Where edge computing *could* help: running the H3 geo index at edge PoPs. If a driver in London is querying for nearby riders, routing through a US origin server adds 100ms+. Running a Redis Geo replica at the London PoP means sub-10ms queries. This is what Uber does with their city-sharded location clusters — it's effectively edge deployment without calling it a CDN.
>
> WebSocket connections also benefit from edge termination — Cloudflare or AWS Global Accelerator terminates the WS connection at the nearest PoP and proxies over an optimized backbone to the origin, reducing RTT for the driver's heartbeats."

---

## Part 6: Closing Questions Interviewers Ask

---

### Q16: "What's the hardest part of this system to get right?"

> "The geospatial matching at scale. The core insight is that driver location is a stream, not a database — it needs to be treated as ephemeral, ultra-high-throughput time-series data, not transactional records. Every architectural decision downstream — Redis Geo vs PostgreSQL, Kafka buffering, H3 indexing — flows from getting that foundational choice right.
>
> The second hardest is the distributed locking for driver assignment. Getting this wrong means double-booking, which is a terrible user experience and a trust-destroying failure. You need *both* the application-level Redis lock and the database-level optimistic lock as defense-in-depth."

---

### Q17: "What would you build next if you had more time?"

> "Three things in priority order:
>
> 1. **ETA prediction service** — currently I'm using Google Maps. A first-party ML model trained on historical trip data from our own network would be more accurate and cheaper. Uber does this — their ETA service ingests historical GPS breadcrumbs to predict travel time better than any third-party map.
>
> 2. **Driver supply forecasting** — a model that predicts where demand will be in 30 minutes, and incentivizes drivers to pre-position there. This reduces wait times and surge events.
>
> 3. **Fraud detection** — GPS spoofing (fake location), trip fraud (completing trips without driving), and payment fraud are significant problems. A streaming anomaly detection layer consuming the location stream would catch these in real time."

---

## Quick-Reference: Numbers to Know

| Metric | Value |
|--------|-------|
| Active drivers (global peak) | 5 million |
| Driver heartbeat frequency | Every 4 seconds |
| Location writes/second | ~1.25 million |
| Active rides (peak) | 3 million |
| Target match time | < 2 seconds |
| Driver offer timeout | 15 seconds |
| Search radius (initial) | 5 km |
| Redis Geo query time | < 1 ms |
| PostGIS fallback query time | ~50 ms |
| WebSocket connections per server | ~50,000 |
| WS servers needed (3M rides) | 60+ |
| Trip DB shard strategy | By city/region |
| Breadcrumb retention | 30 days (Cassandra) |
| Kafka replication factor | 3 (across 3 AZs) |

---

*Files in this series:*
- `Uber_System_Design_2026_05_12.md` — full architecture and design
- `Uber_Failure_Analysis_2026_05_12.md` — failure modes and mitigations
- `Uber_Interview_QA_2026_05_12.md` ← this file
