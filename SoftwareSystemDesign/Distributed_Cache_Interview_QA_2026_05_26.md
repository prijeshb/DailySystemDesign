# Distributed Cache — Interview Q&A
**Date:** 2026-05-26

---

## How Interviewers Open This Topic

- *"Design a distributed cache like Redis/Memcached."*
- *"How would you add caching to our existing system?"*
- *"Our DB is getting hammered. What do you do?"*
- *"Design a key-value store."* (overlaps with cache)

---

## Round 1 — Fundamentals

**Q: What is a cache and why do we need it?**
A: A cache stores frequently accessed data in fast memory (RAM) to avoid repeated expensive operations (DB queries, API calls). Reduces latency from ~5ms to <1ms and offloads DB.

**Q: What are the main caching strategies?**
A: Cache-aside (app manages cache), Read-through (cache fetches on miss), Write-through (write to cache + DB together), Write-back (write to cache, async to DB), Write-around (write to DB only, invalidate cache).

**Q: What is cache eviction? Name the policies.**
A: When cache is full, eviction removes keys to make room. Policies: LRU (least recently used — most common), LFU (least frequently used), TTL-based (time expiry), Random, FIFO.

**Q: How do you implement LRU in O(1)?**
A: HashMap for O(1) key lookup + Doubly-Linked List for O(1) move-to-front and tail removal. Node stores key+value; map key → node pointer.

---

## Round 2 — Distributed Concepts

**Q: How do you shard a cache across multiple nodes?**
A: Use consistent hashing. Hash key to a ring [0, 2³²). Each node owns a segment. Keys route to nearest clockwise node. Adding/removing a node only migrates ~1/N keys (vs. mod-N which remaps everything).

**Q: What are virtual nodes and why use them?**
A: Each physical node is assigned K positions (virtual nodes) on the ring. This balances load more evenly — without vnodes, uneven ring arcs cause some nodes to get 5× more keys.

**Q: What is the thundering herd problem? How do you fix it?**
A: When a popular key expires, many concurrent requests all MISS and hit DB simultaneously. Fix: (1) TTL jitter to spread expiries, (2) mutex/lock so only one caller fetches DB, (3) stale-while-revalidate to serve old value during refresh, (4) probabilistic early re-fetch (XFetch).

**Q: What is cache stampede?**
A: Same as thundering herd — a surge of DB requests when a hot key expires at once.

---

## Round 3 — Consistency & Correctness

**Q: How do you handle cache invalidation?**
A: Options: (1) TTL expiry — simple but stale until TTL; (2) Event-driven invalidation via CDC (Debezium) — DEL key when DB row changes; (3) Write-through — always update cache on write; (4) Versioned values — store version number, compare on read.

*"Cache invalidation is one of the two hard problems in CS"* — interviewers love this quote; acknowledge it.

**Q: Write-through vs. Write-back — when to use each?**
A: Write-through: both cache and DB updated synchronously — strong consistency, higher write latency. Good for financial data. Write-back: write cache only, async flush to DB — low latency, risk of data loss on crash. Good for counters, analytics.

**Q: Can you guarantee strong consistency with a cache?**
A: Not easily. Race condition: Process A reads from DB (value=1), Process B writes to DB (value=2) and invalidates cache, then Process A writes value=1 to cache → stale data. Solutions: distributed locks, compare-and-swap on write, or accept eventual consistency for cache layer.

**Q: What is a hot key? How do you detect and fix it?**
A: A key that receives disproportionate traffic (e.g., celebrity's profile). Detect: monitor per-shard QPS; outlier shard = hot key. Fix: (1) Replicate key across N nodes, client reads from random one; (2) Local in-process cache on app servers for top-K keys; (3) Dynamic shard splitting.

---

## Round 4 — Failure & Reliability

**Q: What happens if a cache node fails?**
A: Keys on that shard become misses. Traffic falls to DB. If DB can't handle it: cascading failure. Prevention: replica per shard with auto-promotion (Redis Sentinel). Recovery: re-warm cache gradually (rate-limited reads from DB).

**Q: How does Redis Sentinel work?**
A: 3+ Sentinel processes monitor primary. If primary unreachable for >N ms, Sentinels vote (quorum needed). Winner promotes replica to primary, notifies clients via pub/sub. New primary begins accepting writes.

**Q: What is split-brain in a cache cluster?**
A: Network partition divides cluster. Both partitions elect a primary → two nodes accept writes → divergent state. After heal: conflict. Fix: quorum-based writes (need majority ACK), or refuse writes in minority partition.

**Q: What circuit breaker pattern applies to caching?**
A: If cache fails repeatedly (N errors in T seconds), circuit opens: all requests go directly to DB. After 30s, probe with one request; if cache recovers, close circuit. Prevents cache downtime from amplifying into DB overload.

---

## Round 5 — Design Decisions (Trade-off Questions)

**Q: When would you NOT use a cache?**
A: (1) Write-heavy workload with no re-reads — cache is wasted. (2) Every request accesses unique data — no locality. (3) Data must always be fresh (real-time stock prices). (4) Data is already fast (in-memory DB like Redis IS the primary store).

**Q: How do you size a cache?**
A: Identify hot 20% of keys serving 80% of traffic. `Cache size = avg_object_size × hot_key_count × replication_factor`. Monitor hit rate; if hit rate < 80%, increase cache size or review eviction policy.

**Q: Memcached vs. Redis — which would you choose?**
A: Memcached: simpler, multi-threaded, pure key-value — best for pure caching at scale. Redis: richer data structures (sorted sets, lists, pub/sub, streams), persistence, replication, cluster — best when you need more than cache (sessions, leaderboards, queues).

**Q: How do you prevent cache poisoning?**
A: (1) Validate data before caching. (2) Use compare-and-swap (SET only if version matches). (3) Short TTLs. (4) Treat cache as best-effort; always have DB as source of truth.

---

## Round 6 — Follow-up Deep Dives

**Q: How would you implement a distributed LRU cache across N nodes?**
A: Use consistent hashing to route each key to one node. Each node runs its own LRU independently. For cross-node eviction ordering: impractical — instead, each node manages its own memory budget. Accept that global LRU is approximate.

**Q: How does Netflix EVCache handle multi-region caching?**
A: EVCache (Memcached-based) replicates writes to all regions synchronously within region, asynchronously across regions. Reads always local region — low latency. Accepts eventual consistency across regions. Uses write-around for videos (CDN handles distribution).

**Q: How would you cache database query results vs. computed results?**
A: DB queries: cache with key = `table:id` or query hash. Computed results: cache with key = `fn:input_hash`, longer TTL. Challenge: invalidation. DB cache: invalidate on row change (CDC). Computed: invalidate on input change or use TTL.

**Q: What's the difference between cache and CDN?**
A: Cache is a general in-memory store close to your application servers (reduces DB load). CDN is geographically distributed cache close to end users (reduces latency for static assets). CDN caches at the edge (PoPs); app cache sits in your data center.

---

## Rapid-Fire Answers

| Question | Answer |
|---|---|
| Default eviction policy in Redis? | `noeviction` (returns error); recommended: `allkeys-lru` |
| Redis data persistence? | RDB (snapshots) or AOF (append-only log) or both |
| Max memory per Redis node? | Typically 32–64 GB; above that, shard |
| Redis Cluster hash slots? | 16384 slots distributed across nodes |
| TTL for session data? | 30 min typical; slide on activity |
| Cache hit rate target? | > 90%; < 80% means cache is undersized or poorly keyed |
| What is a write coalesce? | Batch multiple writes to same key within a window before flushing to DB |
