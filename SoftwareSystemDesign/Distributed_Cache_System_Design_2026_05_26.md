# Distributed Cache — System Design
**Date:** 2026-05-26

---

## 0. First-Principles Thinking

**Do we even need a cache?**
Every database call costs ~1–10 ms. At 100K RPS that's 100K db ops/sec — unacceptable.
A cache stores recent/hot results in memory (< 1 ms reads), absorbing the load.

**When NOT to cache:** financial transactions needing real-time accuracy, write-heavy workloads with no re-reads, data with zero locality (every request unique).

**Core insight:** A cache trades *consistency* for *speed and scalability*.

---

## 1. Entities

| Entity | Description |
|---|---|
| Key | String identifier (e.g. `user:123:profile`) |
| Value | Opaque bytes (JSON, protobuf, serialised object) |
| TTL | Expiry time per entry |
| Node | Single cache server (CPU + RAM) |
| Shard | Partition owning a key range |
| Replica | Hot standby of a shard |
| Client | Application server talking to cache |

---

## 2. Actions / Operations

- **GET(key)** → value or MISS
- **SET(key, value, ttl)**
- **DEL(key)**
- **MGET / MSET** — batch ops
- **INCR / DECR** — atomic counter
- **EXPIRE(key, ttl)** — update TTL
- **FLUSH** — clear all (dangerous, admin only)

---

## 3. Data Flow

### Cache HIT
```
Client → Cache Node → return value → Client
```

### Cache MISS
```
Client → Cache Node (miss)
       → Client fetches from DB
       → Client writes back to Cache (SET)
       → return value to Client
```

### Write Strategies (tradeoff table)

| Strategy | How | Consistency | Latency |
|---|---|---|---|
| **Write-Through** | Write DB + Cache together | Strong | Higher (2 writes) |
| **Write-Back (Behind)** | Write Cache, async flush to DB | Eventual | Low (risk: data loss on crash) |
| **Write-Around** | Write DB only, invalidate cache | Eventual | Medium (next read is miss) |
| **Read-Through** | Cache fetches DB on miss automatically | Eventual | Miss penalty once |

**Recommended default:** Write-Through for correctness; Write-Back for high-write, loss-tolerant workloads.

---

## 4. High-Level Design

```
            ┌─────────────────────────────────────────┐
            │            Application Servers           │
            └────────────┬────────────────────────────┘
                         │  Cache Client (SDK)
                         ▼
            ┌─────────────────────────────────────────┐
            │         Cache Cluster (N shards)         │
            │  ┌──────┐  ┌──────┐  ┌──────┐          │
            │  │Shard0│  │Shard1│  │Shard2│  ...      │
            │  │ P+R  │  │ P+R  │  │ P+R  │          │
            │  └──────┘  └──────┘  └──────┘          │
            └─────────────────────────────────────────┘
                         │
            ┌────────────▼────────────┐
            │       Database          │
            └─────────────────────────┘
```

**P = Primary node, R = Replica node (per shard)**

**Routing:** Client SDK hashes key → consistent hash ring → target shard primary.

---

## 5. Low-Level Design

### 5.1 Consistent Hashing (Why not naive mod-N?)

Naive: `shard = hash(key) % N`
Problem: Adding/removing a node remaps ~N-1/N keys → massive cache invalidation storm.

**Consistent Hash Ring:**
- Hash space = 0 to 2³² − 1 (ring)
- Each node owns an arc on the ring
- Key maps to the nearest clockwise node
- Adding a node: only keys between new node and its predecessor migrate (~1/N keys)
- **Virtual nodes (vnodes):** Each physical node gets K virtual positions → better load balance

```
Ring: [0 ──── Node A (vnode a1,a2) ──── Node B ──── Node C ──── 2^32]
Key hash falls between a2 and B → assigned to B
```

### 5.2 Eviction Policies

| Policy | Evicts | Best For |
|---|---|---|
| **LRU** | Least recently used | General workloads |
| **LFU** | Least frequently used | Skewed popularity |
| **TTL / Lazy** | Expired keys on access | Session data |
| **Random** | Random key | Uniform access patterns |
| **FIFO** | Oldest inserted | Queue-like data |

**Implementation of LRU:** Doubly-linked list + HashMap. O(1) get + O(1) update.

### 5.3 Cache Invalidation Strategies

| Method | Mechanism | Problem |
|---|---|---|
| TTL expiry | Key auto-expires | Stale window = TTL duration |
| Event-driven | DB change triggers DEL/SET | Needs CDC pipeline (Debezium) |
| Write-through | App always updates cache on write | Works well, adds write latency |
| Cache-aside + version | Store version with value, compare on read | Complex client logic |

### 5.4 Replication

- **Primary-Replica:** Writes go to primary, async replicate to replicas. Reads can be served from replicas (read scaling).
- **Redis Replication:** Primary streams replication log to replicas. On primary failure, Sentinel/Cluster promotes a replica.
- **Trade-off:** Async replication → replica may lag → stale reads possible.

### 5.5 Sharding Deep-Dive

| Approach | Description | Trade-off |
|---|---|---|
| **Client-side sharding** | SDK decides shard | No central coordinator; client logic |
| **Proxy-based** | Twemproxy / Envoy routes | Central bottleneck; single point of failure |
| **Cluster mode** | Nodes gossip, self-routing (Redis Cluster) | Complex, resharding takes time |

### 5.6 Hot Key Problem

A single key (celebrity post, viral item) hits one shard with 10× normal load.

**Solutions:**
1. **Key replication** — store `hot_key_0`, `hot_key_1` … `hot_key_N`; client reads random replica.
2. **Local client cache** — app server keeps in-process LRU (e.g., Guava Cache) for top-K keys.
3. **Request coalescing** — on MISS, only one request goes to DB; others wait (thundering herd prevention).

### 5.7 Thundering Herd (Cache Stampede)

**Scenario:** Popular key expires → 10K simultaneous MISSes → 10K DB queries.

**Solutions:**
1. **Mutex/lock on miss** — first caller acquires lock, fetches DB, sets cache; others wait.
2. **Probabilistic early expiry** — re-fetch key slightly before TTL using formula: `expiry - β * log(random())`.
3. **Stale-while-revalidate** — serve stale value while async background refresh happens.

### 5.8 Memory Sizing

```
Cache size = (avg object size) × (hot key count) × (replication factor)
Example: 1 KB × 10M hot objects × 2 replicas = 20 GB
```

Rule of thumb: Cache the **top 20% of keys** that serve **80% of traffic** (Pareto).

---

## 6. Key Trade-offs

| Decision | Option A | Option B | Choose when |
|---|---|---|---|
| Write strategy | Write-through (strong) | Write-back (fast) | A: financial; B: counters/analytics |
| Eviction | LRU | LFU | LRU: recency matters; LFU: popularity matters |
| Replication | Sync (strong) | Async (fast) | Sync: can't tolerate stale; Async: high write throughput |
| Sharding | Client-side | Cluster mode | Client-side: simple; Cluster: large scale |
| Hot key | Key replication | Local cache | Replication: spread load; Local: ultra-low latency |

---

## 7. Capacity Estimation

- **Throughput target:** 1M RPS
- **P99 latency target:** < 1 ms
- **Data size:** 500 GB total hot data
- **Node RAM:** 64 GB each
- **Nodes needed:** 500 GB / 64 GB ≈ 8 primary nodes + 8 replica nodes = **16 nodes**
- **Network:** 1M RPS × 1 KB avg = **1 GB/s** — 10 GbE NICs per node

---

## 8. Real-World References

| Company | Cache | Learning |
|---|---|---|
| Facebook | Memcached (Tao) | Hierarchical caching, regional replication |
| Twitter | Redis (Twemproxy) | Proxy-based sharding, hot-key mitigation |
| Netflix | EVCache (Memcached) | Multi-region replication, write-around strategy |
| GitHub | Redis Cluster | Cluster mode, Sentinel for HA |
| Slack | Redis | Pub/Sub for presence, LRU for session data |

**Blog references:** Facebook's "Scaling Memcache" (NSDI 2013), Netflix EVCache blog, Redis internals docs.
