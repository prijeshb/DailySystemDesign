# Distributed Key-Value Store — System Design
**Date:** 2026-06-12  
**Difficulty:** Hard  
**Real-world analogs:** Amazon DynamoDB, Apache Cassandra, Redis Cluster, Riak

---

## First Principles Check

> Do we really need a distributed KV store vs a single DB?

| Question | Answer |
|----------|--------|
| Can 1 Postgres handle it? | At 100K reads/sec → yes with read replicas. But adds write bottleneck and no horizontal write scale. |
| Why distributed? | Need to survive node failures, scale writes horizontally, no single point of failure |
| Why KV and not full SQL? | KV gives O(1) lookup by primary key; SQL needed only for range queries / aggregations |

**Decision:** Build distributed KV when you need: write scale + high availability + low latency, and can live without complex queries.

---

## Requirements

### Functional
- `PUT(key, value)` — create or update
- `GET(key)` → value or `NOT_FOUND`
- `DELETE(key)` — soft-delete (tombstone)
- Key: max 256 bytes. Value: max 1MB.

### Non-Functional
- p99 read latency < 10ms, write < 20ms
- 99.99% availability
- Horizontal scale: add/remove nodes without downtime
- Tunable consistency (eventual default, strong available)
- Durability: no data loss on single node crash

### Scale Estimate
```
100K reads/sec  + 50K writes/sec
10M unique keys × avg 10KB value = 100GB dataset
10 nodes → 10K reads/sec each (comfortable)
Replication factor N=3 → 300GB total storage across cluster
```

---

## Entities & Actions

### Entities
| Entity | Fields |
|--------|--------|
| **KeyValuePair** | key (string), value (bytes), version (VectorClock), deleted_at (timestamp/null), ttl (optional) |
| **Node** | node_id, ip:port, status (UP/DOWN/SUSPECT), vnodes[] |
| **VNode** | token (hash ring position), owner_node_id |
| **HintedHandoff** | key, value, target_node_id, created_at |

### Actions
```
Client → Coordinator (any node):
  1. Hash key → find position on ring
  2. Walk clockwise → select N replica nodes
  3. Send request to all N in parallel
  4. Wait for W acks (write) or R responses (read)
  5. Return result or error
```

---

## Data Flow

### Write Path
```
Client ──PUT(k,v)──► Coordinator
                         │
                    hash(key) → token
                    find N replicas on ring
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
           Node 1     Node 2     Node 3
           WAL→       WAL→       WAL→
           MemTable   MemTable   MemTable
              │          │          │
              ACK        ACK      (slow)
              └──────────┘
         Coordinator: 2 ACKs received (W=2) → return OK to client
         Node 3 still writes asynchronously
```

### Read Path
```
Client ──GET(k)──► Coordinator
                        │
                   find N replicas
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
      Node 1 → v:3   Node 2 → v:3   Node 3 → v:2 (lagging)
         └──────────────┘
    R=2 responses received → pick highest version → return v:3
    Read-repair: send v:3 to Node 3 in background
```

---

## High Level Design

```
┌─────────────────────────────────────────────────────┐
│                     Client SDK                       │
│  - Consistent hash to pick coordinator node         │
│  - Retry with exponential backoff + jitter          │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│              Any Node (Coordinator Role)             │
│  - Request Router (consistent hash ring)            │
│  - Quorum coordinator (wait for W/R acks)           │
│  - Failure detector (gossip-based)                  │
└──────┬───────────────┬──────────────────┬───────────┘
       │               │                  │
┌──────▼──┐      ┌─────▼───┐      ┌──────▼──┐
│ Node A  │      │ Node B  │      │ Node C  │
│ (vnodes)│      │ (vnodes)│      │ (vnodes)│
│ Storage │      │ Storage │      │ Storage │
│ Engine  │      │ Engine  │      │ Engine  │
└─────────┘      └─────────┘      └─────────┘
   LSM Tree         LSM Tree         LSM Tree
```

**Components:**
- **Client SDK** — knows ring topology, hashes keys, retries
- **Coordinator** — any node can route; no dedicated master
- **Gossip Service** — spreads node status changes O(log N) rounds
- **Storage Engine** — LSM tree per node (see Low Level Design)
- **Compaction** — background merge of SSTables
- **Repair Service** — anti-entropy using Merkle trees

---

## Low Level Design

### 1. Consistent Hash Ring

```
Ring: [0 ──────────────────────────────────── 2^128)

Physical nodes get V virtual nodes each:
  Node A: tokens [t1, t5, t9, ...]
  Node B: tokens [t2, t6, t10, ...]
  Node C: tokens [t3, t7, t11, ...]

PUT("username:123"):
  hash("username:123") = h
  Walk ring clockwise from h
  First 3 tokens → Node A (primary), Node B, Node C = replicas

Why virtual nodes?
  Without: add 1 node → only 2 neighbors rebalance
  With V=150: load spreads across all nodes evenly
  Node fails: 150 vnodes → 150 neighbors each absorb a small slice
```

**Trade-off:** More vnodes = better balance but more metadata overhead. V=150 is standard (DynamoDB default).

### 2. LSM Tree (Storage Engine per Node)

```
Write:
  1. Append to WAL (sequential write, durable)
  2. Insert into MemTable (in-memory sorted skip-list, typically 64MB)
  3. When MemTable full → flush to disk as SSTable (immutable sorted file)

Read:
  1. Check MemTable (newest data)
  2. Check SSTable L0 (newest flushed)
  3. Check SSTable L1, L2 ... (older)
  4. Bloom filter per SSTable → skip files that can't contain key (O(1) per file)
  5. Return first found value

Compaction (background):
  Merge SSTables → remove deleted keys (tombstones) + duplicates
  Reduces read amplification (fewer files to check)
```

**LSM Trade-offs:**
| | LSM Tree | B-Tree |
|--|---|---|
| Writes | Fast (sequential append) | Slower (random I/O, update in place) |
| Reads | Slower (multiple files) | Fast (single tree traversal) |
| Space amplification | Higher (until compaction) | Lower |
| Use | Write-heavy KV stores | Read-heavy, SQL databases |

### 3. Quorum Configuration

```
N = replication factor (default 3)
W = write quorum (how many ACKs before returning success)
R = read quorum (how many responses to collect before returning)

Key rule: R + W > N → at least one overlap → always reads fresh data

Common configs:
  R=1, W=3  → write-heavy, fast reads, slow writes (rarely used)
  R=2, W=2  → balanced, strong consistency
  R=1, W=1  → max throughput, eventual consistency
  R=3, W=1  → all replicas must agree to read (strong, slow)

Interview: "Which do you recommend?"
→ R=2, W=2, N=3: quorum intersects, tolerates 1 node failure, p99 < 10ms
```

### 4. Vector Clocks

```
Purpose: detect concurrent writes (conflicts), track causal order

Format: map of {node_id → logical_counter}
  Node A writes → {A:1}
  Node A writes again → {A:2}
  Node B sees A's write → writes → {A:2, B:1}  (causal successor)
  
Concurrent writes (conflict):
  Node A: {A:1}    ← not descended from B's clock
  Node B: {B:1}    ← not descended from A's clock
  Neither dominates → conflict!

Conflict resolution options:
  1. Last-write-wins (LWW): use timestamp → simpler, loses data
  2. Application-level merge: return both, client resolves (Amazon's shopping cart)
  3. CRDTs: data structure that auto-merges (counters, sets)
```

### 5. Gossip Protocol

```
Every T=1s, each node:
  1. Randomly selects K=3 peers
  2. Sends: "here's my view of cluster state (each node's heartbeat counter)"
  3. Peer merges: take max heartbeat per node

Node failure detection:
  Node X's heartbeat stops incrementing → after Φ threshold → marked SUSPECT
  After timeout → marked DOWN
  
Propagation: O(log N) gossip rounds → all nodes know about failure
  With 100 nodes: ~7 rounds (100 × 1s = <7s to detect failure globally)
```

---

## Trade-offs Summary

| Decision | Choice | Why | Cost |
|----------|--------|-----|------|
| Hash ring with vnodes | Consistent hash + V=150 | Even load, graceful rebalance on node add/remove | Metadata overhead for vnode table |
| Storage engine | LSM tree | Write-heavy workload; sequential writes fast | Read amplification; need bloom filters |
| Consistency | Tunable (default R=2,W=2) | Most use cases need "read your writes" | W=2 slightly higher write latency |
| Conflict resolution | LWW (timestamp) default | Simple, predictable | Rare data loss on true concurrent write |
| Leader | None (leaderless) | No SPOF, any node can coordinate | No total ordering; conflicts possible |

---

## Real-World Reference
- **DynamoDB** (2007 paper): consistent hashing + quorum + eventual consistency → influenced entire NoSQL generation
- **Cassandra**: DynamoDB + Bigtable hybrid; gossip-based membership
- **Voldemort** (LinkedIn): open-source DynamoDB clone
- **Engineering blog**: https://www.allthingsdistributed.com/2007/10/amazons_dynamo.html (Werner Vogels' original Dynamo paper)
