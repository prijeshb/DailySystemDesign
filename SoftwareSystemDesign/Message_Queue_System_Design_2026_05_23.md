# Distributed Message Queue — System Design
**Date:** 2026-05-23 | **Difficulty:** Hard | **Real-world:** Apache Kafka, RabbitMQ, AWS SQS

---

## 0. First Principles — Do We Even Need This?

**Problem:** Service A wants to talk to Service B. Why not call B directly?

| Direct Call | Message Queue |
|---|---|
| A blocks until B responds | A fires and forgets |
| B outage → A fails | B outage → messages buffer |
| A must know B's address | A just knows the topic |
| Burst traffic overwhelms B | Queue absorbs spikes |

**Decision:** Use a queue when **decoupling, buffering, fanout, or async processing** is needed. Do NOT add one for simple request-response flows — it adds latency and operational overhead.

---

## 1. Entities

| Entity | Description |
|---|---|
| **Producer** | Publishes messages to a topic |
| **Topic** | Logical channel (e.g., `order-placed`) |
| **Partition** | Ordered sub-unit of a topic; unit of parallelism |
| **Message** | Key + Value + Headers + Timestamp + Offset |
| **Offset** | Monotonically increasing ID within a partition |
| **Broker** | Server that stores and serves messages |
| **Consumer** | Reads messages from partitions |
| **Consumer Group** | Set of consumers sharing work across partitions |
| **Leader Replica** | Primary partition owner; handles reads/writes |
| **Follower Replica** | Passive copy; takes over on leader failure |
| **Coordinator** | Manages consumer group membership & offset commits |
| **ZooKeeper / KRaft** | Cluster metadata store (leader election) |

---

## 2. Actions / Operations

```
Producer  →  [publish(topic, key, value)]
Consumer  →  [subscribe(topic, group_id), poll(), commit(offset)]
Admin     →  [create_topic(name, partitions, replication_factor)]
           →  [alter_topic(), delete_topic()]
Broker    →  [replicate(), elect_leader(), compact(), expire()]
```

---

## 3. Data Flow

### 3.1 Publish Path
```
Producer
  │
  ├─ Partitioner (hash(key) % num_partitions  OR  round-robin if no key)
  │
  ├─ Local batch buffer (linger.ms + batch.size)
  │
  └─► Leader Broker (Partition X)
           │
           ├─ Append to commit log (page cache write)
           │
           ├─ Replicate to ISR followers (In-Sync Replicas)
           │
           └─ ACK to producer (acks=0 / 1 / all)
```

### 3.2 Consume Path
```
Consumer (Group G)
  │
  ├─ Fetch from Leader (partition assigned by Coordinator)
  │
  ├─ Process message
  │
  └─ Commit offset → __consumer_offsets topic (or sync if needed)
```

### 3.3 Message Storage Layout
```
/data/topics/order-placed/
  partition-0/
    00000000000000000000.log   ← segment file
    00000000000000000000.index ← offset → file position
    00000000000000000000.timeindex
  partition-1/
    ...
```

---

## 4. High-Level Design

```
┌─────────────┐     publish      ┌──────────────────────────────────┐
│  Producers  │ ───────────────► │           Broker Cluster          │
│  (services) │                  │                                    │
└─────────────┘                  │  ┌──────────┐  ┌──────────┐      │
                                 │  │ Broker 1 │  │ Broker 2 │  ... │
┌─────────────┐     subscribe    │  │ (Leader) │  │(Follower)│      │
│  Consumers  │ ◄─────────────── │  └──────────┘  └──────────┘      │
│  (groups)   │                  │                                    │
└─────────────┘                  │  ┌──────────────────────────────┐ │
                                 │  │    ZooKeeper / KRaft          │ │
                                 │  │  (leader election, metadata) │ │
                                 │  └──────────────────────────────┘ │
                                 └──────────────────────────────────┘
                                          │
                                 ┌────────▼────────┐
                                 │  Object Storage  │ ← tiered storage
                                 │  (S3 / GCS)      │   (cold segments)
                                 └──────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Partitioning Strategy

**Why partition?** Single ordered log can't scale. Partitions = parallelism unit.

```
Rule: max_consumer_parallelism = num_partitions
      (extra consumers in a group sit idle)

Key-based:   same key → same partition → ordering guaranteed per key
Round-robin: no key → spread load, no ordering guarantee
Custom:      hash(geo_region) → regional affinity
```

**Trade-off:** More partitions = more parallelism BUT more open file handles, more leader elections, slower rebalances.

### 5.2 Replication & Consistency

```
replication_factor = 3  (tolerate 2 broker failures)
min.insync.replicas = 2

acks=all + min.insync.replicas=2:
  → Producer only gets ACK after 2 replicas confirm write
  → Guarantees no data loss even if leader dies immediately after

ISR (In-Sync Replicas): followers within replica.lag.time.max.ms
  → Follower that falls behind is removed from ISR
  → Leader only waits for ISR, not all replicas
```

### 5.3 Offset Management

```
Auto-commit (at-most-once risk):
  consumer polls → auto-commits offset → crashes before processing
  → message lost

Manual commit after processing (at-least-once):
  consumer polls → processes → commits offset
  → on crash, re-reads and re-processes (idempotent consumers needed)

Exactly-once (Kafka Transactions):
  producer: transactional.id + begin/commit transaction
  consumer: isolation.level=read_committed
  → reads only committed messages
```

### 5.4 Message Retention & Compaction

| Mode | Behavior | Use Case |
|---|---|---|
| **Time-based** | Delete after N days | Event logs, analytics |
| **Size-based** | Delete oldest when > N GB | Bounded storage |
| **Compaction** | Keep last value per key | Changelog, state store |

**Log compaction flow:**
```
Cleaner thread scans dirty segments
Builds key → latest_offset map
Rewrites segment keeping only latest per key
Old records with null value (tombstones) → deleted after grace period
```

### 5.5 Consumer Group Rebalancing

**Trigger:** consumer joins/leaves/crashes, partition added.

```
1. Coordinator detects change (heartbeat timeout or explicit join)
2. All consumers revoke partitions (stop-the-world)
3. Coordinator assigns partitions via assignor strategy
   - RangeAssignor: contiguous ranges
   - RoundRobinAssignor: spread evenly
   - StickyAssignor: minimize movement (preferred)
4. Consumers resume from last committed offset

Problem: rebalance = pause for all consumers
Solution: Incremental Cooperative Rebalance (Kafka 2.4+)
  → Only affected partitions revoked, others continue
```

### 5.6 Producer Idempotency

```
enable.idempotence=true adds:
  PID (Producer ID) + sequence_number per partition

Broker deduplicates:
  if seq_num == expected → accept
  if seq_num < expected  → duplicate, discard
  if seq_num > expected  → out-of-order, error

Sequence numbers reset on producer restart → use transactions for cross-session idempotency
```

### 5.7 Throughput Optimizations

```
Producer side:
  - Batching: linger.ms=5, batch.size=64KB → fewer round trips
  - Compression: snappy/lz4 per batch → ~4x size reduction

Broker side:
  - Zero-copy: sendfile() syscall → kernel reads → NIC directly (no userspace copy)
  - Page cache: OS manages hot data in RAM; sequential reads = cache-friendly

Consumer side:
  - Fetch min.bytes=64KB → reduce small fetches
  - fetch.max.wait.ms=500 → long-polling
```

---

## 6. Capacity Estimation

```
Target: 1M messages/sec, avg 1KB each

Write throughput:  1 GB/s raw
With replication:  3 GB/s disk writes (RF=3)
Retention 7 days:  7 * 86400 * 1GB = ~600 TB total

Broker count: 600TB / 6TB per disk = 100 brokers (minimum)
              + headroom: ~150 brokers

Partitions: 1M msg/s, each partition handles ~50K msg/s → ~20 partitions per topic
```

---

## 7. Key Trade-offs

| Decision | Option A | Option B | Winner & Why |
|---|---|---|---|
| Durability vs Latency | acks=all (slow) | acks=1 (fast) | **acks=all** for financial; **acks=1** for metrics |
| Storage | Local disk | Tiered (S3) | **Tiered** for cost at scale; local for low latency |
| Ordering | Global | Per-key | **Per-key** — global requires 1 partition = no scale |
| Consumer model | Push (broker sends) | Pull (consumer fetches) | **Pull** — consumer controls rate, avoids overwhelm |
| Metadata store | ZooKeeper | KRaft (built-in Raft) | **KRaft** — removes external dep, faster failover |
