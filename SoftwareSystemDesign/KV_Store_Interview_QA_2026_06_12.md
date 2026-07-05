# Distributed Key-Value Store — Interview Q&A
**Date:** 2026-06-12

---

## Opening Questions (Scoping)

**Q: How would you design a distributed key-value store?**

> Start with clarifying questions before drawing anything.

"Before I design, let me clarify requirements:
- What's the scale — reads/writes per second, dataset size?
- Do we need strong consistency or is eventual consistency acceptable?
- What's the expected value size? (affects storage engine choice)
- Any TTL/expiry support needed?
- Single-region or multi-region?

Assuming: 100K reads/sec, 50K writes/sec, ~100GB dataset, eventual consistency acceptable but tunable, single-region to start."

---

## Core Concept Questions

**Q: Why not just use consistent hashing without virtual nodes?**

Without virtual nodes, each physical node gets one position on the ring. Problems:
1. Adding node: only 1/N keys rebalance instead of distributing evenly
2. Node failure: entire load shifts to one neighbor → hotspot
3. Heterogeneous nodes: can't assign more slots to larger nodes

With V=150 virtual nodes per physical node:
- Even load distribution: each node handles ~1/N of keyspace across 150 segments
- Graceful rebalance: new node takes 1-2 vnodes from each existing node
- Heterogeneous hardware: bigger machine → more vnodes assigned

---

**Q: What's the difference between write-ahead log and SSTable?**

| | WAL | SSTable |
|--|---|---|
| When written | Before MemTable insert | When MemTable is full (flush) |
| Format | Sequential, append-only, unsorted | Sorted by key, immutable |
| Purpose | Crash recovery | Long-term storage + reads |
| Durability | Immediate (fsync) | After flush completes |
| Read use | Not used for reads | Yes (with bloom filter) |

WAL is the durability guarantee. SSTable is the queryable on-disk state. Together: durable + efficient reads.

---

**Q: If R=2, W=2, N=3, can we ever get stale reads?**

Technically no — with R+W > N, at least one node is in both quorums → overlap → always returns freshest data.

BUT there's a subtlety: **time between write and read**. If two writes happen concurrently and the read overlaps:
- Write 1 lands on A+B
- Write 2 lands on B+C (before A gets write 1)
- Read sees A (write 1) + B (write 2) → returns higher version

The vector clock detects if write 2 causally follows write 1. If concurrent → returns conflict.

Real risk: **clock skew with LWW**. If timestamps are unreliable, even quorum reads can return wrong winner. Fix: vector clocks.

---

**Q: How does read-repair work and why is it async?**

On every GET:
1. Coordinator asks R=2 replicas
2. A returns version=5, B returns version=4
3. Coordinator returns version=5 to client
4. **In background**, coordinator sends write-repair to B: "update to version=5"

Why async? Making it synchronous would:
- Block client response until B confirms repair
- 3x network round trip added to read latency
- B might be slow or down → read fails when it shouldn't

Trade-off: async repair means the next read to B might still see stale data for a brief window. Acceptable for eventual consistency.

---

**Q: When would you use a vector clock vs. last-write-wins?**

| | LWW | Vector Clocks |
|--|---|---|
| Complexity | Simple | Complex (conflict detection + resolution) |
| Data safety | May lose concurrent writes | Never loses data (both kept) |
| Conflict resolution | Automatic (pick newer) | Manual (application decides) |
| Clock dependency | Yes (vulnerable to skew) | No |
| Use when | Updates are idempotent / overwrites OK | Shopping cart, account balance, any "merge" semantics |

Amazon Dynamo uses vector clocks for shopping cart — "add item" must never be silently lost.
For user profile updates (email change), LWW is fine — user will just update again if wrong.

---

**Q: Explain the Merkle tree anti-entropy process.**

```
Problem: After network partition heals, 2 nodes may have diverged.
         Can't send all 10M keys to compare — too expensive.

Solution: Merkle tree
  Build tree where:
    - Leaf = hash(value) for a key range (e.g., token range [0, 1000))
    - Parent = hash(left_child + right_child)
    - Root = single hash for entire dataset

  Compare two nodes:
    A.root == B.root? → identical → no repair needed
    A.root ≠ B.root? → diverged somewhere → go deeper
    Compare left subtrees → diverged? go deeper
    Compare right subtrees → diverged? go deeper
    → Drill down to exact key range that differs in O(log N) comparisons
    → Transfer only those keys
```

Real cost: comparing 10M keys = 100GB transfer. Merkle tree = ~50KB metadata exchange + targeted repair.

---

## Follow-Up / Deep-Dive Questions

**Q: How do you handle hot keys? (e.g., a viral post that everyone is GETting)**

One key → one vnode → one primary node → that node overwhelmed.

Mitigation options:
1. **Read-through cache layer (Redis)** in front of KV store — absorbs 99% of reads for hot keys
2. **Key scatter**: client-side trick — `hot_key` → stored as `hot_key_0`, `hot_key_1`, ..., `hot_key_9` round-robin across nodes. Read = pick random shard. (scatter/gather)
3. **SFU-style fan-out at edge**: CDN or API gateway caches hot GET responses
4. **Adaptive replication**: detect hot key → create extra replicas on ring for it dynamically

"For interview, mention all 4 but say: in practice, Redis cache in front eliminates 99% of the problem with minimal complexity."

---

**Q: How do you handle deletes? Why not just remove the key?**

Problem: if you delete a key and the node crashes, on recovery (via WAL replay / Merkle repair), the old value might reappear from another replica that hasn't received the delete.

Solution: **Tombstone** (soft delete)
```
DELETE key → store: key → {value: null, deleted: true, timestamp: T}
On read: check if tombstone → return NOT_FOUND

Tombstone propagates to all replicas via normal quorum write
Eventually: compaction removes tombstone + old value after gc_grace_period
```

`gc_grace_period` (default 10 days in Cassandra): window where tombstone must survive before compaction. If you compact too early and a recovering node shows up after `gc_grace_period`, it may resurrect the deleted key.

---

**Q: What happens when you add a new node?**

```
1. New Node D joins → broadcasts via gossip → all nodes update ring view
2. D gets assigned V=150 new vnodes (tokens assigned by admin or random)
3. For each new vnode token on D:
   a. Find old owner (node that currently holds that token range)
   b. Stream data from old owner to D
   c. Once D confirms receipt → update ring → D starts accepting requests
4. Old owner still handles requests during transfer (streaming, not cutover)
5. After transfer + confirmation → old owner removes those keys
```

**Key: zero-downtime.** Data streams in background. Ring update is atomic (via gossip). No read interruption during rebalance.

---

**Q: How is this different from Redis Cluster?**

| | DynamoDB-style (this design) | Redis Cluster |
|--|---|---|
| Architecture | Leaderless (any node = coordinator) | Master-replica per shard |
| Consistency | Tunable (eventual default) | Strong within shard (single master) |
| Hash ring | Consistent hashing + vnodes | Fixed 16384 hash slots |
| Replication | Multi-master (leaderless quorum) | Single master per slot |
| Data size | Designed for large datasets (TBs) | Best for hot working set in RAM |
| Failure failover | Gossip + automatic via quorum | Cluster bus detects + promotes replica |

Redis Cluster: simpler, lower latency for cache use cases. Dynamo: better for durable storage at scale with tunable consistency.

---

**Q: How would you make this strongly consistent (like Zookeeper / etcd)?**

Current design: leaderless, AP system (available during partition).

For CP (consistent + partition tolerant):
1. **Raft consensus**: elect a leader per shard, all writes go through leader
2. Leader commits to log → replicates to followers → majority ACK → return success
3. Reads: go to leader only (or any node with ReadIndex for linearizable reads)

Cost: leader = single point of write bottleneck per shard. Partitions cause unavailability.

"Zookeeper/etcd choose CP. DynamoDB/Cassandra choose AP. Choose based on use case: bank transactions → CP. Shopping cart, user sessions → AP is fine."

---

## Interviewer Gotcha Questions

**Q: "Your design doesn't handle concurrent writes to the same key correctly."**

> This is a probe — they want to know if you understand the conflict.

"You're right. With leaderless multi-master replication, two clients can write the same key to different coordinators simultaneously. The system detects this via vector clocks — neither clock dominates — and treats them as siblings (conflict).

Resolution depends on application:
- LWW: take higher timestamp (risk: clock skew loses data)
- Application-level: return both versions; caller picks winner + writes back
- CRDTs: if value is a counter or set, auto-merge is deterministic

For a general KV store, I'd default to LWW with NTP and HLC to minimize clock skew risk, and surface vector clocks for clients that need conflict detection."

---

**Q: "You said W=2 ensures durability. But what if the disk on both nodes fails before fsync?"**

"Correct — fsync is the durability guarantee. If both machines lose power before fsync completes:
1. WAL not persisted → those writes lost
2. W=2 told the client 'success' → client assumes durability

This is a known trade-off. Mitigations:
- Use SSDs with power-loss protection (capacitor holds write buffer through power loss)
- Use synchronous replication to a third AZ for critical data
- Accept a very small data loss window as part of the AP system contract

Spanner (Google) solves this with TrueTime + synchronous replication across 3 zones but at the cost of higher write latency (~10ms vs ~1ms)."

---

## One-Liners for Key Concepts

| Concept | One-line answer |
|---------|----------------|
| Why consistent hashing? | Move only k/N keys on node add/remove instead of all keys |
| Why LSM tree? | Sequential writes are 10-100x faster than random I/O (B-tree) |
| Why quorum? | Overlap between write set and read set guarantees no stale read |
| Why gossip? | O(log N) propagation, no single coordinator, decentralized |
| Why Merkle trees? | Find diverged key ranges in O(log N) comparisons instead of full scan |
| Why hinted handoff? | Short-term replica outage shouldn't drop writes |
| Why tombstones? | Soft delete prevents deleted keys from resurrecting via stale replicas |
| Why virtual nodes? | Even load distribution and graceful rebalance on topology change |
