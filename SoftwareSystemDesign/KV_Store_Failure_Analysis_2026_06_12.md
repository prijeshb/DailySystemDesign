# Distributed Key-Value Store — Failure Analysis
**Date:** 2026-06-12

> Failure-first: assume every component will fail. Design for recovery, not just happy path.

---

## Failure Map

```
Client SDK ──► Coordinator ──► Replica Nodes (N=3)
                                    │
                              Storage Engine (LSM)
                                    │
                          ┌─────────┴────────┐
                         WAL              MemTable / SSTable

Failures at each layer:
  1. Single replica node crash
  2. Coordinator node crash mid-request
  3. Network partition (replicas split into two groups)
  4. Slow / partial write (W met but third replica behind)
  5. WAL corruption on recovery
  6. Compaction failure (SSTable corruption)
  7. Clock skew (bad timestamps → wrong LWW)
  8. Gossip partition (stale membership view)
```

---

## 1. Single Replica Node Crash

**Scenario:** Node B crashes while serving writes. N=3, W=2.

**Impact without mitigation:** Writes that landed on B may be lost. Clients reading from B after recovery see stale data.

**Prevention:**
```
WAL (Write-Ahead Log):
  Every write appended to WAL before MemTable insert
  On crash → on restart: replay WAL → MemTable rebuilt exactly
  
  Crash at t=100ms (after WAL write): replayed → no data loss
  Crash at t=50ms (before WAL write): write treated as never received
                                       → client retries (idempotent key)
```

**Recovery: Hinted Handoff**
```
Node B is DOWN (detected via gossip in ~5s)
Coordinator needs W=2. Remaining: Node A, Node C → can still write (W met).
Coordinator also stores:
  "I have a write for key K → originally for Node B"
  → saved as HintedHandoff{key, value, target=NodeB}

Node B recovers:
  Coordinator delivers hints → Node B catches up
  Hint TTL = 24h: if B doesn't recover in 24h, hint discarded
                  → full repair via Merkle tree kicks in instead
```

**After-node-recovery read inconsistency:**
- Node B may serve stale reads for keys updated during its downtime
- Fix: **Read Repair** — on every quorum read, if one replica returns older version, coordinator sends write-repair to that replica in background

---

## 2. Coordinator Node Crash Mid-Request

**Scenario:** Client sent PUT. Coordinator wrote to Node A but crashed before writing to B and C.

**Impact:** Partial write (W not met). Client gets no response (connection timeout).

**Why it's safe:**
```
Client:
  Sent request → timeout → assumes failure → retries to different coordinator
  
New coordinator:
  Same request arrives with same key/value
  Node A already has the write → returns ACK
  Nodes B, C write fresh → W=2 met → success

Idempotency requirement:
  PUT is naturally idempotent (overwrite same value = same result)
  Only concern: if value changed between retries → use conditional PUT
    PUT(key, value, if_version=V) → only writes if current version = V
    Otherwise returns VERSION_CONFLICT
```

**Coordinator is stateless for routing** — any node can be coordinator. No recovery needed; client simply retries another node.

---

## 3. Network Partition

**Scenario:** Datacenter split. 2 replicas in AZ-1, 1 replica in AZ-2. Network between AZs breaks.

```
AZ-1: [Node A, Node B]     AZ-2: [Node C]
         ╳ network partition ╳
```

**CAP Theorem choice (Dynamo-style: AP system):**
```
Option 1 (CP): reject writes until partition heals → downtime
Option 2 (AP): allow both sides to continue → may diverge → resolve after

DynamoDB/Cassandra choose AP:
  AZ-1: can still achieve W=2 (A+B) → continues serving writes ✓
  AZ-2: only W=1 possible (C alone) → depends on W config
        If W=2 required → AZ-2 rejects writes (correct: can't guarantee durability)
        If W=1 configured → AZ-2 accepts → diverges from AZ-1
```

**Partition heal — Anti-Entropy with Merkle Trees:**
```
Problem: After heal, A and C may have diverged for some keys.
How to find which keys differ without comparing all data?

Merkle Tree per node:
  Leaf nodes: hash(value) for each key range
  Parent nodes: hash(children)
  Root: single hash representing entire dataset
  
  A's root ≠ C's root → trees diverge somewhere
  Binary search down tree → find exact key ranges that differ
  Exchange only those keys → efficient repair
  
  Without Merkle: compare all 10M keys → 100GB data transfer
  With Merkle: O(log N) comparisons to find diverged subset
```

**Conflict resolution on heal:**
```
Node A: key="user:1" value="Alice" version={A:3}
Node C: key="user:1" value="Alicia" version={C:1}

Neither version descends from the other → concurrent write → CONFLICT

Resolution strategies:
  1. Last-Write-Wins (timestamp): take newer timestamp → simple, may lose data
  2. Vector clock dominance: if {A:3} > {C:1} in all components → take A's
  3. Sibling return: return both versions to client → client resolves
     (DynamoDB returns both; application picks/merges and does a final write)
```

---

## 4. Slow Replica (W Met But Third Lags)

**Scenario:** W=2, N=3. Node C is slow (GC pause, disk busy). Coordinator got ACK from A+B → returned success. C is still processing.

**What if C crashes now?**
```
A and B have the data. C never wrote it.
On C recovery: hinted handoffs + Merkle tree repair fill the gap.
No data loss — quorum ensures data on at least 2 nodes.
```

**What if a READ goes to C before repair?**
```
GET request → Coordinator asks A, B, C (R=2)
A: version=5  B: version=5  C: version=4 (stale)

Coordinator: R=2 responses from A+B → return version=5 (correct)
             Also sends write-repair to C: "update to version=5"
C catches up via read-repair (passive, background)
```

**Rule:** Coordinator always picks highest version from R responses. Read repair is best-effort, async — doesn't slow down the read.

---

## 5. WAL Corruption / Disk Failure

**Scenario:** Node A's disk partially fails. WAL file corrupted.

**Detection:**
```
Each WAL entry has: [CRC32 checksum][length][payload]
On replay: compute CRC32 → compare → mismatch → corruption detected
```

**Recovery options:**
```
Option A: Truncate WAL at last clean entry
  → Replay valid entries → MemTable partially rebuilt
  → Request full repair from replicas via Merkle tree for missing keys
  → Slower startup but safe

Option B: Restore from last SSTable checkpoint + partial WAL
  → SSTable = already flushed MemTable (durable point-in-time)
  → Only replay WAL entries written after last SSTable flush
  → Reduces WAL replay scope significantly

Checksum on SSTables too:
  Each 4KB block has CRC32
  Read a corrupted block → immediately return error + trigger replica fetch
```

**What if all 3 replicas lose data simultaneously?** (catastrophic)
```
→ That's why we have cross-region replication
→ Primary cluster in us-east-1, async replica in eu-west-1
→ RPO = replication lag (~100ms) — acceptable data loss window
→ RTO = promote eu-west-1 replica + update DNS → ~1-5 min
```

---

## 6. SSTable Compaction Failure

**Scenario:** Background compaction crashes midway. Partially written output SSTable.

**Safe by design:**
```
Compaction writes to a NEW SSTable file (not in-place)
Input SSTables remain untouched until new SSTable is fully written + verified

If compaction crashes:
  Input SSTables still valid → data intact
  Partial output SSTable → detected by missing "compaction-complete" marker → deleted
  Compaction restarts from scratch

Rule: compaction is a copy-on-write operation. Never modify in-place.
```

**Compaction pressure (write stall):**
```
Writes much faster than compaction → SSTables accumulate → read amplification grows
L0: 10+ files → read must check all → latency spikes

Mitigation: write throttling when L0 count > threshold
  If L0_count > 10: slow writes by 50%
  If L0_count > 20: stop writes entirely until compaction catches up
  
This is visible in Cassandra/RocksDB as "write stall" — must be monitored.
```

---

## 7. Clock Skew (Last-Write-Wins Risk)

**Scenario:** Two clients write the same key at the same time. LWW resolution uses timestamp. One node's clock is 500ms ahead.

```
Node A clock: 10:00:00.500  → write timestamp = 500ms
Node B clock: 10:00:00.000  → write timestamp = 000ms  (correct time)

LWW picks: Node A's write (higher timestamp) ← but A's clock was WRONG
Result: correct write discarded, wrong write wins
```

**Mitigations:**
```
1. NTP + PTP (Precision Time Protocol):
   Keep clocks within ~1ms across nodes
   TrueTime (Google Spanner): GPS + atomic clocks, uncertainty bounded to <7ms

2. Hybrid Logical Clocks (HLC):
   max(physical_clock, last_seen_HLC) + logical_increment
   Monotonically non-decreasing even if NTP adjusts backward
   Used in CockroachDB, YugabyteDB

3. Vector clocks (no timestamp dependency):
   Detect true concurrent writes → return conflict → application resolves
   No clock skew risk

For interview: "LWW is simple but loses data on clock skew. For strong consistency, use quorum + vector clocks."
```

---

## 8. Gossip Partition (Stale Membership View)

**Scenario:** Network hiccup isolates Node X from gossip. X thinks others are down; others think X is suspect.

**False positive: X marked DOWN but actually UP**
```
Clients stop sending requests to X → wasted capacity
X is healthy but isolated

Mitigation:
  Φ-accrual failure detector: doesn't use hard threshold
  Tracks heartbeat arrival time distribution → computes probability of failure
  Only mark DOWN when Φ > 8 (very high confidence)
  Reduces false positives from transient network blips
```

**X serving stale ring topology:**
```
X doesn't know about new Node D that just joined
X routes PUT("key:5") to wrong replica set

Mitigation:
  Client SDK also has ring topology
  SDK periodically fetches ring metadata from multiple nodes
  If coordinator routes to wrong node → that node forwards + returns hint
  "I'm not the right replica for this key; try node D"
```

---

## Defense-in-Depth Summary

```
Layer 1: WAL → durability on single node crash
Layer 2: Quorum (W=2) → survive 1 replica crash without data loss
Layer 3: Hinted Handoff → graceful short-term replica outage
Layer 4: Read Repair → passive stale data healing
Layer 5: Merkle tree anti-entropy → active repair after partition heal
Layer 6: Gossip + Φ failure detector → accurate, fast failure detection
Layer 7: Cross-region replication → survive datacenter loss
```

Each layer handles a different failure scenario. No single mechanism covers all.
