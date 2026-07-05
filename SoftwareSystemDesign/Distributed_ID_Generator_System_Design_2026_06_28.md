# Distributed ID Generator (Snowflake) — System Design
*Date: 2026-06-28*

---

## First Principles — Do We Even Need This?

**Problem:** Every entity in a distributed system needs a unique identifier.

**Why not use database auto-increment?**
Single DB auto-increment works at small scale. At 10K writes/sec across 5 shards, each shard auto-increments independently → ID collisions between shards. Multi-master step-increment (shard 1: 1,6,11… shard 2: 2,7,12…) works but is operationally fragile: adding a shard breaks the step invariant.

**Why not UUID v4?**
UUID v4 is 128-bit random. Problems:
1. **No ordering** — random IDs cause B-tree index page splits → 3–5× slower inserts at 100M+ rows
2. **No embedded timestamp** — can't decode "when was this created?" without a DB lookup
3. **Storage** — 128 bits vs 64 bits doubles index size

**Why not timestamp alone?**
Two requests in the same millisecond = same ID. Collision rate at 10K req/ms is 100%.

**Core insight:**
We need a 64-bit integer that is:
1. **Globally unique** across all machines
2. **Sortable by creation time** (monotonically increasing)
3. **Generated without coordination** (no single point of failure)
4. **Embeds a timestamp** (decodable, useful for partitioning)

Snowflake solves exactly this.

---

## Entities & Actions

**Entities:**
- `ID Generator Node` — stateless service with a fixed machine ID
- `Client Service` — any service (Order, User, Event) that requests an ID
- `Zookeeper / Config Store` — assigns unique machine IDs to nodes

**Actions:**
- `generateID()` → returns 64-bit integer
- `decodeID(id)` → returns {timestamp, machineID, sequence}
- `batchGenerate(n)` → returns n sequential IDs (for bulk inserts)

**Data Flow:**
```
Client → ID Generator Node → [read local clock, increment sequence] → return ID
                           ↕
                    ZooKeeper (startup only: claim machine ID)
```

No DB write on the hot path. Every ID is generated purely in memory.

---

## Scale Estimates

**Target:** A large platform (Twitter-scale)
- 500M new objects/day across all entity types (tweets, events, orders)
- Peak: 10× avg = 50K IDs/sec sustained bursts up to 500K/sec
- ID size: 64 bits = 8 bytes

**Snowflake bit layout (64-bit):**
```
 1 bit  | 41 bits          | 10 bits     | 12 bits
 sign=0 | ms since epoch   | machine ID  | sequence
        | (Jan 1 2024)     | (0–1023)    | (0–4095)
```

| Component | Capacity |
|-----------|----------|
| Timestamp | 41 bits → 2^41 ms = 69 years before overflow |
| Machine ID | 10 bits → 1,024 unique nodes |
| Sequence | 12 bits → 4,096 IDs per millisecond per node |

**Max throughput per node:** 4,096/ms = **4.096M IDs/sec**
**Total cluster (10 nodes):** 40M IDs/sec — far exceeds any realistic need.

**Storage impact:**
- 1B IDs/day × 8 bytes = 8 GB/day of pure ID storage (negligible)
- Index benefit vs UUID: 64-bit vs 128-bit → 50% index size reduction

---

## AWS Cost (Back of Envelope)

| Component | Config | Unit Cost | Monthly Cost |
|-----------|--------|-----------|--------------|
| ID Generator nodes (m5.large × 5) | 5 nodes for HA | $0.096/hr | $346 |
| ZooKeeper ensemble (t3.medium × 3) | quorum | $0.042/hr | $91 |
| Load Balancer (ALB) | 1 | $0.016/hr + LCU | ~$50 |
| CloudWatch + monitoring | — | — | ~$20 |
| **Total** | | | **~$507/month** |

**This is one of the cheapest critical services you'll ever run.** Most cost is in network/monitoring overhead, not compute.

### Budget Constraint Example

**Startup constraint: $100/month for ID generation**
- Solution: Run 2 nodes (t3.micro × 2 = $15/month), use Redis INCR as fallback
- Limitation: No ZooKeeper → machine IDs hardcoded via env vars
  - Risk: DevOps accidentally deploys two containers with same machine ID → ID collisions
  - Mitigation: Startup assertion — each node checks its machine ID is unique against a Redis set on boot
- Limitation: Only 2 nodes → no geographic redundancy
  - Risk: Both nodes fail → no ID generation → all downstream writes block
  - Mitigation: UUID v7 fallback in client SDKs (auto-switch if generator unreachable >100ms)

---

## High-Level Design

```
┌──────────────────────────────────────────────────┐
│                   Clients                        │
│  (Order Svc, User Svc, Event Svc, Payment Svc)   │
└───────────────┬──────────────────────────────────┘
                │ HTTP/gRPC
        ┌───────▼────────┐
        │  Load Balancer  │
        └───┬────────┬───┘
            │        │
   ┌────────▼──┐  ┌──▼────────┐
   │ ID Node 1 │  │ ID Node 2 │  ... (up to 1024 nodes)
   │ machID=1  │  │ machID=2  │
   └────────┬──┘  └────────┬──┘
            │               │
        ┌───▼───────────────▼───┐
        │   ZooKeeper Ensemble  │
        │  (machine ID lease)   │
        └───────────────────────┘
```

**ID generation is local to each node — no coordination on hot path.**

---

## Low-Level Design

### Snowflake ID Generation (Pseudocode)

```python
class SnowflakeGenerator:
    EPOCH = 1704067200000  # Jan 1 2024 UTC in ms
    MAX_SEQ = 4095  # 12-bit max

    def __init__(self, machine_id: int):
        assert 0 <= machine_id <= 1023
        self.machine_id = machine_id
        self.last_ms = -1
        self.seq = 0
        self.lock = threading.Lock()

    def generate(self) -> int:
        with self.lock:
            now = current_ms()

            if now < self.last_ms:
                # Clock went backward — critical failure path
                raise ClockBackwardError(f"Clock skew: {self.last_ms - now}ms")

            if now == self.last_ms:
                self.seq = (self.seq + 1) & self.MAX_SEQ
                if self.seq == 0:
                    # Sequence exhausted — wait for next ms
                    now = wait_next_ms(self.last_ms)
            else:
                self.seq = 0

            self.last_ms = now
            ts = now - self.EPOCH

            return (ts << 22) | (self.machine_id << 12) | self.seq
```

### Machine ID Assignment via ZooKeeper

```
On startup:
1. Node connects to ZooKeeper
2. Creates ephemeral sequential znode: /idgen/nodes/machine-{id}
3. The znode sequence number becomes machine_id
4. On crash: ephemeral node deleted → machine_id freed for reuse
```

### Client SDK (with fallback)

```python
def get_id(retries=3):
    try:
        return id_service.generate()  # ~0.1ms p99
    except ServiceUnavailable:
        # Fallback: UUID v7 (time-ordered, 128-bit)
        # Flag the ID so downstream knows it's a fallback
        return uuid7()
```

### Decode ID (for debugging)

```python
def decode(id: int) -> dict:
    EPOCH = 1704067200000
    ts    = (id >> 22) + EPOCH
    mach  = (id >> 12) & 0x3FF
    seq   = id & 0xFFF
    return {"ts_ms": ts, "machine_id": mach, "sequence": seq,
            "datetime": datetime.fromtimestamp(ts/1000)}
```

---

## Trade-offs Table

| Decision | Chosen | Alternative | Why Chosen | Cost of Choice |
|----------|--------|-------------|------------|----------------|
| 64-bit int | ✅ Snowflake | UUID v4 (128-bit) | B-tree friendly, sortable, embeds timestamp | 69-year expiry; need epoch rotation plan |
| Machine ID via ZooKeeper | ✅ | Hardcoded env var | Self-healing on crash; no manual ops | ZK adds infra complexity; startup latency |
| In-memory sequence | ✅ | Redis INCR | Zero network hop; ~10ns vs ~1ms | Lost sequence on crash (fine — clock advances) |
| HTTP API | ✅ | In-process library | Language-agnostic; centralised monitoring | One extra network hop (~1ms) vs 0 |
| Timestamp in ms | ✅ | Timestamp in µs | 41 bits → 69 years sufficient | <4096 IDs per ms per node; µs buys more at the cost of fewer machine/sequence bits |
| UUID v7 fallback | ✅ | Block all writes | Availability over strict ID format | Downstream must handle 2 ID formats |

---

## Real-World References

| System | Approach | Key Detail |
|--------|----------|------------|
| **Twitter Snowflake** | 41ms + 10 machine + 12 seq | Original; open-sourced 2010 |
| **Instagram** | 41ms + 13 shard + 10 seq | Per-shard generation inside Postgres via `pl/pgsql` — no separate service |
| **Discord** | 42ms + 10 worker + 12 seq + 1 process | Epoch: 2015-01-01 |
| **Sonyflake** | 39ms (10ms units) + 8 seq + 16 machine | Longer machine ID space for huge clusters |
| **MongoDB ObjectID** | 4B ts + 5B machine + 3B seq | 96-bit; not integer-sortable in standard sense |
| **UUID v7 (RFC 9562)** | 48ms + 12 rand_a + 62 rand_b | No coordination, 128-bit, time-sorted |
