# Distributed ID Generator — Failure Analysis
*Date: 2026-06-28*

---

## Failure Map

```
Client → Load Balancer → ID Generator Node → ZooKeeper
   ↑                           ↑
   └── SDK Fallback        Local Clock (NTP)
```

Failures by component:

1. ID Generator Node crashes
2. Clock goes backward (NTP correction)
3. ZooKeeper is unavailable
4. Machine ID conflict (duplicate assignment)
5. Sequence exhaustion
6. Load balancer fails
7. Client gets wrong ID (silent corruption)
8. Epoch overflow (69-year timebomb)

---

## 1. ID Generator Node Crashes

**What happens:**
- In-flight requests fail with connection reset
- In-memory sequence state is lost
- Ephemeral ZooKeeper znode is deleted → machine ID freed

**Impact:**
- Requests routed to this node fail until it restarts (~10–30s)
- If only 1 node exists → all clients block

**Prevention:**
- Deploy ≥2 nodes behind a load balancer (N+1 redundancy)
- Client SDK: retry on failure with exponential backoff (max 3 retries × 50ms)
- If all nodes fail: UUID v7 fallback in SDK

**Recovery:**
- Node restarts → requests ZooKeeper for new machine ID (may get different ID from before — that's fine, machine IDs are just uniqueness tokens)
- New node ID is different → sequence starts at 0 → no collision

**Remaining risk:** Both nodes fail simultaneously. Mitigation: multi-AZ deployment (one node per AZ).

---

## 2. Clock Goes Backward (NTP Correction)

**What happens:**
- OS NTP daemon steps the clock backward by N ms to correct drift
- Generator sees `current_ms < last_ms` → ClockBackwardError

**Why this is dangerous:**
- If we silently use `last_ms` as current time, IDs are still correct
- If we ignore it, two requests at "same" ms get same sequence → **collision if sequences also match** (extremely unlikely but possible under load)

**Mitigation strategies (in order of preference):**

| Strategy | Mechanism | Trade-off |
|----------|-----------|-----------|
| **Spin-wait** | `while current_ms < last_ms: sleep(1ms)` | Adds latency ≤ NTP correction (~128ms max) |
| **Refuse + alert** | Raise exception, page on-call | No incorrect IDs; service disruption |
| **Use last_ms** | Treat backward clock as same ms | Safe if correction < 1ms and seq space available |
| **Hardware clock (TSC)** | Use CPU Time Stamp Counter | Monotonic, ~0 drift, requires kernel tuning |

**Real-world:** Twitter's original Snowflake raises an exception and waits. Discord uses a monotonic clock source, ignoring wall time corrections.

**AWS context:** EC2 uses `chrony` (not ntpd). Maximum correction is gradual slew (±500ppm) — never a step > 1ms unless instance was paused/migrated. On instance migration: clock can jump. Mitigation: detect VM migration via metadata service, briefly pause ID generation.

---

## 3. ZooKeeper Unavailable

**What happens:**
- New generator nodes cannot start (can't get machine ID)
- Running nodes are unaffected (machine ID already claimed in memory)

**Impact:**
- Zero impact on in-flight ID generation
- New deployments / restarts blocked

**Prevention:**
- ZooKeeper ensemble: 3 or 5 nodes (tolerates 1 or 2 failures)
- Cache machine ID in local disk on successful assignment: `echo $MACHINE_ID > /var/lib/idgen/machine_id`
- On startup: if ZooKeeper unreachable, read from disk cache (with TTL check)

**Recovery:**
- ZooKeeper restores → new nodes can register
- Disk-cached nodes continue without ZooKeeper indefinitely

**Risk of disk cache:** Two containers accidentally start with same cached ID after a cloning event. Mitigation: include container ID in machine ID hash, detect collision at ID-decode time.

---

## 4. Machine ID Conflict (Duplicate Assignment)

**What happens:**
- Two generator nodes have same machine ID → IDs with same {timestamp, machineID} can have duplicate sequences → **collision**

**How it happens:**
- Hardcoded machine IDs in config (ops error)
- ZooKeeper session expired silently; node keeps old ID; another node claims same ID
- Container clone / snapshot of a running node

**Detection:**
- Downstream collision detection: unique constraint in DB catches it (noisy but late)
- ID audit service: periodically sample IDs and check {ts, machID} uniqueness

**Prevention:**
- Never hardcode machine IDs in production
- ZooKeeper heartbeat: if heartbeat fails > 30s, node pauses ID generation until re-registered
- ID generation node: on ZK session expiry event → immediately stop generating

**Recovery:**
- Identify conflicting node via logs
- Forcibly kill one node
- Audit recent IDs for duplicates; re-generate any conflicted entity (usually idempotent)

---

## 5. Sequence Exhaustion

**What happens:**
- 4,096 IDs/ms/node exhausted → generator must wait for next millisecond

**When does it happen?**
- Single node: >4,096 req/ms = >4.096M req/sec per node
- This is practically impossible for a single service to trigger without hardware-saturating the node first

**Real risk scenario:**
- Thundering herd: 100 client services all burst simultaneously → 10M req/sec to single node
- Mitigation: LB distributes across nodes; single node never sees full load

**If it does happen:**
- Generator spins: `while current_ms() == last_ms: pass` (busy-wait)
- Max wait: 1ms → p100 latency spike of 1ms, completely acceptable

**Capacity planning:**
- At 10 nodes × 4M IDs/sec = 40M IDs/sec cluster capacity
- Twitter peak: ~500K/sec → 80× headroom

---

## 6. Load Balancer Fails

**What happens:**
- All clients lose connectivity to ID service
- ID generation stops

**Prevention:**
- AWS ALB: managed service, 99.99% SLA, multi-AZ by default
- Health checks: ALB pings `/health` on each node every 30s; removes unhealthy nodes

**Recovery:**
- If ALB itself fails: DNS failover to secondary ALB in another region (~60s TTL)
- Client-side: SDK has hardcoded fallback endpoints for direct-to-node connection if LB is unreachable >500ms

---

## 7. Silent ID Corruption (Bit Flip / Network Corruption)

**What happens:**
- Network or memory error flips a bit in ID → wrong timestamp or machine ID decoded, but value still "looks valid"

**Prevention:**
- TCP provides checksum — network corruption is extremely unlikely
- Application layer: embed ID checksum in high-value entities (orders, payments) as a secondary field
- At-rest: IDs stored in 64-bit integer columns (not strings) — no encoding corruption

---

## 8. Epoch Overflow (69-Year Timebomb)

**What happens:**
- Epoch is Jan 1 2024. 41-bit ms timestamp overflows after 69 years → **~Jan 2093**
- After overflow: timestamp wraps → IDs appear from 1970 → sort order breaks → index corruption

**This is a known, planned problem (like Y2K).**

**Mitigation options:**

| Option | When | How |
|--------|------|-----|
| **Re-epoch** | ~2083, 10 years before overflow | Change epoch constant → IDs restart from 0 for new epoch |
| **Extend to 63-bit** | Any time | Use 42-bit timestamp (140 years) by stealing 1 bit from machine or sequence |
| **Migrate to UUID v7** | If 64-bit becomes limiting | 128-bit, time-ordered, no expiry concern |
| **Separate ID ranges** | Before overflow | "New IDs" start at 2^63 + offset, "old IDs" < 2^62 |

**Engineering blog note:** Discord documented this in 2023 as a future concern and plans to re-epoch in 2053 (halfway point), giving a clean migration window.

---

## Failure Summary Table

| Failure | Probability | Impact | Detection | MTTR |
|---------|------------|--------|-----------|------|
| Node crash | Medium | Medium (partial) | ALB health check <30s | 30s restart |
| Clock backward | Low | Low-Medium | Internal exception | Auto-wait ≤128ms |
| ZooKeeper down | Low | Low (running nodes unaffected) | ZK health check | Minutes (ZK HA) |
| Machine ID conflict | Very Low | High (silent collision) | DB unique violation | Hours (audit) |
| Sequence exhaustion | Extremely Low | Low (1ms wait) | Latency spike | Automatic 1ms |
| LB failure | Very Low | High (complete outage) | Synthetic probe | 60s DNS failover |
| Epoch overflow | Planned 2093 | High (sort corruption) | None (planned) | 10yr migration |
