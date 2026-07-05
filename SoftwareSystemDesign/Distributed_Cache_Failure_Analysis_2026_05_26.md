# Distributed Cache — Failure Analysis
**Date:** 2026-05-26

---

## Failure Framework

For each component, ask three questions:
1. **What fails?** (the fault)
2. **What breaks downstream?** (blast radius)
3. **How do we prevent / detect / recover?** (mitigation)

---

## 1. Cache Node Failure

### Fault
A cache node crashes (OOM kill, hardware failure, network partition).

### Blast Radius
- All keys on that shard → **cache MISS**
- Traffic falls through to DB
- If popular shard: DB overload → cascading failure

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | Primary + Replica per shard. On primary death, replica promotes. |
| **Detection** | Health check heartbeats every 1s. Sentinel / cluster gossip detects failure in 5–15s. |
| **Recovery** | Auto-promotion of replica (Redis Sentinel). Re-warm cache from DB with rate-limited reads. |
| **Design** | Circuit breaker in client: if cache down → go to DB but rate-limit; don't allow DB stampede. |

**Redis Sentinel:** 3 Sentinel nodes vote. Quorum = 2. If primary unreachable for >30s, Sentinel promotes replica and updates clients via pub/sub.

---

## 2. Cache Cluster Partition (Split-Brain)

### Fault
Network split divides cluster into two halves. Both halves think the other's primary is dead and elect new primaries.

### Blast Radius
- Two primaries accepting writes → divergent data
- After partition heals: conflict, data loss

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | Odd number of nodes (3, 5) for quorum. Raft-based leader election. |
| **Detection** | Monitor write acknowledgements — if drops, suspect partition. |
| **Recovery** | Last-write-wins (timestamp) or application-level merge. |
| **Design** | Redis Cluster uses 16384 hash slots. Minority partition → refuses writes (CP over AP for write path). |

---

## 3. Hot Key / Hot Shard

### Fault
One key (viral content, global counter) receives 100× more traffic than other keys. Its shard becomes CPU-bound.

### Blast Radius
- Shard CPU at 100% → high latency for all keys on that shard
- Not just the hot key — **innocent bystander keys** also degrade

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | Key replication: store N copies `key_0..key_N`, read randomly. |
| **Detection** | Shard-level CPU/QPS monitoring; alert on outlier shards. |
| **Recovery** | Dynamically split hot shard. Promote more vnodes to distribute keys. |
| **Design** | Local in-process cache (Guava, Caffeine) in app server for top-K keys — absorbs final mile. |

---

## 4. Cache Stampede (Thundering Herd)

### Fault
High-traffic key expires simultaneously for all requesters.

### Blast Radius
- N concurrent MISSes → N DB queries → DB overload
- DB crashes → entire service down

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | TTL jitter: `ttl = base_ttl + random(0, base_ttl * 0.1)` to spread expiries. |
| **At MISS** | Mutex/distributed lock: only one request fetches DB; others wait or serve stale. |
| **Background refresh** | Probabilistic early expiry (XFetch algorithm). |
| **Stale serving** | Serve stale value while async refresh happens (acceptable for feeds, not transactions). |

---

## 5. Client-Side Failures

### Fault A: Client Can't Reach Cache (Network issue)
- **Blast Radius:** Every request bypasses cache → DB takes full load
- **Fix:** Client-side circuit breaker. After N failures, open circuit → DB-only mode. Close after 30s probe.

### Fault B: Serialisation Bug (Bad data written)
- **Blast Radius:** Reads get corrupt values; application crashes on deserialisation
- **Fix:** Versioned value schema (`v1:...`). On MISS or decode error, fall back to DB. Alert on decode error rate spike.

### Fault C: Cache Poisoning (Stale write)
- **Blast Radius:** Wrong data served, hard to detect
- **Fix:** Conditional SET (`SET key val NX` or compare-and-swap with version token). DB is source of truth; use TTL as safety net.

---

## 6. Memory Exhaustion (OOM)

### Fault
Cache node runs out of memory. OS OOM killer kills the process.

### Blast Radius
- Node dies → same as node failure above
- If eviction policy wrong: important keys evicted, DB bombarded

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | Set `maxmemory` + eviction policy in Redis config. Alert at 80% usage. |
| **Detection** | Memory usage metric per node. Alert before OOM. |
| **Recovery** | Scale out: add shard. Or increase node RAM. |
| **Design** | Separate caches for different TTL tiers (hot session vs. long-lived config) to avoid cross-contamination. |

---

## 7. Replication Lag

### Fault
Primary-replica replication is async. Primary dies; replica promoted but is 5s behind.

### Blast Radius
- 5s of writes lost (RPO = 5s)
- Reads from new primary = slightly stale
- For strong-consistency use cases: data inconsistency

### Mitigation
| Layer | Solution |
|---|---|
| **Prevention** | Use sync replication for critical keys (Redis `WAIT` command: blocks until N replicas ack). |
| **Detection** | Monitor replication offset lag metric. Alert if lag > threshold. |
| **Recovery** | Re-fetch from DB on version mismatch. |
| **Design** | Clearly separate: cache for *best-effort* fast reads vs. DB for *authoritative* reads. Application layer decides. |

---

## 8. Cascading Failure Pattern

```
Cache node dies
  → Cache miss rate spikes to 100% on that shard
    → DB connection pool exhausted
      → DB query latency rises
        → App servers timeout waiting
          → App servers retry (amplifying load)
            → Full outage
```

**Prevention Stack:**
1. **Retry with exponential backoff + jitter** — don't amplify load
2. **Circuit breaker** — stop hammering dead dependencies
3. **Load shedding** — drop low-priority requests under extreme load
4. **Bulkhead** — isolate cache connections per service; one service's stampede doesn't kill others

---

## 9. Failure Matrix Summary

| Failure | Detection | Prevention | Recovery | RTO |
|---|---|---|---|---|
| Node crash | Heartbeat / gossip | Primary + Replica | Auto-promote | 15–30s |
| Network partition | Split-brain detection | Quorum writes | Conflict resolution | Minutes |
| Hot key | Shard CPU monitor | Key replication | Dynamic re-sharding | Immediate |
| Stampede | DB latency spike | TTL jitter + mutex | Stale serve | Seconds |
| OOM | Memory metric | maxmemory + alert | Scale out | Hours |
| Replication lag | Offset lag metric | Sync replication | DB fallback | Seconds |
| Cascading | Error rate / latency | Circuit breaker | Load shed | Minutes |
