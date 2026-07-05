# Rate Limiter — Failure Analysis
**Date:** 2026-05-17 | Failure-First Design

---

## Failure Map

```
Config DB ──► Config Cache ──► Rate Limiter ──► Redis Cluster ──► Backend
    F1             F2              F3               F4              F5
```

---

## F1: Config DB Fails (Rules Source of Truth)

**Impact:** Can't fetch/update rate limit rules.

**What breaks:**
- New rules can't be written
- Config cache serves stale rules (which is OK for reads)

**Solutions:**
1. **Stale cache TTL extension** — on DB failure, extend TTL to 10 min (normally 60s)
2. **Hardcoded defaults** — fallback rule: 100 req/min per user if no rule found
3. **DB replica** — promote read replica for rule reads
4. **Multi-region Config DB** — active-active PostgreSQL with CockroachDB

**Trade-off:**
- Stale rules = wrong limits applied (acceptable — limits lag by minutes, not broken)
- Hardcoded defaults = may be too strict or too loose for some endpoints

---

## F2: Config Cache (Local In-Process) Fails

**Impact:** Every request hits Config DB directly.

**Detection:** Latency spike on rule lookups.

**Solution:**
- Config cache is in-process LRU (no external dep) — it can't "fail"
- But: cache miss storm after pod restart → staggered startup TTL
- Mitigation: Cache-aside with jitter on TTL (55-65s random) to prevent thundering herd

---

## F3: Rate Limiter Node Fails (App Layer)

**Impact:** 1/N of traffic loses rate limiting.

**Scenarios:**

### 3a: Node crashes mid-request
- Request already forwarded to backend, counter not incremented
- **Impact:** Slightly under-counted (acceptable)
- **Fix:** Decrement counter on backend failure response (best-effort)

### 3b: Rate limiter pod OOM / restart
- New pod starts with no local state (all state in Redis — stateless design)
- Recovery: instant — pull state from Redis on next request

### 3c: All rate limiter nodes fail
- **Fail Open:** Bypass rate limiter, all traffic passes → backend overload risk
- **Fail Closed:** Block all traffic → service unavailability
- **Recommended:** Fail open with circuit breaker + alert
  ```
  if (redis_unavailable && config_unavailable) {
    log.alert("Rate limiter degraded — fail open")
    allow_request()  // accept risk of abuse temporarily
  }
  ```
- **Why fail open?** Rate limiter failure ≠ intentional attack; better UX

---

## F4: Redis Fails (Counter Store)

Most critical failure. Redis is the heart of rate limiting.

### 4a: Single Redis Node Fails

**Setup:** Redis Sentinel (1 primary, 2 replicas)
```
Primary ──(async replication)──► Replica1, Replica2
         ◄── Sentinel monitors ──┘
```
- Sentinel detects failure in ~30s, promotes replica
- **Data loss window:** Last ~100ms of writes (async replication lag)
- **Impact:** ~100ms of requests not counted → slight over-admission
- **Mitigation:** Sync replication (WAIT command) for write-concern = 1 replica

### 4b: Redis Primary/Replica Network Split

```
Scenario: Primary alive but can't reach replicas
          Sentinel promotes replica → SPLIT BRAIN
          Both nodes accept writes
```
- **Solution:** Sentinel quorum (need 2/3 sentinels to agree on promotion)
- **MIN_SLAVES_TO_WRITE=1:** Primary rejects writes if can't reach replica
  - Trade-off: Availability ↓ but no split brain

### 4c: Redis Cluster Node Fails (1 of 3 shards down)

```
Cluster: Shard1 (slots 0-5460) | Shard2 (5461-10922) | Shard3 (10923-16383)
         If Shard2 fails → users hashing to slots 5461-10922 unprotected
```
- **With replicas:** Auto-failover in ~15s (cluster-node-timeout)
- **Without replicas:** Those slot ranges fail → fail open for affected users
- **Real world:** Run 3 shards × 3 replicas = 9 nodes total

### 4d: Redis Cluster Overload (Hot Key)

```
Problem: celebrity user / IP making 100K RPS all hitting same shard
Key: "rl:user_vip:POST/api/tweet:..." → shard 2 saturated
```
- **Solution A:** Local rate limiting first (per-pod counter, 10ms window)
  - If local count > limit/num_pods × 1.5 → DENY without Redis
- **Solution B:** Counter sharding — split one key into N sub-keys
  ```
  rl:user123:POST:1716000060:shard0
  rl:user123:POST:1716000060:shard1
  ...shard9
  Total = sum of all shards (read all, write one random)
  ```
- **Trade-off:** Solution B adds N Redis reads — use only for known hot keys

### 4e: Redis Latency Spike (>10ms)

- **Cause:** Memory pressure, network congestion, GC pause
- **Detection:** P99 latency monitoring on Redis ops
- **Mitigation:**
  1. Timeout + fail open: `redis.set_timeout(2ms)` — if >2ms, allow request
  2. Local cache fallback: use last-known counter + local increment
  3. Pre-emptive: Reserve 20% Redis memory for fragmentation

---

## F5: Backend Services Fail

Rate limiter allows request, backend dies.

**Impact:** Rate limit was "consumed" for a failed request.

**Solution:**
- Rate limiter is a gateway concern — does NOT refund tokens on backend failure
- Why? Backend failure ≠ rate limit credit; could be abused
- **Exception:** 5xx from backend within 10ms → retry with same request_id (deduped)

---

## F6: Clock Skew (Distributed Time Problem)

**Problem:** Fixed window uses timestamps. If nodes have different clocks:
```
Node1 thinks window resets at T=60, Node2 thinks T=62
→ 2-second window where both accept requests → over-admission
```

**Solution:**
1. Use **Redis server time** (not client time): `TIME` command before INCR
2. NTP sync on all nodes (max drift: 1ms with chrony)
3. Use relative TTL (`EXPIRE`) not absolute timestamps for window resets

---

## F7: Thundering Herd After Redis Restart

**Problem:** Redis restarts empty. All counters gone. Clients hammer backend.

**Solution:**
- Redis persistence: RDB snapshot + AOF for counter recovery
- **Trade-off:** AOF fsync adds ~1ms write latency
- Alternative: Accept the brief over-admission window (< 60s) — counters self-heal

---

## F8: Config Deployment — New Rule Doesn't Propagate

**Problem:** Admin sets limit to 10 req/min (down from 1000). Cached nodes still have old rule for 60s.

**Solution:**
1. Rule change → publish invalidation event to all nodes via Redis Pub/Sub
2. Nodes subscribe to `config:invalidate` channel → clear local cache
3. **Fallback:** TTL ensures max 60s staleness even without pub/sub

---

## Failure Severity Matrix

| Failure | Severity | Impact | MTTR | Mitigation |
|---|---|---|---|---|
| Redis primary fails | HIGH | 30s unprotected | 30s | Sentinel auto-failover |
| Redis cluster shard fails | HIGH | 1/3 users unprotected | 15s | Replica per shard |
| Config DB fails | LOW | Stale rules | N/A | Cache TTL extension |
| Rate limiter node fails | LOW | 1/N traffic unprotected | Instant | Stateless + Redis |
| Clock skew | MEDIUM | Over-admission | N/A | Redis server time |
| Hot key | MEDIUM | Shard saturation | N/A | Local pre-filter |
| Redis OOM | CRITICAL | All traffic unprotected | 5 min | Memory reservation |

---

## Chaos Engineering Checklist

- [ ] Kill Redis primary → verify Sentinel failover < 30s
- [ ] Kill 1 of 3 Cluster shards → verify 2/3 users still protected
- [ ] Restart rate limiter pod → verify stateless recovery
- [ ] Inject 50ms Redis latency → verify fail-open behavior
- [ ] Send 10x traffic spike → verify no shard saturation
- [ ] Deploy new rule → verify propagation within 60s

