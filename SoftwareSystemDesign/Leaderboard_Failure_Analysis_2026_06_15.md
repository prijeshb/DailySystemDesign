# Real-time Leaderboard — Failure Analysis
**Date:** 2026-06-15

---

## Component Dependency Map

```
Components:
  A. Redis Cluster (leaderboard store)
  B. Score Service (write path)
  C. Kafka + Score Consumer (async persistence)
  D. PostgreSQL (durable score store)
  E. Leaderboard Service (read path)
  F. Player Service (metadata)
  G. Game Servers (upstream score producers)

Write path:  G → B → A → Kafka → Consumer → D
Read path:   Client → E → A → F
```

---

## A. Redis Failure

### A1: Single Redis Node OOM

**Scenario:** Memory spikes during tournament — 10M players all scoring simultaneously. Redis hits `maxmemory`.

**What Redis does by default (`maxmemory-policy = allkeys-lru`):**
- Evicts least-recently-used members from any key
- A sorted set member (a player's score) gets evicted silently
- Player shows up with rank = nil → "unranked" displayed to user

**Why this is worse than an error:**
Silent data corruption. Score existed, now missing. Can't distinguish "never scored" from "evicted."

**Prevention:**
```
maxmemory-policy noeviction
# Redis now returns OOM error on new writes rather than evicting
# Score Service catches OOM → returns 503 to Game Server → retry later
# Data integrity preserved
```

Alert at 70% memory utilization. Auto-scale Redis instance before hitting limit.

**Recovery:**
Replay Kafka `score.submitted` topic from last PostgreSQL checkpoint to rebuild sorted sets.

---

### A2: Redis Primary Fails (Redis Cluster Failover)

**Scenario:** One Redis primary dies. Redis Cluster promotes a replica.

```
Before:                    After failover:
Slot 0-5460:  Primary-1    Slot 0-5460:  Replica-1A promoted → Primary
              Replica-1A              ↑
              Replica-1B         ~10-30s failover window
```

**Impact during failover (10-30s):**
- Writes to that slot: `CLUSTERDOWN` or `MOVED` errors
- Score Service: must retry with exponential backoff

**Data loss risk:**
Redis async replication — primary may have processed writes that haven't reached replica yet. Default: up to last async batch is lost.

**Mitigation:**
For high-stakes writes (tournament finals):
```redis
WAIT 1 0  # sync at least 1 replica before returning success
          # 0 timeout = non-blocking check (use with caution — can block under load)
```

For normal gameplay: accept potential loss of 1-2 score updates on failover. The score will be resubmitted on next match anyway.

**Trade-off of WAIT:** Adds latency on every write. Reserve for "this match determines tournament winner" writes only.

---

### A3: Redis Network Partition (Split-Brain)

**Scenario:** Network partition between Redis nodes. Score Service can still reach one set of nodes; those nodes can't reach each other.

**With Redis Cluster + quorum slots:**
- Cluster requires majority of masters reachable to accept writes
- Minority partition becomes read-only → Score Service gets write errors
- Better than silent split-brain

**Prevention:**
- Odd number of Redis masters (3 or 5) to ensure quorum
- `cluster-require-full-coverage no` → partial cluster still serves reads for available slots

---

## B. Score Service Failure

### B1: Score Service Crashes Mid-Write

**Scenario:** Service crashes after writing to Redis but before publishing to Kafka.

**Impact:** Score in Redis (live leaderboard correct) but not in PostgreSQL (persistence gap).

**Detection:** On restart, compare Redis ZSCORE vs PostgreSQL score for recent players.

**Prevention — Transactional Outbox Pattern:**
```
Score Service:
  1. Redis ZADD (fast path)
  2. Write event to outbox table in PostgreSQL within DB transaction
  3. Outbox poller publishes to Kafka

→ If crash after step 1 but before step 2:
  Redis has score, PostgreSQL doesn't.
  On restart: replaying Kafka won't help (event never published).
  
Alternative: Accept eventual consistency.
  Periodic reconciler runs every 5min:
  scan recent Kafka events → compare to PostgreSQL → fix gaps
```

**Simpler approach for most cases:**
- Redis AOF persistence (every write persisted to disk)
- On restart, Redis reloads from AOF — leaderboard intact without Kafka replay

---

### B2: Duplicate Score Submission

**Scenario:** Game Server submits score, network timeout, retries. Score submitted twice with same idempotency_key.

```
Game Server:  POST /scores {idempotency_key: "match_7743_player_123"}
Score Service:  
  1. SET idem:match_7743_player_123 1 NX EX 86400
  2. If returns 0 → already processed → return 200 OK (idempotent)
  3. If returns 1 → new → proceed with ZADD GT
```

**ZADD GT as second defense:**
Even without idempotency key, `ZADD GT` means duplicate submission of same score = no-op.

**What if score increases between retries?**
Idempotency key locks to first-seen value. Only final submitted score counts. For games where players improve score within a session, use last-writer-wins (no idempotency key, just ZADD GT on every submission).

---

## C. Kafka / Consumer Failure

### C1: Kafka Cluster Unavailable

**Scenario:** Kafka is down. Score Service can't publish.

**Write path impact:** Redis writes succeed (leaderboard live) but persistence to PostgreSQL halted.

**Score Service behavior:**
```
try:
    kafka.produce(score_event)
except KafkaUnavailable:
    # Do NOT fail the write — Redis already updated
    # Log to dead-letter store (e.g., local disk buffer or Redis list)
    RPUSH dl:score_events {event_json}
    # Background job retries from dead-letter list when Kafka recovers
```

**Recovery:** When Kafka recovers, drain dead-letter list in order. ZADD GT ensures out-of-order replays are safe.

---

### C2: Score Consumer Falls Behind (Lag)

**Scenario:** Consumer is slow; Kafka lag grows. PostgreSQL is N minutes behind Redis.

**Impact:** Only affects historical reporting and analytics — live leaderboard (Redis) is unaffected.

**Detection:** Alert on consumer group lag > 100K events or > 5 min.

**Prevention:**
- Consumer group with multiple partitions → parallel consumption
- ClickHouse (analytics) consumer can be deprioritized without affecting live leaderboard

---

## D. PostgreSQL Failure

### D1: PostgreSQL Primary Down

**Scenario:** PostgreSQL primary fails. Replica promotion takes 30-60s.

**Impact on live leaderboard:** Zero — reads and writes go through Redis. PostgreSQL is only the persistence layer.

**Impact:** Score events pile up in Kafka. Consumer retries until PostgreSQL is back.

**Recovery:**
- Promote read replica to primary (automated with Patroni/pg_auto_failover)
- Consumer resumes from last committed Kafka offset
- No leaderboard data loss (Redis intact, Kafka has all events)

---

## E. Leaderboard Service Failure

### E1: Leaderboard Service Crashes

**Scenario:** All Leaderboard Service pods crash (bad deploy, OOM).

**Impact:** Clients can't fetch leaderboards.

**Mitigation:**
- Kubernetes auto-restarts pods (should recover in 10-30s)
- CDN-cached top-100 responses (30s TTL) serve stale but valid data during restart
- Circuit breaker at API gateway: fail fast with cached response

---

## F. Upstream Failure (Game Server)

### F1: Game Server Can't Reach Score Service

**Scenario:** Score Service is up but network issue prevents Game Server from submitting.

**Impact:** Scores not submitted, players unranked for that session.

**Game Server behavior:**
```
# Retry with exponential backoff
for attempt in 1..5:
    try POST /scores
    if success: break
    sleep(2^attempt seconds + jitter)

# If all retries fail:
Queue event locally → retry on next game start
Accept: some scores lost if game server crashes before retry
```

---

## F2: Score Service Overloaded (Viral Tournament)

**Scenario:** 10x normal traffic during a major tournament. Score Service can't keep up.

**Load shedding strategy:**
```
Rate limit per game_id:
  Normal: 50K updates/sec
  Tournament mode: 500K updates/sec (pre-scaled)

If still overloaded:
  Accept only scores that improve player's existing rank (ZSCORE check before ZADD)
  Reject "definitely won't change top-1000" scores during peak
  → protects top-leaderboard accuracy at cost of long-tail accuracy
```

**Pre-scaling:** Known tournaments → scale Redis and Score Service before event. Monitor game lobby fill rate as leading indicator.

---

## Full Failure Matrix

| Component | Failure Mode | Live Leaderboard | Score Persistence | Recovery |
|-----------|-------------|------------------|-------------------|----------|
| Redis Node | Single node OOM | Partial (evicted members show nil) | Unaffected | `noeviction` policy + scale up |
| Redis Primary | Failover | 10-30s degraded | Kafka buffers events | Auto-failover + WAIT for critical writes |
| Score Service | Crash | Last Redis state preserved | Events queued in Kafka | Auto-restart, replay from Kafka |
| Kafka | Full outage | Unaffected | Halted (events in dead-letter) | Drain dead-letter on recovery |
| PostgreSQL | Primary down | Unaffected | Consumer retries | Patroni failover, consumer resumes |
| Leaderboard Svc | Crash | CDN cache serves stale | Unaffected | Auto-restart |
| Game Servers | Can't submit | Stale scores | Retry queue locally | Exponential backoff |

---

## Chaos Engineering Checklist

1. Kill Redis primary node → verify failover time < 30s, no data corruption
2. Send duplicate score_events with same idempotency_key → verify single ZADD
3. Fill Redis memory to 95% → verify `noeviction` returns OOM error, not silent eviction
4. Kill Kafka cluster → verify live leaderboard unaffected, dead-letter buffer fills
5. Replay old Kafka events out of order → verify ZADD GT ignores lower scores
6. Network partition between Redis cluster nodes → verify cluster refuses minority-partition writes
7. Corrupt one sorted set member → verify reconciler detects mismatch vs PostgreSQL
