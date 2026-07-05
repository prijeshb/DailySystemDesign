# Online Code Judge — Failure Analysis
**Date:** 2026-06-05

---

## Failure-First Framework

For each component, ask:
1. What happens if **this component fails**?
2. What happens if the component **before it fails**?
3. What happens if the component **after it fails**?

---

## Component 1: API Gateway

### This fails
- Users cannot submit or load problems
- **Mitigation:** Multiple instances behind a load balancer with health checks; circuit breaker returns 503

### Before it (Client) fails
- Duplicate submissions from retry (browser double-click, network timeout)
- **Mitigation:** Idempotency key (UUID per submit action) checked in Redis before inserting

### After it (Submission Service) fails
- Gateway returns 502 to user
- **Mitigation:** Retry with exponential backoff; submission not created → safe to retry

---

## Component 2: Submission Service

### This fails (crashes after Kafka publish but before DB write)
- Kafka has message, DB has no record → judge runs → tries to update non-existent row
- **Mitigation:** Write DB first (status=PENDING), then publish Kafka. If Kafka publish fails → mark DB as FAILED, tell user to resubmit.
- **Outbox Pattern:** Write submission + Kafka message atomically in DB (outbox table), separate process polls and publishes.

### Before it (API Gateway) fails
- No new submissions reach service — graceful, no data loss

### After it (Kafka) fails
- Messages cannot be enqueued → submission stays PENDING in DB
- **Mitigation:** Service retries Kafka publish with exponential backoff; DB row persists so user can poll

---

## Component 3: Kafka (Message Queue)

### This fails (broker down)
- New submissions can't be enqueued
- Workers stop getting jobs → submissions queue in DB as PENDING
- **Mitigation:** Kafka cluster with 3 brokers + replication factor 3; leader election < 30s
- **Fallback:** Submission Service holds message in memory retry buffer (bounded queue, shed load if full)

### Partition becomes unavailable
- Only submissions for that partition stall
- **Mitigation:** Kafka auto-reassigns partition leader within ~30s

### Messages delivered twice (at-least-once)
- Judge processes same submission twice → duplicate DB write
- **Mitigation:** Worker checks Redis `SET NX` on submission_id before processing (idempotent dedup lock, TTL = 10min)

---

## Component 4: Judge Worker

### Worker crashes mid-execution
- Kafka offset not committed → message redelivered to another worker
- Duplicate dedup key already in Redis → second worker skips → submission stays PENDING forever
- **Fix:** Redis dedup key has TTL (10min). On crash, TTL expires → next delivery processes it.
- Worker sets DB status=RUNNING at start. If stuck in RUNNING > 5min → cron job resets to PENDING and re-publishes.

### Sandbox escape (security failure)
- Malicious code breaks out of nsjail
- **Mitigation:** Defense in depth:
  1. nsjail restricts syscalls (seccomp whitelist)
  2. Worker runs on isolated VM/node (not shared with DB or queue)
  3. Network policy: judge nodes have no egress to internal services
  4. Node recycled after N executions (prevents state accumulation)

### Worker runs out of disk (test case cache fills up)
- LRU eviction policy on local cache; max 80% disk use threshold → evict oldest problems

### Time limit bypass (sleep/fork bomb)
- Wall-clock SIGKILL enforced by worker parent process (not inside sandbox)
- Fork bomb: nsjail PID namespace + max PIDs limit (e.g., 64 PIDs)
- Memory bomb: cgroup memory.limit_in_bytes → process killed with MLE

### Worker pool exhaustion (all workers busy during contest)
- New jobs pile up in Kafka → latency spikes
- **Mitigation:** Auto-scale on `consumer_lag` metric (e.g., Kubernetes HPA); pre-warm before contest

---

## Component 5: Result Database (Postgres)

### Primary fails
- Reads degrade; writes fail
- **Mitigation:** Postgres with synchronous replica; automatic failover (Patroni/RDS Multi-AZ)
- Workers retry result write with backoff (up to 3 retries)

### Submission table too hot (write bottleneck during contest)
- 10K submissions/min = ~167 writes/sec → Postgres handles this fine up to ~5K writes/sec
- At 50K/min: shard by user_id or use append-optimized Postgres partition by created_at

### Dirty read / race condition (two workers write same submission)
- Use `UPDATE submissions SET ... WHERE id=$1 AND status='RUNNING'` — only one wins; second becomes no-op

---

## Component 6: WebSocket Gateway (Result Push)

### Gateway crashes with active connections
- Users lose live updates; fall back to polling
- **Mitigation:** Client-side fallback: if WS disconnects, poll `/submissions/{id}` every 2s

### Result message lost (Kafka consumer dies between consume and WS push)
- User never sees verdict
- **Mitigation:** Client polls after WS disconnect; WS is "nice to have" — DB is source of truth

---

## Component 7: S3 (Test Case & Code Storage)

### S3 unavailable
- Judge workers can't fetch test cases → all submissions fail
- **Mitigation:** Local SSD cache on each worker covers popular problems; uncommon problems queue up
- Workers emit `SYSTEM_ERROR` verdict (not WA/TLE) — admin can re-judge after recovery

### Test case updated mid-contest
- Some workers run old, some run new test cases → inconsistent verdicts
- **Mitigation:** Test case versioning + atomic swap: upload new version → update `problem.test_case_version` in Redis in single transaction → workers detect version mismatch and refresh

---

## Component 8: Redis (Dedup + Leaderboard)

### Redis crashes
- Dedup lost → possible duplicate judge runs (idempotent DB update = safe, but wastes compute)
- Leaderboard data lost → rebuild from Postgres submissions table (slow ~10s)
- **Mitigation:** Redis Sentinel or Cluster; leaderboard periodically snapshotted to Postgres

---

## Cascading Failure Scenario: Contest Starts

```
T+0:00  Contest starts — 50K users hit submit simultaneously
T+0:01  API Gateway rate limit kicks in (per-user: 1 req/sec) — absorbs burst
T+0:02  Kafka topic has 10K messages/min backlog
T+0:03  Judge workers auto-scale: 10 → 100 instances (30s lag)
T+0:03  During scale-up: submissions queue in Kafka, users see "Pending"
T+0:05  S3 GetObject throttled → test case fetches slow
T+0:05  Workers serve from local SSD cache — top 100 problems cached
T+0:10  Worker pool fully scaled, lag clears, verdicts flowing
T+0:30  Redis leaderboard at 50K ZADD ops/min — single-shard limit hit
T+0:30  Shard leaderboard by contest_id across 3 Redis nodes
```

---

## Summary: Failure Modes & Mitigations

| Component | Key Failure | Mitigation |
|---|---|---|
| API Gateway | Duplicate submits | Idempotency key in Redis |
| Submission Service | Kafka publish fails | Outbox pattern / retry |
| Kafka | Broker down | 3-node cluster, RF=3 |
| Kafka | Duplicate delivery | Worker dedup via Redis NX |
| Judge Worker | Crash mid-run | RUNNING timeout + re-queue |
| Judge Worker | Fork bomb | nsjail PID limit |
| Judge Worker | Infinite loop | Wall-clock SIGKILL |
| Judge Worker | Pool exhaustion | Autoscale on consumer lag |
| Postgres | Primary failure | Synchronous replica, auto-failover |
| S3 | Unavailable | Local SSD cache on workers |
| Redis | Crash | Sentinel; leaderboard rebuild from DB |
| WebSocket | Disconnect | Client falls back to polling |
