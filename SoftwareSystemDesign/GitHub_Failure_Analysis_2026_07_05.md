# GitHub — Failure Analysis
**Date:** 2026-07-05

---

## Failure Map

```
[Client] → [Git Server] → [Object Store (S3)] → [Postgres] → [Redis] → [Kafka] → [Webhooks/CI]

Component fails → what breaks, what survives, how to recover
```

---

## F1: Git Push Server Crashes Mid-Upload

**Component:** Git HTTP/SSH server  
**Trigger:** OOM, hardware failure, deploy during active push

**Blast radius:**
- Active pushes: broken pipe error to client
- Completed pushes on other servers: unaffected
- Repo state: unchanged (refs not updated)

**Why no corruption:**
```
Push sequence (order matters):
  Step 1: Receive objects → write to server temp dir
  Step 2: Validate pack integrity (SHA verification)
  Step 3: Upload objects to S3 (atomic per object)
  Step 4: UPDATE refs SET sha=new_sha WHERE repo_id=X AND name='refs/heads/main'
  
Crash at step 1: temp dir lost, S3 untouched, refs unchanged → clean state
Crash at step 2: same as step 1
Crash at step 3: partial S3 writes → dangling objects → cleaned by weekly GC
Crash at step 4: all objects in S3, ref unchanged → client retries, re-uploads nothing (server has objects), ref update succeeds
```

**Client behavior:** `git push` retries automatically (built into git protocol).  
**Re-push efficiency:** "have/want" negotiation means only missing objects re-sent (usually near zero).

**Detection:** LB health check fails → remove from rotation in 10s.

---

## F2: Object Store (S3) Unavailable

**Component:** AWS S3  
**Trigger:** S3 regional outage (rare but ~4 events historically per year across all regions)

**Blast radius:**
- New pushes: fail (can't store objects)
- Clones: fail (can't serve objects)
- Web UI: can load repo metadata (from Postgres/Redis), but not file contents
- PR diff view: fails if diff not cached in Redis

**What survives:**
- Repo list, PR list, user profiles (Postgres)
- Cached diffs in Redis (TTL 24h)
- Webhook deliveries already queued in Kafka (consumers backlog, deliver on recovery)

**Mitigation:**
```
Option 1: Multi-region S3 replication
  Objects replicated to us-east-1 + eu-west-1
  On primary failure → serve from replica
  Cost: 2× storage ($23K/month → $46K/month)
  RTO: ~5-10 min (DNS failover)

Option 2: Cross-cloud (S3 + GCS)
  Primary: S3. Async replication to GCS.
  Failover: serve from GCS read endpoint
  Cost: storage in both + transfer fees
  Complexity: high (different object formats, auth)

Practical: Accept <1hr/year S3 downtime for 95% of customers.
           For enterprise tier: offer multi-region replication as paid add-on.
```

**S3 read degradation (throttling, not full outage):**
```
S3 returns 503 SlowDown on hot prefixes (>3500 PUT/s per prefix)
Fix: randomize object prefix (already done: sha[0:2] = 256 pseudo-random prefixes)
Result: 256 prefixes × 3500 PUT/s = 896K PUT/s capacity → no throttling concern
```

---

## F3: Postgres Primary Fails

**Component:** RDS Postgres primary  
**Trigger:** HW failure, AZ outage, memory corruption

**Timeline:**
```
t=0s:   Primary unresponsive
t=10s:  RDS health check fails
t=40s:  Automatic failover initiated (replica promoted)
t=70s:  DNS TTL expires, app reconnects to new primary
t=90s:  Full recovery

Total: ~90s downtime
```

**Blast radius during 90s window:**
- All writes: fail (connections to old primary timeout)
- Reads: partial (depends on read replica routing; replica may still accept reads)
- Auth: stateless JWT still validates (no DB needed for token verification)
- Git operations: reads of cached refs (Redis, TTL=60s) continue for 60s

**Data loss risk:**
```
Async replication: replica may be 1-10ms behind primary
  Commits in-flight (not yet replicated): LOST on failover
  Probability: very low (only affects writes in the exact crash instant)
  
Sync replication: zero data loss, but:
  Primary waits for replica ACK → +5-10ms per write
  Replica network partition → primary stalls
  
GitHub choice: async replication + accept <1ms data loss window
               vs sync replication + risk primary stall on replica issues
```

**Mitigation:**
```
1. Connection pool (PgBouncer): transparently reconnects to new primary
2. Retry middleware: all DB writes retry 3× with 100ms backoff
3. Idempotent writes: ref updates use INSERT ON CONFLICT UPDATE (safe to retry)
4. Circuit breaker: if DB unreachable for >5s → return 503, don't queue up threads
```

---

## F4: Redis Cache Cluster Fails

**Component:** Redis cluster (caching refs, diff results, sessions)  
**Trigger:** OOM eviction storm, cluster partition, node failures

**Blast radius:**
- Ref cache miss → fall through to Postgres (slower but correct)
- Diff cache miss → recompute from scratch (expensive: 2-5s per PR view)
- Rate limit state lost → brief rate limit bypass (30-60s window)

**What survives:** All data is in Postgres/S3. Redis is a performance layer, not source of truth.

**Thundering herd on Redis recovery:**
```
Redis comes back → thousands of cache misses simultaneously
→ All requests hit Postgres at once → Postgres overload

Fix: Cache stampede prevention (mutex pattern)
  On cache miss:
    SET rebuild_lock:{key} NX EX 5   → only one request rebuilds
    Got lock? → fetch from DB → populate cache → release
    No lock?  → wait 100ms → retry read (cache will be warm)
    
  Also: stagger cache TTLs with jitter
    TTL = base_ttl + random(0, jitter)
    Prevents all keys expiring simultaneously
```

**Redis HA:**
```
Redis Sentinel: 3-node sentinel monitoring 1 primary + 1 replica
  Primary fail → Sentinel elects replica as new primary
  Failover time: ~30s
  
Redis Cluster: 3 shards × 2 nodes = 6 nodes
  Shard fail → serve from replica
  Max 1 shard unavailable at once (partial degradation, not full outage)
```

---

## F5: Kafka Broker Failures

**Component:** Kafka event bus  
**Trigger:** Broker crashes, disk full, network partition

**Blast radius:**
- Webhook events: not delivered to dispatcher consumers
- Search indexer: new commits not indexed (search goes stale)
- Notification service: PR notifications delayed

**Data safety:**
```
Kafka replication factor = 3 (min.insync.replicas = 2)
Producer acks = all (wait for 2/3 replicas to confirm)

Single broker fail → 2 brokers still have all messages → no data loss
2 brokers fail simultaneously → if < 2 in-sync replicas → producer blocked
```

**Outbox safety net:**
```
All Kafka producers also write to outbox table in Postgres BEFORE publishing:
  push_event_outbox (id, repo_id, event, payload, published=false, created_at)

If Kafka fails:
  1. Producer gets error → event stays in outbox (published=false)
  2. Outbox poller (every 30s): SELECT * FROM outbox WHERE published=false
  3. Retries Kafka publish
  4. On success: UPDATE outbox SET published=true

On Kafka recovery:
  Normal publishing resumes
  Outbox catches up any missed events
  Consumers deduplicate: check event_id against seen set (Redis SADD with TTL)
```

---

## F6: Webhook Endpoint Failure (External System Down)

**Component:** External webhook consumer (customer's CI server)  
**Trigger:** Customer's Jenkins/CI down, slow, or returning errors

**Blast radius:**
- Customer's CI pipelines: not triggered (builds not started)
- Our system: webhook queue backs up if many such endpoints fail simultaneously

**Retry strategy:**
```
Attempt 1:  immediately
Attempt 2:  1s delay
Attempt 3:  5s delay
Attempt 4:  30s delay
Attempt 5:  5min delay
Attempt 6:  30min delay
→ After 6 failures: deactivate webhook, email owner

Total retry window: ~36 minutes
```

**Circuit breaker (protect our system from slow endpoints):**
```
Per-host circuit breaker:
  State: CLOSED (normal) → error rate >50% in 5min → OPEN
  OPEN: reject all calls to that host immediately (no network call)
  After 10min: HALF-OPEN (allow 1 probe call)
  Probe success → CLOSED
  Probe fail → OPEN again

Benefit: 100 repos with webhooks to same dead host
  → Circuit opens after first repo's failures
  → Remaining 99 repos' webhooks fail-fast immediately (no 10s timeout × 100)
```

**What if customer's endpoint is slow (5s instead of dead):**
```
Hard timeout: 10s per webhook call
Parallel processing: each webhook in separate goroutine
Per-host concurrency limit: max 20 concurrent calls to any single host
→ Slow host can't monopolize dispatcher thread pool
```

---

## F7: Diff Service Memory Exhaustion (Huge PR)

**Component:** Diff computation worker  
**Trigger:** PR with 50K changed files (automated dependency upgrade, mass rename)

**Failure mode:** Worker OOM → crashes → PR shows empty diff

**Detection:**
- Worker pod memory > 80% threshold → alert
- Diff job duration > 30s → alert

**Graceful degradation:**
```
Diff worker config:
  max_files_to_diff: 3000
  max_file_size_bytes: 1_000_000  (1MB)
  max_memory_per_job: 512MB
  timeout_per_job: 60s

On limit hit:
  Return partial diff + metadata:
  {
    "truncated": true,
    "total_files_changed": 50000,
    "files_shown": 3000,
    "reason": "diff too large"
  }

Frontend: shows banner "Showing 3,000 of 50,000 changed files. View all files →"
```

**What if user needs to review all 50K files?**
```
Paginated diff API:
  GET /repos/{owner}/{repo}/pulls/{number}/files?page=2&per_page=100
  
  Each page request computes diff for 100 files only → bounded memory
  Frontend: lazy loads pages as user scrolls
```

---

## F8: Search Index Corruption / Stale

**Component:** Elasticsearch code search index  
**Trigger:** Indexer crash mid-update, ES shard corruption, schema migration

**Blast radius:**
- Code search returns stale or missing results
- New commits not appearing in search

**Detection:**
- Index lag metric: time since last indexed commit per repo
- Alert if lag > 5min for active repos

**Recovery:**
```
Partial corruption (1-2 shards):
  ES automatic shard recovery from replica
  RTO: ~5min

Full index corruption or schema migration:
  1. Create new index with new mapping
  2. Replay all commits from Kafka (if retained) or from S3 (walk all refs)
  3. When new index is caught up → atomic alias swap
     (ES index alias: search always hits alias, alias points to active index)
  4. Delete old index

Replay time: depends on repo count. 1M repos × 50 commits avg = 50M indexing operations.
At 10K ops/sec (ES ingest): 50M / 10K = 5000s ≈ 1.4 hours to full rebuild
During rebuild: stale search results (acceptable; better than blocking)
```

---

## F9: Repository Deletion Cascades

**Component:** Repository service  
**Trigger:** User deletes repo (intentional or accidental)

**Risk:** Deletion cascades to all objects, PRs, webhooks, forks.

**Safety mechanism:**
```
Soft delete (default):
  repos.deleted_at = now()    ← visible only in recovery queries
  30-day recovery window:
    User: Settings → Danger Zone → Recover this repository
    No data deleted for 30 days

Hard delete (after 30 days):
  1. Cancel all webhooks (stop delivery)
  2. Mark all objects as orphaned (don't delete yet)
  3. GC job (weekly): collect orphaned objects not reachable from any live ref
  4. Delete from S3 in batches (not all at once to avoid S3 throttling)
  5. Delete Postgres rows (cascades via FK)

Fork protection:
  If repo has forks → can't delete (or must delete all forks first)
  Object deduplication: fork still points to parent's objects
  Parent deleted → fork must have its own copy of all shared objects
  Pre-deletion job: copy shared objects to each fork's S3 namespace
```

---

## Recovery Playbook Summary

| Failure | RTO | RPO | Primary Fix |
|---------|-----|-----|-------------|
| Git server crash | <30s (LB reroutes) | 0 (stateless) | LB health check |
| Postgres primary fail | ~90s | <10ms data loss | RDS auto-failover |
| S3 region outage | ~30min | 0 (eventual) | Multi-region replication |
| Redis cluster fail | Immediate (DB fallback) | 0 | Fail-through to Postgres |
| Kafka broker fail | 0 (replica takes over) | 0 (acks=all) | Replication factor 3 |
| Webhook consumer lag | Auto-scale pods | 0 (Kafka retains) | HPA on consumer lag |
| Diff OOM | Immediate (graceful degrade) | 0 | Truncation + pagination |
| Search index corrupt | 1-2h (rebuild) | Stale during rebuild | Index alias + replay |
| Accidental repo delete | 30-day window | 0 | Soft delete |
