# Distributed Job Scheduler — Failure Analysis
*Date: 2026-05-22 | Series: Daily System Design*

---

## Failure Map

```
[Client/API] → [Job Store] → [Trigger Engine] → [Kafka] → [Worker] → [External System]
     F1             F2              F3              F4        F5              F6
```

---

## F1 — API Layer Fails

**Scenario**: Client POSTs job creation; API pod crashes mid-write.

**Impact**: Job may or may not be written to DB. Client gets 5xx.

**Prevention**:
- Idempotency key on job creation: `POST /jobs` with header `Idempotency-Key: <uuid>`
- Server stores `(idempotency_key, response)` for 24h; on retry → return stored response
- Client retries safely → no duplicate job

**Detection**: API health checks; client-side retry with same idempotency key.

---

## F2 — Job Store (Database) Fails

### F2a — Primary DB Crashes

**Impact**: All reads/writes fail. No jobs can be created or triggered.

**Prevention**:
- Synchronous replication to standby (PostgreSQL streaming replication)
- Automatic failover via Patroni or AWS RDS Multi-AZ (< 30s failover)
- Trigger Engine pauses (detects DB unavailability) → resumes on reconnect

**Recovery**: Promote standby → Trigger Engine reconnects → resumes normal tick.

### F2b — DB Overloaded (Too Many Heartbeat Writes)

**Impact**: Slow queries, lock contention, cascading slowdowns.

**Prevention**:
- Batch heartbeat updates: collect 100 worker heartbeats → 1 batch UPDATE
- Use separate heartbeat table (hot) vs job_runs table (cold)
- Connection pooling via PgBouncer

**Trade-off**: Batching heartbeats means watchdog detection is slightly delayed (acceptable: miss 1 batch = 15s delay before reaping).

### F2c — Shard Unavailable

**Impact**: Jobs on that shard cannot be triggered.

**Prevention**:
- Shard-level health check in Trigger Engine; skip unavailable shard
- Alert on shard down; manual failover
- Each shard has its own replica for HA

---

## F3 — Trigger Engine Fails

### F3a — Single Trigger Engine Crashes

**Impact**: Jobs scheduled during downtime are NOT triggered. Cron jobs miss their window.

**Prevention**:
- Run 2–3 Trigger Engine replicas
- **Leader election** via etcd/ZooKeeper: only leader triggers; followers on standby
- On leader crash: follower wins election in < 5s → takes over

**Catch-up logic**: On startup, query all jobs where:
```sql
next_run_at BETWEEN (now() - MAX_CATCH_UP_WINDOW) AND now()
AND status = 'IDLE'
```
Trigger missed jobs immediately (if policy allows catch-up; some orgs skip missed runs).

### F3b — Trigger Engine Runs Twice (Split-Brain)

**Scenario**: Network partition → two nodes both think they're leader → both trigger same job.

**Prevention**:
- Optimistic locking on job row (see Design doc §5.1) ensures only one enqueue wins
- Deduplication index on `(job_id, scheduled_at)` — second insert fails silently
- **Never rely solely on leader election** for correctness; use DB-level idempotency as defense-in-depth

---

## F4 — Kafka (Message Queue) Fails

### F4a — Kafka Broker Crashes

**Impact**: Messages can't be produced or consumed; jobs pile up in ENQUEUED state but not processed.

**Prevention**:
- Kafka replication factor = 3 (tolerate 1 broker loss without data loss)
- `acks=all` on producer: wait for all ISR replicas to confirm
- Trigger Engine buffers in-memory for < 30s; retries produce

**Detection**: Kafka consumer lag metrics; alert on lag > threshold.

### F4b — Kafka Partition Leader Fails

**Impact**: 30–60s pause on that partition while new leader is elected.

**Prevention**: `min.insync.replicas = 2` ensures data is safe. Workers simply pause briefly; jobs retry delivery.

### F4c — Kafka Full (Disk Full)

**Impact**: Producers blocked; new jobs can't be enqueued.

**Prevention**:
- Set retention by size, not just time (`log.retention.bytes`)
- Alert at 70% disk
- Dead-letter topic has shorter retention (7 days vs 30)

### F4d — Message Stuck in Queue (Consumer Never Acks)

**Impact**: Same message redelivered indefinitely; causes infinite retry loop.

**Prevention**:
- Set `max.delivery.count` → after N redeliveries, route to DLQ topic
- DLQ monitored separately; human review for poison pills

---

## F5 — Worker Fails

### F5a — Worker Crashes Mid-Job

**Impact**: Job is RUNNING but will never complete. Lease expires.

**Prevention**:
- Watchdog detects expired lease (heartbeat missed > TTL)
- Marks run as FAILED; Retry Manager re-enqueues
- Worker on startup: scan for any RUNNING jobs it owned → mark them FAILED

**Recovery time**: TTL (60s) + watchdog interval (30s) = up to 90s before requeue.

**Tuning**: Lower TTL = faster recovery but more false positives. Raise if jobs take > 60s legitimately.

### F5b — Worker Hangs (Job Loops Forever)

**Impact**: Heartbeat keeps extending lease → watchdog never reclaims → job runs forever.

**Prevention**:
- Hard timeout per job: job-level `max_execution_seconds` config
- Worker enforces timeout via goroutine/thread kill after deadline
- Separate `max_wall_clock_seconds` watchdog (absolute cap regardless of heartbeat)

### F5c — Worker OOM / Node Dies Suddenly

**Impact**: Entire worker process killed; Kafka offset not committed → message redelivered.

**Prevention**: This is *intentional* — message redelivery is the recovery mechanism. Ensure jobs are idempotent (deduplicate on `run_id`).

### F5d — All Workers Down (Deploy, Outage)

**Impact**: Kafka consumer lag grows; jobs queue up.

**Prevention**:
- Rolling deploys: keep 50% workers live during deploy
- Kafka retention = 7 days → can recover from up to 7 days of backlog if workers return
- Auto-scale workers based on consumer lag (KEDA / custom metrics)

---

## F6 — External System Fails (Job's Dependency)

**Scenario**: Job sends email → SMTP server down. Job calls payment API → API returns 503.

**Prevention**:
- Job should catch transient errors → throw retriable exception → Retry Manager handles
- Non-retriable errors (400 Bad Request) → mark FAILED immediately, no retry (no point)
- Circuit breaker per external dependency: if 5 consecutive failures → open circuit → fail fast for 60s

**Classification:**
```
Retriable:   503, 429, timeouts, connection refused
Not-retriable: 400, 401, 403, 404, invalid payload
```

---

## F7 — Clock Skew Between Nodes

**Scenario**: Trigger Engine node's clock is 5 minutes behind → triggers jobs 5 minutes late.

**Prevention**:
- All nodes sync via NTP; alert on drift > 1s
- Use DB server time for `now()` comparisons, not local node time
  ```sql
  WHERE next_run_at <= NOW()  -- DB's clock, not app server's
  ```

---

## F8 — Thundering Herd on Recovery

**Scenario**: Queue backs up for 2 hours; all workers come back online → 10,000 jobs flood out simultaneously.

**Prevention**:
- Rate limit dequeue per worker (e.g., max 10 concurrent jobs per worker)
- Kafka partitions naturally spread load
- Priority queue: process `queue.critical` first, throttle `queue.low`

---

## Failure Summary Matrix

| Failure | Detection | Recovery Time | Prevention |
|---------|-----------|---------------|------------|
| API crash | Health check | < 30s (k8s restart) | Idempotency key |
| DB primary down | Replication lag alert | < 30s (Patroni) | Synchronous standby |
| Trigger Engine crash | etcd TTL | < 5s (follower election) | Leader election + catch-up |
| Trigger split-brain | — | Immediate | Optimistic lock + dedup index |
| Kafka broker down | Consumer lag | < 60s (leader election) | RF=3, acks=all |
| Worker crash mid-job | Heartbeat timeout | 60–90s | Watchdog + idempotent jobs |
| Worker hangs | Max execution TTL | Per job config | Hard timeout |
| All workers down | Consumer lag spike | Minutes (autoscale) | Rolling deploy + lag alert |
| External API down | Error rate | Per retry policy | Circuit breaker + classification |
| Clock skew | NTP monitoring | Immediate if caught | DB-side NOW() |
| Thundering herd | CPU/throughput spike | Seconds | Rate limiting + priority queues |
