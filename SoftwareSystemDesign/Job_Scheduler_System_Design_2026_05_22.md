# Distributed Job Scheduler — System Design
*Date: 2026-05-22 | Series: Daily System Design*

---

## 0. First Principles — Do We Even Need It?

**Problem**: Run tasks (send emails, generate reports, resize images) reliably at a scheduled time or in response to events — without blocking the user-facing request.

- Without a scheduler → business logic blocks HTTP threads, missed deadlines, no retry, no observability
- With a scheduler → async execution, retry on failure, rate control, audit trail
- **Worth it?** Yes. Every non-trivial product has recurring jobs (billing, notifications, ML pipelines). The question is *what kind* of scheduler.

**Key trade-off up front:**

| Approach | Simple but limited | Scalable but complex |
|----------|--------------------|----------------------|
| Cron on one machine | ✅ zero infra | ❌ single point of failure |
| Distributed scheduler | ❌ operational overhead | ✅ HA, retry, sharding |

> Decision: build distributed. At scale (millions of jobs/day), a single cron box cannot survive.

---

## 1. Entities

| Entity | Key Attributes |
|--------|---------------|
| **Job** | `job_id`, `type` (cron/one-time), `cron_expr`, `payload`, `status`, `created_at` |
| **JobRun** | `run_id`, `job_id`, `scheduled_at`, `started_at`, `finished_at`, `status`, `attempt`, `output` |
| **Worker** | `worker_id`, `host`, `heartbeat_at`, `capacity`, `tags` |
| **Queue** | `queue_name`, `priority`, `visibility_timeout` |
| **DeadLetterQueue** | `dlq_id`, `original_run_id`, `failure_reason`, `payload` |

**Status lifecycle for JobRun:**
```
PENDING → ENQUEUED → RUNNING → SUCCESS
                              → FAILED → (retry) → ENQUEUED
                                       → (max retries) → DEAD_LETTER
```

---

## 2. Actions

| Action | Actor | Latency Target |
|--------|-------|----------------|
| `createJob(spec)` | API client | < 200ms |
| `triggerJob(job_id)` | Trigger Engine (cron tick) | < 1s from scheduled time |
| `enqueue(run)` | Trigger Engine | async |
| `dequeue()` | Worker | < 50ms |
| `executeJob(run)` | Worker | job-defined |
| `heartbeat(run_id)` | Worker | every 15s |
| `markComplete/Failed(run_id)` | Worker | < 200ms |
| `retryJob(run_id)` | Retry Manager | on failure |
| `requeueStuckJobs()` | Watchdog | periodic sweep |

---

## 3. Data Flow

```
[Client / API]
      │ createJob / triggerJob (on-demand)
      ▼
[Job Store (PostgreSQL)]
      │ persisted job spec
      ▼
[Trigger Engine — Scheduler Process]
  ├── Reads job specs with next_run_at ≤ now
  ├── Creates JobRun record (PENDING)
  └── Publishes message to [Message Queue (Kafka/SQS)]
              │
              ▼
      [Worker Pool]
        ├── Worker dequeues message (visibility lock)
        ├── Updates JobRun → RUNNING
        ├── Executes job logic
        ├── Sends heartbeat every 15s
        └── On finish → marks SUCCESS or FAILED
                           │
                FAILED ────▼────────────────────────────┐
                      [Retry Manager]                    │
                       ├── attempt < max_retries?        │
                       │   └── re-enqueue with backoff   │
                       └── else → [Dead Letter Queue]────┘
                                        │
                                [Alerting / Manual Review]
```

---

## 4. High-Level Design

```
                    ┌─────────────────────────────────┐
                    │           API Layer              │
                    │  (REST/gRPC — job CRUD, status)  │
                    └────────────┬────────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │         Job Store (Primary DB)       │
              │  jobs | job_runs | dlq               │
              │  PostgreSQL — partitioned by job_id  │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │          Trigger Engine              │
              │  - Polls next_run_at ≤ now           │
              │  - Leader election (ZooKeeper/etcd)  │
              │  - Shards jobs by job_id % N         │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │       Message Queue (Kafka)          │
              │  Topics: queue.default, queue.high   │
              │  Retention: 7 days                   │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │         Worker Pool (auto-scale)     │
              │  - Pull-based consumers              │
              │  - Isolated per job type (optional)  │
              │  - Heartbeat → Job Store             │
              └──────────────────┬──────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │        Watchdog / Reaper             │
              │  - Scans RUNNING with no heartbeat   │
              │  - Reclaims & re-enqueues after TTL  │
              └─────────────────────────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Trigger Engine — Avoiding Duplicate Triggers

**Problem**: Two trigger engine instances could both see the same job and enqueue it twice.

**Solution — Optimistic Lock:**
```sql
UPDATE jobs
SET status = 'ENQUEUING', last_triggered_at = now()
WHERE job_id = $1
  AND next_run_at <= now()
  AND status = 'IDLE';
-- Only one process wins the UPDATE (row-level lock)
```
The winner enqueues; the loser gets 0 rows updated and skips.

**Next run calculation:**
```
next_run_at = cron.next(now(), cron_expr)
```
Updated atomically after successful enqueue.

---

### 5.2 Worker — Visibility Timeout & Heartbeat

Kafka doesn't natively support "visibility timeout" like SQS. Options:

| Queue | Visibility Timeout | Notes |
|-------|--------------------|-------|
| SQS | Native | Simple, AWS-only |
| Kafka + DB lock | Manual via `lease_expires_at` column | Portable |
| Celery + Redis | `VISIBILITY_TIMEOUT` config | Common in Python |

**Implementation with DB lease:**
```
Worker dequeues message:
  1. INSERT job_run (status=RUNNING, lease_expires_at = now() + 60s)
  2. Execute job
  3. Every 15s: UPDATE lease_expires_at = now() + 60s (heartbeat)
  4. On done: UPDATE status = SUCCESS/FAILED
```

**Watchdog query (runs every 30s):**
```sql
SELECT run_id FROM job_runs
WHERE status = 'RUNNING'
  AND lease_expires_at < now() - INTERVAL '30 seconds';
-- These workers are dead → requeue
```

---

### 5.3 Retry Strategy

```
Attempt 1 → fail → wait 30s
Attempt 2 → fail → wait 90s   (30 * 3^1)
Attempt 3 → fail → wait 270s  (30 * 3^2)
Attempt N (max) → DLQ
```

**Jitter** (prevents thundering herd on retry):
```
wait = base * (3 ^ attempt) + random(0, base)
```

**Idempotency requirement**: Jobs MUST be idempotent. The scheduler guarantees *at-least-once* execution. Worker receives `run_id`; job logic should use it as idempotency key.

```
// Before doing work, check:
if (cache.get("job:done:" + run_id)) return; // already done
// Do work
cache.set("job:done:" + run_id, true, TTL=24h);
```

---

### 5.4 Sharding the Job Store

**Problem**: Millions of jobs, single PostgreSQL won't scale.

**Shard key**: `job_id` (UUID → hash → shard)

```
shard = murmur3(job_id) % NUM_SHARDS
```

**Watch out**: Cron query ("give me all jobs where next_run_at ≤ now") must hit ALL shards. Each shard's Trigger Engine handles its own slice.

**Alternative**: Use a time-series-aware scheduler DB (e.g., TimescaleDB or a sorted-set in Redis: `ZADD scheduled_jobs <epoch> <job_id>`).

---

### 5.5 Priority Queues

```
Kafka topics:
  queue.critical  (billing, alerts)
  queue.high      (user-triggered)
  queue.default   (reports, exports)
  queue.low       (cleanup, archival)
```

Workers can be pinned to specific topics or consume all with priority ordering.

---

### 5.6 Deduplication

**Problem**: Job triggered twice (network retry, bug).

**Solution**: Unique constraint on `(job_id, scheduled_at)` in `job_runs` table. Second insert fails → caught, not enqueued again.

```sql
CREATE UNIQUE INDEX ON job_runs(job_id, scheduled_at);
```

---

## 6. Key Trade-offs

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Kafka over SQS | Kafka | Replay & audit ✅, ops overhead ❌ |
| Pull-based workers | Pull | Back-pressure natural ✅, latency slightly higher ❌ |
| PostgreSQL for job store | PG | ACID + familiar ✅, harder to shard ❌ |
| At-least-once delivery | Yes | Simple ✅, jobs must be idempotent ❌ |
| DB-based heartbeat | Yes | Centralized truth ✅, DB write every 15s per worker ❌ |
| Exponential backoff + jitter | Yes | Avoids thundering herd ✅, delayed retry ❌ |

---

## 7. Scale Estimation

```
Jobs: 10M jobs/day = ~115 jobs/sec
Job Runs: avg 1.1 runs/job (10% retry rate) = ~127 runs/sec
Worker throughput: 1 job/5sec avg → need ~635 workers
Kafka throughput: 127 msgs/sec → trivial for Kafka
DB writes: 127 job_run inserts/sec + heartbeats
Heartbeats: 635 workers × 1 hb/15s = ~42 writes/sec
```

One PostgreSQL primary handles this comfortably (thousands of writes/sec). Shard at 100× scale.

---

## 8. Real-World References

- **Airflow**: DAG-based scheduler, uses PostgreSQL + Celery workers; leader election via DB row lock
- **Temporal.io**: Durable execution engine; uses event sourcing for job state
- **LinkedIn Azkaban**: Hadoop workflow scheduler; multi-tenant, UI-first
- **Sidekiq (Ruby)**: Redis-backed; uses `ZADD` sorted sets for scheduled jobs
- **AWS Step Functions**: Managed; each step is a state machine node
- **Uber Cadence**: Open-source Temporal precursor; activity + workflow separation

**Engineering blog refs:**
- Airflow's scheduler loop: apache.org/airflow/docs/scheduler
- Temporal's approach to exactly-once: temporal.io/blog/workflow-engine-principles
- Shopify on Sidekiq at scale: engineering.shopify.com
