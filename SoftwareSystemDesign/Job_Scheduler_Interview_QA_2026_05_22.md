# Distributed Job Scheduler — Interview Q&A
*Date: 2026-05-22 | Series: Daily System Design*

---

## Opening Questions (Scope & Requirements)

**Q: Design a job scheduling system.**

*What to clarify first:*
- One-time jobs or recurring (cron)?
- Expected job volume? (1K/day vs 10M/day)
- Latency requirements? (trigger within 1s or best-effort?)
- Job execution time? (seconds or hours?)
- Retry policy needed?
- Do we need exactly-once or at-least-once?

*Good opening answer:*
> "I'll design a system that supports both one-time and recurring (cron) jobs, targeting millions of jobs/day, sub-second trigger latency, with at-least-once delivery and configurable retry. Jobs must be idempotent since we can't guarantee exactly-once."

---

## Core Design Questions

**Q: How do you prevent the same job from being triggered twice?**

Two layers:
1. **Optimistic lock on job row**: Trigger Engine does `UPDATE jobs SET status='ENQUEUING' WHERE status='IDLE' AND next_run_at <= now()`. Only one process wins.
2. **Unique constraint**: `UNIQUE(job_id, scheduled_at)` on `job_runs` — second insert fails, preventing double enqueue.

Never rely on "only one scheduler runs" — network partitions can cause split-brain. Always add DB-level idempotency.

---

**Q: What if the scheduler process itself crashes?**

Leader election (etcd/ZooKeeper). Secondary takes over in < 5s. On startup, winner scans for missed jobs:
```sql
SELECT * FROM jobs WHERE next_run_at BETWEEN (now()-WINDOW) AND now() AND status='IDLE'
```
Trigger missed runs if catch-up is enabled (depends on business requirement — some jobs must only run at their exact time; others want catch-up).

---

**Q: How do you handle a worker that crashes mid-job?**

Heartbeat + watchdog pattern:
- Worker sends heartbeat every 15s, updating `lease_expires_at`
- Watchdog scans RUNNING jobs where `lease_expires_at < now() - 30s`
- Those are reclaimed → marked FAILED → Retry Manager re-enqueues

Trade-off: TTL too short = false positives (healthy long-running jobs killed). TTL too long = slow recovery.

---

**Q: How do you ensure jobs are idempotent?**

You can't enforce it — the job author must implement it. But you can help:
- Provide `run_id` to each job execution as a unique idempotency key
- Provide a standard "check-and-set" pattern:
  ```
  if (redis.get("done:" + run_id)) return;  // skip
  // do work
  redis.set("done:" + run_id, 1, EX=86400);
  ```
- Document this as a contract: "The scheduler guarantees at-least-once; your job must handle duplicates."

---

**Q: Walk me through how a cron job gets triggered.**

```
1. On job creation → compute next_run_at from cron_expr, store in DB
2. Trigger Engine ticks every second → SELECT jobs WHERE next_run_at ≤ now()
3. For each hit → optimistic UPDATE → create JobRun (PENDING) → publish to Kafka
4. Trigger Engine updates next_run_at = cron.next(now(), cron_expr)
5. Worker consumes from Kafka → marks RUNNING → executes → marks SUCCESS/FAILED
```

---

## Follow-up / Deep-dive Questions

**Q: How do you scale the Trigger Engine?**

Shard by `job_id`:
- Split jobs into N buckets: `bucket = murmur3(job_id) % N`
- Trigger Engine instance K handles bucket K
- Each instance has its own leader + failover
- Adding a new instance → rehash (consistent hashing minimizes churn)

---

**Q: How would you implement priority queues?**

Multiple Kafka topics: `queue.critical`, `queue.high`, `queue.default`, `queue.low`.

Workers can be:
- **Dedicated**: only consume one topic (guaranteed capacity for critical)
- **Multi-consumer**: consume all topics with priority weighting (70% critical, 20% high, 10% default)

Trade-off: dedicated workers → resource waste. Shared workers → lower-priority tasks can starve critical if worker pool is small.

---

**Q: How do you handle a job that takes 6 hours to run?**

Challenges:
- Heartbeat must keep extending lease for 6 hours
- Worker node may be replaced during a rolling deploy

Solutions:
- Set `max_execution_seconds = 7h` for this job type (per-job config)
- Support **checkpointing**: job saves progress state to DB; on restart, resume from checkpoint
- Design long jobs as multiple smaller chained jobs (DAG approach — like Airflow)

---

**Q: What if you need exactly-once execution?**

Hard to guarantee in distributed systems. Options:
- **External transactions**: wrap job execution + mark-done in a single DB transaction (only works if job's side effect is a DB write)
- **Saga pattern**: job publishes intent → external system deduplicates on idempotency key
- **Temporal/Cadence**: provides durable execution with event-sourced state — effectively exactly-once semantics at the workflow level

Most systems settle for at-least-once + idempotent jobs. Tell the interviewer: "Exactly-once requires the job's side effects to be transactional with our state store, which constrains what jobs can do."

---

**Q: How does your Dead Letter Queue (DLQ) work?**

After `max_retries` exhausted:
- JobRun status → `DEAD_LETTER`
- Original payload written to DLQ table (and optionally DLQ Kafka topic)
- Alert fires to on-call
- Engineer can inspect, fix, and manually re-trigger

DLQ jobs should NEVER be auto-retried without human review — they likely indicate a bug or bad payload.

---

**Q: How do you monitor this system?**

Key metrics:
```
Job trigger lag    = actual_start - scheduled_at   (target < 1s p99)
Queue depth        = Kafka consumer lag per topic
Failure rate       = FAILED runs / total runs (alert > 1%)
DLQ depth          = unresolved dead-letter jobs (alert > 0)
Worker utilization = running jobs / worker capacity
Watchdog reclaims  = stale jobs reclaimed/hour (sudden spike = worker instability)
```

Dashboards: Grafana. Alerts: PagerDuty on trigger lag > 10s, DLQ > 5, failure rate spike.

---

**Q: How would you handle a "thundering herd" when 100,000 jobs are scheduled at midnight?**

Multiple strategies:
1. **Jitter at schedule time**: spread `next_run_at` across a window: `midnight + random(0, 300s)` if exact timing isn't required
2. **Kafka partitioning**: naturally distributes load across consumer group
3. **Rate limiting dequeue**: each worker processes max N concurrent jobs
4. **Priority queuing**: midnight batch jobs → `queue.low`; don't let them starve business-critical jobs

---

**Q: SQL vs NoSQL for job store?**

| | SQL (PostgreSQL) | NoSQL (DynamoDB) |
|-|-----------------|-----------------|
| ACID transactions | ✅ (critical for optimistic lock) | ❌ limited |
| Cron query (next_run_at ≤ now) | ✅ simple index scan | ⚠️ requires GSI or scan |
| Horizontal sharding | ❌ manual | ✅ native |
| Familiarity / tooling | ✅ | varies |

**Recommendation**: Start with PostgreSQL. Add sharding at 10× scale. The ACID guarantees simplify the deduplication logic significantly.

---

**Q: Compare your design to Airflow. What would you do differently?**

| | Airflow | Our Design |
|-|---------|------------|
| Scheduling | Python DAGs, single scheduler | API-defined jobs, sharded trigger engine |
| Execution | Celery/K8s executor | Pull-based Kafka consumers |
| State | PostgreSQL | PostgreSQL |
| UI | Built-in | Build separately |
| Scale | Medium (1 scheduler is a SPOF in older versions) | HA by design |
| Failure recovery | HA mode in Airflow 2.x (multiple schedulers) | Leader election + follower standby |

Key Airflow limitation: Python-centric, DAG parsing overhead, less suitable for generic microservice jobs. Our design is language-agnostic and more horizontally scalable.

---

## Red Flags to Avoid in Interview

❌ "We use cron on the server" → no HA, no retry, no observability  
❌ "We'll use a distributed lock to ensure only one trigger" → lock alone isn't enough without DB idempotency  
❌ "Workers will process each job exactly once" → can't guarantee without extra complexity; say at-least-once + idempotent  
❌ "We'll just retry forever" → need max_retries + DLQ or you'll loop on poison messages  
❌ Forgetting heartbeat → can't detect crashed workers  

---

## Strong Signals in Interview

✅ Separate concerns: Trigger Engine vs Worker vs Watchdog  
✅ Optimistic lock + DB unique constraint for deduplication  
✅ Explicit at-least-once contract + idempotency key pattern  
✅ Heartbeat + lease-based failure detection  
✅ DLQ for unrecoverable jobs  
✅ Observability: lag, failure rate, watchdog reclaim rate  
✅ Mention real-world systems (Airflow, Temporal, Sidekiq) and their trade-offs
