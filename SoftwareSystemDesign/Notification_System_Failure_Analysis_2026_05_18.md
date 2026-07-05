# Notification System — Failure Analysis (2026-05-18)

## Failure-First Mindset

For every component: **What if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?**

---

## Component Map

```
Producer → Notification API → Kafka (fanout) → Fan-out Workers → Kafka (channel) → Channel Workers → 3rd-party Gateway → User Device
                                                       ↓
                                                  Redis (prefs)
                                                       ↓
                                                  DeliveryLog DB
```

---

## Failure Scenarios

---

### 1. Notification API Fails

**Symptom:** Producers get 5xx; notifications not published to Kafka.

**Impact:** Notifications silently dropped if producer doesn't retry.

**Prevention:**
- API is stateless → multiple instances behind load balancer
- Health checks route traffic away from crashed instances in <30s
- Producers: retry with exponential backoff + idempotency key (safe to retry)

**What if ALL API instances fail?**
- Kafka producer buffer on producer side: buffer for 60s
- Alert triggers; auto-scaling kicks in
- Circuit breaker on producers: fail open (log + drop non-critical), fail closed for CRITICAL

---

### 2. Kafka Fan-out Topic Fails / Partition Leader Election

**Symptom:** Fan-out workers stall; lag grows.

**Impact:** Notifications delayed by seconds to minutes.

**Prevention:**
- Kafka replication factor = 3; min.insync.replicas = 2
- Leader election takes ~30s; during this, producers buffer (acks=all)
- Separate topics for CRITICAL vs LOW priority — leader failure on LOW doesn't block CRITICAL

**What if entire Kafka cluster is down?**
- This is catastrophic. Mitigation:
  - Multi-AZ Kafka deployment (most leader failures are single-AZ)
  - For CRITICAL notifications: fallback path — write directly to a secondary queue (SQS) which fan-out workers also consume
  - Acceptable degradation: LOW/marketing notifications delayed; CRITICAL have fallback

---

### 3. Fan-out Worker Fails Mid-Processing

**Symptom:** Kafka message consumed, worker crashes before writing to channel queues.

**Impact:** Notification silently lost (if offset committed before write).

**Prevention:**
- Kafka consumer: **commit offset AFTER successful write to channel queues** (not before)
- If worker crashes: Kafka redelivers from last committed offset → idempotency key prevents duplicate send
- At-least-once delivery guarantee maintained

**What if fan-out worker is slow (consumer lag builds)?**
- Monitor: consumer lag alert at >10K messages
- Auto-scale fan-out worker instances (more Kafka partitions = more parallelism)
- Fan-out is CPU-bound (preference lookup + filtering); horizontal scale is effective

---

### 4. Redis (Preference Cache) Fails

**Symptom:** Cache miss on every user lookup; hammers preference DB.

**Impact:** DB overload → cascading failure → fan-out workers stall.

**Prevention:**
- Redis Sentinel / Redis Cluster with automatic failover (15-30s RTO)
- Fan-out workers: on Redis connection error, fall back to DB directly (degraded mode)
- DB connection pool sized for this: normal 500 conns, burst 5000 conns
- Circuit breaker: if DB latency > 200ms, shed LOW priority fan-outs, continue CRITICAL

**What if Redis and DB both fail?**
- Fan-out halts for all non-CRITICAL
- CRITICAL: hardcoded "send to all channels" (no preference filtering) — better to over-notify than miss account security alerts
- Alert + manual intervention

---

### 5. Channel Worker Fails (e.g., Push Worker Crashes)

**Symptom:** Kafka push topic consumer lag grows; no push notifications delivered.

**Impact:** Push notifications delayed until worker restarts/scales.

**Prevention:**
- Multiple channel worker instances per channel
- Kafka redelivers on crash (same offset behaviour as above)
- Dead-letter queue after 5 failed attempts

**What if ALL push workers fail for extended time?**
- TTL on low-priority notifications: if message age > TTL → drop (stale notification is worse than none)
- User opens app → in-app feed still available (separate path, not affected)
- Email/SMS workers unaffected (separate topics)

---

### 6. 3rd-Party Gateway Fails (FCM / APNs / SendGrid / Twilio)

**Symptom:** Channel worker gets 5xx or timeout from gateway.

**Impact:** Notifications not delivered to end users.

**Prevention:**
- Retry with exponential backoff (5s → 30s → 2m → 10m → 30m)
- Circuit breaker per gateway:
  - CLOSED: normal
  - OPEN: after 5 consecutive failures in 60s → stop calling, return error immediately
  - HALF-OPEN: probe every 30s
- Multi-provider fallback:
  - Push: FCM primary → APNs is separate (they don't share failures)
  - SMS: Twilio primary → Vonage fallback (switch on circuit open)
  - Email: SendGrid primary → AWS SES fallback

**What if FCM is down for 1 hour?**
- Push notifications queued in Kafka (replay possible)
- Once FCM recovers: channel workers drain queue
- TTL check: if notification is >1h old and LOW priority → drop
- For CRITICAL: no TTL, deliver whenever possible

**What if gateway consistently rejects our token? (Rate limit hit)**
- Gateway returns 429 with Retry-After header
- Honour the header; pause sending; resume at indicated time
- Implement per-gateway rate limiter (token bucket) in channel worker

---

### 7. DeliveryLog DB Fails (Cassandra)

**Symptom:** Channel workers cannot write delivery status.

**Impact:** Cannot track delivery; cannot enforce idempotency on retry.

**Prevention:**
- Cassandra: multi-AZ, RF=3, consistency level = LOCAL_QUORUM
- If write fails: channel worker logs to local disk (WAL-style), retries async
- Idempotency: if DeliveryLog is unavailable, check Redis cache for recent sends (short TTL=5min)
- If Redis also missing: risk of duplicate delivery — acceptable (at-least-once guarantee)

---

### 8. In-App Feed DB Fails

**Symptom:** Users see empty notification feed.

**Impact:** Users don't see in-app notifications; push/email unaffected.

**Prevention:**
- Separate DB from DeliveryLog (failure isolation)
- Read replica for feed queries; primary for writes
- Redis sorted set cache: serve from cache on DB failure
- Graceful degradation: return cached last 20 notifications, show "Feed may be outdated" banner

---

### 9. Consumer Group Rebalance (Kafka)

**Symptom:** During scaling events, Kafka rebalances partitions across workers → brief pause in consumption.

**Impact:** Processing pauses for seconds.

**Prevention:**
- Use cooperative (incremental) rebalancing protocol (Kafka 2.4+)
- Set `session.timeout.ms` appropriately (not too low → spurious rebalances)
- Avoid rebalances during peak hours: scale during off-peak with min/max instance bounds

---

### 10. Large Fan-out Event (Thundering Herd)

**Symptom:** Marketing blast to 50M users posted; fan-out workers overwhelmed.

**Impact:** Real-time notifications delayed for all users.

**Prevention:**
- Dedicated Kafka topic for batch/marketing (`notif.fanout.batch`)
- Batch workers are separate instances from real-time workers
- Rate-limit batch fan-out: 50K dispatches/sec (use token bucket per worker)
- Schedule marketing blasts off-peak (2am–6am in target timezone)
- Back-pressure: if Kafka channel lag > 1M, throttle batch fan-out rate

---

## Failure Impact Matrix

| Component Fails | Immediate Impact | Degraded Mode | Recovery |
|---|---|---|---|
| Notification API | No notifications triggered | Producer retries | Auto-scale, LB reroutes |
| Kafka fanout topic | Fan-out stalls | Producer buffers | Leader election ~30s |
| Fan-out Worker | No per-user dispatch | Kafka redelivers on restart | Auto-scale / restart |
| Redis prefs cache | Slow fan-out | Direct DB fallback | Sentinel failover |
| Push Channel Worker | Push delayed | Queue accumulates, drains on recovery | Auto-scale |
| FCM/APNs | Push not delivered | Retry + DLQ | Circuit breaker, resume on recovery |
| DeliveryLog DB | No status tracking | Local WAL + retry | Cassandra node replacement |
| In-App DB | Empty in-app feed | Serve Redis cache | Read replica failover |

---

## Key Design Decisions Under Failure

1. **At-least-once over exactly-once** — simpler, idempotency handles duplicates
2. **Separate queues per priority** — CRITICAL channel never blocked by LOW failures
3. **TTL on messages** — avoids delivering stale notifications after long outages
4. **Multi-provider SMS/email** — no single point of failure at gateway level
5. **Fail open for preference lookup** — over-notify rather than miss CRITICAL alerts
