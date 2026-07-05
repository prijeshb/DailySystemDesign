# Notification System — Failure Analysis
*Date: 2026-06-03*

---

## Failure-First Thinking

For each component: **What if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?**

---

## Component 1: API Layer

### It fails (API server crash)
- **Impact:** Producers cannot enqueue notifications
- **Prevention:** Multiple API server instances behind load balancer; health checks auto-remove dead instances
- **Detection:** LB health check, alerting on 5xx spike

### Component before (Producer / App Server) fails
- **Impact:** Notifications never sent — silent failure for end-user
- **Prevention:** Producer should use outbox pattern: write intent to local DB before calling Notification API. A background job retries if API call fails.
- **Recovery:** At-least-once delivery from producer side

### Component after (Kafka) fails
- **Impact:** API accepts request but can't enqueue — message lost
- **Prevention:** API should return 503 if Kafka write fails; producer retries
- **Alternative:** Fallback to DB-backed queue (write to `pending_notifications` table), drain when Kafka recovers

---

## Component 2: Kafka (Message Queue)

### It fails (Kafka broker down)
- **Impact:** Workers stop consuming; notifications pile up or are lost
- **Prevention:**
  - Replication factor ≥ 3 across brokers
  - `min.insync.replicas = 2` — write only succeeds if 2 replicas acknowledge
  - Kafka persists to disk; messages survive broker restart
- **Recovery:** Kafka auto-elects new leader; consumers resume from last committed offset

### Component before (Notification Service) fails mid-write
- **Impact:** Partial batch written; some messages duplicated on retry
- **Prevention:** Kafka producer idempotency (`enable.idempotence=true`); exactly-once semantics per partition
- **Note:** Idempotency key in message payload as secondary guard

### Component after (Worker) fails mid-processing
- **Impact:** Message consumed but not delivered; lost if offset committed early
- **Prevention:**
  - Commit offset AFTER successful send (or after writing to log DB)
  - If worker crashes, message re-delivered to another worker instance
  - Max retries + dead letter queue (DLQ) for poison messages
- **DLQ handling:** Messages in DLQ alert on-call; manual replay after fix

---

## Component 3: Push Worker (FCM / APNs)

### Worker process crashes
- **Impact:** In-flight message not delivered; Kafka offset not committed
- **Prevention:** Kafka re-delivers to another worker instance (consumer group rebalance)
- **Detection:** Consumer lag metric alert (lag > threshold = workers slow/dead)

### FCM / APNs is down (3rd-party failure)
- **Impact:** Push notifications fail for all users
- **Prevention:**
  - Retry with exponential backoff (1s → 2s → 4s → max 5 min)
  - After N retries, fall back to next channel (email or SMS)
  - Circuit breaker: if FCM error rate > 50% for 30s, open circuit, stop sending to FCM, drain via fallback
- **Recovery:** Circuit breaker half-opens every 60s to probe FCM health

### FCM returns invalid token
- **Impact:** Message permanently undeliverable to that device
- **Prevention:** Mark token inactive; do not retry; remove from future sends
- **Cleanup:** Periodic job to purge inactive tokens older than 90 days

### APNs / FCM rate limit hit
- **Impact:** 429 responses; messages dropped if not handled
- **Prevention:**
  - Token bucket rate limiter per vendor in Redis
  - Back-pressure: if approaching limit, enqueue with delay
  - Batch send API (FCM supports up to 500 messages/batch)

---

## Component 4: SMS Worker (Twilio)

### Twilio API down
- **Impact:** SMS not delivered
- **Prevention:**
  - Secondary SMS provider (e.g., Vonage/Nexmo) as failover
  - If Twilio fails N times → route to backup provider automatically
- **Cost note:** SMS is expensive; only critical notifications use SMS; others degrade gracefully

### Phone number invalid / unsubscribed
- **Impact:** SMS fails with carrier error
- **Prevention:** Handle STOP messages from carriers (webhook); mark user phone as opted-out in DB
- **Compliance:** TCPA (US), GDPR — must respect opt-outs immediately

---

## Component 5: Email Worker (SendGrid / SES)

### Email provider down
- **Impact:** Emails not sent
- **Prevention:** Failover to AWS SES if SendGrid fails; both support SMTP + API
- **Recovery:** Retry queue with 15-min intervals (email has looser SLA than push)

### Bounce / spam complaints
- **Impact:** Domain reputation degraded; future emails land in spam
- **Prevention:**
  - Handle bounce webhooks → mark email invalid
  - Handle spam complaints → suppress user from future sends
  - Monitor bounce rate (< 2%) and complaint rate (< 0.1%)

---

## Component 6: User Preference DB (MySQL)

### DB primary fails
- **Impact:** Cannot read preferences; notifications may be sent to wrong channel
- **Prevention:** MySQL with sync replication; automatic failover (RDS Multi-AZ or Orchestrator)
- **Fallback:** Cache preferences in Redis (TTL 5 min); serve from cache if DB unavailable

### Cache (Redis) fails
- **Impact:** Cannot check idempotency keys or rate limits; risk of duplicate sends
- **Prevention:** Redis cluster with replicas; AOF persistence
- **Degraded mode:** Disable idempotency check temporarily; accept risk of occasional duplicate; alert and recover

---

## Component 7: Log DB (Cassandra)

### Node fails
- **Impact:** Writes may fail for that partition
- **Prevention:** Replication factor = 3; `QUORUM` write consistency; any 2-of-3 nodes sufficient
- **Recovery:** Cassandra auto-repairs via hinted handoff

### Log write fails (worker can't log delivery)
- **Impact:** Delivery status unknown; no visibility for debugging
- **Prevention:** Log write is non-blocking (fire-and-forget with local buffer); notification still sent
- **Mitigation:** Delivery receipts from FCM/APNs as secondary source of truth

---

## Cascading Failure Scenarios

### Scenario 1: Traffic spike (marketing blast to 50M users)
```
Fan-out service dumps 50M messages → Kafka lag spikes → Workers overwhelmed
→ FCM rate limit hit → retries flood the queue → Kafka lag grows further

Prevention:
- Rate-limit fan-out (1M/min max)
- Separate "bulk" queue with lower worker priority vs. transactional queue
- Pre-announce large campaigns; pre-scale workers
```

### Scenario 2: Stale device tokens accumulate
```
Millions of inactive tokens → FCM calls fail → Workers spend time on dead tokens
→ Throughput drops → Real notifications delayed

Prevention:
- Weekly job: send test ping to each token, deactivate on failure
- Track "last active" per token; skip tokens not seen in 6 months
```

### Scenario 3: Preference DB slow (not down)
```
Each notification requires preference lookup → P99 latency spikes
→ Notification Service threads blocked → Queue backup

Prevention:
- Always serve preferences from Redis cache (DB is source of truth, cache is hot path)
- Cache-aside with write-through on preference update
```

---

## Summary: Key Reliability Patterns Used

| Pattern | Applied To |
|---------|-----------|
| Idempotency keys | Dedup on producer retry |
| Dead Letter Queue | Poison messages in Kafka |
| Circuit Breaker | 3rd-party vendor failures |
| Fallback channel | Push → Email → SMS on failure |
| Outbox pattern | Producer-side reliability |
| Cache-aside | User preferences (Redis) |
| Replication | Kafka (RF=3), Cassandra (RF=3), MySQL (Multi-AZ) |
| Exponential backoff | All 3rd-party API calls |
| Consumer lag alerts | Worker health monitoring |
| Rate limiting | Per-user spam prevention + vendor limits |
