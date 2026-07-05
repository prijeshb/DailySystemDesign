# Notification System — Failure Analysis
*Date: 2026-06-26*

---

## Failure-First Thinking

> For every component: what happens if IT fails, what happens if the component BEFORE it fails, what happens if the component AFTER it fails?

---

## 1. Notification API Layer Fails

### Scenario
All API pods crash (OOM, bad deploy) while upstream services attempt to publish events.

### Impact
- Upstream services get HTTP 503
- If no retry in upstream → events lost permanently
- If upstream retries → event duplication risk when API recovers

### What Upstream Services Should Do
```
Option A (Fire-and-forget): Upstream accepts loss. Bad for critical notifications.

Option B (Outbox Pattern — correct):
  Upstream writes intent to its own DB outbox table first:
    INSERT INTO notification_outbox (type, user_id, payload) VALUES (...)
  Separate outbox publisher polls and sends to Notification API
  On success: DELETE from outbox
  On failure: retry with exponential backoff

  Crash of Notification API:  Outbox retries until API recovers. No loss.
  Duplicate send:             Notification API's idempotency_key deduplicates.
```

**Prevention:**
- API behind ALB with health checks; unhealthy pods removed in 10s
- Auto-scaling: scale out on CPU >70% or request queue depth
- Blue-green deploy to avoid simultaneous pod restart

---

## 2. Kafka (Event Bus) Fails

### Scenario
Kafka broker leader election, network partition between zones, or disk full.

### Impact
- Dispatcher cannot consume new events → delivery paused
- Events queued in Kafka (durable): no loss if producers have ACK=all
- If Kafka partition leader unavailable: producers block until leader election (~30s)

### Upstream Write Path Degradation
```
Normal: API → Kafka (ACK=all) → success response
During Kafka outage:
  API cannot publish → must decide: block or store locally

Option A: Block upstream producer (default Kafka behavior with ACK=all)
  → API returns 503 to caller → upstream uses outbox to retry
  → Risk: if outage is long (>1hr), outbox grows large

Option B: Dual-write buffer
  On Kafka failure → API writes event to local SQS queue (fallback)
  SQS → re-publishes to Kafka when Kafka recovers
  Advantage: no blocking, upstream unaffected
  Cost: additional SQS queue, complexity
```

**Recovery:**
- Kafka: min.insync.replicas=2 out of 3 → one broker can die without write loss
- Kafka replication factor=3 across AZs → AZ failure transparent
- After recovery: consumers resume from last committed offset; no events lost

---

## 3. Dispatcher Service Fails

### Scenario
Dispatcher crashes mid-processing (after reading from Kafka, before committing offset).

### Impact
- Kafka consumer group rebalances → partition reassigned to another Dispatcher
- New Dispatcher re-reads from last committed offset → same message processed again
- This is by design: Kafka at-least-once delivery

### Duplicate Notification Problem
```
Dispatcher reads Kafka message → sends to FCM → crashes before committing offset
Kafka rebalance → new Dispatcher re-reads same message → sends to FCM again
User gets 2 push notifications for same event
```

### Rebalance During Processing (Kafka-Specific)
```
Dispatcher A processing partition 3 message
New Dispatcher joins consumer group → rebalance triggered
Partition 3 reassigned to Dispatcher B
Both A and B may process same message simultaneously
```

Fix: pause consumption on rebalance, finish in-flight messages first
```python
consumer.subscribe(topics, on_revoke=finish_inflight_and_commit)
```

**Prevention — Three Layers:**
```
Layer 1: DB unique constraint (primary dedup guard)
  Before sending: INSERT INTO notification_records (event_id, channel, status='processing')
  ON CONFLICT (event_id, channel) DO NOTHING
  RETURNING id → if null → already processed → skip
  Commit Kafka offset regardless (safe to move on)

Layer 2: Commit offset AFTER delivery record written
  message = kafka.poll()
  insert_delivery_record(event_id, channel)   # guard inserted
  provider.send(recipient, payload)
  kafka.commit(message.offset)                # commit only after success
  → crash before commit → message replayed → Layer 1 catches duplicate

Layer 3: Provider-level idempotency
  FCM: collapse_key deduplicates for same device+key within 4s window
  SES: no native dedup → rely on Layers 1&2
```

### Partial Fan-out Failure
```
Dispatcher fans out to push + email + SMS
Crashes after push succeeds, before email sent

On restart: Kafka replays message, Dispatcher retries all channels
  Push:  delivery record exists → Layer 1 skips → correct
  Email: no record yet → sends fresh → correct
  SMS:   no record yet → sends fresh → correct

Each channel independently idempotent via its own delivery record row.
```

---

## 4. Push Provider (FCM / APNS) Fails

### Scenario
FCM service returns 5xx or times out. APNS unresponsive.

### What Fails
- Push notification not delivered
- User doesn't know about event until they open app

### Detection
```python
@circuit_breaker(failure_threshold=5, recovery_timeout=60)
def send_push(token, payload):
    response = fcm_client.send(token, payload, timeout=3s)
    if response.status_code == 500:
        raise ProviderException()
    return response
```

### Failure Handling (Component AFTER Dispatcher fails)
```
FCM circuit OPEN (5 failures in 30s):
  1. Move to retry queue with delay: 60s → 120s → 300s → 900s → 3600s
  2. If push unavailable >5 min: fallback to in-app notification (WebSocket/DB polling)
  3. If push unavailable >30 min: fallback to email for critical notifications

APNS failure (iOS):
  No fallback push provider — Apple controls the entire stack
  → Queue with exponential backoff
  → After 24h pending: send email if user has email registered
```

### Token Invalid Response (Not a Failure, But Important)
```
FCM: NotRegistered → delete token from Redis + DB immediately
APNS: 410 Unregistered with timestamp → if timestamp > token's registered_at → delete

Never retry to an invalid token (wastes resources, FCM may throttle the entire app key)
```

**Real-world (Uber):** FCM instability in 2019 caused batch retry storm → temporary service degradation. Fix: retry with jitter + circuit breaker to prevent thundering herd.

---

## 5. Email Provider (SES) Fails

### Scenario
AWS SES returns 5xx, or SES rate-limits the account (hard bounce rate >5%).

### Impact
- Transactional emails (OTP, password reset) not delivered
- User cannot log in or reset password

### Rate-Limit-Induced Failure (Bounce Rate)
```
Email bounces > 5% → SES suspends sending → ALL email blocked
This is a provider-enforced safety limit (prevents spam blacklisting)

Prevention:
  - Maintain suppression list: never send to previously bounced emails
  - SES handles this automatically via SNS delivery webhooks
  - Monitor bounce rate dashboard: alert if > 2% (before SES limit)
  - Hard bounce (invalid address): add to permanent suppression
  - Soft bounce (mailbox full): retry 3× over 24h, then suppress
```

### Failover to SendGrid
```
Circuit breaker state: SES OPEN
  → Route to SendGrid (pre-configured secondary)
  → SendGrid API key in Secrets Manager
  → Same template, same idempotency key
  → Higher cost: SendGrid ~2× SES cost per email
  → Budget: justify for critical emails only

Non-critical emails (marketing, digest):
  → If SES down: pause bulk sends, queue in DLQ
  → Resume when SES recovers
  → Users see digest 1-2 hours late: acceptable
```

---

## 6. Redis (Cache/Rate Limit Store) Fails

### Scenario
Redis primary fails, Sentinel elects new primary (30-60s window).

### Impact
- Device token lookups fail → cannot look up where to send push
- Rate limiting fails → must decide: fail-open or fail-closed
- User preferences fail → cannot check DND, channel preferences

### Fail-Open vs Fail-Closed for Rate Limiting (#59)
```
Option A: Fail-Closed (reject all notifications during Redis outage)
  → 60s Redis outage = 60s of zero notifications
  → OTP requests fail → user cannot log in
  Unacceptable for critical notifications

Option B: Fail-Open (skip rate limiting during Redis outage)
  → Some users may receive slightly more than rate limit during outage
  → OTP still delivered, critical events still processed
  → Acceptable trade-off: 60s over-delivery vs 60s zero delivery

Recommendation: Fail-Open for critical/normal, Fail-Closed for bulk/marketing
```

### Device Token Fallback
```
Redis down → try Postgres (source of truth)
  SELECT token, platform FROM devices WHERE user_id = $1 AND active = true

Latency: Redis: <1ms vs Postgres: ~10ms
At 43,500 push/sec: 43,500 × 10ms = would overwhelm Postgres if all misses
→ Must not fall through to Postgres for all traffic

Fix: Local in-memory LRU cache (per Dispatcher pod)
  100K most active users' tokens cached in heap (LRU eviction)
  TTL: 60s (refresh from Redis/Postgres on miss)
  Redis failover 60s: most active tokens served from local cache
  Cold tokens (inactive users): fail gracefully, queue for retry
```

---

## 7. Database (Notification Records) Fails

### Scenario
Postgres primary crashes; failover to replica takes 60-120s.

### Impact
- Cannot write delivery records → duplicate delivery risk on retry
- Cannot update status → tracking/audit incomplete

### Handling
```
During failover window (60-120s):
  Dispatcher: continue sending to providers (don't block on DB)
  Delivery records: publish to Kafka topic: notifications.delivered
    { event_id, channel, status, provider, provider_msg_id, sent_at }

Separate DB Writer service consumes notifications.delivered:
  Normal operation: writes to Postgres in real-time
  Postgres down:    Kafka retains messages (durable, cheap)
  After failover:   DB Writer resumes from last committed offset → catches up

Trade-off: during outage, tracking is delayed not lost.
  Notification delivery itself is unaffected — Dispatcher never blocks on DB.
```

**Why Kafka beats Redis buffer here:**
- Kafka is already in the system — no new infra
- Durable on disk (not RAM) — 10× cheaper than Redis for this use case
- Survives both Postgres AND Redis failures independently
- DB Writer can be a simple consumer — no complex retry logic needed

---

## 8. SMS Provider (Twilio) Fails

### Scenario
Twilio API returns 5xx or is unreachable.

### Impact
- Critical SMS (OTP) not delivered
- User cannot complete 2FA → locked out

### Fallback Chain
```
Twilio fails (circuit OPEN after 3 failures):
  1. Try AWS SNS SMS (separate carrier routing, different reliability profile)
  2. Both fail → fallback to:
     a. Email OTP (if user has email + less secure but functional)
     b. TOTP via authenticator app (if user has set up)
     c. Backup codes

Emergency protocol (both SMS providers down):
  Support team alerted
  Temporary: allow login without 2FA for low-risk sessions (new device detection still active)
  Risk: acceptable for 15min outage window, monitored and auto-reversed
```

---

## 9. Downstream: WebSocket Gateway Fails (In-app notifications)

### Scenario
WebSocket server crashes; users' persistent connections drop.

### Impact
- In-app notifications not pushed to connected users
- Users don't see real-time updates

### Recovery
```
On reconnect (clients auto-reconnect within 5s):
  Client sends: { last_seen_notification_id: "notif_9872" }
  Server: SELECT * FROM notifications WHERE id > $1 AND user_id = $2 ORDER BY id
  → sends all missed in-app notifications
  
  Works because in-app notifications stored in DB before WebSocket delivery attempt
  → No loss, just slightly delayed (reconnect time)
```

---

## Cascading Failure Scenario: Notification Storm

**Trigger:** System backlog clears after 30min outage → all queued notifications process simultaneously

```
Normal: 58K/sec
Post-outage burst: 30min × 58K = 104M queued notifications
All workers spin up → 104M/sec attempt → FCM rate limit hit → FCM returns 429

Without protection:
  Workers retry immediately → more 429s → retry storm → FCM bans app key for 1hr

With protection (exponential backoff + jitter):
  Worker receives 429 from FCM:
    Wait = min(2^attempt × 100ms + random(0,100ms), 60s)
    attempt=1: ~200ms, attempt=2: ~400ms, ...
  
  At the queue level:
    After outage: max_workers limited to 2× normal (not 10×)
    Gradual scale-up: +10 workers/minute, monitor FCM response rate
    If 429 rate > 5%: pause scale-up, hold current worker count
```

---

## Failure Summary Matrix

| Component | Failure Mode | Impact | Prevention | Recovery Time |
|-----------|-------------|--------|------------|---------------|
| API Layer | Pod crash | Event loss (if no outbox) | Upstream outbox pattern | 10s (ALB health check) |
| Kafka | Broker crash | Producer blocks | min.insync.replicas=2, RF=3 | 30s (leader election) |
| Dispatcher | Crash mid-process | Duplicate notifications | Idempotency key (Redis NX) | Immediate (SQS retry) |
| FCM | 5xx / timeout | Push not delivered | Circuit breaker, retry backoff | Up to provider recovery |
| SES | Suspension | Email blocked | Bounce rate monitoring, fallback to SendGrid | Minutes to hours |
| Redis | Primary failure | Rate limit/token miss | Fail-open rate limit, local LRU cache | 60s (Sentinel) |
| Postgres | Primary failure | Record tracking gap | Buffer in Redis, write after failover | 60-120s |
| Twilio | API down | OTP not delivered | Failover to SNS SMS → email OTP | 30s (circuit breaker) |
| WebSocket GW | Crash | In-app notification missed | Client reconnect + catch-up query | 5s (reconnect) |
