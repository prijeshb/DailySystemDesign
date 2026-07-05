# Notification System — System Design
*Date: 2026-06-03*

---

## 0. Do We Even Need a Dedicated Notification System?

**First-principles check:**
- Can the app server just call Twilio/FCM directly? Yes — for < 10K users.
- Why not? At scale: blocking API calls slow down business logic, retries become complex, no visibility into delivery, rate limits per vendor need central management, different channels (push/SMS/email) need unified logic.
- **Decision:** Yes, we need a dedicated async notification service once you have multi-channel delivery, > 1M users, or SLA requirements on delivery.

---

## 1. Entities

| Entity | Key Attributes |
|--------|---------------|
| User | user_id, preferences (channel, quiet hours, locale) |
| Notification | notif_id, type, title, body, data_payload, priority |
| Template | template_id, channel, locale, body_template |
| DeviceToken | token, user_id, platform (iOS/Android/Web), active |
| NotificationLog | notif_id, user_id, channel, status, attempt_count, sent_at |
| Subscription | user_id, topic, opted_in |

---

## 2. Actions (APIs)

```
POST /notify
  body: { user_id, type, data, priority }
  → enqueues notification job

POST /notify/bulk
  body: { user_ids[], type, data }
  → fan-out to per-user jobs

PUT  /users/{id}/preferences
  body: { channels, quiet_hours, locale }

POST /devices/register
  body: { user_id, token, platform }

GET  /notifications/{notif_id}/status

POST /topics/{topic}/publish          ← broadcast (e.g. breaking news)
```

---

## 3. Data Flow

```
Client / Service
      │  POST /notify
      ▼
API Gateway → Auth → Rate Limiter
      │
      ▼
Notification Service
  1. Validate & enrich (fetch user prefs, template)
  2. Check quiet hours / DND
  3. Deduplicate (idempotency key)
  4. Priority queue selection
      │
      ├─► High-priority queue  (Kafka topic: notif-high)
      └─► Normal queue         (Kafka topic: notif-normal)
              │
        ┌─────┴──────┐
        ▼            ▼
   Push Worker   Email Worker   SMS Worker
   (FCM/APNs)   (SendGrid)     (Twilio)
        │
        ▼
   Delivery Receipt → Log DB (Cassandra)
        │
        ▼
   Analytics / Dashboard
```

**Fan-out flow (bulk):**
```
POST /notify/bulk (1M users)
  → Notification Service writes one "broadcast job" to Kafka
  → Fan-out workers read user segment, expand to per-user messages
  → Per-user messages → channel queues
```

---

## 4. High-Level Design

```
                    ┌──────────────────────────────────────────┐
                    │           Notification Platform           │
                    │                                          │
Producers ──────►  API Layer (REST/gRPC)                       │
(App Servers,       │                                          │
 Cron Jobs,         ▼                                          │
 Events)       Notification Service                            │
               (validate, enrich,     ◄── User Pref DB (MySQL) │
                dedup, route)         ◄── Template DB (MySQL)  │
                    │                                          │
                    ▼                                          │
              Kafka Cluster                                    │
           ┌────────┴────────┐                                 │
           ▼                 ▼                                 │
     Priority Queue     Normal Queue                           │
           │                 │                                 │
    ┌──────┴──────┐   ┌──────┴──────┐                         │
    ▼             ▼   ▼             ▼                          │
Push Workers  SMS Workers  Email Workers                       │
(FCM/APNs)   (Twilio)     (SendGrid/SES)                      │
    │                                                          │
    ▼                                                          │
Log DB (Cassandra)  ──► Analytics (ClickHouse)                 │
                                                              │
                    └──────────────────────────────────────────┘
```

---

## 5. Low-Level Design

### 5.1 Notification Service

```python
class NotificationService:
    def send(self, request: NotifyRequest):
        # 1. Idempotency check
        if cache.exists(request.idempotency_key):
            return cached_result

        # 2. Fetch user preferences
        prefs = user_pref_db.get(request.user_id)
        if not prefs.opted_in(request.type):
            return  # silently drop

        # 3. Quiet hours check
        if in_quiet_hours(prefs.quiet_hours, prefs.timezone):
            schedule_for_later(request)
            return

        # 4. Select channel (preference order: push > email > sms)
        channels = prefs.preferred_channels or ["push"]

        # 5. Render template
        for channel in channels:
            msg = template_engine.render(request.type, channel, prefs.locale, request.data)
            queue = HIGH_Q if request.priority == "high" else NORMAL_Q
            kafka.produce(queue, NotificationJob(msg, channel, request.user_id))

        # 6. Store idempotency key (TTL: 24h)
        cache.set(request.idempotency_key, "sent", ttl=86400)
```

### 5.2 Push Worker (FCM/APNs)

```python
class PushWorker:
    def process(self, job: NotificationJob):
        tokens = device_token_db.get_active(job.user_id)
        for token in tokens:
            try:
                resp = fcm.send(token, job.payload)
                if resp.is_invalid_token():
                    device_token_db.deactivate(token)
                log_db.write(job.notif_id, token.platform, "delivered")
            except ThrottleException:
                requeue_with_backoff(job, delay=30s)
            except Exception as e:
                if job.attempt < MAX_RETRIES:
                    requeue_with_backoff(job)
                else:
                    log_db.write(job.notif_id, "failed", str(e))
```

### 5.3 Device Token Management

- Tokens expire / rotate (APNs invalidates on app uninstall)
- On 410 Gone from APNs → mark token inactive
- On 400 BadDeviceToken from FCM → delete token
- Store: `device_tokens` table with `(user_id, token, platform, active, updated_at)`

### 5.4 Idempotency

- Producer passes `idempotency_key` (UUID or hash of event_id + user_id)
- Notification Service checks Redis before processing
- If key exists → return early (prevents duplicate sends on retry)
- TTL = 24 hours (covers retry window)

### 5.5 Rate Limiting

Two layers:
1. **Per-user:** Max N notifications/hour per channel (prevent spam)
2. **Per-vendor:** FCM/Twilio/SendGrid each have rate limits → token bucket per vendor in Redis

```
Redis key: rate:{vendor}:{minute_bucket}
INCR → if > limit, delay job or route to overflow queue
```

### 5.6 Fan-out for Broadcast (e.g., 100M users)

```
Option A — Push fan-out (write at send time):
  Pros: simple, immediate
  Cons: 100M messages in queue, high latency for last user

Option B — Pull fan-out (read at delivery time):
  Pros: single message stored
  Cons: complex, need to materialize at read

Decision: Hybrid
  - Celebrities / mass broadcast → pull model (segment-based)
  - Normal users → push fan-out
```

Segment approach:
```
Fan-out Service reads user_ids from segment store in batches of 1000
→ writes 100K Kafka messages (batched per 1000 users each)
→ Workers expand each batch message → individual sends
```

### 5.7 Storage Schema

**notification_log (Cassandra)**
```
PRIMARY KEY: (user_id, created_at)  ← query by user
Columns: notif_id, channel, status, attempt_count, sent_at, delivered_at
```
Why Cassandra? Write-heavy, time-series, high throughput, TTL support.

**user_preferences (MySQL)**
```
user_id PK, push_enabled, email_enabled, sms_enabled,
quiet_start, quiet_end, timezone, locale, updated_at
```

**device_tokens (MySQL)**
```
(user_id, platform) → token, active, updated_at
Index on token for reverse lookup
```

---

## 6. Trade-offs

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Kafka for queuing | Yes | Durable, replayable, but operationally complex vs SQS |
| Separate priority queues | Yes | Low-latency for critical notifs, but more infra |
| Cassandra for logs | Yes | Fast writes, but eventual consistency, no complex queries |
| Idempotency via Redis | Yes | Fast dedup, but Redis can lose data on restart → use Redis AOF |
| Fan-out at write time | For small audiences | Simpler, but not scalable for 100M recipients |
| Template rendering in service | Yes | Centralized, but service must know all templates |
| Push > Email > SMS priority | Default | Cheapest channel first, but user may miss push |

---

## 7. Real-World References

- **Meta:** Uses async notification pipeline with priority lanes; inbox model for in-app
- **Airbnb Engineering Blog:** Notification preferences & DND implementation
- **Uber:** Separate notification service, retries with exponential backoff, fallback channels
- **Apple APNs docs:** Token-based auth (JWT), 4KB payload limit, collapse keys
- **FCM docs:** Batch send API, topic messaging for broadcast
- **AWS SNS + SQS:** Common pattern for fan-out at AWS scale

---

## 8. Key Numbers

| Metric | Estimate |
|--------|----------|
| 1B users, 5 notifs/day | 50K notifs/sec |
| Push notification size | ≤ 4KB (APNs), ≤ 4KB (FCM) |
| Delivery SLA (high priority) | < 1 sec |
| Delivery SLA (normal) | < 30 sec |
| Log retention | 30 days |
| Kafka partition count | ~50 (for 50K msg/sec throughput) |
