# Notification System — System Design
*Date: 2026-06-26*

---

## First Principles — Do We Even Need This?

**Problem:** Users need to know about events affecting them (new message, payment received, ride arriving) without polling the application.

**Why not let each service notify directly?**
- Service A (payments) calls FCM, Service B (chat) calls FCM → duplicated logic, no rate limiting coordination, no unified preferences
- User gets 40 push notifications in 2 minutes → app deleted
- If FCM is down, each service handles fallback independently (or not at all)

**Core insight:** Notification delivery is infrastructure, not product logic. Centralize it. Every service publishes events; one system handles channel selection, rate limiting, deduplication, delivery, and retry.

---

## Scale Estimates

**Assumptions:**
- 500M registered users, 100M DAU
- 50 notifications/user/day → 5B/day = **58,000/sec avg**, peak 3× = **175,000/sec**
- Channel split: 75% push (FCM/APNS), 15% email, 9% in-app, 1% SMS
- Average user: 2.5 devices (phone + tablet + web)
- Average push notification: 512 bytes

**Throughput:**
```
Push:   58K/sec × 75% = 43,500 push/sec
Email:  58K/sec × 15% = 8,700 email/sec = 750M email/day
SMS:    58K/sec × 1%  = 580 SMS/sec    = 50M SMS/day
In-app: 58K/sec × 9%  = 5,220/sec (delivered over existing WebSocket)
```

**Storage:**
```
Notification records: 5B/day × 200 bytes = 1TB/day
Retention: 30 days = 30TB (cold storage after 7 days)
User preferences: 500M users × 2KB = 1TB (fits in distributed cache)
Device tokens: 500M users × 2.5 devices × 200 bytes = 250GB (Redis)
```

---

## AWS Cost Estimate (Back of Envelope)

**Budget at scale (100M DAU, 5B notifications/day):**

| Component | Monthly Volume | Unit Cost | Monthly Cost |
|-----------|---------------|-----------|--------------|
| SQS (priority queues) | 150B messages | $0.40/1M | $60,000 |
| FCM/APNS push | 3.75B pushes | Free (provider) | $0 |
| SNS (if used as push gateway) | 3.75B pushes | $0.50/1M | $1,875 |
| SES email | 750M emails | $0.10/1K | $75,000 |
| Twilio SMS | 1.5B SMS | $0.0079 each | **$11,850,000** |
| EC2 workers (150 m5.xlarge) | 150 × $0.192/hr | 24×30 | $20,736 |
| Redis (ElastiCache r6g.2xl) | 10 nodes | $0.484/hr | $34,848 |
| **Total (with SMS)** | | | **~$12M/month** |
| **Total (no SMS)** | | | **~$192K/month** |

**Budget constraint — SMS is a revenue killer:**
- SMS at scale = $12M/month
- Most companies: SMS reserved for critical security events (OTP, fraud alerts) only
- Standard limit: max 2 SMS/user/day, only for specific event types
- Alternative: WhatsApp Business API ($0.005/msg for utility = 60% cheaper)

**Engineering limitation from budget:**
- $200K/month budget → must skip SMS for all non-critical events
- Email capped at 750M/day → must use digest batching for lower-priority events
- At $75K/month email cost, digest reduces volume 10× → saves $67K/month

---

## Entities & Actions

### Entities
```
User                id, email, phone, timezone, language
Device              id, user_id, token, platform (ios/android/web), last_seen_at
UserPreferences     user_id, channel_id, enabled, dnd_start, dnd_end, digest_mode
NotificationEvent   id, user_id, type, payload, priority, idempotency_key, created_at
NotificationRecord  id, event_id, channel, status, sent_at, provider_msg_id, attempts
Template            id, type, channel, locale, subject_template, body_template
Provider            id, channel, name, api_key, priority_order, circuit_state
```

### Actions
- Register/update device token
- Set user notification preferences (per-channel, per-type)
- Publish notification event (from any upstream service)
- Fan-out event to user's channels and devices
- Rate check before delivery
- Render template with payload
- Deliver via provider (FCM, SES, Twilio)
- Track delivery receipt
- Retry failed deliveries (backoff)
- Aggregate digests

---

## Data Flow

```
Upstream Service (Chat, Payments, Ride)
    │  POST /events   { type, user_id, payload, priority, idempotency_key }
    ▼
[Notification API] ─── deduplicate (Redis SET NX) ───► discard duplicate
    │
    ├── write NotificationEvent to DB (Postgres)
    │
    └── publish to Kafka topic: notifications.raw
                │
    ┌───────────┼──────────────────┐
    ▼           ▼                  ▼
[Priority Q] [Normal Q]    [Bulk/Digest Q]  ← 3 SQS queues (separate throughput)
    │           │                  │
    ▼           ▼                  ▼
[Dispatcher Service]
    │
    ├── fetch user preferences (Redis → Postgres fallback)
    ├── fetch device tokens (Redis)
    ├── check rate limits (Redis sliding window)
    ├── check DND window (user timezone)
    ├── select channels (push + email + in-app per preference)
    ├── render template (payload → localized text)
    │
    ├──► [Push Worker] → FCM / APNS
    ├──► [Email Worker] → SES / SendGrid
    ├──► [SMS Worker]   → Twilio (priority only)
    └──► [In-app Worker]→ WebSocket Gateway pub/sub
```

---

## High-Level Design

```
                     ┌─────────────────────────────────────┐
Upstream Services    │         Notification Service         │
(Chat, Pay, Rides) ─►│   API Layer (REST + gRPC)            │
                     │   Idempotency → Kafka Publisher       │
                     └────────────────┬────────────────────┘
                                      │ Kafka
                     ┌────────────────▼────────────────────┐
                     │          Dispatcher Cluster          │
                     │  Fan-out │ Pref check │ Rate limit   │
                     └──┬───────────┬─────────────┬────────┘
                        │           │             │
               ┌────────▼──┐  ┌────▼────┐  ┌────▼────────┐
               │Push Workers│  │Email    │  │SMS Workers  │
               │(FCM/APNS) │  │Workers  │  │(Twilio)     │
               └────────┬──┘  │(SES)    │  └────┬────────┘
                        │     └────┬────┘       │
                        │          │            │
                     ┌──▼──────────▼────────────▼──┐
                     │    Delivery Tracking DB       │
                     │  (notification_records)       │
                     └────────────────────────────┬─┘
                                                  │
                     ┌────────────────────────────▼─┐
                     │      Retry Scheduler           │
                     │  (failed + pending requeue)    │
                     └───────────────────────────────┘
```

---

## Low-Level Design

### 1. Priority Queue Architecture

Three separate SQS queues with isolated worker pools:

```
Priority   → critical: OTP, fraud alert, payment failed
             SQS FIFO, max 1M msg/s, 5 retry attempts, DLQ
             Workers: 50 dedicated, reserved capacity

Normal     → new message, likes, follows
             SQS Standard, 10 retry attempts, DLQ
             Workers: 100 auto-scaling (10-200)

Bulk/Digest → newsletters, weekly digest, marketing
             SQS Standard, max throughput limited to 1K/sec
             Workers: 20 auto-scaling (2-50)
```

**Why separate queues (Bulkhead pattern #65):**
- Bulk email storm doesn't delay OTP delivery
- Priority queue workers never share threads with bulk

### 2. Rate Limiting Per User Per Channel

```python
# Sliding window rate limit using Redis (see #8 Caching Patterns)
def check_rate_limit(user_id, channel, notification_type):
    key = f"ratelimit:{user_id}:{channel}:{notification_type}"
    limits = {
        ("push", "social"):    (20, 3600),   # 20/hr
        ("push", "critical"):  (100, 3600),  # no effective limit
        ("email", "marketing"): (1, 86400),  # 1/day
        ("email", "transact"): (50, 86400),  # 50/day
        ("sms", "any"):        (2, 86400),   # 2/day hard cap
    }
    max_count, window = limits.get((channel, notification_type), (10, 3600))
    
    now = time.time()
    pipe = redis.pipeline()
    pipe.zremrangebyscore(key, 0, now - window)   # remove old entries
    pipe.zcard(key)
    pipe.zadd(key, {str(now): now})
    pipe.expire(key, window)
    _, count, _, _ = pipe.execute()
    
    return count < max_count
```

**Trade-off:** Rate limiting adds ~1ms per notification (Redis round-trip). Not batching this would mean 2 Redis calls/notification. Batch with Lua script to atomic check-and-increment.

### 3. Device Token Management

```
FCM/APNS tokens expire or rotate when:
  - User reinstalls app → new token
  - App update on iOS → token may rotate
  - Push notification rejected (410 Gone) → token invalid

Schema (Redis Hash):
  devices:{user_id} → {
    "device_1": { token: "abc...", platform: "ios", updated_at: 1700000000 },
    "device_2": { token: "xyz...", platform: "android", updated_at: 1700000001 }
  }

Token update:
  App registers on startup → POST /devices/register
  → HSET devices:{user_id} device_id {token, platform, now}

Token cleanup (after delivery):
  FCM response: NotRegistered → DELETE token from Redis + DB
  APNS response: 410 Unregistered → same
  Tokens not updated in 90 days → background job cleans up
```

**Why Redis, not Postgres, for tokens?**
- Token lookup on every notification (high read volume)
- Tokens update frequently (app startup)
- Acceptable to lose Redis and rebuild from DB (tokens also stored in Postgres as source of truth)

### 4. Template Rendering

```python
Template (stored in Postgres, cached in Redis, TTL 5min):
{
  type: "ride_arriving",
  channel: "push",
  locale: "en",
  title: "{{driver_name}} is arriving",
  body: "Your ride arrives in {{eta_minutes}} minutes"
}

Rendering:
  template = get_template(notification_type, channel, user_locale)
  rendered = jinja2.render(template, payload)
  # payload = { driver_name: "Arjun", eta_minutes: 3 }
  # → title: "Arjun is arriving", body: "Your ride arrives in 3 minutes"
```

**Trade-off (cache vs fresh templates):**
- Cache hit: 0ms render, but stale template if marketer changes copy
- Cache TTL 5min: max 5min delay for template updates — acceptable
- Invalidate on publish via Redis pub/sub for immediate propagation

### 5. Provider Failover

```
Push delivery attempt:
  1. Try FCM primary (circuit breaker #32 wraps it)
     - Timeout: 3s, threshold: 5 failures → OPEN 60s
  2. If FCM circuit OPEN → route to alternate FCM endpoint or delay 60s
  3. For APNS: similar but APNs has no backup — if down, queue with 10min delay

Email delivery:
  Primary: SES (cheaper)
  Secondary: SendGrid (OPEN when SES fails)
  Tertiary: queue in DLQ, retry every 15min

SMS delivery:
  Primary: Twilio
  Secondary: AWS SNS SMS (different routing, may reach different carriers)
  If both fail: convert to email or push if user has those channels
```

### 6. DND (Do Not Disturb) + Digest Mode

```
User preference:
  dnd_start: "22:00" (local time)
  dnd_end: "08:00" (local time)
  digest_mode: true (batch non-critical into hourly digest)

Dispatcher logic:
  user_tz = "Asia/Kolkata"
  local_time = now.astimezone(user_tz)

  if is_dnd(local_time, prefs) and priority != CRITICAL:
    # Store in digest queue with delivery_after = next DND end
    enqueue_for_later(notification, delivery_after=next_dnd_end(user_tz))
  elif prefs.digest_mode and priority == NORMAL:
    # Add to hourly digest batch
    append_to_digest(user_id, notification)
  else:
    dispatch_immediately(notification)
```

**Digest batching (saves ~70% email volume):**
```
Hourly job: group pending digest notifications per user
  SELECT user_id, COUNT(*), array_agg(notification_id)
  FROM pending_digest
  WHERE delivery_after <= now()
  GROUP BY user_id

  → render single digest email: "5 new notifications: ..."
  → send 1 email instead of 5
  → mark all as sent
```

### 7. Delivery Tracking

```sql
notification_records (
  id            UUID PRIMARY KEY,
  event_id      UUID REFERENCES notification_events(id),
  channel       VARCHAR(20),    -- push, email, sms, in_app
  status        VARCHAR(20),    -- queued, sent, delivered, failed, bounced
  provider      VARCHAR(20),    -- fcm, apns, ses, twilio
  provider_msg_id VARCHAR(255), -- FCM message_id, SES MessageId
  attempts      INT DEFAULT 0,
  last_error    TEXT,
  sent_at       TIMESTAMPTZ,
  delivered_at  TIMESTAMPTZ,
  created_at    TIMESTAMPTZ DEFAULT now()
)
```

**Delivery confirmation:**
- FCM: success response contains message_id; FCM Data callback if app sends receipt
- APNS: success response only (no delivered receipt — iOS doesn't confirm)
- SES: delivery events via SNS webhooks → update status to DELIVERED/BOUNCED
- SMS: Twilio delivery webhooks (carrier-confirmed)

---

## Key Trade-offs

| Decision | Choice | Reason | Trade-off |
|----------|--------|--------|-----------|
| Fan-out timing | Async via Kafka | Upstream service not blocked | 100ms-2s notification delay |
| Priority separation | 3 separate queues | OTP never delayed by bulk | More infra, more ops overhead |
| Token store | Redis (Postgres backup) | Sub-ms lookup | Extra sync complexity |
| Template cache | Redis TTL 5min | Fast render | Stale templates up to 5min |
| Rate limiting | Per-user sliding window | Prevents spam | Redis round-trip per notification |
| Email digests | Hourly batching | Reduces cost 70% | User sees notification 0-60min late |
| SMS | Priority only | $11M/month at scale | Users don't get SMS for social events |
| Provider fallback | Circuit breaker chain | Resilience | Adds 3s timeout before fallback |

---

## Real-World Reference

- **Airbnb**: Centralized notification service "Merlot" — Kafka fan-out, multi-channel, preference management. Blog: [Airbnb Tech 2021]
- **Uber**: "UNS" (Uber Notification Service) — priority tiers, multi-region, handles 10M+ notifications/hour
- **LinkedIn**: "Voyager" — digest mode reduces email volume by 80%, reduced unsubscribes by 30%
- **Apple APNS**: Max payload 4KB, silent push for background refresh, priority=10 (immediate) vs priority=5 (power-efficient)
- **FCM**: Max payload 4KB, Data messages (app handles) vs Notification messages (OS handles), collapse_key for dedup

---

## Summary

```
Scale:  58K notifications/sec avg, 175K/sec peak
Latency: critical <2s, normal <30s, bulk minutes
Cost:   ~$192K/month (no SMS) → $12M/month (with SMS at scale)
Key:    SMS budget forces channel restriction
        Digest mode saves 70% email volume
        Bulkhead queues keep OTP unaffected by bulk storms
        Circuit breakers handle provider outages gracefully
```
