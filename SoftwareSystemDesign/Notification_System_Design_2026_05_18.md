# Notification System — System Design (2026-05-18)

## First Principles: Do We Need It?

> "Do we really need a notification system at all?"

Yes — when an action in system A must trigger awareness in system B (user or service) **asynchronously**, without coupling A to the delivery mechanism. Email, push, SMS, in-app — these are all delivery channels, not business logic. Centralising them:
- Prevents every team from re-implementing SMTP/APNs/FCM
- Gives one place to enforce rate limits, preferences, deduplication
- Isolates delivery failures from core flows

---

## Scope & Scale Assumptions

| Parameter | Value |
|---|---|
| DAU | 100M users |
| Notifications/day | 1B (10 per user avg) |
| Peak QPS (fan-out writes) | ~500K/s |
| Channels | Push (iOS/Android), Email, SMS, In-App |
| Latency SLA | Push < 5s, Email < 30s, SMS < 60s |
| Delivery guarantee | At-least-once |
| Idempotency | Required (dedup on consumer side) |

---

## Entities

```
User           { user_id, preferences{channel, quiet_hours, opt_outs} }
Notification   { notif_id, type, payload, priority, ttl, idempotency_key }
Template       { template_id, channel, locale, body }
Subscription   { user_id, device_token, channel, active }
DeliveryLog    { log_id, notif_id, user_id, channel, status, attempt, ts }
```

---

## Actions

1. **Trigger** — producer calls NotificationService with event + user_id(s)
2. **Fan-out** — expand user_id list → individual notification records
3. **Preference check** — filter by user opt-outs, quiet hours, channel caps
4. **Render** — merge payload + template → final message per channel
5. **Dispatch** — send to channel gateway (APNs, FCM, SendGrid, Twilio)
6. **Ack / Retry** — log delivery status; retry on failure
7. **Read** — user fetches in-app notification feed

---

## Data Flow

```
[Producer Service]
     |  event{type, user_ids[], payload}
     v
[Notification API]  — validates, assigns notif_id, idempotency check
     |
     v
[Fan-out Queue]  ← Kafka topic: "notif.fanout"  (partitioned by notif_id)
     |
     v
[Fan-out Workers]
  - pull user preference from cache (Redis)
  - filter opt-outs / quiet hours
  - write per-user per-channel tasks to delivery queues
     |
     +——> Kafka: "notif.push"
     +——> Kafka: "notif.email"
     +——> Kafka: "notif.sms"
     +——> Kafka: "notif.inapp"  → written to In-App DB
     |
[Channel Workers] (one consumer group per channel)
  - render template
  - call 3rd-party gateway
  - write DeliveryLog
  - on failure → DLQ
```

---

## High-Level Design

```
                        ┌──────────────────────────────────────────┐
  Producers ──────────► │        Notification API (stateless)       │
                        └──────────────┬───────────────────────────┘
                                       │ publish
                                       ▼
                              ┌─────────────────┐
                              │  Kafka (fanout)  │  partition by notif_id
                              └────────┬────────┘
                                       │ consume
                                       ▼
                              ┌─────────────────┐
                              │  Fan-out Workers │ ← Redis (user prefs cache)
                              └────────┬────────┘
                   ┌───────────────────┼──────────────────┐
                   ▼                   ▼                  ▼
            Kafka(push)          Kafka(email)        Kafka(sms)
                   │                   │                  │
                   ▼                   ▼                  ▼
         [Push Worker]         [Email Worker]      [SMS Worker]
           FCM / APNs            SendGrid           Twilio
                   │                   │                  │
                   └───────────────────┴──────────────────┘
                                       │
                               ┌───────▼──────┐
                               │  DeliveryLog │  (Cassandra / DynamoDB)
                               └──────────────┘

  In-App feed: separate path
  Fan-out Workers ──► write rows to PostgreSQL/Cassandra ──► served via Feed API
```

---

## Low-Level Design

### 1. Idempotency

Every trigger includes an `idempotency_key` (producer-generated, e.g. `order_123_shipped`).

```
Notification API:
  key = SHA256(idempotency_key + user_id + channel)
  Redis SET NX key=1 EX=86400
  if EXISTS → 409, skip

Channel Worker:
  Before calling gateway: check DeliveryLog for (notif_id, user_id, channel, status=SUCCESS)
  if found → skip (handles duplicate Kafka delivery)
```

**Why two layers?** API layer stops duplicate publishes. Worker layer stops duplicate sends from Kafka at-least-once redelivery.

---

### 2. Fan-out Strategies

| Strategy | Use Case | Trade-off |
|---|---|---|
| **Push model** (write to each user's queue at write time) | Low-follower users | Fast read, slow write for celebrities |
| **Pull model** (compute at read time) | High-follower users (>10K) | Slow read, cheap write |
| **Hybrid** | Celebrity threshold | Complexity ↑, but used by Twitter/Instagram |

For notifications (not social feed), push is fine — max fan-out is bounded (marketing blast → batch job, not real-time).

**Marketing blasts** (10M users): use **batch fan-out** worker reading from user segments DB, not a single Kafka message. Rate-limit at 50K dispatches/sec to avoid overwhelming gateways.

---

### 3. Priority Queues

```
Priority levels: CRITICAL (OTP, account security) | HIGH (order updates) | LOW (marketing)

Separate Kafka topics per priority.
Channel workers consume CRITICAL topic first.
LOW messages get TTL=24h; expired messages dropped before send.
```

---

### 4. User Preferences & Caching

```
Preference Service ──► PostgreSQL (source of truth)
                   ──► Redis (TTL=5min cache)

Fan-out Worker:
  prefs = redis.get(user_id)
  if miss → fetch from DB, cache it
  apply: opt_out channels, quiet_hours, frequency caps

Frequency cap (e.g. max 3 push/day):
  Redis counter: INCR notif:push:{user_id}:{date}
  EXPIRE at midnight
```

**Trade-off:** Stale prefs for up to 5 min. If user disables push, they may get 1-2 more. Acceptable for most systems. For CRITICAL, bypass cache.

---

### 5. Delivery Log Schema (Cassandra)

```
Table: delivery_log
  PK: (user_id, date)  ← partition key
  CK: notif_id, channel  ← clustering key
  Columns: status, attempt_count, last_attempt_ts, error_code
```

Why Cassandra: high write throughput, time-series access pattern (fetch last N notifications for a user), no complex joins needed.

---

### 6. Retry & Dead-Letter Queue

```
On gateway failure:
  Exponential backoff: 5s, 30s, 2m, 10m, 30m (max 5 attempts)
  Each retry republished to same Kafka topic with attempt_count++
  attempt_count > 5 → move to DLQ (Kafka topic: "notif.dlq")

DLQ processor (runs every 1h):
  Alert on-call if DLQ depth > threshold
  Manual retry or drop after investigation
```

**Why Kafka for DLQ instead of separate store?** Replay semantics — you can re-consume from any offset. Avoids data loss.

---

### 7. In-App Feed

```
Schema (PostgreSQL or Cassandra):
  notifications(user_id, notif_id, type, payload, is_read, created_at)

Feed API:
  GET /notifications?user_id=X&cursor=<last_ts>&limit=20
  Cursor-based pagination (not offset — avoids drift on inserts)

Caching: Redis sorted set per user (score = timestamp, member = notif_id)
  Max 100 recent notifications cached
  Cache invalidated on new write (fan-out worker pushes to Redis)
  SSE / WebSocket push for real-time in-app badge update
```

---

### 8. Token Management (Push)

```
Device tokens expire / rotate (FCM/APNs).
On send failure with "invalid token":
  Mark subscription as inactive
  Remove from Redis device token cache
  User re-registers token on next app open

Why this matters: sending to stale tokens wastes quota and skews delivery metrics.
```

---

## Key Trade-offs

| Decision | Chosen | Alternative | Why |
|---|---|---|---|
| Async fan-out via Kafka | ✓ | Sync DB writes | Decouples producers; handles burst traffic |
| At-least-once delivery | ✓ | Exactly-once | Exactly-once is expensive; idempotency on consumer side is cheaper |
| Separate queue per channel | ✓ | Single queue | Channel-specific back-pressure isolation |
| Redis for preference cache | ✓ | DB direct | 5min staleness acceptable; saves 10K DB reads/sec |
| Cassandra for delivery logs | ✓ | MySQL | Write-heavy, time-series, no joins needed |
| Push fan-out for notifications | ✓ | Pull at read | Notifications are low-fanout; push gives instant delivery |

---

## Capacity Estimates

```
1B notifications/day = ~11.6K/sec avg, ~500K/sec peak

Fan-out Kafka:
  Partition count: 200 (peak 500K writes/sec, ~2500/partition)
  Retention: 7 days (replay capability)

DeliveryLog writes:
  1B rows/day × 200 bytes = ~200GB/day → Cassandra cluster
  With 3x replication: ~600GB/day raw storage

Redis (preference cache):
  100M users × 200 bytes = ~20GB → fits in single large Redis instance
  Use Redis Cluster for HA

Push tokens:
  100M users × 2 devices × 150 bytes = ~30GB → Redis Hash
```

---

## Real-World References

- **Facebook Notifications** — uses TAO graph + async workers; fan-out at send time
- **Uber's Push Platform** — priority queues, circuit breakers on gateway calls
- **LinkedIn Notifications** — Kafka-based, tiered delivery (real-time vs digest)
- **Apple APNs / Google FCM** — token lifecycle management is critical at scale
