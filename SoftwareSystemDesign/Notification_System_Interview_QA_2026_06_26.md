# Notification System — Interview Q&A
*Date: 2026-06-26*

---

## How Interviewers Open This Problem

> "Design a notification service. Users should receive push, email, and SMS notifications from various parts of the platform."

Or more specific variants:
> "Our system sends too many emails and users are unsubscribing. How would you redesign the notification system?"
> "How would you design a system that sends 100M notifications per day?"
> "We want to add a notification service to our existing microservices platform."

---

## Round 1: Clarifying Questions

**Q (Interviewer): Before you start, what would you ask?**

Good clarifying questions to demonstrate:
1. "What channels do we need? Push, email, SMS, in-app, WhatsApp?"
2. "What's the scale — DAU, average notifications per user per day?"
3. "Do we need real-time delivery, or is eventual delivery acceptable?"
4. "Who are the senders — internal services or external third parties?"
5. "Do we need delivery receipts / tracking?"
6. "What are the SLAs? Critical OTP vs marketing email?"

**Why these matter:** Channel count drives architecture complexity (push alone is simple; push+email+SMS needs provider abstraction). Scale drives whether you need Kafka or can use SQS directly.

---

## Round 2: Core Design Questions

**Q: How does an upstream service (e.g., Payment Service) trigger a notification?**

A: Two options:
- Direct HTTP call to Notification API (simpler, tighter coupling, synchronous)
- Publish event to Kafka, Notification Service consumes (decoupled, async)

Prefer Kafka for production. Payment Service just publishes `payment_succeeded` event with user_id and payload. Notification Service owns the logic of which channels to use. This means Payment Service doesn't need to know anything about FCM or SES.

**Q: What if the same event triggers the notification twice?**

A: Multiple layers — each catches a different duplicate scenario:

**Layer 1 — Idempotency key at ingestion (catches upstream retries):**
Upstream generates a UUID per intent (not per attempt). Notification API does `SET idem:{key} 1 NX EX 3600`. If key exists → discard immediately before any processing.

**Layer 2 — DB unique constraint at dispatch (catches Kafka at-least-once re-delivery):**
Before sending, the Dispatcher inserts a delivery record:
```sql
INSERT INTO notification_records (event_id, channel)
VALUES ($1, $2)
ON CONFLICT (event_id, channel) DO NOTHING
RETURNING id;
-- id is NULL → already dispatched → skip
```

**Layer 3 — Provider-level deduplication (catches Dispatcher restarts mid-send):**
FCM supports `collapse_key` — if two messages with the same collapse_key reach FCM within a 4-second window, only the latest is delivered. Not a true dedup (window is short) but absorbs crash-restart races.

**Layer 4 — Consumer-side event deduplication (catches Kafka consumer group rebalance):**
Kafka can redeliver messages during rebalance. Dispatcher commits offsets only after the delivery record is written to DB. If crash before commit → message redelivered → Layer 2 catches the duplicate.

**Which layer to emphasize in an interview:**
Layer 1 is the clean architectural answer. Layer 2 is the safety net. Mention both — interviewers are specifically checking whether you know idempotency keys alone aren't enough when the system is distributed.

**Q: How do you handle different notification priorities?**

A: Three separate queues (bulkhead pattern):
- Critical queue (OTP, fraud alert, payment fail): dedicated workers, highest throughput
- Normal queue (new message, follow, like): shared workers
- Bulk queue (digest, marketing): rate-limited workers

Why not one queue with priority field? Priority queues can starve low-priority messages under load. Separate queues with separate worker pools guarantee bulk emails can't delay OTPs.

**Q: A user is in India, it's 2am. Should they get a push notification for someone liking their photo?**

A: No — DND check. Dispatcher reads user's timezone and DND window preference. For non-critical notifications during DND: store in a pending-delivery table with a `deliver_after` timestamp. Deliver when DND ends. For critical (OTP, fraud): bypass DND, deliver immediately.

---

## Round 3: Deep Dive Questions

**Q: How does push notification delivery actually work? Walk me through FCM.**

A:
1. App starts → calls FCM SDK → receives device registration token (unique per app+device)
2. App sends this token to your backend via `POST /devices/register`
3. Backend stores token in Redis + Postgres (device token table)
4. To send push: backend → FCM API with `{ token, title, body, data }` → FCM → Google data centers → user's Android device
5. Device must be online (or FCM queues for up to 4 weeks if `time_to_live` set)

Key things interviewers probe:
- "What if the token is stale?" → FCM returns `NotRegistered` → delete token, never retry to it
- "What if user has 3 devices?" → Fan-out: send to all active device tokens in parallel
- "What's the max payload?" → FCM: 4KB total. For large payloads: send `data.notification_id` only, app fetches content from API

**Q: How would you prevent notification spam? User is getting 200 notifications/hour.**

A: Multi-layer rate limiting:
1. Per-user, per-channel, per-notification-type: sliding window counter in Redis
   - Example: max 20 social push notifications per hour
2. Per-notification-type global throttle: "like" notifications batched if >5 in 1 minute → send one aggregated "5 people liked your post"
3. User preference: user can turn off specific types or channels
4. Digest mode: all non-critical notifications into one hourly email

**Q: What does the database schema for notification records look like?**

A: Core tables:
- `notification_events`: one row per incoming event (user_id, type, payload, priority, idempotency_key)
- `notification_records`: one row per delivery attempt per channel (event_id, channel, status, provider, provider_msg_id, attempts)
- `devices`: user_id → device tokens with platform + last_seen
- `user_preferences`: user_id + channel + notification_type → enabled/DND settings

Sharding: notification_events and notification_records shard by user_id. Single user's full history on one shard = efficient queries.

**Q: How do you track whether a push notification was actually seen by the user?**

A:
- Sent: FCM returns 200 + message_id → status = SENT
- Delivered to device: FCM Data callback — app sends `POST /notifications/{id}/delivered` when notification arrives (even if app in background via FCM data message)
- Opened: app sends `POST /notifications/{id}/opened` when user taps notification
- APNS: no delivered receipt. iOS doesn't tell you. You only know: sent (no error) or failed (error code).

For reporting, distinguish sent vs opened. Opened rate = product metric, not infrastructure concern.

---

## Round 4: Scaling and Trade-off Questions

**Q: Notification fan-out to a channel with 1M subscribers (e.g., global announcement). How long does it take?**

A: Back of envelope:
- 1M users × 2.5 devices = 2.5M push notifications
- FCM rate limit: 600K/second (per project)
- Time = 2.5M / 600K = ~4 seconds for push alone

But Dispatcher needs to fan-out before sending. At 43,500 push/sec normal load, we have headroom. Add workers temporarily: 200 workers × 300 FCM calls/sec each = 60,000 push/sec sustained.

For global announcements: pre-generate notification records, stagger delivery by timezone (users in US get it at 9am local time, users in India at 9am IST). No thundering herd.

**Q: How does digest mode reduce email volume? What's the technical implementation?**

A: Instead of sending 20 separate emails when user gets 20 notifications, batch into one digest email.

Implementation:
1. Dispatcher checks: notification type = social, user has digest_mode enabled
2. Instead of queuing email immediately: `INSERT INTO pending_digest (user_id, notification_id, queued_at)`
3. Hourly job: `SELECT user_id, array_agg(notification_id) FROM pending_digest WHERE user_id = $1 GROUP BY user_id`
4. For each user: render one "you have N new notifications" email, send, mark all as sent

Trade-off: user sees notifications up to 1 hour late. Reduces email volume by 70-80% for social notifications. Reduces unsubscribe rate significantly (LinkedIn saw 30% reduction).

**Q: SMS is expensive. How do you decide when to send SMS?**

A: Budget-based channel selection:
1. Define notification types that justify SMS: OTP, fraud alert, flight delay, package delivered
2. All other types: push first, email fallback, never SMS
3. Hard rate limit: max 2 SMS/user/day
4. At $0.0079/SMS with 500M users: if even 1% users get 1 SMS/day = 5M/day = $39,500/day = $1.2M/month
5. WhatsApp Business API at $0.005/msg saves 37%, better for markets where WhatsApp > SMS

---

## Round 5: Failure and Edge Case Questions

**Q: FCM is down for 30 minutes. What happens to all the push notifications?**

A:
1. Circuit breaker opens after 5 failures
2. All push notifications routed to retry queue with backoff: 1min → 5min → 15min → 60min
3. For notifications with TTL < 30min (e.g., "your Uber driver is 2 min away"): discard after TTL, no retry
4. For important notifications: after 30min, fall back to in-app (delivered on next open) + email if critical
5. When FCM recovers: circuit moves to HALF-OPEN → single probe → if success → CLOSED → retry queue drains with rate limiting (don't blast FCM)

**Q: What if a user uninstalls the app but your system keeps sending to their token?**

A:
- FCM returns `NotRegistered` error on first send attempt after uninstall
- Dispatcher catches this: `DELETE FROM devices WHERE token = $1`
- Also mark in Redis: `DEL devices:{user_id}:{device_id}`
- Never retry to that token again
- If user reinstalls: new token registered on first app open, overrides old record

**Q: Two instances of Dispatcher process the same SQS message simultaneously (SQS visibility timeout issue). How do you prevent double delivery?**

A: Three layers (defense in depth):
1. SQS visibility timeout: set longer than processing time (30s processing → 60s visibility timeout)
2. Idempotency key: `SET idem:{key} 1 NX EX 3600` — only one instance gets the lock
3. DB unique constraint: `UNIQUE(event_id, channel)` on notification_records — second insert fails
4. Result: user gets at most 1 notification even if two Dispatchers race

---

## Common Follow-up Questions

**Q: How would you add WhatsApp as a channel?**
Add a new worker type (WhatsApp worker), integrate WhatsApp Business API, store WhatsApp opt-in preference separately (user must explicitly opt in per GDPR), add to channel selection logic. Channel abstraction means core Dispatcher doesn't change.

**Q: How would you A/B test notification copy?**
Template has a `variant` field. On render, assign user to variant (hash(user_id) % 2). Deliver variant A or B. Track open rate per variant in analytics. Winner promotes to default after statistical significance.

**Q: How do you handle GDPR — user requests all their data or deletion?**
Data export: `SELECT * FROM notification_events WHERE user_id = $1` + `notification_records`. Deletion: delete event records + records. Device tokens: delete from Redis + Postgres. User preferences: delete. Idempotency keys: TTL-based, auto-expire in 24h. This is why storing raw payload in notification_events needs caution — may contain PII.

**Q: How is this different from designing a message queue?**
Notification system is a *consumer of* message queues, not the queue itself. The notification system adds: user preference management, channel routing, template rendering, rate limiting, delivery tracking, and provider abstraction. A message queue (Kafka/SQS) just stores and delivers bytes reliably.

---

## Concepts This System Tests

| Concept | Where It Appears |
|---------|-----------------|
| Idempotency (#4) | Deduplication of events and deliveries |
| Kafka + Outbox (#9) | Event bus + upstream durability |
| Caching Patterns (#8) | Device tokens, templates, preferences |
| Circuit Breaker (#32) | FCM/SES/Twilio failover |
| Bulkhead Pattern (#65) | Priority queue isolation |
| Fail-Open vs Fail-Closed (#59) | Rate limit during Redis failure |
| Hybrid Fan-out (#77) | Push to multiple devices per user |
| Sliding Window Rate Limit | Per-user channel rate limiting |
