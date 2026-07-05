# Notification System — Interview Q&A (2026-05-18)

## How Interviewers Open This Problem

> "Design a notification system that sends push, email, and SMS notifications."

Common follow-ups that reveal depth:
- "How do you handle 1 billion notifications per day?"
- "What if a user unsubscribes — how quickly does it take effect?"
- "How do you avoid sending the same notification twice?"
- "What happens if FCM is down for an hour?"
- "How do you handle a marketing blast to 50 million users?"

---

## Round 1: Fundamentals

**Q: Walk me through your high-level design.**

A: Three stages — (1) **Trigger**: producer calls Notification API with event + user list. (2) **Fan-out**: async workers expand user list into per-channel delivery tasks via Kafka. (3) **Dispatch**: channel-specific workers call 3rd-party gateways (FCM, SendGrid, Twilio). Delivery status logged to Cassandra. In-app feed is a separate path writing to a dedicated DB.

---

**Q: Why use Kafka instead of a direct DB queue?**

A: Kafka gives us: (a) replay semantics — if a channel worker fails, we can re-consume from last offset; (b) horizontal scale — partition count directly maps to consumer parallelism; (c) durable pub/sub — multiple consumer groups (push, email, SMS) consume independently from the same topic. A DB queue (polling) adds load on DB and lacks replay. Trade-off: Kafka is operationally heavier.

---

**Q: Why not just call the gateway directly from the API?**

A: Coupling and reliability. If FCM is slow, the API call blocks. If we're sending to 1M users, we'd need 1M concurrent HTTP calls. Async queue decouples trigger from delivery, handles back-pressure, and lets us retry independently.

---

**Q: How do you handle user notification preferences?**

A: Preference Service backed by PostgreSQL, cached in Redis (5-min TTL). Fan-out workers check prefs before writing to channel queues. Trade-off: up to 5-min staleness — user disables push, may get 1-2 more before cache expires. For CRITICAL notifications, bypass cache and read from DB directly.

---

## Round 2: Scale & Performance

**Q: 1 billion notifications/day — how does your system handle this?**

A: ~11.6K/sec average, ~500K/sec peak. Key scaling choices:
- Kafka with 200 partitions for fan-out topic → 2500 messages/partition/sec
- Fan-out workers: stateless, auto-scale based on Kafka consumer lag
- Channel workers: separate pools, scale independently per channel demand
- Redis for preference lookups: O(1) at 100K reads/sec
- DeliveryLog in Cassandra: optimised for high-write, time-series access

---

**Q: How do you handle a marketing blast to 50 million users?**

A: Separate `notif.fanout.batch` Kafka topic consumed by dedicated batch fan-out workers. Rate-limited to 50K dispatches/sec per worker to avoid overwhelming channel queues. Marketing jobs scheduled off-peak. The batch path is completely isolated from real-time notifications — a slow marketing blast never delays an OTP.

---

**Q: How do you enforce "max 3 push notifications per day" per user?**

A: Redis counter: `INCR notif:push:{user_id}:{date}` checked in fan-out worker. TTL set to expire at midnight. Atomic INCR prevents race conditions. If count > 3, skip writing to push queue. Email/SMS caps use the same pattern with different keys.

---

**Q: How do you handle device token invalidation?**

A: When channel worker receives `invalid_token` error from FCM/APNs, it marks the subscription as inactive in the Subscription DB and evicts the token from Redis cache. On next app open, the client re-registers. This prevents wasting gateway quota on dead tokens and skewing delivery metrics.

---

## Round 3: Reliability & Idempotency

**Q: How do you prevent duplicate notifications?**

A: Two layers:
1. **API layer**: producer sends `idempotency_key`; Redis SET NX with 24h TTL rejects duplicate triggers.
2. **Worker layer**: before calling gateway, check DeliveryLog for `(notif_id, user_id, channel, status=SUCCESS)`. If found, skip. This handles Kafka at-least-once redelivery after worker crash.

Why two layers? API layer prevents duplicate publishes; worker layer handles the Kafka replay scenario.

---

**Q: Exactly-once vs at-least-once — which did you choose and why?**

A: At-least-once. Exactly-once delivery requires distributed transactions across Kafka + gateway + DeliveryLog, which is expensive and complex. At-least-once with idempotency on the consumer side achieves the same effective result with far simpler infrastructure. A user getting the same notification twice is a minor UX issue; missing an OTP is a critical failure.

---

**Q: What happens if the fan-out worker crashes mid-processing?**

A: Kafka offset is committed **after** writing to channel queues, not before. If worker crashes, Kafka redelivers from last committed offset. Fan-out worker must be idempotent — re-writing the same message to the channel queue is safe because the channel worker has its own DeliveryLog check. This gives at-least-once fan-out.

---

**Q: FCM is down for 1 hour. What happens?**

A: Circuit breaker opens after 5 consecutive failures. Channel workers stop calling FCM, messages accumulate in `notif.push` Kafka topic (durable). FCM recovers → circuit breaker half-opens → probe succeeds → channel workers drain the queue. LOW priority messages with TTL < 1h are dropped (stale push is worse than none). CRITICAL messages have no TTL and are delivered when FCM recovers. SMS fallback available for CRITICAL if defined in routing rules.

---

## Round 4: Failure & Edge Cases

**Q: What if the user preference cache is stale when a user opts out?**

A: Up to 5-min delay. For most channels, acceptable. For CRITICAL notifications, we bypass cache. To reduce risk: when user updates preferences, immediately invalidate the Redis key (write-through invalidation) — brings staleness window to near-zero for explicit changes.

---

**Q: How do you handle quiet hours?**

A: Fan-out worker checks `user.quiet_hours` from preferences. If current time in user's timezone falls in quiet window, the notification is held — written to a `notif.deferred` topic with a `deliver_after` timestamp. A scheduler worker reads deferred messages and republishes them to the appropriate channel topic at the correct time.

---

**Q: What if a notification is time-sensitive but the user is in quiet hours?**

A: CRITICAL priority overrides quiet hours — OTP, security alerts are delivered immediately regardless. The worker checks `priority` before applying quiet hour logic.

---

**Q: Your DeliveryLog DB (Cassandra) goes down. What breaks?**

A: Idempotency check at worker layer fails → risk of duplicate delivery. Mitigation: short-term fallback to Redis (5-min TTL) for idempotency key tracking. Delivery continues with at-least-once guarantee; duplicates possible during outage window. Once Cassandra recovers, workers drain their local WAL (buffered writes). Alert on-call immediately — extended outage means no delivery tracking.

---

**Q: How does the in-app notification feed scale?**

A: Cursor-based pagination (not offset — avoids drift). Redis sorted set per user (top 100 recent, score = timestamp) for fast read. Fan-out worker writes to both channel queues and in-app DB simultaneously. WebSocket/SSE for real-time badge updates — each API server maintains connections; when fan-out worker writes to in-app DB, it publishes to a `notif.inapp.realtime` topic; WebSocket servers consume and push to connected clients.

---

## Round 5: Deep Dives (Common Follow-ups)

**Q: How would you add a "digest" mode (send one email per day with all notifications)?**

A: Fan-out worker, for users with `email_preference=digest`, writes to a `notif.digest` topic instead of `notif.email`. A digest aggregation worker groups by `(user_id, date)`, collects throughout the day, and at the scheduled time (e.g., 8am user's timezone) renders a combined email template and dispatches via email worker. Uses same retry/DLQ infrastructure.

---

**Q: How would you add analytics — what % of push notifications were opened?**

A: Each push notification includes a unique `notif_id` in the payload. When user taps notification, app sends a tracking event to Analytics Service with `notif_id`. DeliveryLog updated: status → OPENED. Dashboard queries Cassandra or aggregated OLAP store (e.g., ClickHouse) for open rates by type, channel, time. Privacy: anonymise after 90 days.

---

**Q: How would you implement "send to segment" (all users in country X)?**

A: Segment is a query against User DB (`country=X`). Instead of Notification API accepting user_ids[], it accepts a `segment_id`. A segment resolver worker (separate from fan-out) queries User DB in batches of 10K, writes batches to `notif.fanout.batch` topic. Fan-out then proceeds as normal. Segment resolution can take minutes for 50M users — this is expected and acceptable for marketing.

---

## Checklist: What Interviewers Look For

- [ ] Async fan-out (not synchronous)
- [ ] Kafka partition strategy explained
- [ ] Idempotency — two layers (API + worker)
- [ ] At-least-once with rational justification
- [ ] User preferences with cache + invalidation strategy
- [ ] Priority separation (CRITICAL vs LOW)
- [ ] TTL for stale notifications
- [ ] Circuit breaker on 3rd-party gateways
- [ ] DLQ + retry strategy with concrete backoff numbers
- [ ] In-app feed as separate path
- [ ] Capacity estimates (not just hand-wavy)
- [ ] Multi-provider fallback for SMS/email
- [ ] Failure of each component addressed
