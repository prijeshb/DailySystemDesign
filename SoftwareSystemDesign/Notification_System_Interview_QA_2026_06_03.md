# Notification System — Interview Q&A
*Date: 2026-06-03*

---

## Opening / Scoping Questions (Interviewer sets the stage)

**Q: Design a notification system for an app like Facebook.**

*Good candidate response:*
> "Before diving in — a few clarifications. What channels do we need? Push, email, SMS, in-app? What's the scale — how many users and notifications per day? Do we need guaranteed delivery or best-effort? Any latency SLA for critical notifications?"

*Why this matters:* Scoping prevents you from over-designing. Interviewer may say "focus on push and email, 500M users."

---

## Section 1: Core Design Questions

**Q: Why not just call FCM directly from the app server?**

A: Works at small scale. Fails at large scale because:
- Blocking HTTP call slows down business logic
- No retry logic for transient failures
- Can't manage rate limits per vendor centrally
- No visibility into delivery status
- Multi-channel logic scattered across codebase

**Q: How do you ensure a notification is sent exactly once?**

A: Idempotency at two levels:
1. Producer passes an idempotency key (e.g., `sha256(event_id + user_id)`)
2. Notification Service checks Redis before processing; if key exists, skip
3. Kafka producer idempotency (`enable.idempotence=true`) prevents duplicate Kafka messages on producer retry
4. TTL on idempotency key = 24h (covers retry window)

Note: We aim for at-least-once delivery; idempotency converts it to effectively-once.

**Q: How do you handle user preferences and quiet hours?**

A: 
- Store preferences in MySQL (source of truth), cache in Redis (hot path)
- On notification enqueue: check `quiet_hours` in user's timezone
- If in quiet hours: schedule delayed job (enqueue with future timestamp, or use a sorted set in Redis as a delay queue)
- User can set per-channel preferences (push only, no SMS, etc.) and per-topic opt-outs

**Q: How would you handle a broadcast to 100M users?**

A: Fan-out strategy:
- Don't generate 100M messages at request time (latency + memory)
- Fan-out Service reads user IDs in batches of 1000 from segment store
- Writes batched jobs to Kafka; workers expand each batch → individual sends
- Rate-limit fan-out to protect downstream (e.g., 1M users/min)
- Separate "bulk" Kafka topic with lower-priority workers so transactional notifications aren't delayed

---

## Section 2: Follow-up / Deep Dive Questions

**Q: What happens if FCM is down?**

A:
1. Retry with exponential backoff (1s, 2s, 4s... up to 5 min)
2. Circuit breaker: if FCM error rate > 50% in 30s window → open circuit, stop new attempts
3. Fallback: route to next preferred channel (email or SMS)
4. Circuit breaker half-opens every 60s to probe recovery
5. Alert on-call engineer

**Q: How do you handle stale/invalid device tokens?**

A:
- FCM returns `UNREGISTERED` error → delete token from DB
- APNs returns `410 Gone` → deactivate token
- On deactivation: remove from future fan-out
- Periodic cleanup: weekly job pings all tokens, removes ones silent > 6 months

**Q: How do you prevent notification spam to a user?**

A: Rate limiting at user level:
- Redis counter: `rate:push:{user_id}:{hour_bucket}` → INCR with TTL 1h
- If count > threshold (e.g., 20/hour for marketing), drop or delay notification
- Different limits per notification type: transactional (unlimited) vs. marketing (rate-limited)
- User-level opt-outs respected before any rate check

**Q: How does your system handle retries without causing a thundering herd?**

A:
- Exponential backoff with jitter: `delay = min(cap, base * 2^attempt) + random(0, 1s)`
- Jitter spreads retries in time so all failed jobs don't retry simultaneously
- Separate retry queue with dedicated workers (don't mix retries with fresh messages)
- Max retry attempts = 5; after that → DLQ + alert

**Q: How do you design the data model for notification logs?**

A: Cassandra with partition key = `user_id` for user-facing queries:
```
PRIMARY KEY ((user_id), created_at DESC)
```
- Supports: "get all notifications for user X in last 7 days"
- Write-heavy, time-series → perfect for Cassandra
- TTL = 30 days (auto-expire old logs)
- For analytics (delivery rates, channel performance) → async export to ClickHouse/BigQuery

---

## Section 3: Trade-off Questions

**Q: Kafka vs SQS/RabbitMQ — why Kafka?**

| | Kafka | SQS |
|--|-------|-----|
| Replay | Yes (offset-based) | No (once consumed, gone) |
| Throughput | Very high (millions/sec) | High |
| Ordering | Per-partition | Best-effort (FIFO queue option) |
| Ops complexity | High | Low (managed) |
| Use case | Event streaming, replay needed | Simple queue |

At scale, Kafka's replay is valuable: if a worker bug causes bad sends, rewind and replay from last good offset.

**Q: Push vs pull for in-app notifications?**

- **Push (WebSocket):** Real-time, complex to maintain at scale; need sticky sessions or pub-sub
- **Pull (polling):** Simple, works at scale, slight delay
- **Hybrid:** Long-polling or Server-Sent Events (SSE); good balance

Decision: SSE for in-app (simpler than WebSocket for one-way), WebSocket only if two-way interaction needed.

**Q: How do you add a new channel (e.g., Slack DM) without touching existing code?**

A: Channel abstraction:
```python
class NotificationChannel(ABC):
    def send(self, user_id, message) -> DeliveryResult: ...

class SlackChannel(NotificationChannel): ...
class FCMChannel(NotificationChannel): ...
```
Register new channel → routing config updated → no change to Notification Service core.

---

## Section 4: Operational / Scaling Questions

**Q: How do you monitor the health of the notification system?**

Key metrics:
- Kafka consumer lag per topic (worker health)
- Notification delivery rate per channel (% success)
- P99 end-to-end latency (enqueue → delivered)
- 3rd-party error rates (FCM, Twilio, SendGrid)
- DLQ message count (poison pill indicator)
- Device token invalidity rate (data hygiene)

Alerts: Consumer lag > 100K messages → page on-call; FCM error rate > 5% → warning.

**Q: How would you scale to 10x current load?**

- Increase Kafka partitions (horizontal scale for consumers)
- Add more worker instances (stateless, easy to scale)
- Shard user preference DB by user_id range
- Increase Redis cluster size for rate-limiting
- Pre-scale for known events (marketing campaigns)

**Q: How do you debug why a specific user didn't receive a notification?**

1. Check `notification_log` by user_id — was it enqueued?
2. If enqueued: which channel was selected? Was token active?
3. Check FCM/APNs response in log — was it success, invalid token, rate-limited?
4. Check quiet hours — was notification suppressed?
5. Check opt-out preferences — was user opted out of that type?
6. Check idempotency key — was it already sent (duplicate suppression)?

→ This requires full observability in the log DB.

---

## Common Mistakes Candidates Make

1. **Not handling 3rd-party failures** — Assumes FCM always works
2. **Skipping idempotency** — Results in duplicate sends on retry
3. **Single Kafka queue** — All notifications compete; urgent OTP blocked by marketing blast
4. **Synchronous preference lookup** — DB call in hot path; should use cache
5. **No quiet hours logic** — Sending at 3am is a product failure
6. **Forgetting opt-outs / compliance** — TCPA/GDPR violations
7. **Over-designing fan-out** — Starting with push fan-out for 1M users is fine; optimize later

---

## Quick-Fire Answers

| Question | Short Answer |
|----------|-------------|
| How to handle APNs 4KB limit? | Truncate body, send data payload separately if needed |
| What's a collapse key? | FCM feature: newer notification replaces undelivered older one (e.g., score updates) |
| How to send multi-language notifications? | Template DB with locale key; render at send time using user's locale |
| How to A/B test notification copy? | Route % of users to different templates; log template_id in delivery log |
| How to handle unsubscribes? | Webhook from carrier/email provider → update preferences DB → propagate to cache |
