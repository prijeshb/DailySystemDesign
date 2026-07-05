# Slack: Interview Q&A
*Date: 2026-06-25*

---

## How Interviewers Open This Problem

> "Design a team messaging application like Slack."
> "Design a group chat system with channels and workspaces."
> "How would you design the messaging backend for a tool like Slack or Teams?"

**Expected opening from you:**
1. Clarify scale: DAU, messages/day, message size
2. Clarify scope: real-time delivery? search? file uploads? notifications? integrations?
3. Clarify consistency: "Can a user briefly see a message out of order? Can search be slightly stale?" → mostly yes, except message ordering within a channel
4. State the non-obvious: "This is meaningfully different from a simple chat system — it has multi-tenant workspace isolation, channel fan-out at scale, threading, and search. I'll design for these specifically."

---

## Core Q&A

**Q: How do you store messages? Why not PostgreSQL?**

A: Cassandra. Messages are time-series: always appended, never updated (edits create a new record), queried by channel + time window. Cassandra's partition model is perfect: `(channel_id, bucket)` as partition key, `message_id` (TIMEUUID) as clustering key gives time-ordered reads. Write throughput at 70K msg/sec peak is beyond Postgres's comfortable zone (~10-20K writes/sec before contention). PostgreSQL is reserved for relational data: users, workspaces, channel memberships — things that need FK constraints and transactional integrity.

**Q: How does real-time message delivery work?**

A: WebSocket connections. Each user maintains a persistent WebSocket to one of our gateway servers, assigned by consistent hash on user_id (sticky sessions). When a message is sent: it's written to Cassandra, published to Kafka, Fan-out Service subscribes → looks up which server each channel member is connected to (stored in Redis) → delivers via that server's channel. The key implementation detail: connection registry in Redis maps user_id → server_id, so Fan-out knows exactly where to route.

**Q: How do you handle a channel with 50,000 members? That's a lot of fan-out.**

A: Hybrid fan-out. Below ~1,000 members: push-based — fan-out iterates members and publishes to each user's personal delivery queue. Above 1,000 members: pull-based — publish a single notification to the channel topic, clients receive "new message in #announcements" and pull the latest messages via REST. This avoids the O(50K) Redis ops per message that would make large channels prohibitively expensive. The threshold is configurable; Slack has written about using a similar approach.

**Q: How does message threading work?**

A: Each message has an optional `thread_ts` field pointing to the parent message's TIMEUUID. Thread replies are stored in a separate Cassandra table keyed by `parent_message_id` to avoid mixing them into the main channel timeline. This separation keeps the main channel query clean (no thread replies in the feed) while allowing efficient "get all replies for message X" queries.

**Q: How do you implement emoji reactions?**

A: Reactions are stored as a `MAP<TEXT, SET<UUID>>` on the message record in Cassandra — `{"👍": {user1, user2}, "❤️": {user3}}`. This is convenient but has a trade-off: concurrent reactions (two users add 👍 simultaneously) can result in a last-write-wins merge at the Cassandra level. For reactions, this is acceptable — if user A and user B both add 👍 and one is dropped due to a CAS conflict, they retry and it resolves. The alternative (a separate `reactions` table in PostgreSQL) is more consistent but adds a round-trip join to every message fetch.

**Q: How does search work?**

A: Elasticsearch with per-workspace indices (`slack_messages_{workspace_id}`). Messages are indexed asynchronously via a Kafka consumer (Search Indexer). Documents include: content (full-text analyzed), channel_id, user_id, created_at. Queries are filtered to the user's accessible channels (privacy enforcement: private channel messages must not appear in search for non-members). Index lifecycle management (ILM) enforces retention policies: free plan gets 90 days, paid plans get full history via hot/warm tiering.

**Q: How do you enforce workspace isolation? Can a user from workspace A see data from workspace B?**

A: Three layers of isolation:
1. **JWT scoped to workspace_id:** every API call's token contains workspace_id; middleware validates the requested resource's workspace matches the token's workspace.
2. **Database-level scoping:** every PostgreSQL query includes `WHERE workspace_id = $1` as a mandatory filter.  
3. **Elasticsearch index-per-workspace:** no shared index, so a search bug can't leak cross-workspace data.
Never join across workspace_id. Application-layer rule enforced in a shared middleware function that all route handlers call before any DB access.

**Q: What happens when a user is offline? Do they lose messages?**

A: No. Messages are durably written to Cassandra before fan-out. For offline users: the Fan-out Service checks Redis presence → user offline → pushes a notification via APNs/FCM (mobile push). When user comes back online, the client sends its `last_seen_message_id` → server returns all messages with `id > last_seen` from Cassandra. This is the "message replay on reconnect" pattern.

**Q: How do you handle unread counts efficiently?**

A: Redis HASH per user: `HASH unread:{user_id}` → `{channel_id: count}`. On new message: `HINCRBY unread:{user_id} {channel_id} 1` for every channel member. On user reads channel: `HSET unread:{user_id} {channel_id} 0`. Total badge count: sum of all values (can cache this too: `unread_total:{user_id}`). The concern: for a user in 200 channels with 10K active users per channel, `HINCRBY` happens 200 times per message. Batching these into a Lua script or pipeline reduces round-trips.

**Q: How do you prevent message duplication on retry?**

A: Message Service generates a UUID for each message on the client side. The UUID is the Cassandra TIMEUUID used as the primary key. `INSERT INTO messages ... IF NOT EXISTS` — Cassandra's lightweight transaction. If the insert is retried (network failure after write, before ACK), the second insert sees the same primary key and no-ops. Client deduplicates by message UUID when rendering.

**Q: What's your approach to rate limiting?**

A: Rate limiting at the API Gateway layer using Redis sliding window (token bucket). Per workspace: 100 messages/second. Per user: 5 messages/second. Large workspaces on Business plan get higher limits. If Redis is unavailable, circuit breaker opens: fall back to a stateless rate limiter in the API Gateway process (in-memory, per-instance, less precise but better than nothing).

---

## Follow-up / Deep-dive Questions

**Q: How do webhooks work? I send a message to a channel from an external service.**

A: Incoming webhooks: external service POSTs JSON payload to a unique webhook URL we generate. API layer validates the URL token (stored in PostgreSQL `webhooks` table), extracts channel_id, injects a bot user message into the normal message pipeline (same Cassandra write → Kafka → fan-out path). The webhook URL is scoped to a specific channel; rotation supported for security.

**Q: How would you implement message editing and deletion?**

A: **Edit:** Cassandra doesn't support in-place updates well. Options: (a) write a new record with `edited_at` timestamp and `original_message_id` reference, client shows latest version; (b) use Cassandra's upsert (same partition key, same clustering key) — updates the `content` and `edited_at` columns in place. Option (b) is simpler. Deletes: mark `is_deleted=true` (soft delete) — Cassandra tombstones, cleared by compaction. Fan-out publishes an `edit` or `delete` event type so connected clients update their UI.

**Q: How do you handle the case where Kafka is partitioned (network split)?**

A: Kafka uses ZooKeeper/KRaft for leader election. During partition, partition leader is unreachable → producer retries stall. Mitigations:
1. `acks=all` (wait for all ISR) + `retries=MAX_INT` on producer — messages accumulate in producer buffer during partition
2. Kafka producers have `buffer.memory=64MB` — can buffer ~2-3 minutes of traffic at peak
3. If partition exceeds buffer: backpressure to Message Service → 503 to clients with `Retry-After`
4. Kafka is deployed across 3 AZs; a true full-cluster partition is extremely rare

**Q: How does the multi-device experience work? I'm logged in on mobile and desktop simultaneously.**

A: Both devices maintain WebSocket connections (different entries in Redis connection registry: `ws:user:{id}:devices` = SET of {device_id: server_id}). Fan-out delivers to ALL active devices for a user. Unread count syncs: when user reads on mobile, `HSET unread:{user_id} {channel_id} 0` — desktop sees count drop on next presence heartbeat or push from Redis pub/sub. Message ordering is by TIMEUUID (which encodes timestamp) so both devices render identical timelines.

**Q: What database would you use for analytics? How does Slack report "messages sent this week"?**

A: Separate OLAP path. Kafka consumer publishes to a data warehouse (Snowflake or BigQuery). Cassandra is write-optimized but terrible at aggregations across workspaces/users. BigQuery allows: `SELECT COUNT(*) FROM messages WHERE created_at >= NOW()-7d` in seconds. This is the OLTP/OLAP split (Concept #12). The Cassandra operational DB never gets analytics queries.

**Q: How do you handle very long channels (e.g., #general in a 100K-person company)?**

A: This is the "super group" problem. Beyond 50K members, even notification delivery becomes expensive. Slack caps channel membership at 1M but applies aggressive fan-out optimization: large channels use **lazy delivery** — no push to all members, only a lightweight pub/sub notification ("new message in #general"). Clients request the message on notification receipt. Additionally: rate limit messages in large channels (e.g., max 1 message/minute), suggest using announcement channels (broadcast-only, no replies). In the data model, very large channels have a separate partition strategy — bucket by (channel_id, week) not month to keep partition sizes manageable.

---

## Common Mistakes to Avoid

1. **Using PostgreSQL for messages** — doesn't scale to 23K writes/sec. Say "Cassandra because append-only, time-series, known query pattern."

2. **Polling instead of WebSockets** — "5M users × poll every 2s = 2.5M RPS just for polling." WebSockets push on change, dramatically less load.

3. **Naive fan-out for large channels** — forgetting the O(N members) cost. Must mention hybrid fan-out with push/pull threshold.

4. **Forgetting workspace isolation** — not mentioning how you prevent cross-workspace data leakage. Interviewers probe this for multi-tenant systems.

5. **Putting search on the hot path** — Elasticsearch is async/eventual. Don't say "message goes to Elasticsearch before fan-out." Write to Cassandra → Kafka → async indexer.

6. **Not handling reconnect** — what happens when WebSocket drops? Must mention client-side replay using last_seen_message_id.
