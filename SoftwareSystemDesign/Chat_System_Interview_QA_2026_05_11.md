# Chat System — Interview Q&A Guide
**Date:** 2026-05-11  
**System:** Distributed Chat (WhatsApp/Slack style)  
**Tags:** [OPENER] [DEEP-DIVE] [TRAP] [TRADE-OFF] [REAL-WORLD] [FOLLOW-UP]

---

## How to Use This File
- Each question includes: **What the interviewer is testing**, a **strong answer**, and **follow-up chains**
- Practice answering out loud — chat system design is 45 minutes, so time yourself
- Interviewers at FAANG/Staff level expect you to drive trade-off discussion proactively

---

## Q1 [OPENER] — "Design a WhatsApp-like messaging system"

**What they're testing:** Can you scope, clarify requirements, and structure a 45-minute answer?

**Strong opening move:**
> "Before I draw anything, let me clarify scope. Are we designing 1:1 messaging only, or group messaging too? What scale — MAU, messages/day? Do we need message history persistence? Delivery receipts? I want to make sure we spend time on the right parts."

**Minimal viable scope for 45-min interview:**
- 1:1 messaging with delivery receipts ✓
- Group messaging (up to 1,000 members) ✓
- Online/offline presence ✓
- Message history ✓
- Leave out: E2E encryption internals, voice/video, bots

**Answers that impress:**
- Immediately note the core challenge: "The hard part is not storing messages — it's delivering them in real-time to 500M concurrent users with low latency."
- Drive to the WebSocket vs polling decision early: "Do we care about real-time delivery or is near-real-time (polling every 5s) OK?"

**Follow-up the interviewer will ask:**
> "Why WebSocket instead of HTTP long polling?"

*Answer:* WebSocket is bidirectional and persistent — the server can push messages without the client asking. Long polling works but requires a new HTTP connection every N seconds per user, wasting bandwidth and connections. At 150M concurrent users, 1 HTTP connection/5s = 30M new connections/sec — not feasible.

---

## Q2 [DEEP-DIVE] — "How do you guarantee message ordering?"

**What they're testing:** Understanding of distributed ordering, sequence numbers, and the trade-offs.

**Strong answer:**
> "We need ordering within a conversation, not globally across all conversations. We assign a monotonically increasing seq_num per conversation using a Redis INCR. This is atomic and fast. The trade-off is Redis becomes a single dependency for ordering — if Redis is unavailable, we can't assign seq_nums. We mitigate by sharding Redis (each conversation maps to a shard) and having a Snowflake-style timestamp fallback."

**Interviewer trap follow-up:**
> "What if two messages get the same seq_num?" (Redis INCR is atomic — impossible if used correctly, but test if you know why)

*Answer:* Redis INCR is atomic at the command level. Even with multiple chat servers, only one server increments the counter at a time — Redis serializes these operations. Two messages cannot get the same seq_num from INCR.

**Deeper follow-up:**
> "What about ordering across devices? User sends from phone, reads on laptop."

*Answer:* The server echoes sent messages back to all of the sender's devices. The laptop connects and sends its `last_seq_seen` cursor. Server fetches everything after that cursor from Cassandra. Ordering by seq_num is deterministic.

---

## Q3 [TRADE-OFF] — "Why Cassandra for message storage? Why not PostgreSQL?"

**What they're testing:** Ability to match storage technology to access patterns.

**Strong answer:**
> "Messages are almost always written once and read in time-ordered sequence per conversation. That's a perfect match for Cassandra's partition + clustering key model — I partition by conversation_id+bucket, cluster by seq_num. Reads are always sequential scans of a partition, which Cassandra is fast at. PostgreSQL would work for lower scale, but at 100B messages/day (25TB/day), PostgreSQL's MVCC and ACID overhead becomes costly and horizontal scaling requires Citus or manual sharding. Cassandra scales horizontally by default."

**What you give up with Cassandra:**
> "No joins, no multi-row transactions. If I need to atomically update two tables (e.g., insert message + update last_message_at on conversation), I can't. I handle this at the application layer — write message first, then update conversation — accepting brief inconsistency where inbox sort order might lag by a second."

**Interviewer follow-up:**
> "What about DynamoDB vs Cassandra?"

*Answer:* Both are good. DynamoDB is fully managed — no ops overhead. Cassandra gives more control over compaction, consistency levels, and can be cheaper at extreme scale. Pick DynamoDB if you value ops simplicity; pick Cassandra if you have a team comfortable managing it and want lower cost at petabyte scale.

---

## Q4 [TRAP] — "Can you just use a database to store messages and poll for new ones?"

**What they're testing:** Whether you understand why polling doesn't scale.

**The trap:** Many candidates say "yes, just poll every second." The interviewer wants to hear the cost analysis.

**Strong answer:**
> "Polling every 1 second for 500M users = 500M DB queries/second. Even with caching, that's unsustainable. More importantly, 1-second polling means messages appear with up to 1s delay — feels laggy for a chat app. Long polling (server holds request until a message arrives) is better but still requires a connection per user per poll cycle, and the server can't push events proactively. WebSocket is the right answer — 150M persistent connections push messages in < 50ms with no polling overhead."

**Nuance for senior candidates:**
> "In practice, you can do a hybrid: WebSocket for real-time delivery, HTTP polling as fallback for environments where WebSocket is blocked (corporate firewalls, some mobile networks). Slack does this."

---

## Q5 [DEEP-DIVE] — "Walk me through what happens when a message is sent and the recipient is offline."

**What they're testing:** End-to-end message delivery path, queuing, push notifications, catch-up on reconnect.

**Full flow answer (practice saying this in < 3 minutes):**
1. Sender sends message → Chat Server → Kafka
2. Delivery Service reads from Kafka, looks up recipient in Connection Registry (Redis)
3. Redis returns no active session → recipient is offline
4. Delivery Service writes to `undelivered:{user_id}` Redis list (or re-queues for push)
5. Notification Service sends push via APNs/FCM: "New message from Alice"
6. Recipient's device wakes up, opens app → WebSocket connects
7. Client sends `CONNECT { last_seq_seen: {conv_id: last_seq} }`
8. Chat Server queries Cassandra: `SELECT * FROM messages WHERE conv_id=? AND seq > last_seq`
9. Delivers as `MISSED` batch → client renders all missed messages in order
10. Client sends `ACK` → server updates delivery receipt → sender sees ✓✓

**Follow-up:**
> "What if the push notification fails?"

*Answer:* Retry queue with exponential backoff, up to 24 hours. If never delivered, user sees messages when they manually open the app (step 6–9 still works). Messages in Cassandra are the source of truth — push is just an alert.

---

## Q6 [DEEP-DIVE] — "How do you implement the 'delivered' and 'read' receipt system?"

**What they're testing:** Event-driven design, fan-out for receipts, privacy considerations.

**Strong answer:**
> "Delivery receipt (✓✓): When recipient's client receives `NEW_MESSAGE`, it automatically sends `ACK { message_id }` back to its Chat Server. Chat Server publishes a `receipt` event to Kafka (receipt topic). Delivery Service consumes it, routes to sender's Chat Server, which pushes `RECEIPT { message_id, type: delivered }` to sender. Sender's client updates the checkmark.

> Read receipt (✓✓ blue): When user opens a conversation, client sends `READ { conv_id, up_to_seq }`. Same flow — becomes a `receipt:read` event — sender sees blue checkmarks."

**Trade-off discussion (privacy):**
> "WhatsApp lets you disable read receipts. This is a product decision that affects the technical design: if a user has read receipts off, the server simply doesn't fan out read receipt events for their messages. The `last_read_seq` is still tracked server-side (for unread count), just not exposed to the sender."

**Group receipt complexity:**
> "In a group, 'delivered' means delivered to at least one recipient. 'Read' means read by all members. Storing per-user read receipts for a 1,000-member group: `1,000 receipts * 100B messages/day` = significant storage. WhatsApp limits group receipt visibility — you can see who read it but only for small groups."

---

## Q7 [TRADE-OFF] — "How do you handle message fan-out for large groups with 1 million members?"

**What they're testing:** Fan-out at scale, push vs pull model, back-pressure.

**Strong answer:**
> "For small groups (< 100 members), push fan-out is fine: Delivery Service iterates over members, finds online ones, sends gRPC to their Chat Server. For large groups, push fan-out is catastrophic — 1 message * 1M members = 1M gRPC calls. We switch to a pull model: large groups don't fan-out in real-time. Instead, we write the message once to Cassandra + publish to Kafka. Clients poll for new messages when they open the group. This shifts load from delivery to read path, which is more cacheable and bounded."

**Real-world:**
> "WhatsApp uses this exact model for broadcast lists. Discord calls these 'large guilds' and uses a hybrid: the message is published to a 'guild feed' topic, and clients subscribe to specific guild feed channels when they have the app open."

**Follow-up:**
> "Where's the cutoff — 100? 1,000? 10,000 members?"

*Answer:* It depends on your throughput. At 5M msg/sec with avg group size 50, most groups are fine with push. Set the threshold at where fan-out ops/sec exceeds your gRPC pool capacity — profile in load testing. A reasonable starting point is 1,000 members based on WhatsApp/Slack's observed patterns.

---

## Q8 [REAL-WORLD] — "Discord had a major outage in 2022 related to message delivery. What might have caused it and how would you prevent it?"

**What they're testing:** Real-world operational awareness, failure reasoning.

**Context (Discord 2022 Cassandra migration):**
> Discord publicly documented migrating from Cassandra to ScyllaDB. During the process, hot partition issues caused read latency to spike — some conversations had accumulated hundreds of millions of messages in a single Cassandra partition, causing scans to be extremely slow.

**Strong answer:**
> "The root cause was partition size — without bucketing by time, a 5-year-old high-volume conversation can have 100M rows in one Cassandra partition. A single range scan for 'last 50 messages' has to skip through millions of rows. Prevention: always bucket by time (month/year), set a max bucket size alert (> 10M rows = reshard that bucket), and use `TimeWindowCompactionStrategy` to compact only within time windows, not across the entire partition."

**Discord's actual solution:**
> They migrated to ScyllaDB (Cassandra-compatible, written in C++) which handles hot partitions better due to its shard-per-core architecture and more efficient compaction.

---

## Q9 [DEEP-DIVE] — "How does your system handle message idempotency? What prevents duplicate messages?"

**What they're testing:** Understanding of at-least-once delivery and client/server-side dedup.

**Layers of deduplication:**

1. **Client generates UUID before sending:** Same message retried 10 times = same UUID. Server must upsert, not insert.
2. **Kafka idempotent producer:** `enable.idempotence=true` deduplicates at the producer level within a session.
3. **Kafka consumer Bloom filter:** Delivery Service tracks processed `message_id`s for 5 minutes. Duplicate (from consumer retry) suppressed.
4. **Cassandra PK constraint:** `message_id` is part of the PK. Cassandra's "last write wins" at same timestamp = effectively no-op for same message.
5. **Client-side dedup:** Client maintains in-memory set of last 1,000 `message_id`s. Duplicate pushed from server → silently discarded.

**Interviewer trap:**
> "What if two users send identical message text at the same time?"

*Answer:* Those are two different messages with two different client-generated UUIDs. They should both appear. Deduplication is on `message_id`, not content. Content hash dedup would incorrectly suppress legitimate duplicate messages ("yes", "ok", "lol").

---

## Q10 [TRADE-OFF] — "How would you shard the user database? What are the hot spots?"

**What they're testing:** Sharding strategy, hotspot awareness, cross-shard queries.

**Strong answer:**
> "Shard by `user_id` using consistent hashing. This distributes users evenly and keeps user profile lookups on a single shard. The trade-off: queries like 'find all members of group X' span shards if member user_ids are on different shards — solved by maintaining a separate `conversation_members` table that can be sharded by `conversation_id` instead of user_id."

**Hotspot example:**
> "A celebrity user with 100M followers — their UserProfile row gets read 100M times/day. That shard gets hammered. Mitigation: cache celebrity profiles aggressively in Redis (TTL 1 hour) and allow the read replica serving that shard to have more replicas (3x) than average."

**Cross-shard problem:**
> "If you shard messages by user_id, a conversation between two users on different shards requires a scatter-gather query. Better to shard by conversation_id — all messages in a conversation are on one shard, and the conversation is shared by both users anyway."

---

## Q11 [FOLLOW-UP] — "Your system is at 100M users. How would you scale to 1 billion?"

**What they're testing:** Horizontal scaling, bottleneck identification.

**Bottleneck analysis at 1B users:**
| Component | 100M scale | 1B scale change |
|---|---|---|
| Chat Servers | 300 servers (50K connections each) | 3,000 servers → same architecture, just more |
| Kafka | 32 brokers, 256 partitions | 320 brokers, 2,560 partitions; increase replication |
| Cassandra | 30 nodes, 30 PB/year | 300 nodes; consider tiered storage (Cassandra + S3 for old data) |
| Redis | 10-node cluster | 100-node cluster; separate clusters for session, cache, presence |
| PostgreSQL (metadata) | Single primary + 5 replicas | Shard users by ID range across 10 Postgres clusters; or move to CockroachDB |
| Delivery Service | 100 pods | 1,000 pods; partition by conversation_id range for predictable scaling |

**New challenge at 1B:** Global deployment.
> "At 1B users across continents, you need regional deployments. Users in Asia connect to Asia region chat servers. Cross-region messages route through a global message bus. Presence and group membership replicated across regions with eventual consistency. Challenge: ordering across regions — use Hybrid Logical Clocks (HLC) which combine physical and logical timestamps."

---

## Q12 [OPENER VARIANT] — "Design Slack (team-based channels vs person-to-person WhatsApp)"

**What's different from WhatsApp:**
1. **Workspace model:** All users in a workspace share the same conversation namespace. No phone-number-based identity.
2. **Channels (public/private):** Many users can join; history visible to newcomers (unlike WhatsApp groups where you don't see pre-join messages)
3. **Threading:** Messages can have reply threads (sub-conversations). Each thread is a separate seq_num namespace.
4. **Search as a primary feature:** Slack's value prop is searchable history. Elasticsearch becomes critical (not optional).
5. **Integrations/bots:** Many bots send messages programmatically. Rate limiting per bot/workspace becomes important.
6. **Rich messages:** Block Kit JSON for interactive components — `body` field is JSON, not plain text. Schema evolution matters.

**Slack-specific trade-offs:**
- New member joins channel → can read full history → data access control (don't cache by channel, cache by user-channel pair)
- Thread replies vs channel messages: do threads have their own seq_num space? (Yes, simpler isolation)
- Workspace isolation: shard Kafka topics by workspace_id (not just conversation_id) for multi-tenant isolation

---

## Quick-Fire Round (Common Follow-Ups)

**Q: How do you handle timezone differences in message timestamps?**  
A: Store all timestamps in UTC in the database. Client converts to local timezone for display. Never store local time.

**Q: What's the maximum message size?**  
A: 64KB for text (hard limit enforced at API gateway). Larger content → must be media (image/file) uploaded to S3 first, then share URL.

**Q: How do you implement "message reactions"?**  
A: Separate `reactions` table: `(message_id, user_id, emoji)`. Denormalize reaction counts into message row or Redis hash for display. Fan-out reaction updates same as receipt events.

**Q: How do you delete a message?**  
A: Soft delete: set `deleted_at = now()`. Propagate deletion event via Kafka to all connected clients (same fan-out as send). Client replaces message with "This message was deleted." Hard delete from Cassandra after 30 days via background job.

**Q: How do you search message history?**  
A: Index messages in Elasticsearch (async consumer from Kafka). Fields: `sender_id`, `conversation_id`, `body` (full-text), `created_at`. Search API queries ES, returns seq_nums, client fetches full messages from Cassandra by seq_num. Don't store full message body in ES if E2E encrypted — only index metadata.

**Q: What database would you use for the user authentication table?**  
A: PostgreSQL with strong consistency (ACID). Phone/email must be unique — enforced with DB constraint, not just application logic. Cache JWT tokens in Redis to avoid DB hit on every message auth check.

---

## Interview Scoring Rubric

| Level | Expected Depth |
|---|---|
| **L4 / Mid** | Cover core flow (WebSocket, Kafka, Cassandra), mention caching, identify 2–3 failure scenarios |
| **L5 / Senior** | Explicit trade-off for every component, message ordering deep-dive, group fan-out problem, at-least-once + idempotency |
| **L6 / Staff** | Drive the conversation, challenge interviewer's assumptions, introduce pull model for large groups unprompted, discuss cross-region consistency, reference real-world incidents (Discord, WhatsApp) |
| **L7 / Principal** | System evolution over time (100M → 1B), org/team topology implications, build vs buy (Cassandra vs DynamoDB), multi-tenant isolation for Slack-like systems |

