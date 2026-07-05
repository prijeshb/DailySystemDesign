# System Design: Distributed Chat System (WhatsApp / Slack)
**Date:** 2026-05-11  
**Difficulty:** Senior / Staff  
**Real-world refs:** WhatsApp Engineering Blog, Slack Engineering Blog, Discord Engineering Blog, Facebook Messenger (2013 OSDI paper)

---

## 1. Requirements Gathering

### Functional Requirements
1. One-to-one (1:1) messaging between users
2. Group messaging (up to 1,000 members per group)
3. Message delivery receipts: **sent → delivered → read**
4. Online/offline presence (last seen)
5. Message history (persistent, searchable)
6. Media sharing: images, videos, files (up to 100 MB)
7. Push notifications for offline users

### Non-Functional Requirements
| Property | Target |
|---|---|
| Daily Active Users | 500 million |
| Messages per day | 100 billion (avg 200/DAU) |
| Peak messages/sec | ~5 million/sec |
| Message delivery latency (online) | < 100 ms P99 |
| Availability | 99.99% (< 53 min/year downtime) |
| Durability | Zero message loss (at-least-once delivery) |
| Message ordering | Causal ordering within a conversation |
| Storage retention | 7 years |

### Out of Scope (for this design)
- Voice/video calls
- E2E encryption internals (acknowledge as a layer)
- Bot platform

---

## 2. Capacity Estimation

```
Messages/day:       100 billion
Avg message size:   200 bytes (text) + 50 bytes metadata = 250 bytes
Daily storage:      100B * 250 B = 25 TB/day
Yearly storage:     ~9 PB/year
7-year total:       ~63 PB (with replication x3 = ~190 PB raw)

Peak write QPS:     100B / 86400s * 3 (peak factor) = ~3.5M msg/sec
Peak read QPS:      ~10M/sec (reads >> writes; messages fetched on open)

Connections (WebSocket):
  500M DAU, 30% online at peak = 150M concurrent connections
  Each chat server holds ~50K connections → 3,000 chat servers needed

Media:
  10% messages have media (10B/day), avg size 500 KB
  Media storage: 10B * 500KB = 5 PB/day → tiered to cold storage after 30 days
```

---

## 3. Entities & Data Model

### Core Entities

```
User
├── user_id         UUID (PK)
├── phone / email   unique
├── display_name    string
├── avatar_url      string
├── status          ENUM(online, offline, away)
├── last_seen_at    timestamp
└── created_at      timestamp

Conversation
├── conversation_id UUID (PK)
├── type            ENUM(direct, group)
├── name            string (group only)
├── avatar_url      string (group only)
├── created_by      UUID → User
├── created_at      timestamp
└── last_message_at timestamp   ← for inbox sort

ConversationMember
├── conversation_id UUID (FK)
├── user_id         UUID (FK)
├── role            ENUM(admin, member)
├── joined_at       timestamp
└── last_read_seq   bigint      ← cursor for unread count

Message
├── message_id      UUID (PK, client-generated)   ← idempotency key
├── conversation_id UUID (FK, partition key)
├── sender_id       UUID (FK)
├── seq_num         bigint      ← monotonic within conversation (server-assigned)
├── content_type    ENUM(text, image, video, file, system)
├── body            text        ← encrypted blob for E2E
├── media_url       string      ← CDN URL for media
├── reply_to_seq    bigint      ← for thread replies
├── status          ENUM(sent, delivered, read)
├── created_at      timestamp
└── deleted_at      timestamp   ← soft delete

Receipt
├── message_id      UUID
├── user_id         UUID
├── receipt_type    ENUM(delivered, read)
└── timestamp       timestamp
```

### Why UUID as message_id (client-generated)?
- Client generates the ID before sending → **idempotency**: retries don't create duplicates
- Server upserts on `message_id` uniqueness constraint
- Trade-off: UUID collisions (negligible at 2^122 space) vs server-generated sequential IDs (simpler ordering but requires server round-trip before sending)

### seq_num: Server-Assigned Monotonic Counter
- Each conversation has a Redis counter: `INCR conv:{conv_id}:seq`
- Returned to sender upon persistence
- Enables ordering without distributed consensus
- Trade-off: Redis is single point for seq generation → mitigated with Redis Sentinel/Cluster; losing seq state causes gaps (acceptable — gaps don't lose messages)

---

## 4. Actions & API Design

### WebSocket Protocol (Primary)
```
Client → Server events:
  CONNECT     { token, device_id, last_seq_seen: { conv_id → seq } }
  SEND        { message_id, conversation_id, content_type, body, reply_to_seq }
  ACK         { message_id }          ← confirms delivery to client
  TYPING      { conversation_id, is_typing: bool }
  READ        { conversation_id, up_to_seq }

Server → Client events:
  NEW_MESSAGE { message_id, conversation_id, seq_num, sender_id, ... }
  RECEIPT     { message_id, user_id, receipt_type }
  PRESENCE    { user_id, status, last_seen_at }
  MISSED      { messages[] }          ← batch catch-up on reconnect
```

### REST API (Fallback + History)
```
POST   /v1/messages                    Send message (fallback if WS unavailable)
GET    /v1/conversations/{id}/messages?after_seq=N&limit=50   Paginated history
GET    /v1/conversations               Inbox list
POST   /v1/conversations               Create group
PUT    /v1/conversations/{id}/members  Add/remove members
GET    /v1/users/{id}/presence         Presence check
```

---

## 5. Data Flow

### 5.1 Sending a Message (Online Sender, Online Recipient)

```
[Client A]
    │
    │  SEND { message_id=uuid, conv_id, body }  (WebSocket)
    ▼
[Chat Server A]  ← Client A's persistent WS connection lives here
    │
    ├─ 1. Validate auth token (JWT, check Redis session cache)
    ├─ 2. Check membership in conversation (Redis cache, fallback DB)
    ├─ 3. Assign seq_num: INCR conv:{conv_id}:seq  (Redis)
    ├─ 4. Write to Message Store (Cassandra): async, fire-and-forget with ACK
    ├─ 5. Publish to Message Bus (Kafka topic: conv-{shard}):
    │      key = conversation_id (ensures ordering within partition)
    │      value = { message_id, conv_id, seq_num, sender_id, body, ... }
    └─ 6. Return ACK to Client A: { message_id, seq_num, status: "sent" }

[Kafka]
    │
    ▼
[Delivery Service]  (Kafka consumer group)
    │
    ├─ 7. Lookup recipient connections: Service Discovery (Redis pub/sub or
    │      consistent hash ring) → "Client B is on Chat Server B"
    ├─ 8. Forward message to Chat Server B via internal RPC (gRPC)
    └─ 9. If recipient offline: push to Notification Service

[Chat Server B]
    │
    ├─ 10. Push NEW_MESSAGE to Client B over WebSocket
    └─ 11. Client B sends ACK → Chat Server B → Delivery Service
               → updates Message.status = delivered
               → Kafka receipt event → Client A gets RECEIPT

[Cassandra Message Store]
    └─ Written in step 4; read for history/missed messages on reconnect
```

### 5.2 Message Delivery for Offline Recipient
```
[Delivery Service]
    │
    ├─ Recipient not in connection registry
    ├─ Write to Undelivered Queue (Redis List: undelivered:{user_id})
    │   → TTL 30 days (messages expire after 30 days undelivered)
    └─ Publish to Push Notification Service
           → APNs (iOS) / FCM (Android)
           → Notification: "New message from Alice"

[On Recipient Reconnect]
    Client sends: CONNECT { last_seq_seen: { conv_id → last_seq } }
    Chat Server fetches missed messages from Cassandra:
        SELECT * FROM messages WHERE conv_id=? AND seq_num > last_seq
    Delivers as MISSED batch
```

### 5.3 Group Message Fan-Out
```
Group with 1,000 members, sender sends 1 message:

[Delivery Service]
    │
    ├─ Read group membership from cache (Redis SET: group:{conv_id}:members)
    ├─ For each member_id:
    │     ├─ Check connection registry
    │     ├─ Online → push via their chat server (gRPC fan-out)
    │     └─ Offline → queue notification
    └─ Fan-out parallelized: goroutine pool or async tasks

Scale consideration:
  1,000 members * 5M msg/sec group messages = could spike to 5B fan-out ops/sec
  Mitigation: Only large groups (> 100 members) use separate Fan-Out Workers
  Small groups (≤ 100) fan out directly in Delivery Service
  WhatsApp approach: clients pull from server for large groups (pull model)
```

---

## 6. High-Level Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │              Client Layer                    │
                        │  Mobile App / Web App / Desktop App          │
                        └──────────────┬──────────────────────────────┘
                                       │ WebSocket (persistent)
                                       │ HTTPS (fallback / media upload)
                        ┌──────────────▼──────────────────────────────┐
                        │           Load Balancer / API Gateway        │
                        │  (Layer-7, sticky sessions by user_id hash)  │
                        └──┬───────────────────────┬───────────────────┘
                           │                       │
               ┌───────────▼──────┐    ┌──────────▼──────────┐
               │  Chat Servers    │    │  REST API Servers    │
               │  (WebSocket)     │    │  (History, Groups)   │
               │  ~3,000 pods     │    │  ~500 pods           │
               └───────┬──────────┘    └──────────┬───────────┘
                       │                           │
          ┌────────────▼───────────────────────────▼────────────┐
          │                    Kafka Cluster                     │
          │   Topics: messages, receipts, presence, fan-out      │
          │   Partitioned by conversation_id (256 partitions)    │
          └────────────┬────────────────────────────────────────┘
                       │
          ┌────────────▼────────────────────────────────────────┐
          │             Delivery Service (Kafka consumers)       │
          │   Connection Registry lookup → route to Chat Server  │
          └──┬──────────────────────────────────────┬───────────┘
             │                                      │
  ┌──────────▼──────────┐              ┌────────────▼────────────┐
  │  Message Store      │              │  Notification Service   │
  │  (Cassandra)        │              │  (APNs / FCM)           │
  │  Sharded by conv_id │              └─────────────────────────┘
  └─────────────────────┘
             │
  ┌──────────▼──────────┐   ┌──────────────────────┐   ┌──────────────────────┐
  │  Metadata DB        │   │  Redis Cluster       │   │  Media Storage       │
  │  (PostgreSQL)       │   │  - Sessions          │   │  (S3 + CloudFront)   │
  │  Users, Groups,     │   │  - Seq counters      │   │  Upload presigned    │
  │  Memberships        │   │  - Connection reg.   │   │  URLs, CDN delivery  │
  └─────────────────────┘   │  - Presence cache    │   └──────────────────────┘
                            └──────────────────────┘
```

---

## 7. Low-Level Design

### 7.1 Message Store: Cassandra Schema

**Why Cassandra?**
- Append-heavy write pattern (messages never update in place)
- Natural time-series partitioning by conversation
- Linear horizontal scalability
- Tunable consistency (QUORUM for writes, LOCAL_QUORUM for reads)
- Trade-off: No joins, no transactions → application handles consistency

```sql
CREATE TABLE messages (
    conversation_id  UUID,
    bucket           INT,          -- year-month bucket (e.g. 202605) to cap partition size
    seq_num          BIGINT,
    message_id       UUID,
    sender_id        UUID,
    content_type     TEXT,
    body             BLOB,         -- encrypted
    media_url        TEXT,
    reply_to_seq     BIGINT,
    created_at       TIMESTAMP,
    deleted_at       TIMESTAMP,
    PRIMARY KEY ((conversation_id, bucket), seq_num)
) WITH CLUSTERING ORDER BY (seq_num DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_size': 1,
                    'compaction_window_unit': 'DAYS'};
```

**Bucket strategy:** Without bucketing, a conversation active for years accumulates millions of rows in one partition → hotspot. Bucketing by month caps partition size to ~30 days * avg messages/day.

**Read pattern:** `SELECT * FROM messages WHERE conversation_id=? AND bucket=? AND seq_num > ?` → paginated, no full scans.

### 7.2 Connection Registry: Redis

```
Key:   session:{user_id}:{device_id}
Value: { server_id, connected_at, last_ping }
TTL:   30 seconds (refreshed by heartbeat every 10s)

Key:   server:{server_id}:users
Type:  Redis SET
Value: set of user_ids connected to this server
```

When a Chat Server receives a message to forward to `user_id`:
1. `GET session:{user_id}:*` → get server_id
2. gRPC call to `server_id` → push over WebSocket

If Redis lookup returns no session → user is offline → enqueue notification.

### 7.3 Sequence Number Generation

```
Redis INCR conv:{conversation_id}:seq
```

- Atomic, returns next integer
- **Limitation:** Single Redis key per conversation → could bottleneck for viral group chats
- **Mitigation for high-volume convs:** Pre-allocate ranges (INCRBY 100, dispense locally). Risk: gaps in seq_nums on server crash (acceptable — order preserved, no loss).

### 7.4 Message Deduplication (Idempotency)

Client generates `message_id` (UUID v4) before sending. Server does:
```sql
INSERT INTO messages (...) VALUES (...)
ON CONFLICT (message_id) DO NOTHING  -- PostgreSQL syntax
-- Cassandra: LWT not used for perf; instead deduplicate at Kafka consumer
```

Kafka consumer keeps a Bloom filter (or Redis SET with TTL) of recently seen `message_id`s. Duplicate detection window: 5 minutes (enough for client retry storms).

### 7.5 Presence System

```
Design: Heartbeat + Redis TTL

Client sends heartbeat every 10 seconds over WebSocket.
Chat Server updates: SET presence:{user_id} online EX 30

On disconnect / no heartbeat:
  Key expires after 30s → Presence Service detects (keyspace notification)
  → Publishes presence:offline event to Kafka
  → Updates DB last_seen_at
  → Fans out to interested parties (friends, group members online)

Trade-off:
  ✓ Simple, low latency presence
  ✗ 30s lag on detecting disconnect (can show "online" for up to 30s after disconnect)
  ✗ Redis keyspace notifications at scale (150M users * heartbeat every 10s = 15M writes/sec to Redis)
  
  Mitigation: Presence sampling — only update Redis if last update > 5s ago (client-side throttle)
  LinkedIn approach: Presence server subscribes to WebSocket events, not polling
```

### 7.6 Inbox & Unread Count

```
User inbox = sorted list of conversations by last_message_at

Materialized view approach (writes update inbox, avoid full scan):
  On new message → UPDATE conversations SET last_message_at=now() WHERE id=conv_id
  Inbox query:   SELECT * FROM conversations
                 JOIN conversation_members USING(conversation_id)
                 WHERE user_id=? ORDER BY last_message_at DESC LIMIT 20

Unread count:
  ConversationMember.last_read_seq is updated when client sends READ receipt
  Unread = (latest seq_num in conv) - last_read_seq
  Cached in Redis: HASH unread:{user_id} → { conv_id: count }
  Updated on new message (+1) and READ receipt (reset to 0)
```

---

## 8. Component Trade-Off Matrix

| Decision | Choice Made | Alternative | Why This | What You Give Up |
|---|---|---|---|---|
| Real-time transport | **WebSocket** | Long polling, SSE | Bidirectional, low overhead, native on mobile | Harder to load balance (sticky), firewalls sometimes block WS |
| Message storage | **Cassandra** | PostgreSQL, DynamoDB | Write-optimized, horizontal scale, natural time-series | No joins, no multi-row transactions, eventual consistency |
| Message bus | **Kafka** | RabbitMQ, SQS | Durable, replayable, ordered within partition, high throughput | Operationally complex, higher latency than in-memory queues |
| Seq number | **Redis INCR** | DB sequence, Snowflake ID | Simple, fast atomic increment | Redis down = no new messages until recovery; not globally unique |
| Presence | **TTL-based Redis** | Push-based pub/sub | Simple, self-healing (TTL auto-expires stale) | Up to 30s staleness, high write rate for 150M online users |
| Fan-out | **Push (delivery svc)** | Pull (clients poll) | Low latency delivery, users don't need to poll | Large groups need pull hybrid to avoid fan-out explosion |
| Media | **S3 + CDN** | Chat server streaming | Offload bandwidth, global CDN delivery | Higher upload latency (presigned URL round trip), cost |
| Metadata DB | **PostgreSQL** | MySQL, CockroachDB | Strong consistency for user/group data, ACID | Harder to scale writes beyond single primary (read replicas only) |

---

## 9. Sharding Strategy

### Message Store (Cassandra)
- Partition key: `(conversation_id, bucket)` → data collocated per conversation
- Natural distribution: UUID conversation IDs distribute evenly across nodes
- Hot conversation: A viral group with 1M members sends 1M msg/min → single Cassandra partition hot → mitigate with read replicas + caching recent messages in Redis

### Kafka
- Partition by `conversation_id` → messages in same conversation land on same partition → ordering guaranteed
- 256 partitions across 32 brokers (8 partitions/broker)
- Consumer group: Delivery Service scales to 256 parallel consumers

### PostgreSQL (Metadata)
- Shard by `user_id` range for User table (not critical — 500M rows manageable on one large instance with read replicas)
- Or: use Citus/CitusDB for distributed PostgreSQL at scale

---

## 10. Caching Strategy

| Cache Layer | What's Cached | TTL | Invalidation |
|---|---|---|---|
| **Redis: Connection Registry** | user_id → server_id | 30s (heartbeat refresh) | Expire on disconnect |
| **Redis: Presence** | user_id → online/offline | 30s | Expire = offline |
| **Redis: Recent Messages** | Last 50 messages per active conv | 5 min | Write-through on new message |
| **Redis: Group Membership** | conv_id → [user_ids] | 5 min | Invalidate on join/leave |
| **Redis: Unread Counts** | user_id → {conv_id: count} | No TTL | Update on message + read receipt |
| **CDN (CloudFront)** | Media files (images/video) | 7 days | Immutable (content-addressed URL) |
| **Client-side** | Message history (SQLite) | Persistent | Sync on reconnect via seq_num diff |

**Cache Stampede Prevention:** On popular group conversation cache miss, use **probabilistic early expiry** (XFetch algorithm) or Redis SETNX lock before fetching from Cassandra.

