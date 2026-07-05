# Slack: Team Messaging Platform — System Design
*Date: 2026-06-25*

---

## First Principles — Do We Even Need This?

**Problem:** Teams need persistent, organized, searchable communication — not ephemeral like a phone call, not asynchronous like email.

**Why not email?**
- No real-time delivery expectation
- Threads are linear, no channel-based topical grouping
- No presence/status awareness
- No rich integrations or bots

**Why not WhatsApp/Telegram?**
- No workspace isolation (teams can't have separate namespaces)
- No channel model (no #eng-backend vs #design)
- No enterprise audit/compliance features
- No threading (replies inside a message)

**Core insight:** Slack = persistent, organized, real-time messaging with workspace isolation + rich integrations. Every design decision flows from these four requirements.

---

## Scale Estimates

**Assumptions (interview scale):**
- 50M registered users, 20M DAU
- 5M concurrent users (peak)
- Average: 100 messages/user/day = 2B messages/day = **~23,000 msg/sec**
- Peak: 3× = ~70,000 msg/sec
- Messages: avg 200 bytes text payload
- 20% messages carry file attachments (avg 2MB per file)
- Workspaces: 500K active workspaces
- Channels per workspace: avg 20 public + 50 private = 70 channels
- Channel members: median 15, P99 = 5,000, max = 50,000 (announcement channels)

**Storage:**
```
Messages/day:   2B × 200B = 400GB/day text
File uploads:   2B × 20% × 2MB = 800TB/day (unrealistic — actual ~10%)
Realistic files: 2B × 5% × 500KB = 50TB/day
Total storage:  400GB text + 50TB files per day
5-year retention: ~100PB (files dominate)
```

**Bandwidth:**
```
Inbound:   23K msg/sec × 200B = 4.6MB/s text
Outbound:  Each message fans out to ~15 members on avg
           23K × 15 × 200B = 69MB/s text outbound
Peak:      ~210MB/s
```

---

## Entities & Actions

### Entities
```
User            id, email, display_name, avatar_url, status
Workspace       id, name, plan, owner_user_id
Channel         id, workspace_id, name, type (public/private/dm/group_dm), topic, member_count
ChannelMember   channel_id, user_id, joined_at, last_read_message_id
Message         id, channel_id, user_id, content, thread_ts (null if not reply), created_at, edited_at
Reaction        message_id, user_id, emoji
Attachment      id, message_id, file_url, file_type, size_bytes
Webhook         id, workspace_id, channel_id, endpoint_url, events[]
```

### Actions
- Send message to channel / DM
- Reply in thread
- Add reaction to message
- Search messages (full-text, by user, by channel, by date)
- Upload file
- Add integration / configure webhook
- Mark channel as read
- Set user status / presence

### Data Flow (Send Message)
```
1. Client → WebSocket Gateway (keep-alive connection)
2. Gateway → Message Service: validate + persist
3. Message Service: write to Cassandra + publish to Kafka topic: messages.{workspace_id}
4. Fan-out Service: subscribe Kafka → determine active members → push via WebSocket or queue
5. Search Indexer: async consume Kafka → write to Elasticsearch
6. Notification Service: for offline users → mobile push / email digest
```

---

## High-Level Design

```
                    ┌──────────────────────────────────────┐
                    │          Load Balancer               │
                    └──────────────────────────────────────┘
                           │                   │
              ┌────────────┴──┐        ┌───────┴──────────┐
              │  WebSocket    │        │    REST API       │
              │  Servers      │        │    Servers        │
              │ (sticky/ws_id)│        │ (search, upload)  │
              └───────┬───────┘        └───────┬───────────┘
                      │                        │
              ┌───────▼────────────────────────▼───────────┐
              │              Message Service               │
              └───────────────────┬────────────────────────┘
                                  │
                    ┌─────────────▼────────────┐
                    │          Kafka           │
                    │  topics: messages.{ws_id}│
                    └──┬──────────┬────────────┘
                       │          │
             ┌─────────▼──┐  ┌───▼──────────────┐
             │ Fan-out Svc │  │ Search Indexer   │
             └──────┬──────┘  └──────────────────┘
                    │                  │
             ┌──────▼─────┐   ┌───────▼──────────┐
             │ Redis       │   │ Elasticsearch    │
             │ Pub/Sub     │   │ (message search) │
             └──────┬──────┘   └──────────────────┘
                    │
             ┌──────▼──────────┐
             │  WebSocket Svc  │ → push to connected clients
             └─────────────────┘

Databases:
  Cassandra    → messages (time-series, append-only)
  PostgreSQL   → users, workspaces, channels, memberships (relational, transactional)
  Redis        → presence, sessions, channel member cache, unread counts
  S3           → file attachments
  Elasticsearch → full-text search index
```

---

## Low-Level Design

### 1. Message Storage (Cassandra)

**Why Cassandra?**
- Append-only writes (messages never update, only edit creates new version)
- Query pattern is always: get messages in channel X, ordered by time — known query path
- Write throughput: 70K msg/sec peak — beyond Postgres comfortable range
- Time-series data: partition by channel + month → predictable partition size

```sql
-- Primary table: messages by channel
CREATE TABLE messages (
    channel_id    UUID,
    bucket        INT,          -- YYYYMM, limits partition size
    message_id    TIMEUUID,     -- version 1 UUID: time-ordered, unique
    user_id       UUID,
    content       TEXT,
    thread_ts     TIMEUUID,     -- null if top-level; parent message_id if reply
    edited_at     TIMESTAMP,
    reactions     MAP<TEXT, SET<UUID>>,  -- {"👍": {user1, user2}}
    attachments   LIST<UUID>,
    PRIMARY KEY ((channel_id, bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);
-- Reads: WHERE channel_id=X AND bucket=202506 ORDER BY message_id DESC LIMIT 50

-- Thread messages (replies)
CREATE TABLE thread_messages (
    parent_message_id  TIMEUUID,
    message_id         TIMEUUID,
    user_id            UUID,
    content            TEXT,
    created_at         TIMESTAMP,
    PRIMARY KEY (parent_message_id, message_id)
) WITH CLUSTERING ORDER BY (message_id ASC);
```

**Bucket pattern:** One partition per channel per month. Prevents unbounded partitions.

**Tradeoff:** Cassandra can't enforce foreign keys. Message → channel_id validity checked at application layer. Eventual consistency on reactions (MAP<TEXT, SET<UUID>>) is fine — stale reaction count by 100ms is acceptable.

### 2. Relational Data (PostgreSQL)

```sql
-- Workspaces (multi-tenant isolation)
CREATE TABLE workspaces (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    plan        TEXT DEFAULT 'free',
    owner_id    UUID REFERENCES users(id),
    created_at  TIMESTAMP
);

-- Channels
CREATE TABLE channels (
    id           UUID PRIMARY KEY,
    workspace_id UUID REFERENCES workspaces(id),
    name         TEXT,
    type         TEXT CHECK (type IN ('public','private','dm','group_dm')),
    member_count INT DEFAULT 0,
    INDEX (workspace_id, type, name)
);

-- Channel membership — enforces access control
CREATE TABLE channel_members (
    channel_id         UUID REFERENCES channels(id),
    user_id            UUID REFERENCES users(id),
    last_read_ts       TIMEUUID,
    notification_pref  TEXT DEFAULT 'all',
    PRIMARY KEY (channel_id, user_id),
    INDEX (user_id)    -- needed for "all channels for a user"
);
```

**Multi-tenant isolation:**
Every query scopes to workspace_id. Application-layer enforcement: JWT contains workspace_id, every API handler validates user's workspace matches resource's workspace. No cross-workspace data sharing at DB level.

### 3. Fan-out Strategy — Key Design Decision

**Problem:** Message sent to a channel with 50K members. Must deliver to all active members in real-time.

**Naive approach (push to all):**
```
1 message × 50K members × 1 Redis pub/sub publish = 50K WebSocket pushes
23K msg/sec × 50K = 1.15B operations/sec → infeasible
```

**Solution: Hybrid fan-out (See Concept #77)**

```
Small channels (≤ 1,000 members) — Push Fan-out:
  Fan-out Service → look up active members from Redis
  → Publish to each member's personal channel: user:{user_id}:events
  → WebSocket server subscribed to that channel delivers instantly
  Cost: 1 message × up to 1000 Redis publishes

Large channels (> 1,000 members) — Pull Fan-out (lazy):
  Fan-out Service → store message in hot cache: channel:{id}:recent
  → Publish single notification: channel:{id}:new_message (just the channel + ts)
  → Clients polling /channels/{id}/messages?after={last_ts} pull on notification
  Cost: 1 Redis publish → N clients each pull on their own cadence
```

**Why this threshold:**
```
1,000 members × 23K msg/sec × avg 5-member-per-message = 115M Redis ops/sec
Already hitting limits. Large channel fan-out to all = unscalable.
Real Slack uses ~1,000 as threshold (from their engineering blog).
```

### 4. WebSocket Routing

**Problem:** User A (WebSocket on server WS-3) sends message to channel. User B (WebSocket on server WS-7) must receive it.

```
Architecture: each user assigned to a WebSocket server by consistent hash on user_id
  → sticky session: user always reconnects to same server

Connection registry in Redis:
  SET ws:user:{user_id} "WS-7"   EX 3600  -- which server this user is on

Fan-out path:
  Fan-out Service → lookup users' WS servers from Redis
  → Publish directly to ws:{server_id}:inbox Kafka topic
  → Each WS server consumes its own topic → pushes to connected clients
```

**Presence:**
```
User connects  → SET presence:{user_id} ONLINE EX 60
               → background heartbeat every 30s resets TTL
User disconnects → DEL (or let TTL expire)

Query: GET presence:{user_id} → ONLINE or nil (offline)

Channel presence (e.g., "5 members online"):
  SADD channel:{id}:online {user_id} + TTL on individual keys
  SCARD channel:{id}:online → count
```

### 5. Unread Count & Notifications

```
Per user per channel:
  Redis HASH: unread:{user_id} → {channel_id: count}
  HINCRBY unread:{user_id} {channel_id} 1   -- on message received
  HSET    unread:{user_id} {channel_id} 0   -- on channel read

Badge count (total unread):
  HVALS unread:{user_id} → sum all values
  Cache this sum: unread_total:{user_id} → integer
```

### 6. Search (Elasticsearch)

```
Index per workspace: slack_messages_{workspace_id}
Document:
{
  "message_id": "...",
  "channel_id": "...",
  "user_id":    "...",
  "content":    "...",    ← analyzed field (tokenized, lowercased)
  "created_at": "...",
  "thread_ts":  "..."
}

Query (full-text + filters):
POST /slack_messages_{ws_id}/_search
{
  "query": {
    "bool": {
      "must": [{ "match": { "content": "deploy failed" }}],
      "filter": [
        { "term":  { "channel_id": "..." }},
        { "range": { "created_at": { "gte": "2026-01-01" }}}
      ]
    }
  }
}
```

**Concern:** Elasticsearch indexes accumulate. Free tier users have 90-day search retention. ES index TTL enforced via ILM (Index Lifecycle Management) policy.

---

## Budget Constraint — Free vs Paid Tier

**Real Slack limits search to 90 days on free tier.** Why? Elasticsearch cost.

```
Elasticsearch sizing:
  2B messages/day × 200B text × 365 days = 146GB/year text
  With inverted index overhead (~3×): 438GB/year
  10K workspaces × 438GB = 4.38PB Elasticsearch total (clearly infeasible)

Solution: tiered search
  Free plan:   search last 90 days only
               → Elasticsearch index TTL = 90 days via ILM rollover
               → Index size: 90 × ~40GB/day = 3.6TB per 10K workspaces (manageable)

  Pro/Business plan: full search history
               → All messages in ES (older on cheaper r6g.2xlarge data nodes)
               → Hot tier (recent 6 months): r6g.2xlarge ($305/mo/node)
               → Warm tier (6mo+): d3.2xlarge (dense storage, $630/mo/node)

AWS cost (10K workspaces, business plan):
  Elasticsearch:  100 hot nodes × $305 = $30,500/mo
                  200 warm nodes × $630 = $126,000/mo
  Cassandra:       50 i3en.2xlarge × $550 = $27,500/mo
  PostgreSQL:      10 db.r6g.4xlarge × $1,200 = $12,000/mo
  Redis:           20 r6g.2xlarge × $305 = $6,100/mo
  WebSocket EC2:   100 c6i.2xlarge × $250 = $25,000/mo
  S3 (files):      5PB × $23/TB = $115,000/mo
  Kafka (MSK):     30 brokers × $450 = $13,500/mo
  Data transfer:                         $20,000/mo
  ─────────────────────────────────────────────────
  Total estimate:                       ~$375,000/mo for 10K workspaces
  Per workspace:                        ~$37.50/month
  Real Slack Business plan:             $12.50/user × avg 50 users = $625/workspace/month
  Gross margin room:                    ~94% (reasonable for SaaS)
```

**Budget constraint impact:** Elasticsearch is the #1 cost driver. Free tier enforces 90-day limit to keep ES index small. File storage (S3) is #2 — free tier has 5GB file storage cap.

---

## Trade-offs Summary

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Cassandra for messages | High write throughput, natural time-partitioning | No FK constraints, eventual consistency on reactions |
| Hybrid fan-out | Scales to large channels | Pull fan-out adds ~100ms extra latency for large channels |
| WebSocket with sticky sessions | Low latency message delivery | Server failure drops all sessions on that server (need reconnect) |
| Elasticsearch for search | Rich full-text queries, faceted search | High cost → enforces tier limits on search depth |
| Reactions in Cassandra MAP | Simple, no extra table | Concurrent reactions to same message need LWW or CRDT |
| Workspace-sharded Kafka topics | Isolate noisy workspaces | Topic proliferation (500K workspaces = 500K topics, use workspace_id bucketing) |

---

## References
- Slack Engineering Blog: "How Slack Works" (2022) — describes hybrid fan-out threshold
- Slack Engineering: "Flannel — Application-Level Edge Cache" — caching message history
- Cassandra Data Modeling for Messaging: DataStax documentation
