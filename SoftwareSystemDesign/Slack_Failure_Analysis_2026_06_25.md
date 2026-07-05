# Slack: Failure Analysis
*Date: 2026-06-25*

---

## Failure-First Principle
For each component: what if it fails? What if the component before it fails? After it?

---

## Component Map (Dependency Order)

```
Client → Load Balancer → WebSocket Server → Message Service
                                                 ↓
                                   PostgreSQL  Cassandra  Kafka
                                                            ↓
                                                      Fan-out Svc
                                                            ↓
                                                     Redis Pub/Sub
                                                            ↓
                                               WebSocket Servers (delivery)
                                                            ↓
                                                Notification Svc → APNs/FCM
                                                      ↓
                                               Search Indexer → Elasticsearch
```

---

## 1. WebSocket Server Failure

### What fails
User's persistent WebSocket connection drops. Client stops receiving messages.

### If component ahead (Load Balancer) fails
- LB crashes: client TCP connection immediately drops → reconnect to another LB instance
- LB → WS routing breaks: client sees "connection refused" → exponential backoff retry

### If WebSocket Server itself fails
- All users on that server lose connection simultaneously
- Redis connection registry: `ws:user:{id}` still points to dead server
- Fan-out service tries to deliver → publishes to dead server's Kafka topic → no consumer

**Solutions:**
1. **Client reconnects within 5s** (exponential backoff: 1s, 2s, 4s, max 30s)
2. **Registry cleanup:** on connection close, server DELs `ws:user:{id}` from Redis. On crash (no graceful close), TTL expires (60s). During TTL window, Fan-out falls back to queuing in `offline_queue:{user_id}`
3. **Message replay on reconnect:** client sends `last_seen_message_id`. Server returns all messages with `message_id > last_seen` from Cassandra. Client catches up.
4. **Session migration:** new WebSocket server registers new entry in Redis within seconds of reconnect

### If component behind (Kafka consumer) fails
Fan-out Service crash:
- Messages accumulate in Kafka (durable). Consumer group lag builds.
- Kafka retention: 7 days → no message loss even if Fan-out is down for hours
- On restart: Fan-out resumes from last committed offset → processes all buffered messages
- **User experience:** message delivery delayed by minutes → clients can poll REST endpoint as fallback

---

## 2. Cassandra Node Failure

### What fails
Message writes or reads fail for partitions owned by that node.

### If component ahead (Message Service) fails before write
- Message Service crashes after Kafka publish but before Cassandra write
- **Solution:** Fan-out consumed from Kafka but message not stored → client shows message, DB doesn't have it → inconsistency
- **Fix:** Write to Cassandra FIRST, then publish to Kafka. Or: consumer writes to Cassandra as authoritative path (Kafka = source of truth, Cassandra = derived)

### Cassandra node itself fails
- **Replication factor = 3** (RF=3): write quorum W=QUORUM (2 of 3), read quorum R=QUORUM (2 of 3)
- Single node failure: remaining 2 of 3 replicas satisfy quorum → **no downtime**
- **Hinted handoff:** coordinator stores hint for failed node → delivers when it recovers
- **Read repair:** if quorum reads return divergent data → Cassandra auto-repairs stale replica

### If 2 of 3 nodes fail simultaneously (same rack/AZ failure)
- Write quorum (2) impossible → writes fail → messages lost
- **Solution:** Place replicas across 3 AZs (`NetworkTopologyStrategy`). AZ failure only takes 1 of 3 replicas down.

### If component behind (Search Indexer) fails
- Cassandra writes succeed, Kafka has the message
- Search Indexer down: messages not indexed → search results stale
- **No message loss:** Indexer replays from Kafka offset when it recovers
- **User impact:** search returns results up to the point before outage, then catches up

---

## 3. Kafka Failure

### What fails
Message publishing/consuming fails. Fan-out, search indexing, notifications all stop.

### Kafka broker failure
- **Partition replication factor = 3.** Leader election happens automatically (ISR — In-Sync Replicas)
- Single broker failure: leader election completes in ~10s → temporary write unavailability
- **Producer retry:** Message Service retries with exponential backoff + idempotence enabled (`enable.idempotence=true` prevents duplicate on retry)

### Kafka cluster completely down (all brokers)
- **Critical path blocked:** Message Service can't publish → options:
  1. **Write-through bypass:** Message Service writes to Cassandra + directly broadcasts to Fan-out via synchronous gRPC call (degraded, removes Kafka decoupling but maintains delivery)
  2. **Local buffer:** Message Service queues to local disk (Kafka-less emergency buffer), replays when Kafka recovers
  3. **Circuit breaker open:** reject new messages, return 503 to clients with Retry-After header
- **Real Slack approach:** Kafka is not on the hot path for initial delivery to active WebSocket connections. The fan-out is triggered asynchronously; initial delivery goes directly to presence-aware channels. Kafka serves as durable replay buffer for missed messages and async processes.

---

## 4. Redis Failure (Pub/Sub + Presence + Unread Counts)

### Redis single node failure
- Sentinel-managed HA: primary fails → sentinel elects replica in ~30s
- During 30s: fan-out to connected users breaks, presence lookups fail, unread counts unavailable
- **Fan-out fallback:** WebSocket servers fall back to Kafka-based delivery (slightly slower but durable)
- **Unread counts:** stale for 30s, auto-correct on next channel open (client queries REST API as fallback)

### Redis complete cluster failure
**Circuit breaker opens on 5 consecutive failures.**

Fallback behavior per Redis use case:
```
Presence:        → Treat all users as offline → deliver via Notification Svc (push notifications)
                   No real-time typing indicators or online status
                   
Fan-out:         → Fall back to direct Kafka fan-out (slower)
                   Each WebSocket server reads from Kafka directly for its connected users
                   
Unread counts:   → Serve from PostgreSQL (channel_members.last_read_ts vs latest message_id)
                   Slower but correct
                   
Session/Auth:    → JWT-based auth doesn't need Redis → no impact
                   
Rate limiting:   → Disabled during outage (accept all requests) → expose to spam
                   Mitigate: stateless token bucket in API Gateway as backup
```

**Post-recovery:** Redis repopulates from Cassandra for unread counts and from presence heartbeats for online status. Converges within minutes.

---

## 5. PostgreSQL Failure (Users, Workspaces, Channels)

### What's served from PostgreSQL
- User authentication (JOIN + workspace validation)
- Channel membership checks (access control)
- Workspace plan/limits

### PostgreSQL primary failure
- RDS Multi-AZ: automatic failover to standby replica in ~60-120s
- During failover: **new messages blocked** (can't validate channel membership)
- **Mitigation:** Cache channel membership in Redis: `SET channel_member:{channel_id}:{user_id} 1 EX 300`
  - 5-min TTL: users who had recent activity can still send during PostgreSQL failover
  - New users or new channel joins fail gracefully with "try again soon"

### PostgreSQL replica lag
- Reads (channel list, member lookup) go to replica → stale data possible
- **Critical:** channel membership check MUST go to primary (replica lag could allow unauthorized access or deny authorized user)
- **Solution:** Access control reads → primary. Display reads (channel list, user profiles) → replica

---

## 6. Search Service (Elasticsearch) Failure

### Elasticsearch node failure
- Shard replication: each shard has 1 primary + 2 replicas (across 3 AZs)
- Single node failure: replica promoted to primary, search continues
- **Impact:** replication factor 3 tolerates 2 simultaneous node failures

### Elasticsearch cluster failure
- **Search unavailable.** Messages still delivered (search not on hot path).
- Circuit breaker in Search Service: returns empty results + HTTP header `X-Search-Degraded: true`
- Client UI shows "Search temporarily unavailable, please try again later"
- **No data loss:** Search Indexer's Kafka consumer offset not committed → on recovery, re-indexes all missed messages

### Indexing lag (Search Indexer slow)
- Kafka consumer lag builds (visible in monitoring)
- Search results show messages up to N minutes ago
- **Acceptable:** search is inherently slightly stale (ES indexing is async)
- Alert threshold: consumer lag > 100K messages (>~4 seconds at normal rate) → page on-call

---

## 7. Fan-out Service Failure

### All consumer instances crash
- Messages accumulate in Kafka (zero loss)
- **Active WebSocket connections:** receive no real-time updates during outage
- **Client behavior:** notices no heartbeat from server → triggers REST poll: `GET /channels/{id}/messages?after={last_ts}` every 10 seconds (degraded mode polling)
- On Fan-out recovery: resumes from Kafka offset → delivers backlog → clients reconcile

### Fan-out falls behind (consumer lag growing)
- Hot workspace with 50K-member channel floods queue
- **Mitigation:** separate Kafka consumer group per workspace tier (large workspaces on dedicated partition set)
- **Rate limiting:** large channel fan-out rate-limited to 1,000 WebSocket pushes/sec per channel → remaining clients pull on notification

---

## 8. Notification Service Failure (APNs/FCM)

### APNs / FCM failure (Apple/Google push down)
- Out of our control. Messages still in Cassandra.
- **Mitigation:** queue push notifications in SQS with TTL=24h → retry when APNs/FCM recovers
- If TTL expired: message missed → user sees unread badge on next app open

### Notification Service itself crashes
- Messages in Kafka persisted → consumer catches up on recovery
- Users offline during outage miss push notifications
- On next app open: client fetches message history from Cassandra → shows unread messages

---

## 9. File Upload Service / S3 Failure

### S3 bucket unavailable
- File uploads fail → message send blocked if attachment is required
- **Solution:** S3 99.999999999% durability, 99.99% availability. Virtually never fails.
- **Handling:** Retry upload 3× with backoff → show "upload failed, retry?" to user
- Message text (without attachment) always delivered first → attachment fails separately

### File too large / quota exceeded
- **Free plan:** 5GB total file storage cap
- **Enforcement:** before upload, check `workspace.used_storage` in PostgreSQL
- If over quota: return 413 with human-readable error "Upgrade your plan to upload files"

---

## Monitoring & Alerting

| Metric | Alert Threshold | Action |
|--------|-----------------|--------|
| WebSocket connection drops (rate) | >1% of active connections/min | Check WS server health |
| Cassandra write latency P99 | >50ms | Check node load, partition hotspot |
| Kafka consumer lag (Fan-out) | >50K messages | Scale Fan-out consumers |
| Redis hit rate | <95% | Check eviction policy / memory |
| Elasticsearch indexing lag | >100K messages | Scale indexer |
| PostgreSQL replica lag | >1s | Check replica I/O |
| Failed push notifications | >1% | Check APNs/FCM credentials |

---

## Graceful Degradation Tiers

```
Tier 1 (fully operational):
  All features available. Real-time delivery, search, presence, reactions.

Tier 2 (Redis degraded):
  Message delivery via Kafka direct. No real-time presence. Unread from PostgreSQL.
  Search still available (separate stack). File uploads still work.

Tier 3 (Kafka + Redis degraded):
  REST polling mode (10s interval). Messages delivered with delay.
  Write to Cassandra directly. Fan-out queue on local buffer.

Tier 4 (Cassandra degraded):
  New message delivery fails. Historical messages unavailable.
  READ-ONLY mode: can view cached message history up to last known good state.
  Inform users: "Slack is experiencing issues. Messages are not being saved."

Tier 5 (PostgreSQL degraded):
  Cannot authenticate new sessions. Existing sessions (JWT valid) can continue.
  Cached channel memberships serve for ~5 minutes (TTL).
  New users / new channels blocked.
```
