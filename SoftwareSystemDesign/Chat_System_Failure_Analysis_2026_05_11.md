# Chat System — Failure Analysis & Resilience Playbook
**Date:** 2026-05-11  
**System:** Distributed Chat (WhatsApp/Slack style)  
**Principle:** Every component will fail. Design so that failure is contained, recoverable, and observable.

---

## Failure Analysis Framework

For each component we analyze three dimensions:
1. **Internal failure** — the component itself crashes / becomes unresponsive
2. **Upstream failure** — the component feeding data into it fails
3. **Downstream failure** — the component receiving its output fails

Plus: **What the user experiences** and **Recovery path**.

---

## Component 1: Chat Server (WebSocket Host)

### 1A — Internal: Chat Server Pod Crashes Mid-Session

**Scenario:** A pod handling 50,000 active WebSocket connections crashes (OOM, hardware fault, bad deploy).

**What happens without mitigation:**
- All 50,000 connections drop simultaneously
- Messages in-flight (sent by server but not ACK'd by client) are lost
- Connection Registry (Redis) still shows these users on the crashed server for up to 30s (TTL)
- Delivery Service tries to forward messages to dead server → gRPC errors for 30s

**Mitigation:**
1. **Client-side reconnect with exponential backoff + jitter:** Client detects disconnect, reconnects within 1–5 seconds (random jitter prevents thundering herd — 50K clients all reconnecting simultaneously would spike auth servers)
2. **Missed message replay on reconnect:** Client sends `CONNECT { last_seq_seen: {...} }` → server fetches all messages since last_seq from Cassandra → client catches up with zero loss
3. **Connection Registry TTL:** Redis key expires after 30s → Delivery Service stops routing to dead server → messages queue for reconnected client
4. **Graceful shutdown hook:** K8s `preStop` hook sends `GOAWAY` frame to clients, triggering orderly reconnect to different server before pod terminates

**Real-world:** WhatsApp client reconnects in ~2 seconds; Discord documented their 2015 WebSocket migration that reduced dropped-message rates by 40%.

**Recovery SLA:** Messages are never lost (in Kafka/Cassandra). Client experiences a brief disconnect (1–5s) and receives all missed messages upon reconnect.

---

### 1B — Upstream: Kafka Is Unavailable (Chat Server Cannot Publish)

**Scenario:** Kafka cluster is unreachable (network partition, broker failures).

**What happens without mitigation:**
- Chat Server receives SEND from client, cannot publish to Kafka
- Two options: reject the message (bad UX) or buffer locally (risky)

**Mitigation:**
1. **Producer buffer + local queue:** Kafka producer configured with `linger.ms=5, buffer.memory=64MB` — messages buffer in producer for a short window, retried automatically
2. **Circuit breaker:** After 10 consecutive Kafka publish failures, open circuit → Chat Server returns error to client: `{ status: "failed", retry_after: 5s }`
3. **Client retry:** Client shows "!" indicator, retries send after backoff. Because `message_id` is client-generated UUID, retry is idempotent
4. **Kafka producer acks=all:** Ensure message is written to all in-sync replicas before returning success — prevents message loss on broker failover
5. **Fallback path (degraded mode):** Write directly to Cassandra + Redis pub/sub for delivery. Slower, less durable, but functional during Kafka outage

**Trade-off of acks=all:** Higher write latency (+20–50ms) in exchange for zero message loss on broker failure. acks=1 is faster but risks loss if leader crashes before replication.

---

### 1C — Downstream: Delivery Service Is Down (Messages Not Routed)

**Scenario:** Delivery Service pods are all crashing (bad deploy, memory leak).

**What happens:**
- Messages published to Kafka accumulate (Kafka consumer lag grows)
- No fan-out to recipient chat servers → messages not delivered in real-time
- Kafka retains messages (default 7-day retention)

**Mitigation:**
1. **Kafka durability = zero loss:** Messages sit safely in Kafka until Delivery Service recovers
2. **Consumer lag alerting:** Alert fires at > 100K message lag → on-call paged within 2 minutes
3. **Upon recovery:** Delivery Service resumes from last committed offset → delivers all buffered messages in order
4. **User experience:** Recipient sees messages arrive in a burst after recovery. Timestamps preserved (created_at from message), so order is correct.

---

## Component 2: Kafka (Message Bus)

### 2A — Internal: Kafka Broker Failure (1 of 32 Brokers Dies)

**Scenario:** One broker dies; it was the leader for some partitions.

**What happens with proper config:**
1. Zookeeper/KRaft detects leader failure within 10–30 seconds
2. Controller elects a new leader from in-sync replicas (ISR)
3. Producers/consumers transparently retry and connect to new leader
4. Messages in-flight to dead broker: if `acks=all`, they were already replicated → no loss. If `acks=1`, up to last few ms of messages could be lost (acknowledged but not yet replicated)

**Mitigation:**
- `replication.factor=3` (all critical topics)
- `min.insync.replicas=2` — refuse writes if fewer than 2 replicas acknowledge (prevents silent data loss)
- `unclean.leader.election=false` — never elect an out-of-sync replica as leader (would sacrifice durability for availability)

**Real-world:** GitHub's 2012 Kafka incident — single broker failure caused metadata topic corruption because `unclean.leader.election=true`. Fixed by disabling it.

**Recovery time:** ~30 seconds for new leader election. 30s * 5M msg/sec = 150M messages buffered in producers during this window (within Kafka producer buffer limits).

---

### 2B — Upstream: Chat Servers Produce Duplicate Messages (Producer Retries)

**Scenario:** Kafka broker receives a message, writes it, but ACK is lost (network blip). Producer retries → duplicate in Kafka.

**Mitigation:**
1. **Idempotent producer:** `enable.idempotence=true` — Kafka assigns each producer a PID + sequence number; broker deduplicates within a session
2. **Exactly-once semantics (EOS):** For critical paths, use Kafka transactions (`transactional.id`). Higher overhead, used selectively for receipt updates.
3. **Consumer-side dedup:** Delivery Service maintains a Redis Bloom filter of recently processed `message_id`s (5-minute window, 1% false positive rate). Duplicate suppressed before fan-out.

**Trade-off:** Idempotent producers add ~10% overhead vs non-idempotent. EOS adds ~50ms latency for transaction commit. Use EOS only where duplicates cause user-visible problems (e.g., charging, not chat messages where a duplicate is merely annoying).

---

### 2C — Internal: Kafka Topic Partition Is Hot (One Conversation Has 10M Messages/Day)

**Scenario:** A viral broadcast group (1M members, celebrity account) sends messages at 10x normal rate. All go to the same Kafka partition (keyed by `conversation_id`).

**What happens:**
- Single partition maxes out at ~100MB/s → consumer lag spikes → delivery delays

**Mitigation:**
1. **Detect hot conversations:** Monitor per-partition message rate. Flag conversations exceeding 1K msg/min as "hot"
2. **Partition splitting for hot convs:** Add a sub-key suffix: `{conversation_id}-{hash(message_id) % 8}` → spread across 8 partitions. Consumer merges and re-sorts by seq_num.
3. **Pull model for very large groups (>10K members):** Don't fan-out at all. Clients poll the message API. Eliminates delivery service bottleneck entirely. WhatsApp uses this for broadcast lists.

---

## Component 3: Cassandra (Message Store)

### 3A — Internal: Cassandra Node Failure

**Scenario:** 1 of 30 Cassandra nodes dies. Replication factor = 3.

**With Cassandra's consistency model (`QUORUM` writes, `LOCAL_QUORUM` reads):**
- Write quorum = ceil(3/2)+1 = 2 nodes. With 1 dead, 2 remaining → writes succeed
- Read quorum = 2 nodes → reads succeed
- **Zero downtime for reads/writes** as long as ≤1 node fails per replica set

**Data repair after recovery:**
- `nodetool repair` runs anti-entropy repair → re-syncs rows written during downtime
- Hinted handoff: While node is down, coordinators store "hints" for missed writes → replayed when node rejoins (default: 3-hour hint window)
- If node is down > 3 hours: full repair required (expensive but offline process)

**Trade-off:** `QUORUM` vs `LOCAL_ONE`:
- `QUORUM`: Higher consistency, 2-node agreement required → slightly higher latency (~5ms more)
- `LOCAL_ONE`: Faster, but may read stale data if recent write went to a different replica

For message history reads, `LOCAL_ONE` is acceptable (user won't notice 5s stale). For message send (write), `QUORUM` ensures durability.

---

### 3B — Upstream: Kafka Consumer Crashes Mid-Write to Cassandra

**Scenario:** Delivery Service writes message to Cassandra, Cassandra write succeeds, but before committing Kafka offset — service crashes. On restart, same message replayed.

**What happens without mitigation:**
- Duplicate message in Cassandra → user sees message twice

**Mitigation:**
1. **Cassandra `IF NOT EXISTS` LWT (Lightweight Transaction):** `INSERT INTO messages (...) IF NOT EXISTS` → idempotent, second insert is a no-op
2. **Cost:** LWT uses Paxos → 4x more latency than regular insert (~20ms vs 5ms). Used for message inserts only.
3. **Alternative:** Client-generated UUIDs as primary key → Cassandra's natural PK uniqueness prevents duplicates without LWT. If `message_id` already exists → row not overwritten (Cassandra's "last write wins" based on timestamp, but same message_id same timestamp → idempotent).

---

### 3C — Downstream: Message Store Unavailable During Read (History Fetch)

**Scenario:** User opens the app, Cassandra is unresponsive for history fetch (read timeout).

**Mitigation:**
1. **Client-side cache (SQLite on device):** Last 100 messages per conversation stored locally. User sees cached history instantly, even during outage
2. **Redis recent messages cache:** Last 50 messages per active conversation in Redis. Much faster than Cassandra for hot conversations. Redis serves read during Cassandra degradation.
3. **Graceful degradation:** Show stale messages with "Could not load older messages" indicator. Never show blank screen.
4. **Timeout + circuit breaker:** After 2s timeout, serve from cache; after 5 consecutive failures, open circuit for 30s.

---

## Component 4: Redis (Session Registry, Presence, Caches)

### 4A — Internal: Redis Primary Node Fails

**Scenario:** Redis primary crashes. Redis Sentinel promotes a replica in ~15–30 seconds.

**Impact during failover (15–30 seconds):**
- New WebSocket connections: auth fails (JWT validation needs Redis session lookup) → fallback to DB auth
- Presence updates: missed (acceptable — presence has 30s TTL anyway)
- seq_num generation: **BLOCKED** — Redis INCR unavailable → **cannot assign seq_nums → messages cannot be written with ordering**

**Mitigation for seq_num critical path:**
1. **Redis Cluster with slot-based sharding:** Each conversation's seq key maps to a specific slot/shard. Failure of one shard affects only conversations in that slot (1/N of traffic)
2. **Fallback seq generation:** On Redis failure, use a **Snowflake-style timestamp-based ID** as fallback seq: `timestamp_ms * 1000 + random(999)`. Not perfectly sequential, but sortable. Flag these messages as "fallback_seq=true".
3. **Sentinel auto-failover:** `min-slaves-to-write=1, min-slaves-max-lag=10` ensures replicas are up-to-date before accepting writes on new primary

---

### 4B — Upstream: Chat Server Loses Redis Connection

**Scenario:** Network blip between Chat Server and Redis → connection lost.

**What happens:**
- Connection registry lookup fails → Delivery Service routes to wrong/null server
- Messages pile up in offline queue unnecessarily

**Mitigation:**
1. **Redis client connection pool with retry:** `redis.go` / `jedis` pools retry on transient failures automatically
2. **Local in-process cache (2-second TTL):** Cache last-known `session:{user_id}` in memory. 2-second stale window is acceptable for routing.
3. **Multiple Redis read replicas:** Route read operations (presence, group membership) to replicas. Reduce load on primary and provide fallback.

---

### 4C — Cache Stampede on Group Membership Miss

**Scenario:** Popular group's membership cache expires. 10,000 concurrent messages arrive simultaneously → all miss cache → 10,000 parallel Cassandra queries for same data.

**Mitigation:**
1. **Redis SETNX locking (mutex):** First miss acquires lock, fetches from DB, sets cache. Others wait (with timeout).
2. **Probabilistic Early Expiration (XFetch):** Refresh cache slightly before expiry based on fetch time and a random factor: `expiry - fetch_time * beta * log(rand())`. Prevents cold cache entirely.
3. **Never-expire for large groups:** Groups with > 1,000 members use write-through cache — membership changes immediately update Redis rather than waiting for TTL expiry.

---

## Component 5: Delivery Service

### 5A — Internal: Delivery Service OOM Crash

**Scenario:** A fan-out storm (celebrity posts in 1M-member group) causes delivery service to exhaust memory and crash.

**Impact:**
- Kafka consumer offset not committed → messages replayed on restart (duplicates possible)
- Fan-out for in-flight messages lost

**Mitigation:**
1. **Back-pressure:** Kafka consumer `max.poll.records=100` — consume small batches. If processing lag > threshold, pause consumption.
2. **Fan-out worker pool with bounded queue:** Use a fixed goroutine pool (e.g., 1,000 workers). If queue full → apply back-pressure upstream → Kafka consumer slows.
3. **Resource limits:** K8s `limits.memory=8Gi`. OOMKilled pod restarts in ~5s, resumes from last committed offset.
4. **Separate fan-out service for large groups:** Route groups >100 members to dedicated "Bulk Fan-out Service" with separate scaling group.

---

### 5B — Downstream: Push Notification Service (APNs/FCM) Fails

**Scenario:** APNs is having an outage. Offline users don't receive push notifications.

**Impact:** Users don't know they have new messages. They only see messages when they manually open the app.

**Mitigation:**
1. **Retry queue with exponential backoff:** Failed notifications enqueued in Redis with TTL=24h, retried every {1, 2, 4, 8, 16, 32} minutes
2. **Notification de-duplication:** Don't send 100 notifications for 100 unread messages. Batch: "You have 10 new messages from 3 conversations"
3. **Fallback: SMS notification** (for critical alerts, opt-in) via Twilio
4. **Graceful degradation:** Users who open app manually get all messages via missed-message catch-up. Core messaging works; only proactive alerting is degraded.

**Circuit breaker for APNs:** After 10 consecutive failures, stop hitting APNs for 60 seconds. Prevents exhausting APNs connection limits and speeds up recovery detection.

---

## Component 6: Message Ordering Under Network Partitions

### 6A — Split-Brain: Two Chat Servers Both Serve Same User

**Scenario:** User's device switches from WiFi to cellular mid-message. For 2 seconds, both connections are "alive" from Redis' perspective. Two messages from other users land on different servers.

**Impact:** Same user receives duplicate messages, potentially out of order.

**Mitigation:**
1. **Device session versioning:** Each connection has a `connection_version` (monotonic). Redis stores latest version. Chat Server validates: if received message's target version < current version → discard (stale connection).
2. **Client-side dedup:** Client tracks received `message_id`s in a local set (last 1,000). Duplicate → silently discard.
3. **Client-side re-ordering buffer:** Client holds messages for 200ms before rendering. Sort by `seq_num`. Out-of-order messages get sorted correctly within this window.

---

### 6B — Message Ordering Across Multiple Devices (Same User)

**Scenario:** User sends message from phone. Opens laptop 10 seconds later. Phone and laptop have different local `last_seq_seen`.

**Mitigation:**
1. **Per-conversation cursor per device:** `ConversationMember.last_read_seq` is per-user, not per-device. Laptop connects, sends `last_seq_seen` map, fetches all missed messages from Cassandra.
2. **Sent messages sync:** Server echoes all messages sent from any device back to all of the sender's connected devices (fan-out includes sender). Phone and laptop both see the message.

---

## Cascading Failure Scenario: The "Viral Post" Storm

**Trigger:** A celebrity with 5M followers sends a message in a public group of 1M members.

**Cascade without mitigation:**
```
1. Message arrives at Chat Server
2. Kafka publish: 1 message, 1 partition — OK
3. Delivery Service reads message: needs to fan-out to 1M members
4. Delivery Service queries Redis for 1M member sessions: ~100ms for 1M lookups
5. 1M gRPC calls to Chat Servers: overwhelms gRPC connection pools
6. Chat Server pods hit CPU/memory limits, start dropping connections
7. Dropped connections → clients reconnect → thundering herd
8. Redis floods with session lookups: Redis CPU spikes, evicts keys
9. Cache misses → DB queries: PostgreSQL connection pool exhausted
10. Entire system degrades for all users
```

**Mitigation chain:**
```
1. Fan-out rate limiter: Max 10K fan-out/sec per message → spread over 100s
2. Large group detection: >100K members → switch to PULL model (clients poll API)
3. Fan-out circuit breaker: If gRPC error rate >20%, pause fan-out, let messages buffer in Kafka
4. Client reconnect jitter: Clients use random(1s, 10s) backoff, not immediate retry
5. Redis read replicas: Session lookups go to replicas, not primary
6. DB connection pooler: PgBouncer limits DB connections to 100, queues the rest
7. Graceful degradation: Large-group messages delivered within 60s (not 100ms), but delivered
```

**Real-world:** Discord's 2017 "What's new" screen outage — a single endpoint became a fan-out bottleneck when triggered by a popular announcement. Fixed by switching large servers to pull model (documented in Discord Engineering Blog).

---

## Observability: What to Monitor

| Metric | Alert Threshold | Meaning |
|---|---|---|
| WebSocket connection count | Drop > 10% in 60s | Mass disconnect (server crash or network issue) |
| Kafka consumer lag (messages topic) | > 100K messages | Delivery Service falling behind |
| Cassandra write latency P99 | > 50ms | Storage bottleneck |
| Redis memory usage | > 80% | Risk of eviction (evict = message loss for cache) |
| Message delivery latency P99 | > 500ms | End-to-end delivery degraded |
| gRPC error rate (internal) | > 1% | Server-to-server routing failures |
| Push notification failure rate | > 5% | APNs/FCM degraded |
| seq_num generation errors | Any | Redis seq counter unavailable |

**Distributed Tracing:** Every message carries a `trace_id` (OpenTelemetry). Trace: `Client → Chat Server → Kafka → Delivery Service → Chat Server B → Client`. P99 breakdown shows which hop is slow.

---

## Chaos Engineering Tests (à la Netflix Chaos Monkey)

| Test | What It Validates |
|---|---|
| Kill 1 Chat Server pod mid-session | Client reconnects, no message loss |
| Kill Kafka leader for messages topic | Producer retries, no duplicates after recovery |
| Network partition: Delivery Service ↔ Redis | Graceful fallback to DB lookup |
| Inject 200ms latency into Cassandra | Circuit breaker opens, Redis cache serves reads |
| Kill Push Notification service | Retry queue works, messages appear on app open |
| Send 100 messages/sec from 1M-member group | Pull model kicks in, system stays stable |

Run these weekly in staging, monthly in production during low-traffic windows.

