# Collaborative Document Editor — Failure Analysis
**Date:** 2026-05-28 | Companion to: `Collab_Doc_Editor_System_Design_2026_05_28.md`

---

## Failure-First Thinking Framework

For each component ask:
1. What if **this component fails**?
2. What if the component **ahead** (upstream) fails?
3. What if the component **behind** (downstream) fails?

---

## Component 1: WebSocket Gateway

### If WS Gateway itself fails
- **Impact:** All connected clients lose real-time connection. Document edits stop propagating.
- **Detection:** Health check fails, LB removes node from pool.
- **Prevention:**
  - Multiple WS Gateway instances behind L4 LB
  - Client-side reconnect with exponential backoff (1s → 2s → 4s → max 30s)
  - Client queues ops locally during disconnect (IndexedDB)
- **Recovery:**
  - Client reconnects to another gateway node
  - Sends queued ops with base_version to new session
  - Server transforms batch and returns current state
- **Residual risk:** Brief inconsistency window during reconnect (~1-5s). Acceptable.

### If upstream (client network) fails
- **Symptom:** Client sends op, ACK never arrives.
- **Solution:** Client retries op with same op_id (idempotency). Server deduplicates by op_id.
- **Idempotency key:** `op_id = hash(user_id + doc_id + local_timestamp + content)`

### If downstream (Session Service) fails
- **Symptom:** WS Gateway receives op but can't forward it.
- **Solution:** Gateway buffers op in local queue (max 10s), retries. If session recovers within window, op delivered. Else, client gets error and retries.

---

## Component 2: Doc Session Service (Stateful Node)

### If Session Service node crashes (most critical failure)
- **Impact:** All active editors on documents hosted by this node lose connection.
- **State at risk:** In-memory op queue, current version counter, presence data.
- **Detection:** Kafka consumer heartbeat missed, ZooKeeper/etcd session expired.
- **Prevention:**
  - **Redis op buffer:** Every applied op written to Redis before ACK sent. On crash, successor replays from Redis.
  - **Version fencing:** New node reads last committed version from Cassandra + pending ops from Redis. Recomputes document state.
- **Recovery flow:**
  ```
  1. Health check fails → removed from consistent hash ring
  2. Other session nodes pick up orphaned doc_ids (re-hash)
  3. New node for doc_id:
     a. Loads latest snapshot from S3
     b. Replays ops from Cassandra since snapshot_version
     c. Replays pending ops from Redis (TTL 24h)
     d. Sets version counter = max(recovered version)
  4. WS Gateway reconnects affected clients to new node
  5. Clients re-send unACKed ops (idempotent)
  ```
- **Recovery time:** ~5-10s (snapshot load + op replay). Users see brief freeze then resume.

### If upstream (WS Gateway) loses connection to Session Service
- **Symptom:** Gateway can't route ops to session node.
- **Solution:** Gateway marks session node as unavailable, triggers re-hash. Same as node crash.

### If downstream (Kafka publish) fails
- **Symptom:** Op applied but not broadcast to other editors.
- **Solution:**
  - Retry Kafka publish up to 3× with 100ms backoff.
  - If Kafka unavailable: fallback to direct Redis Pub/Sub for fan-out (degraded but functional).
  - If Redis also down: session node directly pushes to WS Gateway (last resort, O(n) connections).

---

## Component 3: Kafka (Pub/Sub)

### If Kafka broker fails
- **Impact:** Op fan-out to other editors stops. Each editor only sees their own ops.
- **Prevention:**
  - Replication factor = 3, min.insync.replicas = 2
  - Topic partitioned by doc_id (all ops for same doc on same partition, preserving order)
- **Recovery:** Leader election among ISR replicas, ~10-30s downtime.
- **Client experience:** Editors see stale view of others' cursors, no remote ops. Their own ops still apply locally.
- **Fallback:** Session node buffers ops in memory during Kafka outage, flushes when Kafka recovers.

### If Kafka is slow (high latency)
- **Impact:** Cursor updates and remote ops arrive late → laggy collaboration feel.
- **Solution:** Separate topics for high-priority ops vs cursor updates. Cursor topic can be lossy (UDP-like).
- **Trade-off:** Two topic categories add complexity but cursor lag is far more noticeable than occasional cursor drop.

---

## Component 4: Cassandra (Revision Store)

### If Cassandra node fails
- **Impact:** New revisions can't be persisted. Existing data still readable from replicas.
- **Replication:** RF=3, QUORUM writes/reads. Tolerates 1 node failure with no data loss.
- **Prevention:** Write to Redis op buffer first, persist to Cassandra async. If Cassandra down, ops accumulate in Redis.
- **Risk:** If Cassandra down >24h AND Redis TTL expires → op data loss. Mitigate with Redis TTL = 48h for op buffer.

### If Cassandra write is slow
- **Impact:** Op buffer grows, memory pressure on Session Service.
- **Solution:** Apply backpressure — if buffer > 5000 ops, throttle new op ingestion (429 to client).
- **Client experience:** Typing feels laggy. Better than data loss.

### If revision data corrupted
- **Detection:** Checksum mismatch on read (op content hash).
- **Recovery:** Restore from S3 snapshot at last known good version + replay clean ops from another replica.

---

## Component 5: Blob Store / S3 (Snapshots)

### If S3 is unavailable
- **Impact:** Cannot load document snapshot on open. Cannot checkpoint new snapshots.
- **Document open:** Fall back to loading from Cassandra (replay all revisions). Slow for old docs.
- **Snapshotting:** Session Service continues running, skips checkpoint. Ops accumulate in Cassandra. Resume checkpointing when S3 recovers.
- **Risk:** Very old documents with >10K revisions will have slow load times during S3 outage.

### If snapshot is corrupted
- **Detection:** JSON parse failure or checksum mismatch.
- **Recovery:** Load previous snapshot version (we keep last 5 snapshots per doc).

---

## Component 6: Metadata DB (PostgreSQL)

### If PostgreSQL fails
- **Impact:** Cannot create new documents, look up permissions, add collaborators.
- **Existing sessions unaffected** (they don't re-check permissions per op).
- **Prevention:** Primary + 2 read replicas, automatic failover (Patroni/RDS Multi-AZ). RTO ~30s.
- **Read replica for permission checks:** Cache permissions in Redis (5 min TTL) so permission checks survive short DB outage.

---

## Component 7: Redis (Op Buffer + Cache)

### If Redis fails
- **Impact:** Op buffer lost (unACKed ops), permission cache gone, presence data gone.
- **Prevention:** Redis Sentinel or Redis Cluster with AOF persistence.
- **Op buffer loss:** Clients must re-send all unACKed ops on reconnect. Session node replays from Cassandra for recent ops. Brief state divergence possible.
- **Cache miss storm:** On Redis recovery, all permission checks hit PostgreSQL simultaneously. Use circuit breaker + gradual cache warm-up.

---

## Cascading Failure Scenarios

### Scenario: Traffic spike (document goes viral — shared to 10K editors)
- **Failure chain:** Single Session Service node overloaded → op queue backs up → OT latency spikes → clients timeout → mass reconnect → thundering herd
- **Prevention:**
  - Cap active simultaneous editors per document (e.g., 50 edit, unlimited view)
  - Viewers receive snapshots + SSE (not WebSocket), reducing Session Service load
  - Rate limit join requests during spike

### Scenario: Network partition between WS Gateway and Session Service
- **Failure:** Gateway thinks session node is dead, re-routes to new node. Old node still running, receiving no ops but potentially sending stale broadcasts.
- **Prevention:** Fencing token (epoch number). New session node increments epoch. Old node's Kafka messages have stale epoch and are rejected by clients.
- **This is a split-brain scenario** — the epoch/fencing approach prevents two nodes claiming authority over same document.

### Scenario: Op applied but Cassandra write fails (partial persistence)
- **Failure:** Op ACKed to client, broadcast to others, but never persisted.
- **Impact:** After all clients disconnect and Redis TTL expires, op is lost. On reopen, document missing those ops.
- **Prevention:**
  - Don't ACK op until Redis write confirmed (synchronous).
  - Cassandra write is async but retried indefinitely via Kafka consumer group.
  - If Cassandra fails permanently, at minimum Redis holds ops for 48h for human recovery.

---

## Idempotency Throughout

| Operation | Idempotency Key | How |
|-----------|----------------|-----|
| Submit op | op_id (client-generated UUID) | Server deduplicates by op_id in Redis set (TTL 1h) |
| Snapshot write | doc_id + version | S3 key includes version; overwrite is safe (same content) |
| Revision insert | (doc_id, version) | Cassandra INSERT IF NOT EXISTS |
| ACK re-delivery | op_id | Client ignores duplicate ACKs (idempotent state update) |

---

## Monitoring & Alerts

| Metric | Threshold | Alert |
|--------|-----------|-------|
| Op end-to-end latency (p99) | > 500ms | PagerDuty |
| Session node op queue depth | > 1000 | Warning |
| Redis memory usage | > 80% | Warning |
| WS connection errors/min | > 1000 | PagerDuty |
| Cassandra write latency p99 | > 200ms | Warning |
| Snapshot age for active docs | > 30 min | Warning |
| Kafka consumer lag | > 10K msgs | PagerDuty |
