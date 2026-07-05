# WhatsApp Failure Analysis
> Date: 2026-07-01 | Failure-first approach: what breaks, what's downstream, how to recover

---

## Component Failure Map

```
Client Device
     │
     ├── [WebSocket Hub]              ← F1
     │         │
     │   [Chat Service]               ← F2
     │         │
     │         ├── [Cassandra]        ← F3
     │         ├── [Redis KDC]        ← F4
     │         └── [Kafka]            ← F5
     │
     ├── [Media Service → S3/CDN]     ← F6, F7
     │
     └── [FCM/APNS]                  ← F8
```

---

## F1: WebSocket Hub Crashes

**Upstream impact:** API Gateway loses the socket connection  
**Downstream impact:** All connected clients on this hub lose real-time delivery

**What the user sees:** "Connecting…" spinner for 30-60s then reconnects

**Detection:** LB health check fails; Presence TTL (120s) expires; no heartbeats from hub

**Prevention:**
- 100+ WS Hub pods across 3 AZs; LB removes unhealthy pod in <10s
- No in-memory state on hub (message routing state is in Redis)

**Recovery flow:**
```
Client: connection drops → exponential backoff reconnect
  delay = min(base_ms × 2^n, 30s) + random(0, 500ms)
  [jitter prevents thundering herd of 50M clients reconnecting simultaneously]

On reconnect:
  1. Client sends {last_received_message_id} in handshake
  2. Chat Service: SELECT * FROM offline_messages WHERE user_id = ? AND message_id > last_received
  3. Delivers missed messages → client ACKs each
  4. Normal operation resumes
```

**Cascade risk:** If ALL hubs down → 500M active users → 500M FCM/APNS pushes simultaneously → Google/Apple rate-limit → push delivery degrades. But no data loss (offline_messages queue is durable).

---

## F2: Chat Service Pod Crashes (1 of N)

**Impact:** ~1/N requests get 502; LB marks pod unhealthy

**Prevention:**
- 200+ pods; LB health check interval = 5s, unhealthy threshold = 2 checks
- Pod removed from pool in <10s

**Recovery:**
```
Client: received 502 → retry with idempotency_key (same key = no duplicate)
  POST /message {idempotency_key: uuid, ciphertext: ...}
  
Cassandra write: IF NOT EXISTS on idempotency_key → exactly-once insert
```

**What must be idempotent:** Every message write uses the client-generated message_id (TIMEUUID) as the Cassandra partition key. Duplicate insert of same TIMEUUID is a no-op (LWT: IF NOT EXISTS) or simply overwrites identical data.

**Pod restart time:** k8s pod = ~30s. No manual intervention.

---

## F3: Cassandra Node Failure

### Case A: 1 of 30 nodes fails (RF=3, W=QUORUM, R=QUORUM)
```
30 nodes, RF=3 → each partition on 3 nodes (1 per AZ)
QUORUM = ceil(3/2)+1 = 2 nodes needed

Node 2 down:
  Write: nodes 1 + 3 → CL=QUORUM satisfied ✓
  Read:  nodes 1 + 3 → CL=QUORUM satisfied ✓
  
Node 2 recovers:
  Coordinator stored "hints" for writes during downtime
  Hinted handoff replays writes to node 2 (default window: 3h)
  Merkle tree anti-entropy repair for data older than 3h window
```
**User impact: zero.**

### Case B: 2 replicas in same group fail simultaneously
```
Nodes 2 + 3 down (same replica group for some partitions):
  Write: only node 1 → CL=QUORUM fails (1 < 2)
  
Impact: affected chat_ids → Chat Service returns 503
User sees: message stays as "Sending…"
```

**Prevention:**
- Spread replicas across 3 AZs (1 replica per AZ): to lose 2 replicas requires 2 AZs to fail
- Monitor node health; auto-replace in ASG within 15 min

**Recovery:**
- Cassandra bootstraps replacement node: streams data from remaining 1-2 replicas
- Repair time: ~2-4 hours for a full node's data
- Application: client retries with 30s timeout; messages queued locally

### Case C: Full datacenter loss (all AZs in one region)
**Pre-requisite:** Cross-region Cassandra replication with LOCAL_QUORUM
```
PRIMARY: us-east-1  (RF=3, 30 nodes)
BACKUP:  eu-west-1  (RF=3, 15 nodes)
Replication: async (LOCAL_QUORUM on write → replicates async to eu)

On us-east-1 failure:
  DNS failover to eu-west-1 (TTL 60s)
  RPO: up to 60s of messages lost (async replication lag)
  RTO: 2-5 minutes (DNS propagation + health checks)
```

---

## F4: Redis KDC Fails

**Impact:** New E2E session setup fails (X3DH key exchange)  
**Existing sessions:** Unaffected — Double Ratchet state is on device, not in Redis

**What breaks:** First-time messages between users who have never chatted

**Prevention:**
- Redis Sentinel (1 primary + 2 replicas): auto-failover in ~30s
- DynamoDB as durable backup for all prekeys

**Fallback:**
```
KDC reads:
  1. Try Redis → 3ms timeout → circuit breaker trips on 5 consecutive failures
  2. Fall back to DynamoDB → ~5ms (vs 0.5ms Redis) — acceptable
  3. Circuit breaker → HALF-OPEN after 30s probe
  4. Redis recovered? → CLOSED, back to normal

User impact: <30s window where new chats take slightly longer to start
No message loss.
```

**One-time prekey exhaustion (separate failure mode):**
```
User A is popular; all of Bob's one-time prekeys consumed
GET /keys/bob returns: {identity_key, signed_prekey, no one_time_prekey}
  → X3DH runs without DH4: slightly weaker but still secure
  → Alert sent to Bob's device: "Upload more prekeys"
  → Bob's app uploads batch of 100 new prekeys in background
```

---

## F5: Kafka Cluster Degraded

**Impact:** Delivery pipeline stalls; messages stored in Cassandra but not pushed to WebSocket Hubs or FCM

**Prevention:**
- 12 brokers across 3 AZs (RF=3, min.insync.replicas=2)
- Producers: acks=all (wait for 2 replicas before ACK)

**Timeline of failure:**
```
t=0:   Kafka broker crashes
t=30s: Kafka leader election (ZooKeeper/KRaft)
t=60s: Producers can't get ACK → Chat Service buffers (32MB/partition)
t=90s: Buffer full → Chat Service returns 503 to clients
t=...: Client retries with backoff + idempotency key
```

**Recovery:**
```
Kafka broker recovers:
  1. New leader elected (or crashed broker replaced)
  2. Producers flush buffered messages
  3. Consumers (Delivery Service) resume from last committed offset
  4. Ordering: Kafka partition preserves order → messages delivered in send order
  5. Duplicates: Delivery Service is idempotent (checks message_id before push)
```

**Fallback path (if Kafka fully down >2min):**
```
Chat Service emergency bypass:
  Write to Cassandra (primary) ✓
  Direct gRPC call to WS Hub (skip Kafka, synchronous) → deliver if online
  If offline: offline_messages written directly
  
Tradeoff: Fan-out logic in Chat Service → couples services → only as emergency bypass
```

---

## F6: Media Service Fails

**Impact:** New media uploads fail. Already-uploaded media: served via CDN (unaffected).

**Prevention:** 3+ Media Service pods, stateless (S3 is the durable state).

**Resumable upload protocol:**
```
Client splits file into 5MB chunks
For each chunk:
  POST /media/upload {file_hash, chunk_number, total_chunks, data}
  Server: write chunk to S3 multipart upload
  Returns: {chunk_received: true}

On pod failure mid-upload:
  Client retries same chunk (idempotent — same chunk_number)
  New pod picks up: checks S3 which parts already uploaded
  Resumes from last successful chunk
  
User sees: upload progress bar pauses, then resumes after retry
```

---

## F7: S3 Regional Outage

**Impact:** New media uploads fail; media NOT in CDN: 404 or timeout

**What still works:**
- Text messages: completely unaffected (no media path involved)
- Cached media: CloudFront serves from cache (TTL 24h for popular, 7d for less popular)
- Stale-if-error: `Cache-Control: stale-if-error=86400` → CDN serves stale content for 24h during S3 outage

**Media not in CDN:**
- User sees spinner then "Media unavailable" with retry button
- Message itself delivered (text/ciphertext envelope received)
- Media download retried on next app open

**Recovery:**
- S3 Cross-Region Replication: failover to replica bucket in secondary region
- Update CloudFront origin to secondary bucket (takes 15-30min to propagate)
- Or: update presigned URL generator to point to replica bucket

---

## F8: FCM/APNS Outage

**Impact:** Offline users don't receive push notifications → don't know to open app

**What still works:**
- Online users: WebSocket delivery unaffected
- Messages: stored in offline_messages queue (Cassandra, TTL 30d)
- All messages delivered when user opens app (WebSocket connects → pulls offline queue)

**No data loss.** Degraded UX only.

**Provider failover:**
```
Push pipeline:
  1. Try FCM (Android + iOS) → timeout 5s → if fail:
  2. Try APNS directly (iOS only)
  3. Log failed delivery → retry queue with backoff (1min, 5min, 30min)
  
SMS fallback: only for account security codes, NOT chat messages
(SMS is expensive and plaintext — defeats E2E encryption goal)
```

---

## F9: AZ-Level Network Partition

**Scenario:** us-east-1b isolated from us-east-1a and us-east-1c

**Impact per service:**
| Service | Impact | Recovery |
|---------|--------|----------|
| Cassandra (RF=3, 1 per AZ) | 1b nodes partitioned → still quorum (2 of 3 AZs) | No writes lost |
| Chat Service pods | 1/3 pods unreachable → LB redirects (health checks) | <10s |
| WebSocket Hubs | 1/3 hubs: clients reconnect to 1a or 1c hubs | 30-60s reconnect |
| Redis Sentinel | Sentinel in 1b isolated → 1a+1c elect new primary | ~30s failover |
| Kafka | 1b brokers partitioned → leaders elect in 1a/1c | ~30s |

**Design principles:**
- All stateful services: Multi-AZ with odd number of replicas (3 or 5)
- No quorum requires 1b specifically → partition doesn't halt service
- Stateless services: LB health checks reroute automatically

---

## F10: Clock Skew Between Servers

**Problem:** Server A's clock is 500ms ahead of Server B
```
Alice sends at t=1000 (Server B): message_id TIMEUUID encodes 1000
Bob   sends at t=999  (Server A): message_id TIMEUUID encodes 1499
Display: Bob's message appears AFTER Alice's even though Bob replied first
```

**Fix:**
1. **NTP sync** on all servers: enforce max skew = 250ms; alert + restart if exceeded
2. **TIMEUUID clock sequence**: when two events happen in same ms on same server, clock sequence breaks tie (deterministic ordering)
3. **Client-side timestamp for display**: show user's local clock for their own sent messages (best UX); server TIMEUUID for deduplication only
4. **Hybrid Logical Clock (HLC)**: `HLC = max(wallClock, received_max_HLC) + 1` — monotonically advances, captures causality

---

## F11: Message Delivered But Shows Wrong Status (Race Condition)

**Scenario:** Delivery ACK and Read receipt arrive out of order
```
1. Bob's device sends read_receipt (opened chat → blue ✓✓)
2. Bob's device sends delivery_receipt (from background queue)
3. Alice sees: blue ✓✓ then gray ✓✓ (regression!)
```

**Fix:** Status transitions are monotonically increasing:
```python
# Server-side: only update if new status > current status
status_rank = {'sent': 0, 'delivered': 1, 'read': 2}

UPDATE message_status
SET status = new_status, updated_at = now()
WHERE message_id = ? AND recipient_id = ?
  AND status_rank[status] < status_rank[new_status]
```
Out-of-order receipts are silently dropped (idempotent, rank-gated).

---

## Failure Priority Summary

| # | Component | Failure Rate | User Impact | Recovery Time | Action |
|---|-----------|-------------|-------------|---------------|--------|
| F3c | Cassandra 2-node | Very rare | High: chat_ids affected | 30min | CRITICAL: multi-AZ, RF=3 |
| F5 | Kafka full outage | Rare | High: no delivery | 5-15min | HIGH: 12-broker cluster, RF=3 |
| F1 | WebSocket Hub | Occasional | Medium: 30-60s reconnect | 30s | HIGH: 100+ pods, jitter reconnect |
| F7 | S3 regional | Rare | Medium: media unavailable | 2-4h | MEDIUM: CRR + CDN stale-if-error |
| F4 | Redis KDC | Occasional | Low: DynamoDB fallback | 30s | MEDIUM: Sentinel + DynamoDB |
| F8 | FCM/APNS | Rare | Low: no push, queue intact | Hours | MEDIUM: provider failover |
| F2 | Chat Service pod | Common | Low: LB reroutes | 10s | LOW: k8s auto-heal |
| F3a | Cassandra 1-node | Occasional | None: quorum works | 2-4h | LOW: monitor, auto-replace |
