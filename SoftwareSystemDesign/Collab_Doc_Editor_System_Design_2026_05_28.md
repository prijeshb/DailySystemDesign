# Collaborative Document Editor — System Design
**Date:** 2026-05-28 | **Difficulty:** Hard | **Companies:** Google, Notion, Dropbox, Microsoft

---

## 1. First Principles — Do We Even Need This?

> *"Do we really need real-time collaboration, or is 'last-write-wins' enough?"*

- **Problem:** Multiple users edit the same document simultaneously. Simple locking is too coarse (blocks everyone). Last-write-wins causes data loss.
- **Core insight:** We need *convergence* — all clients eventually reach the same state, regardless of edit order, without losing anyone's work.
- **Why not a DB transaction per keystroke?** ~10 ops/sec per user × 1M users = 10M TPS. Not feasible for ACID transactions.
- **Conclusion:** We need a conflict-resolution algorithm at the application layer + event-driven sync, not DB-level locking.

---

## 2. Entities

| Entity | Key Attributes |
|--------|---------------|
| **User** | user_id, name, email, sessions[] |
| **Document** | doc_id, owner_id, title, created_at, version |
| **DocumentContent** | doc_id, snapshot (blob), version |
| **Operation** | op_id, doc_id, user_id, type (insert/delete/retain), position, content, timestamp, version |
| **Revision** | rev_id, doc_id, operations[], base_version, created_at |
| **Collaborator** | doc_id, user_id, permission (view/comment/edit), cursor_position |
| **Comment** | comment_id, doc_id, range, text, author_id, resolved |
| **Session** | session_id, user_id, doc_id, ws_connection_id, last_seen |

---

## 3. Actions / APIs

### REST APIs
```
POST   /docs                    → create document
GET    /docs/{doc_id}           → fetch doc + latest snapshot
GET    /docs/{doc_id}/revisions → list revision history
POST   /docs/{doc_id}/share     → add collaborator
GET    /docs/{doc_id}/comments  → fetch comments
POST   /docs/{doc_id}/comments  → add comment
GET    /docs/{doc_id}/export    → export as PDF/DOCX
```

### WebSocket Events (real-time channel)
```
CLIENT → SERVER:
  { type: "op",      doc_id, ops: [Op], base_version }
  { type: "cursor",  doc_id, position, selection }
  { type: "join",    doc_id }
  { type: "leave",   doc_id }

SERVER → CLIENT:
  { type: "op_ack",     op_id, server_version }
  { type: "op_remote",  ops: [Op], user_id, server_version }
  { type: "cursor",     user_id, position, color }
  { type: "presence",   users: [{ id, name, color, cursor }] }
  { type: "snapshot",   content, version }    ← on join or reconnect
```

---

## 4. Data Flow

### Happy Path — User Types a Character

```
User keystroke
  → Client buffers op locally (optimistic apply)
  → Client sends op over WebSocket: { ops, base_version }
  → WS Gateway routes to Doc Session Service (by doc_id)
  → Doc Session Service:
      1. Receives op
      2. Transform op against any concurrent ops (OT or CRDT)
      3. Assigns server_version (monotonic)
      4. Publishes transformed op to Pub/Sub topic: doc-{doc_id}
      5. ACKs to sender: { op_id, server_version }
  → Pub/Sub fans out to all other WS connections for doc_id
  → Other clients receive op_remote, apply transform, update UI
  → Async: Op persisted to Revision Store (Cassandra)
  → Async: Every N ops or T seconds → checkpoint snapshot to Blob Store
```

### User Opens Document

```
GET /docs/{doc_id}
  → CDN cache hit? → return snapshot + version
  → Cache miss → Doc Service fetches latest snapshot from Blob Store
  → WebSocket: client joins doc room, receives presence list
```

---

## 5. High-Level Design

```
┌────────────────────────────────────────────────────────────┐
│                        Clients                             │
│         Browser / Mobile / Desktop (OT client lib)         │
└───────────────┬────────────────────────┬───────────────────┘
                │ HTTPS (REST)           │ WebSocket
        ┌───────▼──────┐        ┌────────▼────────┐
        │   API Gateway │        │   WS Gateway    │
        │  (auth, rate  │        │  (sticky routes │
        │   limiting)   │        │   by doc_id)    │
        └───────┬───────┘        └────────┬────────┘
                │                         │
       ┌────────▼─────────────────────────▼────────┐
       │             Doc Session Service            │
       │  (OT engine, version manager, presence)   │
       └──────┬──────────────┬──────────────────────┘
              │              │
     ┌────────▼───┐   ┌──────▼──────┐
     │  Pub/Sub   │   │  Op Buffer  │
     │  (Kafka)   │   │  (Redis)    │
     └────────┬───┘   └─────────────┘
              │
   ┌──────────▼──────────────────────────┐
   │           Storage Layer             │
   │  ┌──────────────┐  ┌─────────────┐ │
   │  │  Revision DB │  │ Blob Store  │ │
   │  │  (Cassandra) │  │  (S3/GCS)   │ │
   │  └──────────────┘  └─────────────┘ │
   │  ┌──────────────┐                  │
   │  │  Metadata DB │                  │
   │  │  (PostgreSQL)│                  │
   │  └──────────────┘                  │
   └─────────────────────────────────────┘
```

---

## 6. Low-Level Design

### 6.1 Conflict Resolution — Operational Transformation (OT)

**The core problem:** User A inserts "X" at position 5. Simultaneously, User B deletes character at position 3. Both ops are based on version 10. Server receives A's op first.

```
Op from A: Insert("X", pos=5, base_v=10)
Op from B: Delete(pos=3, base_v=10)

Server applies A first → doc version = 11
B's op must be TRANSFORMED against A's op before applying:
  → A inserted before pos 3? No (pos 5 > 3)
  → B's Delete stays at pos=3 (no shift needed)
  → transformed B: Delete(pos=3, base_v=11) ✓

Server applies transformed B → doc version = 12
Server sends transformed B to A's client → A applies it → same state
```

**OT Rules:**
- `insert(p1)` vs `insert(p2)`: if p1 ≤ p2, shift p2 right by 1
- `insert(p1)` vs `delete(p2)`: if p1 ≤ p2, shift p2 right by 1; if p1 > p2, no change
- `delete(p1)` vs `delete(p2)`: if p1 < p2, shift p2 left by 1; if p1 = p2, cancel second delete

**Trade-off: OT vs CRDT**

| Aspect | OT | CRDT |
|--------|-----|------|
| Complexity | Transform fn per op pair | Complex data structures |
| Server role | Central transform server | Peer-to-peer possible |
| Memory | Low (ops only) | Higher (tombstones) |
| Convergence | Guaranteed with correct transform | Guaranteed by design |
| Used by | Google Docs | Figma, Notion |

> **Decision:** Use OT with a central server. Simpler to reason about, avoids tombstone bloat for text docs, Google Docs proven this model at scale.

### 6.2 Doc Session Service (Stateful)

Each document has exactly **one active session node** (via consistent hashing on doc_id). This node:
- Maintains in-memory op queue (Redis-backed for durability)
- Tracks current version counter
- Holds presence state (cursors, active users)
- Performs OT transformations

```python
class DocSession:
    doc_id: str
    version: int          # monotonic counter
    pending_ops: deque    # ops received but not yet persisted
    snapshot_version: int # version of last persisted snapshot
    active_users: dict    # user_id → cursor/selection

    def apply_op(self, op: Op) -> Op:
        # Transform against all ops since op.base_version
        concurrent = self.get_ops_since(op.base_version)
        transformed = op
        for existing_op in concurrent:
            transformed = ot_transform(transformed, existing_op)
        transformed.version = self.version + 1
        self.version += 1
        self.pending_ops.append(transformed)
        return transformed
```

**Trade-off: Stateful vs Stateless session service**
- Stateful: fast in-memory OT, lower latency. Risk: node failure loses state.
- Stateless: each op loads history from DB. Simple but ~10-50ms added latency per op.
- **Decision:** Stateful + Redis op buffer (durability). On node failure, new node replays from Redis.

### 6.3 Persistence Strategy

**Problem:** Can't write every keystroke to a relational DB.

**Solution: Two-tier persistence**
1. **Hot tier (Redis):** Last 1000 ops per document, TTL 24h. Instant replay for reconnects.
2. **Cold tier (Cassandra):** Append-only revisions table, partitioned by doc_id.

```sql
-- Cassandra schema
CREATE TABLE revisions (
    doc_id   UUID,
    version  BIGINT,
    op_type  TEXT,         -- insert | delete | retain
    position INT,
    content  TEXT,
    user_id  UUID,
    ts       TIMESTAMP,
    PRIMARY KEY (doc_id, version)
) WITH CLUSTERING ORDER BY (version ASC);
```

**Snapshotting** (avoid replaying 10M ops):
- Every 500 ops OR 10 min → serialize full doc to JSON → upload to S3
- Store `(doc_id, snapshot_version, s3_key)` in metadata DB
- On open: fetch latest snapshot + replay only ops after snapshot_version

**Trade-off: Snapshot frequency**
- More frequent: faster load time, more S3 writes, higher cost
- Less frequent: slower load, cheaper storage
- **Decision:** Every 500 ops or 10 min, whichever comes first. Balanced for typical doc session.

### 6.4 WebSocket Gateway — Sticky Routing

```
Client connects → WS Gateway assigns connection_id
WS Gateway hashes doc_id → routes to correct Session Server
  (consistent hash ring, same node handles all ops for doc_id)

Session Server subscribes to Kafka topic: doc-{doc_id}
  → any op broadcast reaches all WS connections via Pub/Sub
```

**Trade-off: Sticky routing vs stateless routing**
- Sticky: one node does OT (no cross-node coordination). Requires session affinity.
- Stateless: any node handles request, must coordinate via distributed lock.
- **Decision:** Sticky routing with consistent hashing. Simpler, faster, proven in Docs.

### 6.5 Presence (Cursors)

- Stored in-memory on Session Service + Redis TTL (5s)
- Broadcast via Kafka on every cursor move (throttled to 100ms debounce)
- Each user assigned a color from a deterministic hash of user_id
- Not persisted — presence is ephemeral

### 6.6 Offline Editing & Sync

- Client queues ops locally when offline (IndexedDB / localStorage)
- On reconnect: sends base_version + all queued ops
- Server transforms entire batch, sends back transformed ops + current snapshot if too stale
- Client replays: discard local pending ops, apply server snapshot, re-apply user edits on top

---

## 7. Scaling

| Concern | Solution |
|---------|---------|
| 10M concurrent users | WS Gateway horizontal scale (L4 LB, sticky by doc_id) |
| Hot documents (shared widely) | Read replicas for snapshot; presence fan-out capped at 50 active editors |
| Revision storage growth | Cassandra TTL on old revisions (keep 30 days hot), archive to S3 Glacier |
| Session service bottleneck | Each node handles ~5K concurrent doc sessions; scale out with consistent hashing |
| Large documents | Chunk document into segments (~64KB each); shard ops by segment |

---

## 8. Caching Strategy

| Layer | What | Cache | TTL |
|-------|------|-------|-----|
| CDN | Document snapshot (read-only view) | CloudFront | 60s |
| App | Document metadata (title, permissions) | Redis | 5 min |
| App | Recent ops buffer | Redis | 24h |
| DB | Snapshot S3 key lookup | In-memory | 1 min |

> **Trade-off:** CDN caching snapshot reduces read load 10x but viewers may see 60s stale content. Editors bypass CDN and use WebSocket for live state.

---

## 9. Key Design Decisions Summary

| Decision | Choice | Why |
|----------|--------|-----|
| Conflict resolution | OT (server-side) | Simpler than CRDT for linear text, proven at scale |
| Session routing | Sticky consistent hashing | Single-node OT avoids distributed coordination |
| Persistence | Cassandra append-only + S3 snapshots | High write throughput, cheap bulk storage |
| Real-time transport | WebSocket over SSE | Bidirectional needed (ops go both ways) |
| Presence | In-memory + Redis TTL | Ephemeral, no value in persisting |
