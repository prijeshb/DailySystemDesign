# Collaborative Document Editor — Interview Q&A
**Date:** 2026-05-28 | Companion to: `Collab_Doc_Editor_System_Design_2026_05_28.md`

---

## Opening Questions (Scoping)

**Q: Design a collaborative document editor like Google Docs.**

> Interviewer probes your scoping instincts first. Before designing, clarify:

**You should ask:**
- Real-time (multi-user simultaneous editing) or async (last-write-wins)?
- Text only, or rich text (images, tables, formulas)?
- How many concurrent editors per document? Concurrent documents?
- Offline editing needed?
- Version history required?

**Sample scope answer:** "I'll design for real-time collaborative text editing, up to 50 simultaneous editors per doc, 10M total users, with version history and offline support. I'll focus on the core editing pipeline and touch on comments and sharing."

---

## Round 1: Core Concept Questions

**Q: How do you handle two users editing the same position simultaneously?**

> This is the central question. If you answer "last-write-wins" the interview ends.

**A:** "We need a conflict resolution algorithm. Two main options: Operational Transformation (OT) and CRDTs.

With OT, when two concurrent ops arrive based on the same version, we transform one against the other before applying. For example, if User A inserts 'X' at position 5 and User B deletes position 3, both at version 10 — we apply A first, then transform B's delete to account for A's insertion. The key property is convergence: all clients reach the same state regardless of arrival order.

I'd choose OT with a central transform server. It's simpler to reason about for linear text, has lower memory overhead than CRDTs (no tombstones), and is proven at Google Docs' scale."

---

**Q: Why do you need a central server for OT? Can't clients do it peer-to-peer?**

**A:** "OT requires a total order of operations — a canonical sequence that all clients agree on. A central server is the simplest way to establish this order (assign monotonic version numbers). Without a central authority, you need vector clocks and more complex multi-party OT, which gets exponentially harder. P2P OT has correctness bugs in practice.

CRDTs can work peer-to-peer because they're designed for commutativity and associativity by construction — order doesn't matter. But for a text editor, CRDT implementations carry deleted characters (tombstones) forever, which grows memory unboundedly for long-lived documents."

---

**Q: Walk me through what happens when a user types a character.**

**A:** "The client creates an op: `{type: insert, position: 42, char: 'a', base_version: 157}`. It applies the op locally immediately (optimistic update — the user sees their character instantly). The op is sent over a WebSocket to the session server.

The session server checks: has anything been committed since version 157? If yes, it transforms this op against those newer ops. Then assigns version 158, writes to Redis, broadcasts to all other connected clients via Kafka, and ACKs to the sender with version 158.

Other clients receive the op, transform it against their own pending ops, and apply it. Everyone converges to the same document state."

---

**Q: What is the 'base_version' and why does the client send it?**

**A:** "The base_version tells the server: 'this op was created when I had seen ops up to version N.' The server knows which ops were committed between N and the current version — those are concurrent with the client's op and need to be accounted for in the transform. Without base_version, the server can't know how much has changed since the client last synchronized."

---

## Round 2: Scalability Questions

**Q: Your session service is stateful. How do you scale it?**

**A:** "Each document session lives on exactly one node, determined by consistent hashing on doc_id. Nodes are part of a hash ring. Adding nodes migrates only ~1/N of documents (minimal disruption).

Each session node handles ~5,000 active document sessions. For 1M active documents, we need ~200 nodes. Each node is independent — no cross-node coordination for OT (that's the beauty of sticky routing).

The WS Gateway maintains a lookup of doc_id → session node and routes accordingly. If a node fails, consistent hashing reassigns its doc_ids to neighbors, which reconstruct state from Redis + Cassandra."

---

**Q: What if a very popular document has 50,000 simultaneous viewers?**

**A:** "Two strategies:

First, separate editors from viewers. Editors need the full real-time WebSocket pipeline. Viewers can use SSE (Server-Sent Events) which is unidirectional and cheaper. Viewers receive snapshot + incremental patches, not the full OT pipeline.

Second, limit active simultaneous editors (e.g., 50). Beyond that, users get viewer mode with a queue to become editors.

For the session service, a document with 50K viewers doesn't require 50K WS connections to the session node. The WS Gateway subscribes to Kafka on behalf of viewers, fan-out happens at the Gateway layer, not the Session layer."

---

**Q: How does your storage scale? Documents grow over time.**

**A:** "Two approaches:
1. **Snapshotting:** Every 500 ops, serialize the full doc to S3. On open, load snapshot + replay only ops since then. Even with millions of ops historically, load time is O(ops since last snapshot).
2. **Segmenting large docs:** For very large documents (e.g., a book), split into 64KB segments. Each segment is independently edited and snapshotted. Cross-segment ops are rare (only cursor movements crossing boundaries).

Cassandra stores ops with doc_id as partition key. Old ops (>30 days) are compacted and archived to S3 Glacier. Cassandra row TTL handles automatic cleanup."

---

## Round 3: Failure Questions

**Q: Your session node crashes with 200 active users in a document. What happens?**

**A:** "Before any op is ACKed, it's written to Redis. So the in-memory state can be reconstructed.

The recovery sequence: 1) Health check detects failure in ~5s. 2) Consistent hash ring updates — doc_id reassigned to a new node. 3) New node loads snapshot from S3 + replays Cassandra revisions since snapshot + replays pending ops from Redis (TTL 48h). 4) WS Gateway reconnects clients to new node. 5) Clients re-send any unACKed ops (idempotent via op_id).

Total downtime per document: ~5-10 seconds. Users see a brief freeze, then editing resumes. Version continuity is maintained."

---

**Q: What if the user is offline and edits for 30 minutes, then reconnects?**

**A:** "The client stores all ops in IndexedDB while offline. On reconnect:
1. Client sends `{type: sync, base_version: 157, ops: [op1...opN]}`
2. Server checks: is base_version 157 still in Redis/Cassandra? If yes, transform the batch.
3. If 157 is too old (>30 days, compacted), server sends full snapshot + current version. Client discards local ops, applies snapshot, then re-applies its ops on top.

The tricky case: server's snapshot version is 2000 but client's base_version is 157. Client ops must be replayed against 1843 intermediate ops. We do this as a batch transform. The result may have some ops that conflict irrecoverably (e.g., client deleted text that no longer exists). Those ops are dropped with a notification: 'Some changes could not be applied.'"

---

**Q: Two session nodes both think they're responsible for the same document (split brain). How do you prevent data corruption?**

**A:** "We use fencing tokens (epochs). When a node takes ownership of a document, it increments an epoch stored in ZooKeeper/etcd. All ops from that node are tagged with the epoch.

Clients and other nodes reject ops with a stale epoch. If the old node (thinking it's still alive) tries to broadcast via Kafka, those messages are tagged with epoch N-1 and discarded.

The new authoritative node has epoch N and its ops are accepted. This prevents the 'ghost writer' problem where a zombie node keeps corrupting the document."

---

## Round 4: Design Deep-Dive Follow-ups

**Q: How do you implement the version history feature (Ctrl+Z and "See revision history")?**

**A:** "Two distinct features:
1. **Local undo (Ctrl+Z):** Client-side only. Client maintains a local undo stack of its own ops. Pressing Ctrl+Z generates an inverse op (delete the inserted char, or re-insert the deleted char) and submits it like any other op. It goes through OT like any op — so if others have edited since, the undo might land slightly differently.

2. **Version history (time travel):** Server reconstructs document state at time T by: load the nearest snapshot before T, replay Cassandra ops until T. Present as read-only. Users can 'restore' by submitting ops to revert from current state to historical state (not by rolling back — we preserve history).

Cassandra retains all ops with timestamps, so we can reconstruct any point in time within retention window (30 days hot, longer in Glacier)."

---

**Q: How do you handle concurrent comment threads on text that gets edited?**

**A:** "Comments are anchored to a range (start_offset, end_offset) in the document. When ops are applied, comment ranges are transformed the same way cursor positions are transformed — using the same OT machinery.

If User A comments on chars 10-20, and User B deletes chars 5-15, we transform the comment range: start becomes max(5, 10-5) = 5, end becomes 15-5=10. The comment survives but covers a smaller range.

If the commented range is entirely deleted, the comment becomes 'orphaned' and shown with a warning: 'The commented text was deleted.'"

---

**Q: Design the export feature (doc → PDF/DOCX).**

**A:** "Export is expensive (rendering) and not on the critical path. Run it asynchronously.

1. User clicks Export → API creates an export job: `{doc_id, format, version, user_id}`
2. Job enqueued in Kafka 'export-jobs' topic
3. Export workers (separate service) consume jobs:
   - Fetch snapshot + replay ops to requested version
   - Run headless Chromium (for PDF) or use a DOCX library
   - Upload result to S3, generate presigned URL (TTL 1h)
4. Notify user via WebSocket or email: 'Your export is ready'

Trade-off: async means a delay (5-30s). Acceptable because export is occasional. Alternative: sync with streaming progress updates. More complex for limited gain."

---

## Rapid Fire — Common Follow-ups

| Q | A |
|---|---|
| Why WebSocket over HTTP long-polling? | Bidirectional, lower overhead (no HTTP headers per op), sub-100ms latency needed |
| Why Cassandra over PostgreSQL for revisions? | Append-only, high write throughput, horizontal scale. No need for complex queries. |
| Why Kafka over Redis Pub/Sub for fan-out? | Kafka persists messages (replay for slow consumers), Redis Pub/Sub is fire-and-forget |
| How do you handle 1B documents in PostgreSQL? | Shard metadata by doc_id prefix or use Citus; or move to Cassandra for metadata too |
| What's your RPO/RTO? | RPO: <1 op (Redis ensures durability before ACK). RTO: ~10s (session failover) |
| How do you prevent one user from filling a document with GBs of text? | Document size limit (e.g., 10MB) enforced at Session Service before applying op |
| How do you handle mobile clients with intermittent connectivity? | Same as offline editing — op queue + sync on reconnect. Keep WebSocket alive with heartbeat. |

---

## Self-Assessment Checklist

- [ ] Explained why last-write-wins fails
- [ ] Described OT or CRDT with concrete example
- [ ] Justified sticky routing for session service
- [ ] Explained two-tier persistence (Redis buffer + Cassandra)
- [ ] Covered snapshotting for fast document load
- [ ] Addressed session node failure + recovery
- [ ] Discussed idempotency for op resubmission
- [ ] Mentioned split-brain prevention (fencing tokens)
- [ ] Estimated scale: nodes needed, throughput, latency targets
