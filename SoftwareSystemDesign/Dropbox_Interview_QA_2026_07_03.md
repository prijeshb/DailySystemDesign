# Dropbox — Interview Q&A
**Date:** 2026-07-03  
**Format:** Interviewer question → what they're probing → ideal answer

---

## Opening / Requirements

**Q: Design Dropbox.**

*What they want:* Requirements clarification before diving in.

**Say:**
> "Before I design, a few questions. Are we focusing on file sync across devices, or also real-time collaboration like Google Docs? Just sync? Great. Should we support any file type including binary? How large? Up to 5GB per file — noted. Multi-device per user? Yes. Shared folders? Yes. Offline editing? Yes — that's critical for Dropbox. How many users — let's say 100M registered, 10M DAU."

---

## Scoping / Estimation

**Q: How much storage will this need?**

*Probing:* Can you reason about scale?

**Answer:**
> "100M users × average 10GB per user = 1 PB of raw storage. With deduplication (content-addressed chunks), effective stored data is lower — maybe 600-700 TB if 30% of content is duplicated (common files like OS files, stock images). I'd size S3 at 1 PB with dedup to be safe. Storage cost: ~$23K/mo on S3."

---

**Q: What's the bottleneck at this scale?**

*Probing:* Do you understand where load concentrates?

**Answer:**
> "Upload bandwidth is the obvious concern, but it's not the API server — uploads go directly to S3 via pre-signed URLs. The real bottleneck is the metadata service: every upload requires a commit (write) and every device reconnect requires a change-feed query. At 10M DAU reconnecting in the morning, that's potentially 10M sync reads in a short window — need read replicas on Postgres and caching of the change feed for common namespace queries."

---

## Core Design

**Q: How do you sync files efficiently? Why not just re-upload the whole file on every change?**

*Probing:* Do you know delta sync / chunking?

**Answer:**
> "We split files into 4MB chunks and identify each by SHA-256 of its content. Before uploading, the client asks the server: 'which of these chunk hashes do you already have?' The server returns only the missing ones. Client uploads only those — skipping chunks that already exist.
>
> For a 100MB file where the user edits the last page (1 changed chunk), we upload 4MB instead of 100MB — 96% bandwidth reduction. This also enables global deduplication: if 1,000 users all have the same PDF, we store one copy."

---

**Q: What happens when the same file is edited on two devices while offline?**

*Probing:* Conflict resolution — a classic Dropbox question.

**Answer:**
> "This is a version conflict. When the second device commits its changes, the server detects that the `base_version` it's submitting from is no longer the current version — another commit already advanced it. 
>
> Dropbox's approach: don't silently overwrite. Instead, keep the canonical version as-is, and save the second device's version as a 'conflict copy' named `file (Device conflicted copy 2026-07-03).ext`. Both versions appear in the user's folder. The user sees both and resolves manually.
>
> The alternative — Last Write Wins — is simpler but loses one device's work, which is unacceptable for important files. A third option is CRDTs (used by Google Docs), but those only work for text; Dropbox syncs binary files too."

---

**Q: How does a device know what it's missed while it was offline?**

*Probing:* Sync cursor / change feed design.

**Answer:**
> "Each device stores a `last_sync_cursor` — a monotonically increasing integer. Every file mutation in the system creates a row in the `changes` table with an auto-incrementing `change_id`. When a device comes online, it sends: `GET /changes?since=9420`. The server returns all changes since change_id 9420, in order. The device applies them and updates its cursor.
>
> I use a numeric cursor instead of a timestamp to avoid clock skew between client and server. Timestamps on different machines can differ by seconds; monotonic IDs never go backward or have gaps."

---

**Q: How do you notify other devices in real-time when a file changes?**

*Probing:* Push mechanism choice and fallback.

**Answer:**
> "Devices maintain a WebSocket connection to the Sync Service. When a file commits, the Metadata Service writes to the `changes` table and publishes a lightweight event to Redis pub/sub: `{namespace_id, change_id}`. The Sync Service subscribes and pushes the notification to all WebSocket connections for that namespace.
>
> Fallback: If WebSocket is unavailable (mobile background, network block), clients fall back to polling `GET /changes?since={cursor}` every 30 seconds. Because the change log is in Postgres, no notification is ever permanently lost — polling always catches up."

---

**Q: What happens if the Sync Service WebSocket server crashes?**

*Probing:* Failure-first thinking.

**Answer:**
> "Clients detect the disconnect via ping/pong timeout (~30 seconds). They reconnect with exponential backoff to another Sync Service instance and immediately send their last cursor. The new instance responds with all changes since that cursor — the crash caused a delay but zero data loss. The change log in Postgres is the source of truth, not in-memory state on the Sync Service."

---

## Deep Dives

**Q: How does chunk deduplication work exactly? What if two different files have the same chunk?**

*Probing:* Content-addressed storage understanding.

**Answer:**
> "Chunks are stored in S3 keyed by `SHA-256(content)`. If two files share an identical 4MB block, both file_versions reference the same SHA-256 hash — we store the bytes once. The `chunks` table tracks a `ref_count`. When a file version is deleted, we decrement ref_count for each of its chunks. A background GC job periodically deletes chunks with ref_count=0 from S3.
>
> SHA-256 collision probability: 2^-256 per pair — effectively impossible. We treat collisions as impossible in practice (same as Bitcoin, TLS, etc.)."

---

**Q: How do you handle very large files — say, a 10GB video?**

*Probing:* Multipart upload, progress resumption.

**Answer:**
> "For large files we use S3 multipart upload. Each 4MB chunk is uploaded as a separate S3 part. If the upload fails mid-way, we resume from the last successful part — the client tracks part completion locally. We request fresh pre-signed URLs for the remaining parts.
>
> S3 lifecycle policy: auto-abort incomplete multipart uploads after 24 hours to avoid orphaned parts accumulating storage cost."

---

**Q: If I edit a 100MB file and change one character, what exactly gets uploaded?**

*Probing:* Understanding of fixed-size vs. content-defined chunking.

**Answer:**
> "With fixed 4MB chunks: the character change modifies exactly one 4MB chunk. We re-upload 1 chunk (4MB) instead of 100MB — 96% savings.
>
> However, if you insert a character at the beginning of the file, fixed chunks shift entirely — every chunk changes. This is the limitation of fixed-size chunking. Dropbox actually uses content-defined chunking (Rabin fingerprinting): a rolling hash over a 64-byte window finds natural chunk boundaries based on content. Inserting a character shifts at most 1-2 boundary chunks; the rest are unchanged. Better deduplication for text and code, slightly more complex to implement."

---

**Q: How do you handle the quota — preventing a user from uploading more than their plan allows?**

*Probing:* TOCTOU / atomicity awareness.

**Answer:**
> "Naive approach — read `used_bytes`, check against quota, then update if OK — has a race condition. Two concurrent uploads can both pass the check before either updates the counter.
>
> Fix: atomic SQL update:
> ```sql
> UPDATE users
> SET used_bytes = used_bytes + :file_size
> WHERE id = :user_id
>   AND used_bytes + :file_size <= quota_bytes;
> ```
> If `rows_affected = 0`, quota exceeded — reject. This is atomic at the database level; no separate check needed. As a second defense, S3 bucket policies can enforce per-prefix object size limits."

---

**Q: How would you make sync faster for users on the same local network?**

*Probing:* LAN sync / P2P optimization.

**Answer:**
> "LAN sync. Devices on the same network broadcast availability via mDNS: 'I have these chunk hashes available at this local IP:port.' When Device B needs a chunk, it checks if any LAN peer has it first before going to S3. Direct LAN transfer at 100-1000Mbps vs. internet download at 10-50Mbps — 10-20x faster.
>
> Trade-offs: adds P2P code complexity, firewall traversal issues in enterprise networks (some block mDNS), security review needed (verify chunk authenticity by SHA-256 match even if peer is local). Worth it for office use cases (design team sharing large Figma exports)."

---

## Follow-ups / Edge Cases

**Q: What if a user deletes a file — can they recover it?**

**Answer:** Soft delete (`deleted_at` timestamp, not physical delete). Trash retains versions for 30 days (Dropbox free) or 180 days (Plus). Restore = clear `deleted_at`, restore `current_version_id`. Chunks stay in S3 as long as any FileVersion references them.

---

**Q: How do shared folders work when two users both have permission?**

**Answer:** Shared folder = a Namespace shared between users. Both users' devices sync the same namespace. Changes by either user → change log for that namespace → all devices (of both users) subscribed to that namespace_id get notified. Conflict resolution same as multi-device for a single user.

---

**Q: How would you design version history — the ability to restore any past version?**

**Answer:** FileVersions are immutable append-only. Restore = create a NEW FileVersion pointing to the old version's `chunk_hashes`. Never modify or delete old FileVersions within retention window. Old versions expire after TTL (30-180 days) → GC decrements chunk ref_counts → S3 GC cleans orphaned chunks.

---

**Q: How do you prevent someone from accessing another user's files via chunk hash?**

**Answer:** Pre-signed S3 URLs are scoped to the specific object key. Block Service only generates pre-signed URLs for chunks belonging to the requesting user's namespaces (or shared namespaces they have access to). Guessing a SHA-256 hash of someone else's chunk doesn't help — you'd need a valid pre-signed URL which requires server-side authorization check. S3 bucket is not public; all access is via signed URLs only.

---

## Key Numbers to Know

| Metric | Value |
|--------|-------|
| Chunk size | 4 MB (fixed) or 1-8 MB (Rabin) |
| SHA-256 key | 64 hex chars |
| Pre-signed URL TTL | 15 min (standard), 30 min (large files) |
| WebSocket ping/pong timeout | 30 seconds |
| Polling fallback interval | 30 seconds |
| Sync notification (push) | ~1 second end-to-end |
| Conflict resolution | Conflict copy (no silent overwrite) |
| Postgres Multi-AZ failover | 30-60 seconds |
| Version history (free) | 30 days |
| Storage cost at 1 PB | ~$23K/mo (S3) |
