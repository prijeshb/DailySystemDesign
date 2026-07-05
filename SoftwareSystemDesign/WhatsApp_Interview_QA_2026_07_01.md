# WhatsApp Interview Q&A
> Date: 2026-07-01 | For practice: cover these before looking at answers

---

## Opening (Requirement Clarification)

**Q: Design WhatsApp.**

Before jumping in, say:
> "Let me clarify scope: 1:1 messaging only, or also groups? Do we need E2E encryption, or is server-side encryption acceptable? Media? Multi-device? And what scale — 2B users or startup MVP?"

Interviewers want: structured thinking before solutioning.

Functional requirements (confirm):
- 1:1 text messages
- Group messages (up to 1024 members)
- E2E encryption
- Media (photos, video, audio, documents)
- Delivery receipts (sent/delivered/read)
- Offline delivery (30 days)
- Multi-device support

Non-functional:
- End-to-end latency <100ms (online users)
- 99.99% availability
- No message loss
- 2B users, 100B messages/day

---

## Estimation Questions

**Q: How many messages per second must we handle?**
```
100B messages/day ÷ 86,400 sec/day ≈ 1.16M msg/sec average
Peak (3× average): ~3.5M msg/sec

Storage per message: ~150 bytes (ciphertext + metadata)
Storage/day: 100B × 150B = 15TB
Server keeps messages: 30 days (deleted after delivery + 30d offline grace)
Total Cassandra: 15TB × 30 = 450TB
```

**Q: How much media storage?**
```
10% of messages include media
= 10B media/day
Average size: 500KB (image) to 50MB (video), avg ~500KB
5PB/day new media → not feasible to keep forever
Policy: hot tier (S3 Standard, 7 days), cold tier (S3 Glacier, 90 days), then delete
CDN absorbs 90% of media reads within first 24h
```

---

## Encryption Questions

**Q: Can WhatsApp read my messages?**

No. WhatsApp uses the Signal Protocol:
- Keys are generated on-device, never transmitted to server
- Server receives only ciphertext
- Even a court order cannot produce message content — only metadata (who, when, how often)
- Trade-off: WhatsApp cannot moderate message content (spam/CSAM in messages)

**Q: Walk me through E2E encryption for a new 1:1 conversation.**

Three pieces working together:
1. **X3DH** (one-time): establishes initial shared secret using a 4-DH key exchange with Bob's pre-uploaded keys. Neither party was online at the same time — async key establishment.
2. **Double Ratchet** (every message): advances the session key per message. Compromise of key N doesn't expose keys 1..N-1 (forward secrecy) and system self-heals even after key compromise (break-in recovery).
3. **AES-256** (per message): actual message encryption with the current ratchet key.

**Q: How do group messages work efficiently? Isn't encrypting for 1000 members expensive?**

Without optimization: Alice sends to 1000-member group → 1000 separate encryptions, 1000 network calls.

**SenderKey** pattern:
- Alice generates one 256-bit SenderKey for the group
- Alice distributes SenderKey to each member via their 1:1 E2E session (done once at join time, ~1000 initial messages)
- To send a group message: Alice encrypts once with SenderKey
- Server fans out the SAME ciphertext to all 1000 members
- Each member decrypts with their copy of Alice's SenderKey

Cost per group message: O(1) encrypt, O(N) deliveries of same blob.

**Follow-up: what happens when a member is removed from a group?**
- Removed member has the old SenderKey and could theoretically receive future messages if they intercepted ciphertext
- Fix: key rotation. Remaining N-1 members receive new SenderKeys
- This is expensive (N-1 messages) — WhatsApp throttles rapid member changes in large groups
- Forward secrecy: removed member cannot decrypt new messages after rotation

**Q: What's the difference between X3DH and Double Ratchet?**

| | X3DH | Double Ratchet |
|--|--|--|
| When | Once per new conversation | Every message |
| Purpose | Bootstrap shared secret between strangers | Continuous key evolution |
| Requires | Recipient's pre-uploaded keys (async) | Prior session state |
| Property | Async key agreement (no real-time sync needed) | Forward secrecy + break-in recovery |

X3DH gives you the starting key. Double Ratchet keeps rotating it.

---

## Architecture Questions

**Q: How do you route a message to an online user?**

```
1. Presence Service: when user connects, register in Redis
   SET presence:{user_id}:{device_id} {ws_hub_ip} EX 120
   Heartbeat every 60s refreshes TTL

2. Chat Service receives message:
   GET presence:{recipient_id}:*  (all devices)
   Found → gRPC call to WS Hub at hub_ip → Hub pushes to socket
   Not found (offline) → write to offline_messages + send FCM/APNS push

3. Delivery ACK:
   Recipient device ACKs → Chat Service marks message "delivered"
   Chat Service notifies sender (via sender's WebSocket)
```

**Q: How do you scale WebSocket connections to 500M concurrent users?**

```
Each WS Hub: ~50K concurrent connections (OS file descriptor limit + 8GB memory for socket buffers)
500M users ÷ 50K/hub = 10,000 WS Hub instances
At 3× peak: 30,000 pods → Kubernetes cluster across 3 AZs

Key design:
  - Stateless beyond the TCP connection (routing state in Redis)
  - Consistent hash on user_id → same hub for reconnects (performance, not correctness)
  - On hub failure: clients reconnect → any hub picks up
  - Message queued in offline_messages → delivered on reconnect
```

**Q: Why Cassandra over PostgreSQL for messages?**

| Dimension | Cassandra | PostgreSQL |
|-----------|-----------|------------|
| Write pattern | Append-only, exactly matches LSM-tree (Cassandra's engine) | B-tree: good for updates, slower for pure append at extreme scale |
| Read pattern | "Get last 50 messages in chat X" = partition key + clustering key → O(1) lookup | Same query needs index; still fast, but single-node limit |
| Scale | Linear horizontal: 30 → 60 nodes by adding machines | Vertical first, then complex sharding (Citus, etc.) |
| TTL | Built-in, column-level | Needs background job or partitioned tables |
| Joins | None needed (each partition = one chat) | N/A |
| Transactions | Not needed for message store | Overkill |

For startup: PostgreSQL works fine up to ~50M messages/day on a well-tuned instance.

**Q: How do read receipts work in a group of 1000?**

```
message_status table: (message_id, recipient_id, status)
  On delivery to each device: INSERT (message_id, user_id, 'delivered')
  On read: UPDATE (message_id, user_id, 'read')

Sender queries: "How many read my message?"
  SELECT status, COUNT(*) FROM message_status 
  WHERE message_id = ? GROUP BY status
  Result: {delivered: 873, read: 412}

Performance concern: 1000 writes per group message just for delivery receipts
  → Batch processing: collect receipts for 500ms, bulk insert
  → Separate Cassandra table, not hot path
  → Sender doesn't need real-time count — polling every 5s is fine
```

---

## Failure / Edge Case Questions

**Q: What if a user's one-time prekeys run out?**

The server has no more one-time prekeys for Bob:
- X3DH omits DH4 (the one-time prekey DH operation)
- Session still established using DH1, DH2, DH3 — slightly weaker forward secrecy (because one-time prekey provides per-session uniqueness)
- Acceptable security: still better than most alternatives
- Server alerts Bob's device: "Upload more prekeys" → app does this in background within seconds of next app open

**Q: User changes phones — how do they get their messages?**

WhatsApp maintains a message backup (encrypted) to Google Drive / iCloud:
- Backup key derived from phone number + WhatsApp backup passphrase (E2E)
- Not stored on WhatsApp servers
- On new phone: restore from backup (optional)
- Messages since last backup: delivered from offline_messages queue (up to 30 days)
- Beyond 30 days in offline queue: lost unless backup exists

**Q: How do you handle the thundering herd when 500M clients reconnect after a WS outage?**

```
Jitter on reconnect:
  delay = min(base_ms × 2^attempt, 30_000) + random(0, 3_000)

Example for 500M clients:
  Attempt 1: 0-500ms + random(0, 3000ms) → spread over 3.5s
  Attempt 2: 0-1000ms + random(0, 3000ms) → spread over 4s
  ...
  Max: 30s + 3s random = 33s window
  
Rate: 500M ÷ 33s ≈ 15M connections/sec at peak
  WS Hub accepts ~100K new connections/sec each
  Need: 150+ active hubs to absorb burst (we have 10K total, headroom is fine)
```

**Q: Two users send a message to each other at the exact same millisecond — what happens?**

Each device generates its own TIMEUUID before sending (client-side):
- TIMEUUID includes: 100-nanosecond timestamp + clock sequence (14 bits) + node (MAC address, 48 bits)
- Even if clocks are synced to same millisecond, different MAC addresses → different UUIDs
- No collision possible across devices
- Display order: client shows their own sent messages at top of the tied pair (UX preference)
- Server order: TIMEUUID comparison (deterministic)

**Q: A user reports they sent a message 2 hours ago and it shows "Sent" (single ✓) but not "Delivered". How do you debug?**

Step through the pipeline:
1. **Message in Cassandra?** Query messages table by chat_id, message_id → yes/no
2. **Recipient online during those 2h?** Check presence TTL history + server logs
3. **Offline message in queue?** `SELECT * FROM offline_messages WHERE user_id=? AND message_id=?`
4. **Push notification sent?** Check FCM delivery report for recipient device token
5. **Was push notification received?** FCM reports delivery to device
6. **Device received but failed to ACK?** Check delivery_receipt logs
7. **Recipient's device running low on storage?** App-level issue (common with Android)

Usually: recipient's phone was off / no internet. Message in offline queue. Will deliver when phone is back online.

---

## Follow-up / Deep Dive

**Q: How would you add message search within a conversation?**

Challenge: messages are E2E encrypted → server cannot index them.

Options:
1. **Client-side search only**: decrypt on device, full-text search in local SQLite (WhatsApp's actual approach). No server involvement. Limitation: only messages on this device.
2. **Searchable symmetric encryption (SSE)**: client generates search index tokens locally, uploads encrypted tokens. Server searches tokens without knowing content. Complex to implement correctly.
3. **Sacrifice E2E**: upload plaintext to server, server-side ElasticSearch. WhatsApp won't do this (breaks privacy promise).

WhatsApp chose option 1. Search works, but only on messages already downloaded to device.

**Q: How would you design WhatsApp for enterprise (WhatsApp Business)?**

New entities needed:
- Business profile (verified, catalog, quick replies)
- Business messaging quotas (per 24h conversation window per user)
- CRM integration (webhook on message received)
- Template messages (pre-approved for outbound business-initiated messages)

Key difference from consumer:
- Consumer: unlimited 1:1, no rate limits
- Business: rate-limited outbound, requires 24h window triggered by user, or pre-approved templates

**Q: How do you handle spam/abuse if you can't read messages?**

WhatsApp's approach (without breaking E2E):
1. **Metadata analysis**: who sends to how many new contacts in short time, block rate, report rate
2. **Client-side signals**: app reports if user is sending from multiple rapid-fire new accounts
3. **Hash of media**: client hashes media before encryption, sends hash to server. Server checks hash against known CSAM database (PhotoDNA). If match: report without reading content. (Controversial — requires client-side scanning)
4. **User reports**: recipient can report message (includes few messages as evidence, breaks E2E for reported content only)

This is a real ethical tension in E2E systems — WhatsApp gets this question often.

---

## Common Mistakes to Avoid

| Mistake | Why It's Wrong | Correct Approach |
|---------|---------------|-----------------|
| Server generates keys for users | Server could decrypt all messages | Keys generated on device, never leave device |
| Polling for message delivery | 500M users × every 5s = 2.5B requests/min | WebSocket push (online) + FCM (offline) |
| Single Redis for presence at 2B scale | 1 Redis = 10K connections/sec limit | Redis Cluster (16 shards) or distributed presence per region |
| MySQL for messages | Write volume (1M/sec) crushes single-primary MySQL | Cassandra (LSM-tree, horizontal scale) |
| No offline TTL | Cassandra fills up storing messages forever | 30-day TTL; warn users at day 25 |
| Deliver read receipts to server on every scroll | 1000-person group scrolling = 1000 RPCs | Batch: collect 500ms of receipts, bulk write |
