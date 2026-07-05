# WhatsApp System Design
> Date: 2026-07-01 | Concepts: Signal Protocol, SenderKeys, Offline Queue, Multi-device, Delivery Receipts

---

## First Principles

**Do we need a dedicated messaging service?**
- SMS: no E2E encryption, no media, carrier-dependent delivery
- Email: async (seconds to minutes), no group real-time feel
- Need: <100ms delivery, E2E encrypted, media, groups up to 1024, offline delivery, multi-device

**What makes WhatsApp hard at scale?**
1. Billions of keys to manage (every device has a keypair)
2. Message ordering across distributed nodes
3. Offline delivery without storing plaintext
4. Group messages to thousands — fan-out vs sender keys
5. Multi-device without breaking E2E

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| **User** | phone_number (PK), name, profile_photo_url, last_seen, privacy_settings |
| **Message** | id (TIMEUUID), sender_id, chat_id, type (text/media/audio/video), ciphertext (BLOB), timestamp |
| **Chat** | id (UUID), type (1:1 / group), created_at |
| **ChatMember** | chat_id, user_id, role (member/admin), joined_at |
| **Media** | id, s3_key, mime_type, size_bytes, sha256_hash |
| **DeviceSession** | user_id, device_id, identity_key, signed_prekey, one_time_prekeys[] |
| **OfflineMessage** | user_id, device_id, message_id, chat_id, payload (BLOB), created_at [TTL 30d] |
| **MessageStatus** | message_id, recipient_user_id, status (delivered/read), updated_at |

---

## Actions

1. **Register device** — upload identity key, signed prekey, batch of one-time prekeys to KDC
2. **Start new chat** — fetch recipient's prekeys, run X3DH, establish Double Ratchet session
3. **Send message** — encrypt with Double Ratchet, POST to server, get ACK (single ✓)
4. **Deliver message** — push to online device via WebSocket; or queue + FCM/APNS if offline
5. **Upload media** — client encrypts locally, uploads ciphertext to S3, shares AES key in message
6. **Send delivery receipt** — device ACKs receipt (double gray ✓✓)
7. **Send read receipt** — device sends when chat opened (blue ✓✓)
8. **Create group / add member** — distribute SenderKey to new member via 1:1 E2E session
9. **Remove member** — rotate SenderKey for remaining members (forward secrecy)
10. **Link device** (WhatsApp Web) — phone encrypts and transmits identity to new device

---

## Data Flow

### 1:1 Message
```
Alice's Device
  1. Fetch Bob's {identity_key, signed_prekey, one_time_prekey} from KDC
  2. X3DH → master_secret → Double Ratchet session established
  3. encrypt(plaintext) → ciphertext (server NEVER sees plaintext)
  4. POST /message {chat_id, ciphertext, sender_id}
  5. Chat Service: write to Cassandra messages table
  6. Check PresenceService: Bob online?
     YES → WebSocket Hub: forward to Bob's socket
     NO  → write to offline_messages (TTL 30d) + send FCM/APNS push
  7. Server ACKs Alice → single gray ✓ shown
  8. Bob's device receives → sends delivery_receipt → double gray ✓✓
  9. Bob opens chat → sends read_receipt → blue ✓✓
```

### Group Message (SenderKey)
```
On group creation / when Alice joins:
  Alice generates SenderKey (symmetric, random 256-bit)
  For each group member M:
    Encrypt SenderKey with M's 1:1 E2E session → send individually

To send group message:
  Alice: encrypt_with_senderkey(plaintext) → 1 ciphertext
  POST /message {group_id, ciphertext}
  Server delivers SAME ciphertext to all N members
  Each member decrypts with their copy of Alice's SenderKey

Member leaves:
  Remaining members generate new SenderKeys → redistribute
  (Leaver can no longer decrypt future messages — forward secrecy)
```

### Media Flow
```
1. Client generates random AES-256 key
2. Client encrypts file: AES-256-CBC → encrypted_blob
3. Upload encrypted_blob to Media Service → stored in S3
4. Media Service returns {media_url, sha256_of_ciphertext}
5. Client sends message: {media_url, encrypted_aes_key}
   — encrypted_aes_key is inside the Double Ratchet ciphertext
6. Recipient decrypts message → gets AES key → GETs media_url → decrypts
   Server stores: encrypted blob only. Cannot decrypt.
```

---

## High-Level Architecture

```
[Client App (iOS/Android)]
        │
        ├── WebSocket (persistent, messages)
        └── HTTPS (media upload, API calls)
        │
[API Gateway + L7 Load Balancer]
        │
        ├──► [Chat Service]  ─── Cassandra (messages, chats, members)
        │         │
        │         ├──► [KDC - Key Distribution Center] ─── Redis (prekeys)
        │         │                                    ─── DynamoDB (durable backup)
        │         ├──► [Presence Service] ─── Redis Pub/Sub
        │         │
        │         └──► [Kafka: message.delivery topic]
        │                        │
        │              [Delivery Service]
        │                   │          │
        │            [WS Hub]    [Push Notification]
        │           (online)       (FCM / APNS)
        │
        └──► [Media Service] ─── S3 ─── CloudFront CDN
```

---

## Low-Level Design

### Cassandra Schema
```sql
-- Partition by chat_id, clustered by TIMEUUID (time-ordered)
CREATE TABLE messages (
  chat_id     UUID,
  message_id  TIMEUUID,      -- globally unique, time-ordered
  sender_id   BIGINT,
  type        TEXT,          -- 'text' | 'image' | 'video' | 'audio' | 'doc'
  ciphertext  BLOB,          -- server never decrypts
  PRIMARY KEY (chat_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);

-- Status tracking (high write rate, separate table)
CREATE TABLE message_status (
  message_id   UUID,
  recipient_id BIGINT,
  status       TEXT,         -- 'delivered' | 'read'
  updated_at   TIMESTAMP,
  PRIMARY KEY (message_id, recipient_id)
);

-- Offline queue (TTL auto-cleans after 30 days)
CREATE TABLE offline_messages (
  user_id     BIGINT,
  device_id   UUID,
  created_at  TIMESTAMP,
  message_id  UUID,
  chat_id     UUID,
  payload     BLOB,
  PRIMARY KEY ((user_id, device_id), created_at, message_id)
) WITH default_time_to_live = 2592000;   -- 30 days
```

### X3DH Key Exchange
```
Alice wants to message Bob (first time):
  GET /keys/bob_id
  Response: {
    identity_key:    Ed25519 public key  (permanent)
    signed_prekey:   X25519, signed by identity_key (rotated weekly)
    one_time_prekey: X25519, used once (popped from server supply)
  }

X3DH (4 DH operations):
  DH1 = DH(Alice.identity_key,  Bob.signed_prekey)
  DH2 = DH(Alice.ephemeral_key, Bob.identity_key)
  DH3 = DH(Alice.ephemeral_key, Bob.signed_prekey)
  DH4 = DH(Alice.ephemeral_key, Bob.one_time_prekey)  [if available]
  master_secret = KDF(DH1 || DH2 || DH3 || DH4)

If one-time prekey pool empty:
  Fall back: DH4 omitted → slightly weaker forward secrecy, acceptable
  Alert Bob's device: "Upload more prekeys"
```

### Double Ratchet
```
Each message:
  message_key_N derived from previous ratchet state
  Ratchet advances after each message

Properties:
  Forward secrecy:   compromise of key_N ≠ expose keys 1..N-1
  Break-in recovery: ratchet advances even if attacker has current state
  Out-of-order:      store skipped message keys (up to 1000)
```

### WebSocket Routing
```
Connection: client connects to any WS Hub
  WS Hub registers: SET presence:{user_id}:{device_id} {hub_ip} EX 120
  Refresh every 60s (heartbeat)

Message delivery:
  Chat Service → look up presence:{recipient_id}:* in Redis
  Found (online) → forward to that hub_ip via internal gRPC
  Not found (offline) → offline_messages queue + FCM/APNS

Hub failure:
  TTL expires in 120s → presence entry gone → treated as offline
  Client reconnects → registers with new hub → pulls missed messages
```

---

## Back of Envelope

### Scale
```
2B registered users, 500M DAU
Messages/day: 100B → 1.16M msg/sec peak × 3 = 3.5M msg/sec
Avg message:  150B (ciphertext + metadata)
Storage/day:  100B × 150B = 15TB (messages, deleted after delivery)
Active msgs:  30-day window = 450TB in Cassandra

Media:
  10% messages have media, avg 500KB
  Media/day: 10B × 500KB = 5PB/day
  CDN cache hit (first 24h): 90% → origin load reduced 10×
  S3 tiering: Standard → Glacier after 7 days
```

### AWS Cost Model
```
Component                           Monthly Cost
─────────────────────────────────────────────────
Cassandra (30× r6g.2xlarge)         ~$65,000
Redis (KDC+Presence, 6× r6g.xl)    ~$15,000
Kafka (12 brokers, i3.xlarge)       ~$25,000
Chat Service (200× c6g.2xlarge)     ~$70,000
WebSocket Hubs (100× c6g.xlarge)    ~$35,000
S3 Standard (150TB active media)    ~$3,450
S3 Glacier (cold tier, 1PB)         ~$4,000
CloudFront CDN (100TB/mo egress)    ~$8,500
DynamoDB (KDC key backup)           ~$5,000
FCM/APNS push                       Free
─────────────────────────────────────────────────
TOTAL                               ~$231,000/mo

Cost per message: $231K / 3T msgs/mo ≈ $0.00000008
= $0.08 per million messages (extremely low)
```

### Budget Constraint: Startup Clone ($5,000/mo)
```
Allocated:
  1× Cassandra node (r6g.xlarge)     $400/mo  → SPOF, no replication
  1× Redis for KDC                   $200/mo  → lose prekeys on restart
  3× App servers (t4g.medium)        $210/mo
  S3 (media only, no CDN)            $100/mo  → slow media loads globally
  Misc (LB, monitoring)              $300/mo
  Total:                             ~$1,210/mo (leaves budget for growth)

Hard limits:
  - No Cassandra replication → 1 node crash = data loss
  - No CDN → media download latency 500ms+ for users far from region
  - No WebSocket → fall back to SSE or long-polling
  - Supports: ~50K DAU, ~10M messages/day

Migration path when hitting limits:
  Messages → DynamoDB (managed, $1,500/mo at this load)
  Media → CloudFront (add CDN layer, $500/mo)
  Chat Service → Fargate (auto-scale, no EC2 mgmt)
```

---

## Component Trade-offs

| Component | Choice | Why | Trade-off |
|-----------|--------|-----|-----------|
| **Message store** | Cassandra | Write-optimized (LSM), partition by chat_id, TTL built-in | No JOINs, eventual consistency, schema changes need coordination |
| **E2E encryption** | Signal Protocol | Can't be subpoenaed for content, gold standard | Cannot moderate content; no server-side search |
| **Group encryption** | SenderKeys | O(1) encrypt vs O(N) per message | Key rotation on member leave is expensive (N members re-key) |
| **Delivery** | WebSocket + FCM | Low latency for online users; push for offline | WebSocket needs sticky routing; push adds ~100ms latency |
| **Media storage** | S3 + CDN | Cheap at scale, global distribution | CDN cache misses cold content; S3 is not real-time |
| **Offline TTL** | 30 days | Auto-cleanup, simple | User loses messages if offline >30 days (mitigate: warn at day 25) |
| **Presence** | Redis TTL | Lightweight, auto-expires on disconnect | 120s stale window; user may appear online for up to 2min after closing app |
| **Message ID** | TIMEUUID | Globally unique + time-ordered | Clock skew can cause ordering artifacts across nodes |

---

## Multi-device (WhatsApp Web/Desktop)

```
Linking flow:
  1. User opens WhatsApp Web → displays QR code (contains nonce + server pub key)
  2. Phone scans QR → authenticates the new device
  3. Phone sends to server: {new_device_id, new_device_identity_key}
     encrypted with existing phone session
  4. Server registers new device in DeviceSession for this user_id

Message delivery to linked devices:
  Alice sends to Bob → Bob has: phone + laptop + web
  Chat Service: for each of Bob's devices, deliver same ciphertext
  (Each device has Bob's identity key → can run Double Ratchet)

History: NOT synced retroactively (forward secrecy — past messages
         were encrypted with different ratchet state)
  New linked device sees only messages received while linked.
```

---

## See Also
- [WhatsApp_Failure_Analysis_2026_07_01.md](WhatsApp_Failure_Analysis_2026_07_01.md)
- [WhatsApp_Interview_QA_2026_07_01.md](WhatsApp_Interview_QA_2026_07_01.md)
- Concepts: #106 (X3DH), #107 (Double Ratchet), #108 (SenderKey), #109 (Offline Message Queue), #110 (Multi-device Session), #111 (Delivery Receipt Pattern)
