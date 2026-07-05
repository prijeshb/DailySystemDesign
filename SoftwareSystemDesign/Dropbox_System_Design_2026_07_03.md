# Dropbox System Design
**Date:** 2026-07-03  
**Category:** File Storage & Sync  
**Concepts:** Content-Addressed Chunks, Delta Sync, Offline-First, Conflict Resolution, Sync Cursor

---

## First Principles — Do We Even Need This?

**Problem:** User wants the same file available on every device, automatically. Manual upload/download is error-prone and forgotten.

**Core value:** "Edit once, see everywhere — without thinking about it."

**What makes Dropbox different from Google Drive:**
- Drive = collaborative editing (CRDT, real-time co-editing)
- Dropbox = reliable sync of any file type (binary, code, PSDs) across arbitrary devices, with offline-first behavior and LAN sync

**Minimum viable behavior:**
1. Change file on Device A → appears on Device B (even after A goes offline)
2. Huge files (4GB video) sync efficiently — not re-uploaded whole on each change
3. Conflict when two devices edit offline → no silent data loss

---

## Entities

| Entity | Description |
|--------|-------------|
| **User** | Account with storage quota |
| **Device** | Desktop/mobile client with local file state |
| **File** | A named item in a folder tree |
| **FileVersion** | Immutable snapshot of a file at a point in time |
| **Chunk** | 4MB content-addressed block of file data (SHA-256 keyed) |
| **Namespace** | Isolated file tree (personal or shared folder) |
| **SyncCursor** | Per-device pointer: "last change I've seen" |
| **Share** | Permission grant on a folder |

---

## Actions

- Upload (new file or changed file)
- Download / sync changed file to local
- Share folder with another user
- Browse file history, restore version
- Conflict: two offline devices edit the same file
- Delete and restore from trash

---

## Scale & Back of Envelope

**Assumptions:**
- 100M users, 10M daily active
- Average storage per user: 10 GB
- Total storage: 100M × 10GB = **1 PB**
- Daily uploads: 1 upload/active user/day = 10M files/day ≈ 120 uploads/sec (peak: 600/sec)
- Average file size: 2MB (many small files, some large)
- Chunk size: 4MB → most files = 1 chunk

**Throughput:**
- Upload RPS (peak): ~600 files/sec
- Download RPS (peak): ~1,800 reads/sec (3× upload ratio)
- Metadata reads (sync check): ~5,000 RPS

**AWS Cost Estimate (100M users):**
```
S3 storage:    1 PB × $0.023/GB  = $23,000/mo
S3 GET:        5,000 RPS × 3600s × 720h → ~$1,500/mo  
CloudFront:    ~180 TB/mo egress × $0.085/GB = $15,300/mo
RDS Postgres:  2× db.r5.2xlarge (primary + standby) = $2,400/mo
ElastiCache:   2× cache.r6g.xlarge = $400/mo
EC2 API fleet: 50× c5.xlarge = $6,120/mo
─────────────────────────────────────────────
Total:         ~$50,000/mo at 100M users
Per user:      $0.50/mo (Dropbox charges ~$1/user/mo → profitable at scale)
```

**Budget-Constrained Version ($10K/mo — startup):**
- Support 20M users max
- Cap free tier at 2GB (not 10GB)
- S3 Intelligent-Tiering: files not accessed 30 days → cheaper tier ($0.0125/GB vs $0.023)
- Limit version history: 30 days (Dropbox Plus = 180 days, not feasible on $10K budget)
- No LAN sync server infrastructure (saves bandwidth)
- Trade-off: worse UX for large-file power users, no extended history

---

## Data Flow

```
Client detects file change (inotify/FSEvents/ReadDirectoryChangesW)
         │
         ▼
Split into 4MB chunks, compute SHA-256 per chunk
         │
         ▼
POST /upload/preflight  → Metadata Service
  Request: { filename, folder_id, chunk_hashes: [h1, h2, h3] }
  Response: { chunks_needed: [h2] }  ← only missing chunks
         │
         ▼
PUT /chunks/{sha256} ← direct to S3 via pre-signed URL (skip API server)
  (only upload chunks server doesn't have)
         │
         ▼
POST /files/commit  → Metadata Service
  { namespace_id, parent_folder_id, filename, version, chunk_list: [h1,h2,h3] }
         │
         ▼
Metadata Service persists FileVersion, publishes change event
         │
         ▼
Sync Service notifies other devices (WebSocket/SSE)
  { change_id: 9421, file_id: X, new_version_id: Y }
         │
         ▼
Other device: GET /changes?since=9420 → downloads only new chunks
```

**Key insight:** Chunks are content-addressed. Server stores chunk only once even if 100 users have the same file (e.g., a company logo). Delta sync means only changed 4MB blocks are uploaded.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client (Desktop/Mobile)               │
│  File Watcher → Chunker → Dedup Check → Upload Manager  │
└─────────────────────────────────────────────────────────┘
         │ HTTPS                     │ Direct S3 upload
         ▼                           ▼
┌──────────────────┐         ┌──────────────┐
│   API Gateway    │         │   S3 Bucket  │
│  (rate limit,    │         │  (chunks by  │
│   auth, TLS)     │         │   sha256)    │
└──────────────────┘         └──────────────┘
         │
    ┌────┴──────┬────────────┐
    ▼           ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────────┐
│ Metadata │ │  Block   │ │    Sync      │
│ Service  │ │ Service  │ │   Service    │
│ (files,  │ │(pre-sign │ │ (WebSocket,  │
│ versions,│ │ S3 URLs, │ │  SSE push    │
│ folders) │ │ dedup    │ │  to devices) │
│          │ │ check)   │ │              │
└────┬─────┘ └──────────┘ └──────┬───────┘
     │                           │
     ▼                           ▼
┌──────────┐              ┌───────────┐
│ Postgres │              │   Redis   │
│(metadata,│              │(pub/sub,  │
│ versions,│              │ presence, │
│ shares)  │              │ cursors)  │
└──────────┘              └───────────┘
     │
     ▼
┌──────────────────────┐
│  S3 (chunk storage)  │
│  Content-addressed   │
│  SHA-256 key         │
└──────────────────────┘
```

---

## Low-Level Design

### Database Schema

```sql
-- Users and quotas
users (
  id BIGINT PK,
  email TEXT UNIQUE,
  password_hash TEXT,
  quota_bytes BIGINT DEFAULT 10_737_418_240,  -- 10 GB
  used_bytes BIGINT DEFAULT 0
);

-- Namespaces (personal or shared folder root)
namespaces (
  id BIGINT PK,
  owner_id BIGINT FK → users,
  type ENUM('personal','shared')
);

-- File tree (files and folders, unified)
files (
  id BIGINT PK,
  namespace_id BIGINT FK,
  parent_id BIGINT FK → files (nullable for root),
  name TEXT NOT NULL,
  is_folder BOOLEAN NOT NULL,
  current_version_id BIGINT,         -- NULL for folders
  deleted_at TIMESTAMP,              -- soft delete
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
INDEX: (namespace_id, parent_id, name) UNIQUE WHERE deleted_at IS NULL

-- Immutable file versions (never updated, only appended)
file_versions (
  id BIGINT PK,
  file_id BIGINT FK → files,
  version_number INT NOT NULL,
  size_bytes BIGINT,
  chunk_hashes TEXT[],               -- ordered array of SHA-256 strings
  created_at TIMESTAMP,
  created_by_device_id BIGINT
);
INDEX: (file_id, version_number)

-- Content-addressed chunks (dedup table)
chunks (
  sha256_hash TEXT PK,              -- 64-char hex SHA-256
  size_bytes INT,
  ref_count INT DEFAULT 0,          -- for GC: delete when 0
  created_at TIMESTAMP
);
-- Actual bytes stored in S3 at key: chunks/{sha256_hash}

-- Per-device sync state
devices (
  id BIGINT PK,
  user_id BIGINT FK,
  device_name TEXT,
  platform ENUM('macos','windows','linux','ios','android','web'),
  last_sync_cursor BIGINT DEFAULT 0,  -- last change_id seen
  last_seen_at TIMESTAMP
);

-- Change log (append-only, drives sync cursors)
changes (
  id BIGINT PK AUTO_INCREMENT,       -- monotonic, global
  namespace_id BIGINT,
  file_id BIGINT,
  change_type ENUM('create','update','delete','restore'),
  new_version_id BIGINT,
  changed_at TIMESTAMP DEFAULT NOW()
);
-- Partitioned by namespace_id for query performance

-- Sharing
shares (
  id BIGINT PK,
  folder_id BIGINT FK → files,
  inviter_id BIGINT FK → users,
  invitee_id BIGINT FK → users,
  permission ENUM('view','edit'),
  invited_at TIMESTAMP,
  accepted_at TIMESTAMP
);
```

### Shares: Tricky Edge Cases

The `shares` table looks simple but hides significant complexity. These are the areas interviewers dig into:

**1. Whose quota does a shared file count against?**
```
A shares a 5GB folder with B.
Naive: count against A only (owner pays for storage).
Problem: B can keep uploading into the shared folder indefinitely — A runs out of quota.

Dropbox's actual model:
  Files uploaded by A → count against A's quota
  Files uploaded by B into shared folder → count against B's quota
  Implemented via: file_versions.created_by_user_id → quota deducted from uploader, not folder owner
```

**2. Permission escalation**
```
A shares /team with B at view-only.
Can B re-share /team/project with C at edit?

Rule: You cannot grant a permission you don't have.
  B has view → B can share with C at most view
  B cannot grant edit even if C is trusted

Implementation:
  On share creation: check inviter's permission on the folder
  IF inviter_permission = 'view' AND new_permission = 'edit' → REJECT 403
```

**3. Nested sharing conflicts**
```
A shares /team with B (edit).
A then separately shares /team/project with B (view-only).

Which permission wins for /team/project?

Option A: Most-specific wins (/team/project → view-only)
Option B: Most-permissive wins (edit, from parent)
Option C: Both exist; explicitly granted share on subfolder overrides parent

Dropbox: uses Option C — explicit share on subfolder takes precedence over inherited.
This means: B can edit files in /team but NOT in /team/project.
Complexity: on every permission check, must walk up the folder tree to find the most-specific share record.
```

**4. Revocation — what happens to locally synced files?**
```
A revokes B's access to /team.
B already has all files on their laptop (synced locally).

Behavior options:
  Hard delete: Sync Service pushes delete events to B's devices → local files removed
  Soft: just stop syncing new changes, leave local copies

Dropbox: removes the shared namespace from B's Dropbox folder (hard delete from cloud view),
but does NOT delete already-downloaded files from B's local disk.
Rationale: B may have legitimate local copies; deleting local files without consent is too invasive.

Implementation:
  On revoke: insert DELETE change events for all files in namespace into B's change feed
  B's client removes them from synced folder but NOT from disk (moves to local-only location)
```

**5. Cascading when owner deletes shared folder**
```
A deletes /team (which has 5 members).

Options:
  A) Delete for everyone immediately → B, C, D, E lose the folder
  B) Transfer ownership to most-senior member
  C) Keep folder alive until all members leave

Dropbox: Option A — owner deletion removes the shared folder for all members.
Warning shown before deletion: "This folder is shared with N people. Deleting removes it for all."
Members get notified and have a window to export before hard delete.
```

**6. Owner account deletion / deactivation**
```
A's account is deleted. A owns /team shared with 10 people.

Problem: shared namespace has no owner → no one can manage permissions or increase quota.

Options:
  B) Prompt members to elect a new owner before deletion
  C) Auto-assign to first member who accepted

Dropbox Teams: admin can transfer ownership. Personal accounts: folder is archived (read-only) for 30 days, then deleted.
```

**7. Moving files out of a shared folder**
```
B (with edit) moves a file from /team (shared) to /personal (B's private namespace).

This is ambiguous:
  Is it a copy? (file exists in both)
  Or a move? (removed from shared, now in personal)

If move: A (owner) loses the file from the shared namespace → probably not intended.

Rule: You can only move files within the same namespace.
Cross-namespace is always a copy (new file_id, new file_version).
The original remains in /team; the copy is in /personal.
Prevents accidental data removal from shared folders.
```

**8. Permission check at every file access (N+1 problem)**
```
To check if user B can read file at /team/design/assets/logo.png:
  Check: does B have a share on /team/design/assets? No
  Check: does B have a share on /team/design? No
  Check: does B have a share on /team? Yes, view

Naive: walk up tree for every request = O(depth) DB queries per read

Fix: materialized permission cache
  On share grant/revoke → recompute and cache B's effective permission for each namespace
  Store in Redis: perm:{user_id}:{namespace_id} → view|edit|none
  TTL = 60s (short enough to catch revocations quickly)
  Read path: check Redis first → O(1)
```

---

### Sync Cursor — How It Works
```
Client Device state:
  last_sync_cursor = 9420  (last change_id seen by this device)

Sync request:
  GET /changes?namespace_id=42&since=9420&limit=100

Server response:
  [
    { change_id: 9421, file_id: 100, change_type: update, new_version_id: 55 },
    { change_id: 9422, file_id: 101, change_type: delete },
  ]

Client:
  For each change → apply locally
  Update last_sync_cursor = max(change_ids seen) = 9422

Why cursor not timestamp?
  - No clock skew between client and server
  - Monotonic → no gaps, no duplicates
  - Simple: "give me everything after ID 9420"
```

### Chunking Algorithm (Fixed vs Content-Defined)
```
Fixed-size (4MB):
  File → chunks of exactly 4MB (last chunk smaller)
  Pro: simple, predictable
  Con: insert 1 byte at beginning → ALL chunks shift → everything re-uploads

Content-Defined Chunking (Rabin Fingerprinting):
  Sliding 64-byte window over file bytes
  When window_hash & MASK == MAGIC: chunk boundary
  Average chunk size = 4MB, varies 1-8MB
  Pro: insert 1 byte → only 1-2 nearby chunks change → better dedup for text/code
  Con: more complex to implement

Interview answer: Start with fixed 4MB. Mention Rabin for optimization.
Dropbox production: uses content-defined chunking (they documented this in their tech blog)
```

### Conflict Resolution
```
Scenario: User edits file.txt on laptop (offline) AND phone (offline)
  Laptop: version 3, committed first → file_id=100, version_id=55
  Phone:  version 3, committed second → server sees version conflict

Server detection:
  POST /files/commit { file_id: 100, base_version: 3, ... }
  Server checks: current version of file_id=100 is already 55 (not base_version=3)
  → CONFLICT

Resolution options:
  A) Last Write Wins (LWW):
     Overwrite with phone's version, laptop's changes lost
     Fast, simple. Used when: user rarely edits offline simultaneously

  B) Create Conflict Copy (Dropbox's actual approach):
     Keep laptop's version as the canonical file
     Create: "file (John's conflicted copy 2026-07-03).txt" with phone's version
     User sees both, manually resolves
     Pro: zero data loss
     Con: user has to manually clean up

Dropbox uses B. Google Drive uses real-time CRDT to prevent conflict at all (different use case).
```

### Block Service — Pre-signed URL Flow

**How pre-signed URLs work (mechanics):**
```
Server generates a URL by signing a "string to sign" with the AWS Secret Key using HMAC-SHA256:

  StringToSign = "AWS4-HMAC-SHA256\n"
               + timestamp + "\n"
               + credential_scope + "\n"    ← date/region/service
               + SHA256(CanonicalRequest)    ← includes: method, path, query params, headers

  Signature = HMAC-SHA256(signingKey, StringToSign)

Resulting URL:
  https://bucket.s3.amazonaws.com/chunks/abc123sha256hex
    ?X-Amz-Algorithm=AWS4-HMAC-SHA256
    &X-Amz-Credential=AKID/20260703/us-east-1/s3/aws4_request
    &X-Amz-Date=20260703T120000Z
    &X-Amz-Expires=900             ← 15 minutes
    &X-Amz-SignedHeaders=host
    &X-Amz-Signature=<64-char hex>

S3 validates:
  1. Reconstruct the same StringToSign from the URL
  2. Recompute HMAC using its copy of the Secret Key
  3. Compare → match → allow. Tampered URL → mismatch → 403.

Key properties:
  - The URL is self-contained auth — no session/cookie needed
  - Anyone who possesses the URL can use it until it expires
  - Tied to exactly: one HTTP method + one S3 key + one bucket + one expiry
```

**Flow:**
```
  1. Client → Block Service: POST /upload/presign { sha256_hash, size_bytes }
  2. Block Service checks:
     a. Is user authenticated? (JWT/session check)
     b. Does user have edit permission on the target namespace?
     c. Does chunk already exist? (dedup) → return "already_have_it"
  3. Hash missing → generate S3 pre-signed PUT URL scoped to exact key
     Response: { upload_url: "https://s3.../chunks/{sha256}?X-Amz-Signature=..." }
  4. Client → S3 directly: PUT {upload_url} body=chunk_bytes
  5. S3 → stores at key: chunks/{sha256_hash}
  6. Client verifies: SHA-256(uploaded_bytes) == requested hash (self-check)
  7. Client → Metadata Service: POST /files/commit
```

**Common mistakes that attackers exploit:**

**Mistake 1: Generating a GET URL for any hash the client asks for**
```
Buggy server:
  GET /download/presign?hash={user_input}
  → generates S3 GET URL for chunks/{user_input}
  → returns URL to caller

Attack:
  Attacker knows (or guesses) another user's chunk hash → requests GET URL → downloads their data
  SHA-256 hashes of common files (PDFs, OS files) are public knowledge

Fix:
  Before generating any GET URL, verify:
    SELECT 1 FROM file_versions fv
    JOIN files f ON f.id = fv.file_id
    WHERE fv.chunk_hashes @> ARRAY[:requested_hash]
      AND f.namespace_id IN (/* namespaces user has access to */)
  If no row → 403. Never trust the hash alone as proof of access.
```

**Mistake 2: Overly broad S3 key scope in the signature**
```
Buggy: signing for key prefix  "chunks/"
  → URL works for ANY object under chunks/
  → attacker modifies the key in the URL → still valid signature

Fix: always sign for the exact full key: "chunks/abc123...64chars"
  URL tampering → signature mismatch → S3 returns 403
```

**Mistake 3: URL leakage through logs or Referer headers**
```
The signature lives in the query string — it's part of the URL.

If the client app logs full request URLs → signature is in plaintext logs.
If a pre-signed URL is used as a page resource (src="...") and the page has
  external resources → Referer header sends the full URL (including signature) to third parties.

Fix:
  - Never log full URLs containing X-Amz-Signature
  - Use POST-based S3 Presigned POST for browser uploads (signature in form body, not URL)
  - For server-side downloads: use pre-signed URLs only for direct client use, never embed in page src attributes
```

**Mistake 4: Long TTL → leaked URL gives prolonged access**
```
TTL = 24 hours → URL shared accidentally in Slack/email/bug report
  → attacker has 24h to download or overwrite the object

Fix:
  Upload PUT URLs: 15 minutes max (client uploads immediately or re-requests)
  Download GET URLs: 5 minutes for sensitive data (generate fresh on each access)
  Never issue > 1 hour TTL for user-generated content
```

**Mistake 5: Public S3 bucket — pre-signed URLs become irrelevant**
```
If the S3 bucket policy has s3:GetObject for "*" (any principal):
  No pre-signed URL needed at all — attacker hits the object URL directly
  https://bucket.s3.amazonaws.com/chunks/abc123 → 200 OK

This is the most common real-world breach vector:
  Twitch (2021): 125GB internal data exposed via public S3 bucket
  Capital One (2019): misconfigured WAF + EC2 role → 100M records exfiltrated from S3

Fix:
  Enable "S3 Block Public Access" at the AWS account level (overrides any bucket policy)
  Principle of least privilege: bucket policy explicitly denies s3:* without pre-signed conditions
```

**Mistake 6: Not verifying SHA-256 of downloaded chunk on the client**
```
Client requests chunk "abc123..." and receives bytes from CDN/S3.

If CDN is compromised or MITM is present → can serve different bytes for the same URL.
If client blindly writes bytes to disk → file reconstructed with attacker-injected content.

Fix: client always computes SHA-256(received_bytes) and compares to expected hash.
  Mismatch → discard, retry from different source.
  This is the same guarantee that makes LAN sync safe from rogue peers.
```

**Mistake 7: Allowing PUT to an existing chunk (overwrite attack)**
```
Dedup model: chunk "abc123" stored once, referenced by many file_versions.
If attacker can PUT to an existing chunk key:
  → overwrites S3 object with malicious bytes
  → all file_versions referencing that hash now serve attacker's content

Fix: S3 bucket policy denying s3:PutObject if object already exists.
  Or: use S3 Object Lock (WORM mode) for chunk objects — once written, immutable.
  Pre-signed URL for PUT only generated when Block Service confirms chunk does NOT exist.
```

### LAN Sync (P2P optimization)
```
Problem: Two devices on same LAN both need a large file.
Default: Both download independently from S3 → wastes bandwidth, slow.

LAN Sync:
  Sync Service broadcasts on local multicast: "Device A has version 7 of file.txt"
  Device B: "I need version 7 too" → connect to Device A directly (local TCP)
  Download chunks from Device A (100Mbps LAN) vs S3 (10-20Mbps internet)

Implementation:
  - mDNS/Bonjour for device discovery
  - Each device announces: device_id, namespace_id, chunks_available[]
  - Direct chunk transfer (HTTP on ephemeral port)
  - Fallback to S3 if LAN peer unavailable

Cost saving: Large file (1GB) from LAN peer = $0 vs ~$0.09/GB from CloudFront

Trade-off:
  Pro: 5-10x faster sync within office, saves egress cost
  Con: adds P2P code complexity, firewall traversal issues on enterprise networks
```

---

## Trade-off Summary

| Decision | Chosen | Trade-off |
|----------|--------|-----------|
| Fixed-size chunks (4MB) | ✓ Start | Simpler, but inserts cause more re-upload vs Rabin |
| Content-addressed storage | ✓ | Dedup is free, but GC needed when ref_count=0 |
| Sync cursor vs timestamp | Cursor ✓ | No clock skew, but need monotonic change_id sequence |
| Conflict copy vs LWW | Conflict copy ✓ | Zero data loss, but user sees confusing duplicates |
| Direct S3 upload (pre-signed) | ✓ | No bandwidth bottleneck, but auth must be delegated carefully |
| WebSocket push vs polling | Both | WebSocket = fast, polling = fallback (30s interval) |
| LAN sync | Optional | Faster + cheaper, but P2P complexity |

---

## References
- Dropbox Engineering Blog: "Sync Engine Rebuild" (2020) — describes production sync cursor model
- Dropbox Blog: "How we've scaled Dropbox" — block-level dedup details
- Alex Xu: System Design Interview Vol. 1 — Chapter 15 (Google Drive, similar patterns)
- Dropbox Paper: "Magic Pocket" — internal multi-exabyte block storage system
