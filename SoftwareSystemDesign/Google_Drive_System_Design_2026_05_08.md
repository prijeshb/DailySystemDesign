# System Design: Google Drive (Distributed File Storage & Sync)
## Interview Preparation Guide

**Date**: May 8, 2026  
**Scope**: Scalable cloud file storage, sync, and sharing system  
**Concepts Covered**: Chunking, Deduplication, Versioning, Sync Conflict Resolution, Sharding, Idempotency, CDN, Caching, Consistency  

---

## 1. REQUIREMENTS GATHERING

### Functional Requirements
- **Upload**: Users can upload files of any size (documents, images, videos)
- **Download**: Users can download/stream files on any device
- **Sync**: Files auto-sync across multiple devices in near real-time
- **Share**: Share files/folders with specific users or via public link
- **Versioning**: Maintain revision history; ability to restore older versions
- **Search**: Search files by name, content (for supported types), and metadata
- **Quota**: Each user has a storage quota (e.g., 15 GB free)
- **Offline Mode**: Access cached files when offline; sync when back online

### Non-Functional Requirements

| Metric | Target | Notes |
|--------|--------|-------|
| **Availability** | 99.99% | Storage layer is critical infrastructure |
| **Durability** | 99.999999999% (11 nines) | Data must never be lost |
| **Upload Latency** | <500ms for small files (<1MB) | Chunked async for large files |
| **Download Latency** | <200ms TTFB | CDN-assisted |
| **Sync Delay** | <5 seconds across devices | Eventual consistency is acceptable |
| **Scale - Users** | 2B users total, 500M DAU | Like Google Drive real scale |
| **Scale - Storage** | 15 exabytes total | Petabyte-scale per region |
| **File Size** | Up to 5 TB per file | Videos, disk images |
| **Throughput** | 10M file operations/day | Uploads + downloads + syncs |

### Out of Scope
- Real-time collaborative editing (Google Docs internals)
- Email integration
- Third-party app ecosystem (OAuth app integrations)

---

## 2. ENTITY DISCOVERY

### Core Entities

```
User
├── user_id (UUID)
├── email
├── quota_bytes (default: 15GB)
├── used_bytes
└── created_at

File (metadata record)
├── file_id (UUID)
├── owner_id → User
├── name
├── mime_type
├── size_bytes
├── current_version_id → FileVersion
├── parent_folder_id → Folder
├── is_deleted (soft delete)
├── created_at
└── updated_at

FileVersion
├── version_id (UUID)
├── file_id → File
├── version_number (monotonic)
├── size_bytes
├── checksum (SHA-256 of full file)
├── chunk_ids[] → Chunk[]   (ordered list)
├── created_at
└── created_by → User

Chunk
├── chunk_id (SHA-256 of chunk content)  ← content-addressed!
├── size_bytes
├── storage_path (bucket + object key)
└── ref_count (how many versions reference this chunk)

Folder
├── folder_id (UUID)
├── owner_id → User
├── name
├── parent_folder_id (null = root)
└── created_at

SharePermission
├── permission_id
├── resource_id (file_id or folder_id)
├── resource_type (FILE | FOLDER)
├── grantee_id → User (null = public link)
├── access_level (VIEWER | EDITOR | OWNER)
├── share_token (for public links)
└── expires_at
```

**Key Design Decision**: `chunk_id = SHA-256(chunk_content)` — this is **content-addressing**. Identical chunks across files share the same storage object. This enables global deduplication.

---

## 3. ACTIONS (API DESIGN)

### Upload Flow
```
POST /api/v1/files/initiate-upload
Body: { name, size_bytes, mime_type, parent_folder_id, checksum }
Response: { file_id, upload_id, chunk_size, chunks_needed, presigned_urls[] }

PUT /api/v1/uploads/{upload_id}/chunks/{chunk_index}
Body: binary chunk data
Headers: Content-MD5 for integrity
Response: { chunk_id, received: true }

POST /api/v1/uploads/{upload_id}/complete
Body: { chunk_ids[] in order }
Response: { file_id, version_id }
```

### Download Flow
```
GET /api/v1/files/{file_id}
Response: file metadata

GET /api/v1/files/{file_id}/download
Response: 302 redirect to CDN-signed URL (or presigned S3 URL)

GET /api/v1/files/{file_id}/versions
Response: [{ version_id, version_number, size_bytes, created_at }]

GET /api/v1/files/{file_id}/versions/{version_id}/download
Response: 302 redirect to specific version
```

### Sync Flow
```
GET /api/v1/sync/delta?since={cursor}&device_id={device_id}
Response: { changes: [{ event_type, file_id, ... }], next_cursor }

POST /api/v1/sync/heartbeat
Body: { device_id, last_sync_cursor }
Response: { server_cursor, has_changes }
```

### Share Flow
```
POST /api/v1/files/{file_id}/share
Body: { grantee_email, access_level, expires_at }
Response: { permission_id, share_link }

GET /api/v1/share/{share_token}   ← public access
Response: file metadata + download URL
```

---

## 4. DATA FLOW

### Upload Data Flow (Chunked)

```
Client                    API Gateway           Upload Service        Chunk Store (S3)
  |                           |                      |                     |
  |-- POST initiate-upload -->|                      |                     |
  |                           |-- validate quota --->|                     |
  |                           |<-- upload_id --------|                     |
  |<-- { upload_id,           |                      |                     |
  |     presigned_urls[] } ---|                      |                     |
  |                           |                      |                     |
  |--- PUT chunk[0] --------> presigned URL (bypass API, direct to S3) --->|
  |--- PUT chunk[1] --------> presigned URL -------------------------------->|
  |--- PUT chunk[N] --------> presigned URL -------------------------------->|
  |                           |                      |                     |
  |-- POST complete --------->|                      |                     |
  |                           |-- verify chunks ---->|                     |
  |                           |-- create FileVersion--|                    |
  |                           |-- update File.current_version_id           |
  |                           |-- publish sync event -> Message Queue      |
  |<-- { file_id, version_id }|                      |                     |
```

**Why presigned URLs for chunks?** Bypasses the API server for bulk data transfer — reduces latency, cost, and API server load.

### Sync Data Flow (Delta Sync)

```
Device A uploads file
  → Upload Service publishes: { event: FILE_UPDATED, file_id, user_id, cursor: 1042 }
  → Sync Event Queue (Kafka topic: user-{user_id}-changes)

Device B (same user, different device)
  → Long-polls: GET /sync/delta?since=1041
  → Sync Service reads from Kafka: returns event for file_id
  → Device B downloads delta: only changed chunks (diff between versions)
  → Device B updates local state
```

**Cursor-based sync**: Each user has a monotonically increasing event log. Devices track their last-seen cursor. On reconnect, they replay from their cursor — making sync inherently idempotent.

---

## 5. HIGH-LEVEL ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CLIENT TIER                                      │
│  Web Browser │ Desktop Client (Mac/Win/Linux) │ Mobile App (iOS/Android) │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │ HTTPS
┌──────────────────────────▼──────────────────────────────────────────────┐
│                         CDN (Cloudflare / Akamai)                        │
│  - Caches static assets, thumbnails, public shared files                 │
│  - TLS termination, DDoS mitigation                                      │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────────┐
│                       API GATEWAY (Kong / AWS ALB)                       │
│  - Auth (JWT validation), Rate limiting, Request routing                 │
└──┬──────────┬───────────┬────────────┬────────────┬───────────────────┘
   │          │           │            │            │
┌──▼──┐  ┌───▼───┐  ┌────▼────┐  ┌───▼────┐  ┌───▼────┐
│Auth │  │Upload │  │Download │  │Metadata│  │ Sync   │
│Svc  │  │Service│  │Service  │  │Service │  │Service │
└──┬──┘  └───┬───┘  └────┬────┘  └───┬────┘  └───┬────┘
   │          │           │            │            │
   │    ┌─────▼─────┐     │       ┌───▼────┐  ┌───▼────────┐
   │    │ Chunk     │     │       │Postgres│  │  Kafka     │
   │    │ Dedup     │     │       │(Metadata│  │ (Sync      │
   │    │ Service   │     │       │DB)     │  │  Events)   │
   │    └─────┬─────┘     │       └───┬────┘  └───┬────────┘
   │          │           │            │            │
   │    ┌─────▼─────────────────────────────────── ▼────────┐
   │    │              Object Storage (S3 / GCS)              │
   │    │     Bucket: chunks/{shard_prefix}/{chunk_id}        │
   │    │     Bucket: thumbnails/{file_id}/                   │
   │    └────────────────────────────────────────────────────┘
   │
┌──▼──────────────────────┐
│   Redis Cluster          │
│   - Session cache        │
│   - Quota cache          │
│   - Chunk existence cache│
│   - Sync cursor cache    │
└─────────────────────────┘
```

---

## 6. LOW-LEVEL DESIGN

### 6.1 Chunking Strategy

**Why chunk files?**
- Resume interrupted uploads (only re-upload failed chunks)
- Parallel upload/download (multiple chunks simultaneously)
- Delta sync (only upload chunks that changed between versions)
- Deduplication at chunk granularity

**Chunk Size Selection:**

| Chunk Size | Pros | Cons |
|-----------|------|------|
| 1 MB | Fine-grained dedup, small retry unit | High metadata overhead (5TB file = 5M chunks) |
| 8 MB | Good balance | Less dedup granularity |
| 64 MB | Low metadata overhead | Large retry unit, poor dedup for text files |

**Decision**: Use **variable-size chunking** (CDC - Content-Defined Chunking). Target 4–8 MB chunks. CDC uses a rolling hash (Rabin fingerprint) to find natural split boundaries in content. This maximizes deduplication when files are slightly modified — only changed regions produce new chunks.

```python
# Rabin fingerprint CDC (simplified concept)
def find_chunk_boundaries(data: bytes, target_size=4_000_000) -> list[int]:
    window_size = 48
    mask = (1 << 13) - 1  # ~8KB average with variance
    boundaries = []
    for i in range(len(data) - window_size):
        window_hash = rabin_hash(data[i:i+window_size])
        if window_hash & mask == 1:  # boundary condition
            boundaries.append(i)
    return boundaries
```

**Trade-off**: CDC adds CPU cost on client. Alternative: fixed-size chunks (simpler, less optimal dedup). Google Drive uses fixed 256KB blocks; Dropbox uses CDC.

### 6.2 Deduplication (Chunk Dedup)

Since `chunk_id = SHA-256(chunk_content)`:

**Before uploading a chunk**, client sends a "chunk existence check":
```
POST /api/v1/chunks/exists
Body: { chunk_ids: ["sha256:abc...", "sha256:def..."] }
Response: { existing: ["sha256:abc..."], missing: ["sha256:def..."] }
```

Client only uploads missing chunks. This is called **block-level client-side dedup**.

**Dedup Impact**: Studies show 30–50% storage savings in typical user workloads due to common documents, OS files, duplicate photos.

**Security concern**: Convergent encryption — if chunk_id is the hash of plaintext, two users with the same file can "prove" each other has it. Solution: **Key-based deduplication** — encrypt with a key derived from `HMAC(master_key, chunk_hash)`, so dedup only occurs within a user's account (not cross-user). Google Drive does per-file encryption.

### 6.3 Metadata DB Design (PostgreSQL + Sharding)

**Sharding Strategy**: Shard by `user_id` (range or hash sharding). All of a user's files/folders land on the same shard — folder traversal and sync queries stay local.

```sql
-- Files table (sharded by user_id)
CREATE TABLE files (
    file_id       UUID PRIMARY KEY,
    user_id       UUID NOT NULL,         -- shard key
    name          VARCHAR(1024) NOT NULL,
    parent_folder_id UUID,
    current_version_id UUID,
    is_deleted    BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
) PARTITION BY HASH(user_id);

-- FileVersions
CREATE TABLE file_versions (
    version_id    UUID PRIMARY KEY,
    file_id       UUID NOT NULL,
    user_id       UUID NOT NULL,         -- denormalized for shard locality
    version_number INT NOT NULL,
    size_bytes    BIGINT,
    checksum      CHAR(64),             -- SHA-256 hex
    chunk_ids     UUID[] NOT NULL,       -- ordered array
    created_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (file_id, version_number)
);

-- Chunks reference table (global, NOT sharded)
CREATE TABLE chunks (
    chunk_id      CHAR(64) PRIMARY KEY, -- SHA-256 hex
    size_bytes    INT NOT NULL,
    storage_path  TEXT NOT NULL,
    ref_count     INT NOT NULL DEFAULT 0
);

-- Sync Events (append-only log per user)
CREATE TABLE sync_events (
    event_id      BIGSERIAL,
    user_id       UUID NOT NULL,
    event_type    VARCHAR(32),  -- FILE_CREATED, FILE_UPDATED, FILE_DELETED, etc.
    file_id       UUID,
    payload       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, event_id)  -- partitioned by user
);
```

**Indexes**:
```sql
CREATE INDEX idx_files_user_parent ON files(user_id, parent_folder_id) WHERE NOT is_deleted;
CREATE INDEX idx_sync_events_user_cursor ON sync_events(user_id, event_id);
CREATE INDEX idx_versions_file ON file_versions(file_id, version_number DESC);
```

### 6.4 Versioning and Quota Management

**Version retention policy**: Keep last 30 versions OR 30 days, whichever is greater (Google's actual policy). Older versions get garbage collected, freeing chunk ref_counts.

**Quota enforcement**:
- Cached in Redis: `quota:{user_id}` → `{ used_bytes, limit_bytes }`
- Decremented on upload completion, incremented on permanent delete
- Checked before initiating upload (optimistic — race condition possible; hard-enforced at completion)
- Background job reconciles Redis vs DB periodically

**Soft delete + quota**: Deleted files go to Trash, still count against quota. Permanent delete (empty trash) triggers quota reclaim and chunk ref_count decrement. Async garbage collector removes chunks with ref_count = 0.

### 6.5 Sync Conflict Resolution

**Scenario**: User edits the same file on Device A (offline) and Device B (offline). Both reconnect.

**Resolution Strategy**: "Last writer wins with conflict copy" (Dropbox model):
1. Device A uploads first → creates version 2
2. Device B attempts to upload → server detects base version mismatch (expected v1, current is v2)
3. Server creates a conflict copy: `"Report (Device B's conflicted copy 2026-05-08).docx"`
4. Device B gets back: `{ conflict: true, conflict_file_id: "..." }`
5. User sees both versions in the folder

**Alternative (Google Docs model)**: Operational Transformation (OT) or CRDT for fine-grained merging — complex, requires document-type awareness.

**Idempotency in uploads**: Each upload has an `upload_id` (UUID). If the client retries `POST /complete`, the server checks if `upload_id` is already committed and returns the existing result — no duplicate file creation.

### 6.6 Caching Strategy

| Cache Layer | What's Cached | TTL | Invalidation |
|------------|--------------|-----|--------------|
| **CDN** | Public shared files, thumbnails | 1 hour | Cache-busted by version (URL includes version_id) |
| **Redis (App)** | User quota, session, chunk existence set | 5 min / 24h / forever | TTL expiry; explicit on quota change |
| **Client disk** | Downloaded files, offline-available files | User-managed | Sync event triggers re-download |
| **DB query cache** | Recent folder listings | 30 sec | Invalidated on any file change in folder |

**Trade-off: CDN caching vs. freshness**
- Cached public files may serve stale content if the owner updates the file
- Solution: Include `version_id` in CDN URL. Old URL stays cached (stale but still valid for history); new version gets new URL
- For private files: no CDN caching — use short-lived presigned URLs (15 min expiry)

---

## 7. COMPONENT TRADE-OFFS SUMMARY

| Component | Choice Made | Why | What We Give Up |
|----------|------------|-----|-----------------|
| **Object Storage** | S3/GCS over self-hosted | 11-nines durability, no ops burden | Cost (egress fees), vendor lock-in |
| **Metadata DB** | PostgreSQL over DynamoDB | Rich queries, ACID transactions for quota | Horizontal scaling harder; need manual sharding |
| **Chunk Identity** | Content-addressed (SHA-256) | Free deduplication, integrity verification | Convergent encryption concerns; SHA computation cost |
| **Sync Transport** | Long-polling over WebSocket | Simpler infra, works through proxies | Higher latency (up to poll interval), more connections |
| **Conflict Resolution** | Last-writer-wins + conflict copy | Simple, predictable, no data loss | User confusion; manual merge required |
| **Chunking** | CDC over fixed-size | Better dedup for modified files | CPU cost, client-side complexity |
| **Consistency** | Eventual (sync events) over strong | High availability, partition tolerant | Sync delay up to seconds; conflict possibility |

---

## 8. SCALABILITY ANALYSIS

### Capacity Estimation

```
Assumptions:
- 2B users, 500M DAU
- Average user: 5 GB used, 10 files/day operations
- Average file size: 2 MB; average chunk: 4 MB

Storage:
- Total: 2B × 5 GB = 10 exabytes
- With dedup (30% savings): ~7 exabytes
- With replication (3x): ~21 exabytes stored in object storage

Upload throughput:
- 500M DAU × 2 uploads/day × 2 MB = 2 PB/day
- = 23 GB/second peak (assume 2× average) = ~46 GB/s upload bandwidth

Metadata DB:
- Files: 2B users × 50 files = 100B rows (need sharding!)
- Sync events: 500M × 10 events/day × 30 days = 150B rows (time-series partitioning)

API servers:
- 500M × 10 ops/day / 86400s = ~58K ops/sec
- At 1000 RPS/server = ~60 API server instances
```

### Sharding the Chunks Table

The `chunks` table is global and could become a hot bottleneck. Solutions:
1. **Shard by chunk_id prefix** (first 2 hex chars = 256 shards)
2. **Bloom filter in Redis**: "Does chunk exist?" answered in memory before hitting DB
3. **Tiered storage**: Hot chunks (recently accessed) in SSD-backed storage; cold chunks in Glacier-class storage

---

*See companion files: `Google_Drive_Failure_Analysis_2026_05_08.md` and `Google_Drive_Interview_QA_2026_05_08.md`*
