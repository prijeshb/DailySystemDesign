# System Design: Object Storage (Amazon S3)
**Date:** 2026-06-18  
**Category:** Storage Infrastructure  
**Real-world refs:** AWS S3 internals, Facebook Haystack, Azure Blob Storage  

---

## First Principles: Do We Even Need This?

**Why not just use a relational database for files?**
- SQL BLOB columns: max ~1-2GB, terrible read throughput, expensive storage
- File system on one machine: no replication, no scalability, no API

**Why not just use NFS/shared filesystem?**
- NFS: SPOF, doesn't scale writes, no content-based addressing, no global access

**So why object storage?**
- Binary blobs of arbitrary size (bytes to terabytes)
- Need 11 nines durability (one file lost per million per billion years)
- Need cheap storage at petabyte scale
- Need global access with HTTP
- Write-once, read-many pattern (most objects don't change)

**Core insight:** Object storage = flat namespace (no directories), HTTP GET/PUT, content durability is the #1 guarantee.

---

## Entities

| Entity | Description |
|--------|-------------|
| **Bucket** | Logical namespace. `my-company-media` |
| **Object** | File identified by `bucket/key`. Immutable once written. |
| **Version** | Each PUT creates a new version (if versioning enabled) |
| **Part** | Chunk of a multipart upload (5MB–5GB each) |
| **Chunk** | Internal storage unit (64MB). Objects split into chunks for placement. |
| **Metadata** | Size, ETag (MD5), content-type, created_at, storage class, custom headers |

---

## Actions / API

```
PUT    /bucket/key              → upload object (up to 5GB)
GET    /bucket/key              → download object
DELETE /bucket/key              → delete (soft or versioned)
HEAD   /bucket/key              → metadata only (no body)
LIST   /bucket?prefix=images/   → paginated key listing

# Multipart (for >5GB or resumable uploads)
POST   /bucket/key?uploads              → initiate, get upload_id
PUT    /bucket/key?partNumber=N&uploadId=X → upload part N
POST   /bucket/key?uploadId=X           → complete multipart
DELETE /bucket/key?uploadId=X           → abort multipart

# Access control
GET    /bucket/key?X-Amz-Expires=3600... → pre-signed URL (no auth header needed)
```

---

## Data Flow

```
                    ┌─────────────────────────────────────────────────┐
                    │                  CLIENT                          │
                    └───────────┬──────────────────┬───────────────────┘
                                │ PUT              │ GET
                                ▼                  ▼
                    ┌───────────────────────────────┐
                    │         API Gateway            │
                    │  (auth, rate limit, routing)   │
                    └────────────┬──────────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │        Upload Service          │
                    │   (chunking, multipart mgmt)   │
                    └──────┬─────────────────────────┘
                           │ write chunks
           ┌───────────────▼──────────────────────────┐
           │           Storage Cluster                  │
           │  Node-1  Node-2  Node-3  ...  Node-N       │
           │  [chunk] [chunk] [chunk]      [chunk]       │
           │  (distributed via consistent hashing)       │
           └───────────────────────────────────────────┘
                           │ on complete
           ┌───────────────▼──────────────────────────┐
           │         Metadata Service                   │
           │  (PostgreSQL sharded + Redis cache)        │
           │  object → [chunk_id, node_id] mapping      │
           └───────────────────────────────────────────┘
```

---

## Back of Envelope

**Assumptions (mid-scale, not full AWS):**
```
Objects stored:        1 billion
Avg object size:       1 MB (mix of images, docs, small videos)
Total raw data:        1 PB
Daily new objects:     10M / day → ~115 writes/sec
Daily reads:           100M / day → ~1,160 reads/sec
Peak reads:            10× avg = ~11,600 reads/sec
Avg object size read:  1 MB → peak read throughput = 11.6 GB/sec
```

**Metadata size:**
```
Per-object metadata: ~500 bytes (key + ETag + size + timestamps + storage class)
1B objects × 500B = 500 GB metadata
Fits on a 3-node PostgreSQL cluster (shard by bucket_id hash)
Frequently accessed: cache in Redis → 500GB × 10% hot = 50GB cache
```

**Storage with erasure coding (6+3 Reed-Solomon):**
```
Raw data:   1 PB
After EC:   1 PB × (9/6) = 1.5 PB actual disk used (vs 3 PB for 3× replication)
Disk nodes: 1.5 PB / 18 TB per node = 84 storage nodes
```

---

## AWS Cost vs Build-It-Yourself

**At 1 PB scale, paying S3:**
```
Storage:   1 PB × 1024 GB × $0.023/GB   =   $23,552/month
GETs:      1,160 req/s × 86400 × 30 days × $0.0004/1000  =  $1,204/month
PUTs:      115 req/s × 86400 × 30 days × $0.005/1000     =  $1,490/month
Data out:  11.6 GB/s peak → avg ~3 GB/s = 7.8 PB/month × $0.09/GB  =  $720K/month ← DATA EGRESS IS THE KILLER

Total S3: ~$746K/month at 1PB + traffic
```

**Building it yourself:**
```
84 storage nodes × $10K/node (server + 2× 18TB drives) = $840K capex
Amortize over 3 years: $23K/month hardware
Add ops team (3 engineers × $20K/month fully loaded) = $60K/month
Network: $20K/month
DIY total: ~$103K/month for 1PB
```

**Break-even:** DIY wins at ~150-200TB scale IF traffic is high. Below that, managed S3 cheaper.

**Budget constraint example:**
Budget $50K/month for storage + delivery.
- Option A (S3): $50K buys ~2.1PB storage but no egress budget → force CDN-only access
- Option B (DIY): $50K buys 40-50 nodes → ~720TB usable, enough for 500GB objects
- **Limitation:** With $50K budget, force all reads through CDN (cache hit rate >90%) — only origin pull pays egress

**Trade-off:** CDN caching reduces S3 egress 90%, but content with low cache hit rate (unique user downloads) still bleeds $. For that use case: self-hosted or use CDN with origin shield.

---

## High Level Design

```
┌──────────┐     ┌─────────────┐     ┌───────────────────┐
│  Client  │────►│ API Gateway │────►│   Upload Service  │
└──────────┘     │ (auth/TLS)  │     │  (chunking, EC)   │
                 └──────┬──────┘     └────────┬──────────┘
                        │                     │ write
                        │ GET                 ▼
                        │            ┌─────────────────┐
                        │            │  Storage Nodes  │
                        │            │  (Ceph / custom)│
                        │            └────────┬────────┘
                        │                     │ locations
                        │            ┌────────▼────────┐
                        └───────────►│Metadata Service │
                                     │ (Postgres+Redis)│
                                     └─────────────────┘
```

**On write:**
1. API Gateway authenticates (JWT / HMAC-SHA256 of request)
2. Upload Service receives bytes, chunks into 64MB parts
3. For each chunk: SHA256 hash = chunk_id (content-addressed)
4. Consistent hash of chunk_id → target node set (6+3 EC shards)
5. Write 6 data shards + 3 parity shards to 9 distinct nodes across ≥3 racks
6. All 6 data shards acknowledged → write is durable
7. Metadata Service records: `(bucket, key, version_id) → [chunk_ids, node_ids]`
8. Return `ETag: {md5_of_object}` to client

**On read:**
1. API Gateway authenticates
2. Metadata Service: `(bucket, key) → chunk_ids + node_ids`
3. Fetch 6 data shards in parallel from 6 nodes
4. If any node slow/down: fetch from 3 parity nodes + decode (Reed-Solomon)
5. Stream reassembled bytes to client

---

## Low Level Design

### Metadata Schema

```sql
-- Sharded by bucket_id (consistent hashing → Vitess or Citus)
CREATE TABLE objects (
    bucket_id       UUID,
    key             TEXT,
    version_id      UUID DEFAULT gen_random_uuid(),
    size            BIGINT,
    etag            CHAR(32),           -- MD5 of complete object
    content_type    VARCHAR(128),
    storage_class   VARCHAR(16) DEFAULT 'STANDARD',
    is_deleted      BOOLEAN DEFAULT FALSE,
    is_latest       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (bucket_id, key, version_id)
);

CREATE TABLE object_chunks (
    version_id   UUID,
    seq_no       INT,                   -- 0, 1, 2, ... (order of chunks)
    chunk_id     CHAR(64),             -- SHA256 hex of chunk content
    chunk_size   INT,
    PRIMARY KEY (version_id, seq_no)
);

CREATE TABLE chunk_locations (
    chunk_id     CHAR(64),
    shard_index  SMALLINT,             -- 0-5 = data, 6-8 = parity
    node_id      UUID,
    rack_id      VARCHAR(32),
    disk_path    TEXT,
    PRIMARY KEY (chunk_id, shard_index)
);

-- Multipart uploads
CREATE TABLE multipart_uploads (
    upload_id    UUID PRIMARY KEY,
    bucket_id    UUID,
    key          TEXT,
    storage_class VARCHAR(16),
    expires_at   TIMESTAMPTZ,          -- abort if not completed
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE upload_parts (
    upload_id    UUID,
    part_number  INT,                  -- 1 to 10,000
    etag         CHAR(32),
    size         INT,
    chunk_id     CHAR(64),
    PRIMARY KEY (upload_id, part_number)
);
```

### Chunk Storage on Disk (Each Node)

```
/data/chunks/{prefix_2_chars}/{chunk_id}
  e.g.: /data/chunks/ab/abcdef1234...

Contents: raw binary shard (data or parity bytes)
Alongside: {chunk_id}.meta → { original_chunk_size, ec_index, checksum }

Bloom filter per node: loaded in memory on startup
  → "does chunk X exist on this node?" in O(1) before disk read
  → 1B chunks × 10 bits/item ÷ 8 = 1.25GB per node (acceptable)
```

### Erasure Coding (Reed-Solomon 6+3)

```
Object chunk (64MB):
  → Split into 6 equal data shards (10.67MB each)
  → RS(6,3): compute 3 parity shards
  → Total: 9 shards × 10.67MB = 96MB for 64MB of data (1.5× overhead)
  → Place on 9 nodes, 1 shard per node, no two shards on same rack

Recovery:
  Any 6 of 9 shards → full data recovery
  Tolerate 3 simultaneous node failures OR 3 simultaneous disk failures
  
Trade-off: EC recovery requires reading 6 shards + computation (Reed-Solomon decode)
           vs replication: just read 1 replica (simpler, faster)
           EC wins: 1.5× overhead vs 3× for triple replication
           EC loses: latency on degraded reads (must compute, not just copy)
```

### Consistent Hashing for Node Selection

```python
# Pick 9 nodes for a chunk's 9 EC shards
def select_nodes(chunk_id: str, all_nodes: List[Node]) -> List[Node]:
    ring = ConsistentHashRing(all_nodes, virtual_nodes=150)
    # Walk clockwise from chunk_id hash
    # Skip nodes on same rack as already selected
    candidates = ring.get_n_nodes(chunk_id, n=9, rack_aware=True)
    return candidates

# Rack-awareness: no two of the 9 shards on same rack
# So: 3 racks × 3 shards = 9 placements
# Even if rack-level outage → only 3 shards lost → still 6 remaining → recoverable
```

### Pre-Signed URL (HMAC-SHA256)

```
Generation (server-side):
  canonical_string = "{method}\n{bucket}\n{key}\n{expires}\n{content_type}"
  signature = HMAC_SHA256(secret_key, canonical_string)
  url = "https://storage.example.com/{bucket}/{key}"
       + "?X-Expires={unix_ts}"
       + "&X-Signature={hex(signature)}"

Validation (per request — no auth header needed):
  1. Check X-Expires > now() (not expired)
  2. Recompute canonical_string from request
  3. Verify HMAC — if match: authorize
  4. No DB lookup needed (stateless validation)

Use case: 
  - Client uploads directly to storage (bypass API server)
  - Short-lived public URLs for private objects (share photo for 1 hour)
  - CDN origin auth (CDN uses pre-signed to fetch from private bucket)
```

### Multipart Upload Flow

```
1. Initiate:
   POST /bucket/key?uploads → upload_id = uuid4()
   Server: INSERT INTO multipart_uploads(...)
   
2. Upload parts (client can parallelize):
   PUT /bucket/key?partNumber=1&uploadId=X → part bytes
   Server: chunk part, store on nodes, INSERT upload_parts(upload_id, 1, etag, chunk_id)
   
3. Complete:
   POST /bucket/key?uploadId=X { parts: [{PartNumber:1, ETag:"abc"}, ...] }
   Server:
     a. Verify all parts present + ETags match
     b. In transaction:
        INSERT object_versions(bucket, key, version_id)
        INSERT object_chunks(version_id, seq_no, chunk_id) for all parts in order
        DELETE multipart_uploads + upload_parts
     c. Return ETag = MD5(concat(part_ETags))

4. Abort:
   DELETE /bucket/key?uploadId=X
   Server: mark expired, background job cleans orphaned chunks
```

---

## Trade-offs

| Decision | Choice | Pro | Con |
|----------|--------|-----|-----|
| Erasure coding vs replication | EC 6+3 | 1.5× storage overhead vs 3× | Degraded read requires 6 fetches + decode |
| Content-addressed chunks | SHA256(content) = chunk_id | Automatic deduplication across objects | Can't update chunk in place; always new chunk |
| Flat namespace (no real dirs) | key = "a/b/c.png" | Infinite scalability | Prefix listing is a scan, not tree traversal |
| Metadata in SQL | PostgreSQL sharded | ACID for atomic version commits | Metadata write is on critical path |
| Pre-signed URLs | Stateless HMAC | No DB lookup on verify, infinite scale | Can't revoke early (must wait for expiry) |
| Storage class tiers | STANDARD → IA → GLACIER | Cost: $0.023 → $0.01 → $0.004/GB | Glacier retrieval: 3-5 hours, not instant |

---

## Storage Classes & Lifecycle

```
STANDARD     → $0.023/GB — frequent access
STANDARD_IA  → $0.01/GB, $0.01/GB retrieval — infrequent access (>30 days)
GLACIER      → $0.004/GB, $0.03/GB retrieval, 3-5h — archive (>90 days)
DEEP_ARCHIVE → $0.00099/GB, 12h retrieval — long-term archive (>180 days)

Lifecycle rule:
  - After 30 days of no access → transition to IA
  - After 90 days → Glacier
  - After 365 days → expire (delete)
  
Implementation:
  Background job: daily scan of objects WHERE last_accessed < threshold
  UPDATE objects SET storage_class = 'IA'
  Trigger chunk migration: copy EC shards to cheaper disk tier
```

---

## Real-World References

- **Facebook Haystack (2010):** First published design for blob storage. Key insight: filesystem metadata overhead kills small-file performance. Store millions of photos in large barrel files; maintain needle (offset, size) index in memory.
- **AWS S3:** Publicly uses erasure coding for Reduced Redundancy Storage tier; standard durability uses 11 9s achieved via cross-AZ replication + EC.
- **Ceph RADOS:** Open-source implementation of object storage with CRUSH algorithm for rack-aware placement (similar to what's described here).
- **Azure Blob Storage:** "Extent-based" storage — chunks are 256MB "extents" placed on storage nodes. Three replicas within one data center; geo-replication optional.
