# Interview Q&A: Object Storage (S3)
**Date:** 2026-06-18

---

## How Interviewers Ask This

Opening:
- "Design Amazon S3"
- "Design a scalable object storage system"
- "How would you design a file storage service that can store billions of files?"
- "Design the storage backend for an image-hosting platform"

---

## Opening Questions

**Q1: What exactly is object storage? How is it different from a file system?**

> Object storage is a flat namespace of key-value pairs where values are arbitrary binary blobs. There's no directory tree — just `bucket/key`. Each object is immutable once written (update = new version). Access is via HTTP GET/PUT, not filesystem syscalls. This makes it infinitely horizontally scalable — you can shard by key without complex tree rebalancing.
>
> File systems have hierarchical directories, support random in-place writes, and have file descriptors. Object storage doesn't — which is actually a feature: immutability enables content-addressed caching and trivial replication.

**Q2: Walk me through what happens when I call `PUT /my-bucket/photo.jpg`**

> 1. API Gateway authenticates the request (HMAC-SHA256 signature of request headers against the user's secret key)
> 2. Upload Service receives the bytes, splits into 64MB chunks
> 3. For each chunk: compute SHA256 → content-addressed chunk_id
> 4. Consistent hash of chunk_id → pick 9 storage nodes (rack-aware, no two on same rack)
> 5. Apply Reed-Solomon(6,3): 6 data shards + 3 parity shards → write one shard to each of 9 nodes
> 6. Wait for all 6 data shard ACKs (minimum for durability)
> 7. Write metadata atomically: `(bucket, key) → [chunk_ids + node_ids]`
> 8. Return HTTP 200 + `ETag: {md5_of_object}`

**Q3: Why chunk at 64MB specifically?**

> Trade-off between overhead and parallelism.
> - Too small (1MB): metadata record per chunk → huge metadata volume, many small disk seeks
> - Too large (1GB): can't parallelize well, single node bottleneck for large objects
> - 64MB: one chunk fits in OS page cache nicely; allows good parallelism for objects larger than 64MB; metadata manageable
>
> This is a tunable parameter. S3 multipart parts are 5MB–5GB; Facebook Haystack uses even smaller (photos are ~100KB so one file = multiple tiny objects).

---

## Drill-Down: Storage Layer

**Q4: Why erasure coding over 3× replication?**

> 3× replication: 1 PB data → 3 PB storage. Can tolerate 2 simultaneous failures.
> Reed-Solomon 6+3: 1 PB data → 1.5 PB storage. Can tolerate 3 simultaneous failures.
>
> EC is more storage efficient and actually more fault-tolerant for same overhead. The cost is: degraded reads require fetching 6 shards + decoding (heavier than replication's "just read from another replica"). For hot objects, we could cache the reassembled object at the CDN edge — so degraded read cost only matters on cache miss.
>
> Facebook uses XOR-based 10+4 erasure coding for their cold storage (blob archive), while using 3× replication for hot storage. Match the trade-off to access pattern.

**Q5: What is content-addressed storage and why use it?**

> chunk_id = SHA256(chunk_bytes). Two objects with identical 64MB blocks share the same chunk on disk — automatic deduplication. Also: chunk_id is self-verifying — recompute SHA256 on read; if mismatch → data is corrupt.
>
> Downside: you can't update a chunk in-place. Any change = new chunk_id = new chunk. This is fine for object storage (objects are immutable). Problematic for databases (frequent small updates).

**Q6: How does the consistent hash ring handle a storage node being added or removed?**

> With V=150 virtual nodes per physical node, adding one node only moves ~1/N of chunks (moved from adjacent ring positions). Removing: chunks owned by the removed node redistribute to neighbors.
>
> For object storage this is gentler than cache (we don't need to re-fetch; we trigger a background rebalance job that moves affected chunks to their new homes). During rebalance, reads still work — old node serves until new node is confirmed.

---

## Drill-Down: Metadata

**Q7: Why SQL (PostgreSQL) for metadata, not a NoSQL store?**

> Metadata writes need to be atomic across multiple tables (objects, object_chunks, chunk_locations). A metadata commit must be all-or-nothing. SQL transactions give us this cleanly.
>
> Also: metadata has clear relational structure (bucket → objects → chunks → locations). SQL joins for "list all chunks for an object" or "find all objects in a bucket with prefix X" are natural.
>
> Cassandra would be a reasonable choice if we needed extreme write throughput with denormalized access patterns, but metadata write rate is manageable (115 writes/sec → trivial for Postgres). Shard by bucket_id when metadata outgrows single Postgres instance.

**Q8: What's the consistency model for reads-after-writes?**

> Default S3 as of 2020: strong read-after-write consistency. A successful PUT is immediately visible to GET.
>
> Implementation: metadata commit is the "visibility point." We write metadata only after all 6 data shards are confirmed. Any subsequent GET hits the metadata service, finds the record, reads the chunks. The metadata write is synchronous; no eventual consistency lag.
>
> If read hits a metadata replica that hasn't caught up yet → RYOW (Read-Your-Own-Writes) pattern: route first read after write to primary metadata node for 5 seconds (sticky primary window).

---

## Drill-Down: Access Control

**Q9: How does a pre-signed URL work? What are the security implications?**

> Pre-signed URL = temporary, stateless authorization token for a specific object + operation.
>
> Generation:
> ```
> canonical = "GET\nbucket\nkey\nexpires=1750000000"
> sig = HMAC_SHA256(user_secret_key, canonical)
> url = "/bucket/key?expires=1750000000&sig={sig}"
> ```
> Validation: server recomputes sig from request params. If match + not expired → allow. No DB lookup.
>
> Security implications:
> - Can't revoke early (stateless). If leaked, valid until expiry.
> - Use short TTLs (15 min for sensitive content, 7 days max for uploads)
> - Revocation requires Redis blocklist (adds 1ms lookup per request — trade-off)
> - Best practice: scope to specific IP if client IP is known

**Q10: How would you implement bucket-level access policies? (Like S3 bucket policies)**

> Bucket policy = JSON document attached to bucket stored in metadata DB.
> ```json
> {"Allow": [{"Principal": "user:alice", "Action": "s3:GetObject", "Resource": "bucket/*"}]}
> ```
> On every request:
> 1. Check if requester = bucket owner → full access
> 2. Evaluate bucket policy: does any statement match (principal, action, resource)?
> 3. Evaluate IAM policies attached to requester's identity
> 4. If any explicit Deny → deny. Otherwise, if Allow → allow.
>
> Cache: bucket policies change rarely → cache in Redis (invalidate on policy update via pub/sub). Typical evaluation: <1ms from cache.

---

## Drill-Down: Large File Uploads

**Q11: Walk me through a 100GB file upload.**

> Can't PUT 100GB in one request (API timeout, no resume on failure). Use multipart upload:
>
> 1. Initiate: `POST /bucket/big-file.tar → upload_id = "abc"`
> 2. Split file client-side into 200 × 500MB parts
> 3. Upload all 200 parts in parallel (N=10 concurrent threads): `PUT /bucket/big-file.tar?partNumber=1&uploadId=abc`
> 4. Server stores each part's chunks on storage nodes. Returns `ETag` per part.
> 5. On network failure: note which part failed. Re-upload just that part (resume).
> 6. Complete: `POST /bucket/big-file.tar?uploadId=abc {parts: [{1, etag1}, {2, etag2}, ...]}`
> 7. Server assembles chunk list in order, commits metadata atomically.
>
> Total upload time at 1Gbps: 100GB / 125MB/s = ~800s ≈ 13 minutes. With 10-parallel parts: ~80s ≈ 1.3 minutes.

**Q12: What happens if the client crashes mid-multipart?**

> Parts already uploaded remain on storage nodes. Incomplete uploads don't appear as objects. Client needs to:
> 1. `ListMultipartUploads` → find upload_id for interrupted upload
> 2. `ListParts?uploadId=abc` → see which parts already uploaded (don't re-upload those)
> 3. Re-upload missing parts → complete
>
> Server-side protection: `expires_at` on multipart_uploads (default 7 days). Background job aborts expired uploads and deletes orphaned parts. Prevents disk waste from forgotten incomplete uploads.

---

## Follow-Up: Scaling

**Q13: How would you scale the metadata service to handle 100B objects?**

> 100B objects × 500 bytes metadata = 50TB of metadata. Way beyond single Postgres.
>
> Options:
> 1. **Shard by bucket_id** (consistent hashing → Vitess). Each shard holds a subset of buckets. Cross-shard queries (e.g., "list all objects across all buckets") become scatter-gather — acceptable since that's a rare admin operation.
> 2. **Separate hot metadata (Redis cluster)** from cold (Postgres). object_chunks and chunk_locations are mostly read at object-access time. Hot objects cached in Redis. Cold objects → DB lookup on cache miss.
> 3. **Columnar metadata index** for listing: Elasticsearch index of (bucket_id, key, created_at) for `LIST /bucket?prefix=photos/` queries with fast prefix scan.
>
> Facebook Haystack: kept all needle metadata (offset + size in barrel file) in memory. 100B photos × 10 bytes per needle = 1TB → fit in distributed cache cluster.

**Q14: How do you achieve 11 nines durability?**

> 11 nines = 99.999999999% = lose at most 1 object out of 100 billion per year.
>
> Mechanisms stacked together:
> 1. EC 6+3: tolerate 3 simultaneous node failures. Individual node MTTF = ~years → 3 simultaneous failures extremely rare.
> 2. Rack-aware placement: even if entire rack fails (fire, power) → max 3 shards lost → still recoverable.
> 3. Continuous integrity scrubbing: detect and repair bit rot before it accumulates to unrecoverable state.
> 4. Cross-region replication (optional): replicate to 2nd region for region-level disasters.
> 5. Checksums: detect corruption before it causes silent data loss.
>
> Each layer independently adds ~2-3 nines. Combined: 11 nines is achievable. AWS's S3 durability guarantee is supported by 3 AZ replication — that's the cross-region (well, cross-AZ) layer above EC within each AZ.

---

## Follow-Up: Cost & Tiering

**Q15: How would you implement storage classes (like S3 STANDARD → GLACIER)?**

> Objects start on STANDARD (fast NVMe/SSD-backed HDDs). Lifecycle rules move them to cheaper tiers.
>
> Implementation:
> - `last_accessed` timestamp updated on each GET (async, via Kafka event — not on critical read path)
> - Background lifecycle scanner: daily job, evaluates each object against bucket's lifecycle rules
> - On transition to IA: move chunks from STANDARD nodes to INFREQUENT_ACCESS nodes (cheaper HDDs, denser packing). Update `chunk_locations` and `storage_class`.
> - On transition to GLACIER: move chunks to tape or cold object store. Retrieval = async job (3-5h to restore to STANDARD before download available).
>
> Trade-off: `last_accessed` update on every GET adds async write per read. At 11,600 reads/sec → 11,600 Kafka messages/sec (very manageable). Don't update synchronously on read path (would slow GET).

---

## Common Mistakes Interviewers Catch

| Mistake | Correct Approach |
|---------|-----------------|
| "Store objects in a SQL BLOB column" | SQL BLOB has size limits and terrible throughput. Use separate storage nodes. |
| "Use a single metadata DB for 1B objects" | Shard metadata by bucket_id. 500GB metadata → needs sharding. |
| "3× replication is fine for petabyte scale" | EC is 2× more storage efficient. At PB scale, savings justify added complexity. |
| "Just read from any storage node for GET" | Must locate which nodes have shards via metadata. Can't random-guess. |
| "Object key is a file path with real directories" | Keys are flat strings. Prefix = simulated directory. No tree structure. |
| "Pre-signed URL = full permanent access" | Pre-signed is time-limited and per-operation. Expires and can be scoped. |
| "Multipart upload can be resumed by re-uploading everything" | Only re-upload failed parts. ListParts to find what was already uploaded. |
| "Metadata commit happens before chunk writes" | Chunks must be durable first, THEN commit metadata. Reverse = object pointer exists but no data. |

---

## Quick Clarification Questions (Ask at Interview Start)

1. **Scale:** "How many objects and total data size? This determines sharding and EC vs replication choice."
2. **Durability vs cost:** "Is 11 nines durability a requirement, or is 9 nines + cheaper storage OK?"
3. **Access patterns:** "Read-heavy (images) or write-heavy (log ingestion)? Guides caching strategy."
4. **Consistency:** "Do we need strong read-after-write, or eventual consistency acceptable?"
5. **Multi-tenant:** "Single-company or multi-tenant? Determines access control complexity."
6. **Regions:** "Single-region or globally distributed? Determines cross-region replication need."
