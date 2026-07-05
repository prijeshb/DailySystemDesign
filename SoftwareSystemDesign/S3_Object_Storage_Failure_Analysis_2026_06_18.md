# Failure Analysis: Object Storage (S3)
**Date:** 2026-06-18  
**System:** S3_Object_Storage_System_Design_2026_06_18.md

---

## Failure Map (All Paths)

```
Client → API Gateway → Upload Service → Storage Nodes
                     → Metadata Service
                     → Download Service → Storage Nodes
```

For each component: what if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?

---

## 1. Storage Node Failure

### Scenario A: Single Node Goes Down (During Read)
**Impact:** Chunk shard on that node is unavailable.

**With EC 6+3:** Only 6 shards needed. 1 node down → 8 remaining shards → still 6+ available → transparent recovery.
- Download service fetches 6 of 8 available shards (skips the down node)
- No client-visible error. Latency may increase slightly (must wait for 6 responses from 8 nodes vs 6 nodes)

**Detection:** Health checker pings each node every 10s via HTTP `/health`. After 3 failures → mark node DEGRADED in cluster state (stored in etcd/Consul).

**Prevention:** Background "reconstruction job" triggers when a node is marked DEGRADED:
```
For each chunk shard on DEGRADED node:
  Fetch remaining 6+ available shards
  Decode → reconstruct missing shard
  Write to a healthy replacement node
  Update chunk_locations in metadata DB
```
Priority: reconstruct first the objects with fewest surviving shards (most at-risk).

### Scenario B: Node Goes Down During Write
**Impact:** One of the 9 target nodes for a new chunk is unavailable.

**Fix:** During write, select from the consistent hash ring's next available node (skip DEGRADED nodes). Write to 9 available nodes that exclude DEGRADED ones. If not enough healthy nodes → return 503 with `Retry-After`.

**Trade-off:** Adding node avoidance to write path adds complexity to shard placement.

### Scenario C: Node Fails Permanently (Disk Dead)
**Impact:** All chunks on that node permanently lost their shard.

**Detection:** Node stops responding; manual confirmation of hardware failure.

**Recovery:**
1. Mark node DEAD in cluster state
2. Full reconstruction sweep: enumerate all chunk_locations WHERE node_id = DEAD_NODE
3. Reconstruct each missing shard from remaining 6+ shards
4. Write to new node; update chunk_locations

**Time:** 1M chunks × 64MB × reconstruct time ≈ hours. During reconstruction period, affected chunks have only (9-1)=8 or fewer shards → reduced redundancy. Alerting: if any chunk drops below 6 healthy shards → critical alert.

---

## 2. Storage Node Behind Upload Service Fails

### Node Fails Mid-Write (After Some Shards Written)
**Impact:** Some EC shards written, some not. Write cannot complete with only 5 shards.

**Fix:**
1. Upload Service times out waiting for shard ACK (timeout: 5s per shard)
2. Write is declared failed
3. Upload Service sends DELETE to nodes that already received shards (cleanup)
4. Metadata DB: no entry created (write never committed) → object doesn't exist

**Client sees:** HTTP 500 or 503 → retries with same idempotency key → fresh attempt on new node set.

**Edge case: cleanup DELETE also fails**
- Orphaned shard on node: periodic garbage collector (runs nightly)
- Scan nodes for chunks with no metadata entry → delete
- Or: TTL-based: every shard written gets a 24h "pending" TTL; if metadata commit doesn't happen, shard auto-expires

---

## 3. Metadata Service Failure

### Scenario A: Metadata DB Primary Goes Down
**Impact:** No new objects can be written (metadata commit fails). Reads may still work if chunk_locations cached in Redis.

**With read replica cache:**
- Read path: metadata Redis cache hit → reads continue serving (from cache, TTL 60s)
- Reads for uncached objects fail → HTTP 500

**Failover:** Postgres primary failure triggers automatic failover to replica (Patroni / AWS RDS Multi-AZ). Time: 30-60s. During failover window: writes return 503.

**Client behavior during 30-60s outage:**
- Writes: retry with exponential backoff (1s, 2s, 4s...) up to 60s
- Reads: serve from Redis cache if hit (stale by up to 60s TTL — acceptable for most objects)

**Prevention:**
- Synchronous replica for metadata DB (can't lose object pointers)
- PgBouncer connection pooling in front to handle reconnection transparently

### Scenario B: Redis Cache Goes Down
**Impact:** All reads must hit metadata DB directly. DB read load spikes.

**Fix:**
- Metadata DB is sized for direct read traffic (read replicas for load balancing)
- Redis failure: circuit breaker → bypass cache → read from DB
- DB read replicas provide horizontal read scaling
- Alert triggered: SRE restores Redis from snapshot within minutes

**Trade-off:** Cache miss storm → DB load increases 10×. Mitigation: connection pool limits per replica prevent DB from being overwhelmed (reject at 80% capacity → 429 back to client).

### Scenario C: Metadata Corruption (Bug in Version Commit)
**Impact:** Object pointer exists in DB but chunk_locations missing or wrong → GET returns 404 or corrupt data.

**Prevention:**
- Write-ahead log on metadata updates
- Two-phase commit for metadata writes: "tentative" → validate chunk locations exist → "committed"
- Read-time integrity check: if chunk SHA256 doesn't match stored chunk_id → log error + return 503

---

## 4. API Gateway Failure

### Gateway Crashes
**Impact:** All traffic stops.

**Fix:** Multiple gateway instances behind a load balancer (AWS ALB or Nginx). Health check removes failed instance in <30s. At least 3 instances; N+1 capacity.

### Gateway Overloaded (Too Many Concurrent Uploads)
**Impact:** Latency spikes, connections queued.

**Fix:**
- Rate limiting per user (e.g., max 1000 req/min per access key)
- Separate request queues for uploads (slow, large) vs metadata reads (fast, small)
- Bulkhead: upload thread pool isolated from read thread pool (see Concept #65)

---

## 5. Client Upload Failure (Mid-Upload)

### Client Disconnects During Multipart Upload
**Impact:** Parts uploaded so far sit on storage nodes. Upload never completes. Disk space wasted.

**Fix:**
- Multipart uploads have `expires_at` timestamp (default: 7 days)
- Background job: `SELECT * FROM multipart_uploads WHERE expires_at < NOW()` → abort → delete all uploaded parts
- Client should use `ListMultipartUploads` to find and resume incomplete uploads

**Resume:** Client re-initiates the same upload, but with existing upload_id. Server returns list of uploaded parts. Client only re-uploads missing parts.

### Client Uploads Corrupt Data
**Impact:** Corrupt bytes stored durably (S3 stores what you give it).

**Fix:** ETag (MD5) returned with each PUT. Client can verify ETag matches expected MD5.
```
# Client-side validation:
local_md5 = md5(file_bytes)
PUT /bucket/key
Content-MD5: {base64(local_md5)}     ← server rejects if mismatch
< ETag: "abc123..."                   ← verify matches local_md5
```
Server computes MD5 during write; if `Content-MD5` header present → verify before committing → return 400 on mismatch.

---

## 6. Concurrent Writes to Same Key

### Two Clients PUT /bucket/key Simultaneously
**Impact (without protection):** Last write wins. Depends on race.

**S3 behavior (eventual consistency model):**
- Two concurrent PUTs: whichever metadata commit lands second wins
- Between the two commits: reader may see either version

**Stronger consistency option (conditional writes):**
```
PUT /bucket/key
If-None-Match: *     → only succeed if key doesn't already exist
If-Match: "etag123"  → only succeed if current version matches etag (optimistic lock)
```
Implementation: Postgres `UPDATE objects SET ... WHERE etag = $expected AND key = $key RETURNING *` → 0 rows = conflict → HTTP 412 Precondition Failed.

**Trade-off:** Without conditional writes → simplest, highest throughput, eventual consistency. With conditional writes → serialized updates on same key, lower write throughput for hot keys.

---

## 7. Network Partition (Between Regions)

### Cross-Region Replication Fails
**Impact:** Secondary region has stale objects. If primary region goes down → secondary serves stale data.

**Detection:** Replication lag metric: `lag_bytes = primary_write_lsn - secondary_applied_lsn`. Alert if lag > 1GB or > 5 minutes.

**Recovery:**
- Primary comes back → replication catches up automatically
- If primary permanently lost: promote secondary with `max lag = data loss window`
- RPO (Recovery Point Objective) = replication lag at time of failure

**Trade-off:** Synchronous cross-region replication = zero data loss + high write latency (100ms+ round trip). Asynchronous = low write latency + potential data loss on region failure. Most systems choose async with acceptable RPO.

### Read-After-Write Across Regions
**Scenario:** User writes in us-east-1. Immediately reads from ap-south-1 (different region). Reads stale or 404.

**Fix:**
- Route read to same region as write (use session affinity / GeoDNS pinning)
- Or: metadata service with cross-region "write pointer" — any region can ask "where is this object?" → redirect to authoritative region

---

## 8. Disk Failure (Silent Data Corruption)

### Bit Rot: Disk Returns Wrong Bytes Silently
**Impact:** Object read returns corrupt data. No disk error reported.

**Fix:**
- Checksums stored with each chunk: `chunk_checksum = SHA256(shard_bytes)`
- On every read: recompute SHA256 → compare to stored → if mismatch → use another shard (EC recovery)
- Periodic scrubbing (background): read every shard, verify checksum → repair if corrupt

**Real-world:** Google and Facebook both run continuous integrity scrubbing jobs. Bit rot rate: ~0.001%-0.01% per year per TB. At PB scale: significant number of corruptions per year without scrubbing.

---

## 9. Pre-Signed URL Leakage

### Pre-signed URL Shared Beyond Intended Recipient
**Impact:** Anyone with URL can access private object until expiry.

**Mitigation (built-in):**
- Short expiry (5-15 minutes for sensitive objects)
- Include IP binding: `X-Source-IP: {client_ip}` in canonical string → URL only works from that IP

**Revocation (if needed):**
- Cannot revoke stateless HMAC URL without server-side blocklist
- Solution: track `jti` (unique URL ID) in Redis SET with TTL = URL expiry
- On access: check `SISMEMBER revoked_presigned_urls {jti}` → reject if present
- Trade-off: adds Redis lookup to every pre-signed URL access (1ms)

---

## Failure Summary Table

| Component | Failure | Detection | Recovery | Prevention |
|-----------|---------|-----------|----------|------------|
| Storage node | Node crash | Health check (10s) | EC reconstruction | Rack-aware placement |
| Storage node | Silent bit rot | Periodic scrub | Read alternate shard | SHA256 per-shard checksum |
| Upload Service | Crash mid-write | Client timeout | Client retry, orphan cleanup | Two-phase write + cleanup job |
| Metadata DB | Primary down | Patroni/RDS HA | Auto-failover (30-60s) | Sync replica |
| Metadata Redis | Cache down | Circuit breaker | Serve from DB | Read replicas on DB |
| API Gateway | Crash | LB health check | Route to other instance | N+1 instances |
| Multipart upload | Client disconnects | expires_at TTL | Abort + cleanup job | 7-day TTL on uploads |
| Cross-region | Network partition | Replication lag metric | Catch-up on restore | Async replication + RPO SLA |
| Concurrent write | Race on same key | N/A (by design) | Last-write-wins or 412 | Conditional writes (If-Match) |
