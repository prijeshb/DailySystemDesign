# Failure Analysis: Google Drive System
## Failure-First Approach — All Components

**Date**: May 8, 2026  
**System**: Distributed File Storage & Sync  
**Method**: For each component → What fails? What's ahead/behind it? How do we detect, survive, recover?

---

## FRAMEWORK: THREE-DIMENSIONAL FAILURE ANALYSIS

For every component, ask:
1. **Component itself fails** → Internal failure
2. **Component AHEAD fails** (upstream: what sends data TO this component)
3. **Component BEHIND fails** (downstream: what this component sends data TO)

Plus for each: **Detection** → **Mitigation** → **Recovery** → **Prevention**

---

## COMPONENT 1: API GATEWAY

### 1A. API Gateway Itself Fails
**Scenarios**:
- Process crash (OOM, bug)
- Network interface failure
- All instances overloaded (thundering herd after deploy)

**Detection**: Load balancer health checks (HTTP /health every 5s, 3 failures = removed)

**Mitigation**:
- Run ≥3 instances across AZs; LB routes around failed instance in <15s
- Circuit breakers on downstream services (if Metadata Service is down, gateway returns 503 fast rather than queuing)
- Rate limiting protects against overload

**Recovery**: Auto-scaling group replaces failed instance. New instance is stateless (no local state) — it rejoins immediately.

**Prevention**: Blue/green deployments; canary releases (5% traffic to new version first).

**Data loss risk**: NONE — gateway is stateless, no durable data.

---

### 1B. Upstream (Client) Fails Mid-Request
**Scenarios**:
- Client disconnects after POST /initiate-upload but before uploading chunks
- Mobile app crashes mid-upload

**Impact**: "Zombie upload sessions" — upload_id is created in DB but never completed.

**Mitigation**:
- Upload sessions have TTL (24 hours). Background cleanup job marks expired sessions as ABANDONED.
- Partial chunk uploads to S3 are cleaned up via S3 Lifecycle rules (delete incomplete multipart uploads after 7 days).
- Client implements **exponential backoff + resume**: stores upload_id locally, resumes on next app open.

**Idempotency**: `POST /initiate-upload` can include a client-generated `idempotency_key`. If client retries, server returns the same `upload_id` — no duplicate session created.

---

### 1C. Downstream (Metadata Service) Fails
**Scenarios**:
- Metadata Service is down or slow (p99 latency spike)

**Mitigation**:
- Circuit breaker (Hystrix/Resilience4j): After 5 consecutive failures, gateway opens circuit → returns 503 with `Retry-After` header
- Timeout: 2s hard timeout on downstream calls; fail fast rather than pile up connections
- Health endpoint: Gateway checks Metadata Service /health before routing requests

**User impact**: Upload initiation fails; client shows "Service temporarily unavailable, will retry"

---

## COMPONENT 2: UPLOAD SERVICE

### 2A. Upload Service Itself Fails
**Scenarios**:
- Crash after client sends chunks directly to S3 but before POST /complete is processed
- Service restarts mid-completion transaction

**Impact**: Chunks exist in S3, but FileVersion record was never written to DB.

**Mitigation**:
- **Two-phase approach**: 
  1. Write `FileVersion` record with status=`PENDING` before returning upload_id
  2. POST /complete transitions status to `COMMITTED`
  3. Background reconciler finds PENDING records older than TTL → marks ABANDONED or retries
- **S3 strong consistency**: Since 2020, S3 offers read-after-write consistency — after chunk upload, Upload Service can immediately verify chunk existence.

**Recovery**: Reconciler runs every 5 minutes, finds stuck uploads, either completes or abandons them.

---

### 2B. Upstream (Client) Sends Corrupted Data
**Scenarios**:
- Bit flip in transit (rare with HTTPS, but possible)
- Malicious upload of corrupt data

**Mitigation**:
- Client computes MD5 of each chunk before upload; sends as `Content-MD5` header
- S3 validates MD5 on receive; rejects mismatches with 400
- At POST /complete: Upload Service recomputes SHA-256 of received chunks, compares against `checksum` in initiate-upload payload
- If mismatch: reject upload, mark upload_id as FAILED, inform client to re-upload

**Prevention**: End-to-end integrity: client-computed checksum verified at server. Network layer (TLS) also provides transport integrity.

---

### 2C. Downstream (S3) Fails During Chunk Upload
**Scenarios**:
- S3 service disruption in one region
- S3 throttling (SlowDown errors)
- Presigned URL expires before client uploads (if client was offline)

**Mitigation**:
- **Multi-region S3**: Primary region + replica. On primary failure, failover to secondary (with possible brief inconsistency window).
- **S3 throttling**: Client implements exponential backoff. Upload Service generates new presigned URLs with extended TTL if client requests refresh.
- **Expired presigned URL**: Client calls `POST /uploads/{upload_id}/refresh-urls`, gets new presigned URLs for missing chunks — the upload session remains valid.

---

## COMPONENT 3: OBJECT STORAGE (S3/GCS)

### 3A. Object Storage Itself Fails (Partial / Regional)

**This is the most catastrophic failure — all data lives here.**

**Scenarios**:
- AZ failure (one of 3 AZs goes dark)
- Regional disaster (fire, flood, total region loss)
- Accidental mass deletion (runaway script, ransomware)
- S3 bucket misconfiguration → public exposure

**Mitigation**:

| Failure Type | Mitigation |
|-------------|-----------|
| AZ failure | S3 automatically replicates across ≥3 AZs within region. Transparent failover. |
| Regional disaster | Cross-Region Replication (CRR): async replication to backup region. RPO = minutes. |
| Mass deletion | Object Lock (WORM — Write Once Read Many): Retention lock prevents deletion for N days. Versioning on bucket: delete creates a delete marker, old version survives. |
| Accidental bucket wipe | MFA Delete: requires 2-factor auth to permanently delete versions. Separate AWS account for backup bucket (blast radius containment). |
| Ransomware | Object Lock + WORM. Even if an attacker gains AWS credentials, locked objects cannot be overwritten during retention period. |

**Detection**:
- CloudWatch alarm: S3 5xx error rate > 0.1% → PagerDuty alert
- Replication lag alarm: CRR lag > 15 min → investigate
- Integrity audit: Background job randomly reads chunks and verifies SHA-256 matches stored checksum

**Recovery**:
- AZ failure: Automatic, no action needed. RTO ≈ 0.
- Regional disaster: DNS failover to secondary region (Route53 health checks + failover routing). RTO = 10–30 min. RPO = replication lag (typically < 5 min).

**Real-world example**: In 2017, AWS S3 US-East-1 outage caused widespread internet impact. Root cause: human operator error running maintenance command with wrong parameter. Lesson: **implement change control and dry-run mode for operational commands.** S3 has since added additional safeguards.

---

### 3B. Upstream (Upload Service) Sends Chunks Out of Order
**Scenarios**:
- Parallel chunk uploads arrive at S3 in different order than expected

**Non-issue**: S3 stores each chunk as a separate object keyed by `chunk_id` (SHA-256). Order is maintained in the `chunk_ids[]` array in the FileVersion record. Assembly order is the application's responsibility, not S3's.

---

### 3C. Downstream (Download Service) Cannot Reach S3
**Scenarios**:
- Network partition between Download Service and S3

**Mitigation**:
- Generate presigned URLs that clients use directly — Download Service is not in the hot path of the actual bytes transfer
- VPC endpoint for S3 (private connectivity, avoids public internet) — reduces partition risk
- CDN serves frequently-accessed files — CDN hit = no S3 call needed

---

## COMPONENT 4: METADATA SERVICE & DATABASE

### 4A. Metadata DB Primary Fails
**Scenarios**:
- PostgreSQL primary crashes
- Disk fills up
- Corruption on primary

**Mitigation**:
- **Synchronous streaming replication** to at least 1 replica (PostgreSQL `synchronous_commit = on`). No data loss on primary crash.
- **Automatic failover**: Patroni or AWS RDS Multi-AZ handles failover in ~30s.
- **Read replicas**: 2–3 read replicas serve read traffic (folder listings, file metadata). Primary only handles writes — reduces primary load.

**Trade-off**: Synchronous replication = every write waits for replica ACK → slightly higher write latency (+1–3ms). Alternative: asynchronous replication = lower latency but potential data loss on failover (RPO > 0).

**Detection**:
- Prometheus: `pg_up` metric drops → alert
- WAL lag on replicas > 1s → alert (replica falling behind)

**Recovery**:
- Failover: Patroni promotes replica to primary. DNS (via pg_bouncer) updated. Total RTO ≈ 30–60s.
- Data recovery: WAL archiving to S3 (continuous). Point-in-time recovery possible to any second in retention window.

---

### 4B. DB Shard for a Specific User Range Fails
**Scenarios**:
- One shard (covering users A-F by hash) has disk failure

**Impact**: Users on that shard cannot access metadata — cannot upload, download, or sync.

**Mitigation**:
- Each shard is itself replicated (primary + replicas) — same failover as above
- Shard rebalancing: if a shard is permanently lost, restore from WAL backup to new shard, re-route traffic

**Prevention**: Consistent hashing with virtual nodes — when adding/removing shards, only K/N data moves (not a full reshuffle). Tools: Vitess (MySQL), Citus (PostgreSQL).

---

### 4C. Quota Race Condition
**Scenario**: User uploads two large files simultaneously from two devices. Both pass quota check (used=14.5GB, limit=15GB, each upload is 0.6GB). Both complete → used = 15.7GB (over limit).

**Mitigation**:
```sql
-- Atomic quota reservation using SELECT FOR UPDATE
BEGIN;
SELECT used_bytes, limit_bytes FROM user_quotas
WHERE user_id = $1 FOR UPDATE;  -- row-level lock

-- Check if upload fits
IF used_bytes + upload_size > limit_bytes THEN
    ROLLBACK; -- reject upload
END IF;

-- Reserve quota
UPDATE user_quotas
SET used_bytes = used_bytes + upload_size
WHERE user_id = $1;
COMMIT;
```

**Alternative**: Use Redis atomic operations:
```
INCRBY quota:{user_id} upload_size_bytes
# If result > limit → DECRBY (rollback) and reject
```
Redis single-threaded commands are atomic — no race condition.

---

## COMPONENT 5: SYNC SERVICE & KAFKA

### 5A. Kafka Cluster Fails
**Scenarios**:
- Kafka broker crash
- Network partition between brokers (split brain)
- Topic partition leader election delay

**Impact**: Sync events are not delivered → clients don't learn about other devices' changes.

**Mitigation**:
- **Kafka replication factor = 3**: Each partition has 3 replicas across 3 brokers. Tolerate 2 broker failures.
- **min.insync.replicas = 2**: Producer waits for 2 replicas to ACK before success. If only 1 replica available, write fails → Upload Service retries → backpressure propagates to client.
- **Fallback sync**: Client can always poll `GET /sync/delta?since={cursor}` — Sync Service reads directly from DB sync_events table if Kafka is unavailable (DB is source of truth).

**Recovery**: Kafka self-heals with leader election (ZooKeeper or KRaft). RTO ≈ 10–30s for leader election.

---

### 5B. Client is Offline for Extended Period
**Scenarios**:
- Laptop shut down for 2 weeks, reconnects

**Impact**: Need to replay potentially thousands of sync events.

**Mitigation**:
- Sync events in DB retained for 30 days
- On reconnect: client sends its last cursor → Sync Service returns paginated delta (100 events per page)
- **Snapshot approach**: If cursor is older than 30 days, perform a full re-sync: client gets current file tree state directly from Metadata Service (not event log)

---

### 5C. Duplicate Sync Event Delivery (Kafka At-Least-Once)
**Scenario**: Kafka delivers the same sync event twice (consumer group rebalance, broker hiccup).

**Impact**: Client processes the same update twice.

**Mitigation**: Client-side idempotency — each sync event has a unique `event_id`. Client stores the highest processed `event_id` per event type per file. Duplicate event with lower `event_id` → silently dropped.

---

## COMPONENT 6: CDN

### 6A. CDN Node Fails (Edge POP Down)
**Mitigation**: CDN has 100s of edge nodes globally. Client's DNS resolves to nearest healthy POP. Failed POP → DNS stops resolving to it. RTO ≈ seconds (DNS TTL).

### 6B. CDN Serves Stale File After Update
**Scenario**: User updates a shared public file. CDN still serves old version.

**Mitigation**:
- File download URLs include version_id: `/download/{file_id}/{version_id}/file.pdf`
- Updating file creates new version_id → new URL → CDN miss → fresh fetch from S3
- Old URL (old version) may still be served from cache — this is correct behavior (versioned URL = immutable content)

**Trade-off**: Caching reduces S3 egress cost by 60–80% but means users must use the new URL to get new content. Acceptable for file storage (URLs are always fetched via API first).

### 6C. CDN is Used to Exfiltrate Private Data
**Scenario**: CDN caches a private file via a leaked presigned URL.

**Mitigation**:
- Private files: Never cached by CDN. Presigned URLs are short-lived (15 min) and contain signature that CDN cannot reuse.
- CDN configured to forward auth headers / not cache responses with `Cache-Control: no-store`
- Signed URLs for CDN (CloudFront signed URLs): Include expiry + IP restriction

---

## COMPONENT 7: CLIENT SYNC AGENT

### 7A. Sync Agent Gets Into an Infinite Loop
**Scenario**: File change → upload → sync event → download → file change (write triggers another sync) → infinite loop.

**Mitigation**:
- Sync agent computes checksum of file before and after write. If checksum unchanged → skip upload.
- Track "last write was by sync agent" flag. Changes made by sync agent do not trigger re-upload.

### 7B. Sync Conflict Causes Data Loss
**Scenario**: Conflict resolution deletes one version without user knowing.

**Mitigation**:
- NEVER delete automatically. Always create conflict copy.
- All conflicted versions are preserved in version history.
- User is notified: "A conflict was found. A copy was saved as 'filename (conflict)'."

### 7C. Local Disk Full — Cannot Sync
**Scenario**: Sync downloads a file but local disk is full → partial write → corrupt local file.

**Mitigation**:
- Sync agent checks available disk space before starting download
- Writes to temp file first; atomic rename only on successful complete download
- Selective sync: user can choose which folders to sync locally (reduces disk usage)

---

## FAILURE SCENARIO MATRIX

| Component | Internal Failure | Upstream Failure | Downstream Failure | Data Loss Risk |
|----------|-----------------|-----------------|-------------------|---------------|
| API Gateway | Stateless; LB reroutes | Client disconnect → zombie session | Circuit breaker; 503 response | None |
| Upload Service | Two-phase write; reconciler | Idempotent retry with upload_id | S3 throttling; retry + backoff | None (with reconciler) |
| Object Storage | CRR + WORM + versioning | Corrupt data rejected (MD5/SHA) | CDN serves from cache on S3 hit | Near-zero (11 nines durability) |
| Metadata DB | Sync replication; auto-failover | App-level retry | Read replica lag | Near-zero (WAL archiving) |
| Kafka | RF=3; fallback to DB poll | Backpressure to upload service | Client idempotent dedup | Minimal (sync delay) |
| CDN | DNS failover to other POP | Versioned URLs prevent stale | S3 origin fetch on miss | None |
| Sync Client | Atomic writes; checksum check | Offline queue; resume on reconnect | Conflict copy; never auto-delete | None |

---

## OPERATIONAL RUNBOOKS

### Runbook: Storage Usage Spike Alert
```
TRIGGER: Used storage in region X grows >20% in 1 hour

1. Check if it's legitimate (bulk corporate upload event?)
2. Identify top uploaders: SELECT user_id, SUM(size_bytes) FROM file_versions
   WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY 1 ORDER BY 2 DESC LIMIT 10
3. Check for abuse (same file uploaded N times → dedup should handle; check why not)
4. If runaway process: rate limit the offending user_id; notify account team
5. If legitimate: ensure CRR replication lag is acceptable; scale upload service pods
```

### Runbook: Sync Service Lag Alert (Kafka Consumer Lag > 100K)
```
TRIGGER: Kafka consumer group "sync-delivery" lag > 100,000 messages

1. Check Sync Service pod count and CPU → scale out if at limits
2. Check for poison messages: one malformed event blocking processing
   → kafka-consumer-groups.sh --describe; look for stuck partition
3. If poison message: skip the offset (after investigation): 
   kafka-consumer-groups.sh --reset-offsets --shift-by 1 --topic user-changes --partition N
4. Check DB: sync_events insert rate — is there an event storm from one user?
5. Alert: user-visible sync delay is now >60s; post status page update
```

