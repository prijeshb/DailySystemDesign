# Dropbox — Failure Analysis
**Date:** 2026-07-03  
**Approach:** Failure-first — every component fails; what happens upstream/downstream?

---

## Failure Map

```
Client → API Gateway → Metadata Service → Postgres (primary/replica)
                    → Block Service    → S3
                    → Sync Service     → Redis pub/sub
```

---

## 1. Client-Side Failures

### 1a. Upload Interrupted Mid-Chunk
**What fails:** Network drops during `PUT /chunks/{sha256}` to S3  
**Impact:** Partial chunk in S3 (incomplete upload)

**Prevention:**
- S3 multi-part upload: split each 4MB chunk into smaller S3 parts; S3 handles partial-part resumption
- OR: client detects interrupt, retries full chunk PUT (idempotent — SHA-256 keyed, same bytes = same result)
- S3 `AbortIncompleteMultipartUpload` lifecycle rule: auto-delete stale incomplete uploads after 24h

**Recovery:** Client retries with same SHA-256 key → S3 overwrites cleanly. No data corruption.

---

### 1b. Client Crashes Between Upload and Commit
**What fails:** All chunks uploaded to S3 but `POST /files/commit` never sent  
**Impact:** Chunks exist in S3, but no FileVersion row in Postgres → file appears unchanged to other devices

**Prevention:**
- Client maintains local state file: `{ upload_session_id, chunk_hashes, uploaded_at }`
- On restart: detect incomplete session → re-send `/files/commit` (idempotent: same chunk_list = same version hash)

**Recovery:** Client restart → checks pending commit log → replays commit. Server: if FileVersion already exists with same chunk_list → return existing version (no-op). Safe.

---

### 1c. Two Devices Edit Same File Offline
**What fails:** Version conflict at commit time  
**Impact:** Silent data loss (if not handled) or duplicate files (conflict copy approach)

**Prevention (Dropbox approach):**
```
Server: compare submitted base_version with current_version
  base_version matches → commit as new version → success
  base_version outdated → reject → client creates conflict copy
```

**Recovery:** Server returns 409 Conflict → client saves its version as `{name} ({device} conflicted copy {date}).ext` → both versions visible to user → user resolves manually

**Deeper failure:** What if conflict copy commit also fails? → retry from client local state. Conflict copy is just a new file, no version conflict possible.

---

## 2. API Gateway Failures

### 2a. API Gateway node crash
**Upstream:** Client gets connection refused or timeout  
**Downstream:** Metadata / Block / Sync Services unaffected

**Prevention:** 
- Multiple API Gateway nodes behind load balancer
- Client: exponential backoff + retry (3 attempts, 1s/2s/4s)
- Circuit breaker on client: after 5 failures → queue changes locally, stop retrying for 30s

**Recovery:** Load balancer health check (every 5s) detects crash → stops routing to dead node → failover in <10s. Client retries succeed.

---

### 2b. API Gateway overload (thundering herd at start of day)
**Impact:** All 10M DAU reconnecting at same time → 10M sync requests spike

**Prevention:**
- Jittered reconnect: client waits `random(0, 60)` seconds before connecting on startup (not instant)
- Rate limiting at gateway: per-user 10 RPS, global 50K RPS (shed excess with 429)
- Backoff queue: rejected clients retry with exponential backoff

---

## 3. Metadata Service Failures

### 3a. Metadata Service instance crash
**Upstream effect:** Clients can't commit files, can't list changes → sync stalls  
**Downstream effect:** Postgres unaffected; Sync Service still receives changes already committed

**Prevention:**
- N=3 Metadata Service instances behind load balancer
- Stateless: any instance can handle any request (all state in Postgres)
- k8s auto-restart on crash (< 30s recovery)

**Recovery:** LB removes crashed instance → traffic routes to remaining 2 → client retries. Zero data loss (Postgres is source of truth).

---

### 3b. Postgres Primary Failure
**What fails:** Writes to file metadata (commits, deletes, shares) fail  
**Reads:** Serve from replica (replica lag: typically <1s)

**Timeline:**
```
Primary dies → Replica promotes (RDS Multi-AZ: ~30-60s automatic failover)
During failover: all writes fail → clients queue changes locally
After failover: new primary accepts writes → clients flush queue
```

**Client behavior during Postgres down:**
- Local file watcher still runs; changes queued in local SQLite
- Sync paused indicator shown to user
- Retry uploads every 30s until success

**Prevention:**
- RDS Multi-AZ (synchronous replication to standby): RPO=0, RTO=30-60s
- Clients must tolerate sync being paused (design for eventual consistency)

---

### 3c. Postgres Replica Lag (during high write burst)
**What fails:** Client reads stale change list → downloads already-synced chunks again

**Impact:** Extra network download (wasted bandwidth, not data loss). Idempotent: chunk already exists locally → skip write.

**Prevention:** Sync cursor is server-assigned monotonic ID → even with replica lag, client won't miss events (lag just delays notification). No data corruption.

---

## 4. Block Service Failures

### 4a. Block Service crash during pre-sign
**What fails:** Client can't get S3 pre-signed URL → upload blocked  
**Impact:** Sync paused for file

**Prevention:** N=3 Block Service replicas (stateless). Client retries → hits healthy instance.

---

### 4b. S3 Regional Outage
**What fails:** All chunk uploads and downloads fail for the affected region  
**Impact:** Sync completely halted for users in that region

**This is the worst-case for Dropbox core functionality.**

**Prevention:**
- Multi-region S3 replication: primary bucket (us-east-1) + replica (eu-west-1)
- Read fallback: if primary S3 unavailable → serve chunks from replica
- Upload fallback: route to replica region temporarily
- Cost: 2× storage cost + replication bandwidth

**Recovery:** S3 regional outage historically rare (hours). Users in unaffected regions continue normally. Affected region: clients queue locally until S3 recovers.

**Trade-off:** 2× storage cost vs. availability SLA. Dropbox Business/Enterprise: worth it. Free tier: accept single-region risk.

---

### 4c. S3 pre-signed URL expires before upload
**What fails:** Client gets 403 from S3 on PUT → upload rejected  
**Cause:** Client was slow (large file, slow network), 15-min URL expired

**Prevention:**
- For large files: multipart S3 upload with separate pre-signed URLs per part
- Client detects 403 → re-request new pre-signed URL from Block Service → retry from last successful part
- URL TTL = 30 min for large files (> 100MB)

---

## 5. Sync Service Failures

### 5a. WebSocket Server Crash
**Upstream:** Connected clients lose push notifications  
**Impact:** Clients don't hear about new changes until they reconnect or poll

**Prevention:**
- Client: detect WebSocket disconnect (ping/pong timeout 30s) → reconnect with exponential backoff
- Reconnect: re-establish WebSocket → send `last_sync_cursor` → server replays all missed changes
- Fallback: if WebSocket fails 3× → switch to polling (GET /changes every 30s)

**Key property:** Sync Service is not stateful for changes (changes in Postgres/change log). Crash doesn't lose data; only loses in-flight pushes. Cursor catch-up handles everything.

---

### 5b. Redis pub/sub failure (change notification lost)
**What fails:** File changed on Device A → Sync Service can't publish to Redis → Device B not notified  
**Impact:** Device B misses push notification → delays sync until next poll

**Prevention:**
- Redis Sentinel (3 nodes): auto-failover in ~30s
- During Redis down: Sync Service falls back to polling notification (periodic sweep of recent changes, broadcast to connected WebSockets)
- Polling fallback: every 30s, each client independently polls `/changes?since={cursor}` → catches all missed events

**Trade-off:** During Redis outage, sync latency degrades from ~1s (push) to ~30s (poll). Acceptable — no data loss, just latency.

---

### 5c. Notification lost in Redis (fire-and-forget pub/sub)
**What fails:** Redis published the event but subscriber (Sync Service) not ready → event dropped  
**Impact:** Specific device misses push → waits for next poll

**Prevention:**
- Redis pub/sub is fire-and-forget — accepted trade-off  
- Guarantee: even if push is lost, client polling (`GET /changes?since=cursor`) catches up within 30s
- For critical notifications (share invite, storage full): use durable queue (Kafka/SQS) not pub/sub

---

## 6. Cross-Component Failure: Partial Commit

**Scenario:** FileVersion written to Postgres, but change event not published to Sync Service

```
BEGIN TRANSACTION
  INSERT file_versions (...)
  INSERT changes (...)  ← change_id=9421
COMMIT
                         ← crash here
PUBLISH to Redis: change_id=9421  ← never happens
```

**Impact:** Other devices never get push notification for change_id=9421

**Prevention:** Change is in Postgres (changes table). Fallback polling will catch it.  
OR: Outbox pattern — include Redis publish in transactional outbox. Background publisher reads unpublished outbox rows → publishes → marks sent.

**Recovery:** Client polls every 30s → `GET /changes?since=9420` → change_id=9421 appears → syncs. Max delay: 30s.

---

## 7. Storage Quota Enforcement Failure (TOCTOU)

**Scenario:** User at 9.9 GB uploads 500MB file. Two concurrent uploads could both pass quota check.

```
Upload A: READ used_bytes=9.9GB → check: 9.9 + 0.5 = 10.4 > 10 → DENY (correct)
Upload B (concurrent): READ used_bytes=9.9GB → same check → DENY (correct)

But if both see 9.5GB:
Upload A: READ 9.5 → check: 9.5 + 0.5 = 10 ≤ 10 → ALLOW
Upload B: READ 9.5 → check: 9.5 + 0.5 = 10 ≤ 10 → ALLOW  ← over-quota!
```

**Prevention (TOCTOU fix):**
```sql
UPDATE users
SET used_bytes = used_bytes + 500_000_000
WHERE id = $1
  AND used_bytes + 500_000_000 <= quota_bytes;  -- atomic check + update

-- rows_affected = 0 → quota exceeded → reject upload
-- rows_affected = 1 → proceed with upload
```

**Also:** S3 object tagging with user_id → bucket policy to enforce hard limits at storage layer as backup.

---

## 8. Summary — Failure Severity Matrix

| Component | Failure | Impact | Recovery |
|-----------|---------|--------|----------|
| Client | Crash mid-upload | Paused sync | Retry on restart (idempotent) |
| Client | Offline conflict | Version conflict | Conflict copy (no data loss) |
| API Gateway | Node crash | <10s disruption | LB failover |
| Metadata DB | Primary fail | 30-60s write pause | Multi-AZ failover |
| S3 | Regional outage | Full sync halt | Multi-region replica |
| Redis | Pub/sub fail | 30s sync delay | Polling fallback |
| Sync Service | WS crash | Lost push | Client reconnect + cursor replay |
| Block Service | Crash | Retry on another instance | Stateless, LB routes around |
