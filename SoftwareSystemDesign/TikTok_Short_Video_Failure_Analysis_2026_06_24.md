# TikTok Short Video — Failure Analysis
> Date: 2026-06-24 | Failure-first: component + upstream + downstream

---

## Component Map

```
Client → [API Gateway] → [Upload Service] → [S3 raw] → [Transcoding Queue] → [CDN]
                      ↘ [Feed Service] → [Recommender] → [Redis FYP cache]
                      ↘ [Interaction Svc] → [Kafka] → [Counter Svc / ClickHouse]
                                                     → [Notification Svc]
```

---

## 1. Recommendation Engine Failure

### Scenario A: Recommender Service Down

**What fails:**
- `GET /fyp` returns error → user sees blank feed on app open

**Upstream (Feed Service):**
- Feed Service calls Recommender with 50ms timeout → timeout → needs fallback

**Downstream (no impact):**
- Watch events still flow to Kafka (not dependent on Recommender)

**Solutions:**
1. **Pre-computed FYP cache in Redis** (primary defense)
   - Feed Service reads from `FYP:{user_id}` key directly; Recommender only refreshes it
   - If Redis hit: serve cached candidates, score lightweight in Feed Service
   - If Redis miss AND Recommender down: fall through to trending feed

2. **Trending feed fallback**
   - Pre-computed: top 200 trending videos per region, updated every 5 min
   - Stored in Redis: `trending:{region}` → always available (no ML dependency)
   - Personalization: none, but user still sees content

3. **Circuit breaker on Recommender**
   - 5 failures in 10s → open circuit → skip Recommender, serve trending
   - Every 30s: probe Recommender → if healthy → close circuit, resume ML feed

**Degraded behavior:** User gets trending/popular feed instead of personalized. Engagement drops ~20% (internal estimate from Netflix similar scenario). Acceptable short-term.

---

### Scenario B: Redis FYP Cache Down

**What fails:**
- Pre-computed candidates unavailable
- Feed Service has no candidates to rank

**Solutions:**
1. **Redis Sentinel / Redis Cluster** (primary HA)
   - 3-node sentinel: automatic failover in < 30s
   - Cluster mode: data partitioned, one shard down = only some users affected

2. **Fallback DB pull**
   - Cassandra: `SELECT video_id FROM recent_popular WHERE region = ? LIMIT 500`
   - Higher latency (~50ms vs ~5ms) but functional
   - Add circuit breaker: if Redis flaky → use Cassandra path temporarily

3. **Local cache in Feed Service**
   - Each Feed Service instance caches last-served FYP per user in memory (LRU, 50MB)
   - On Redis miss: serve from local cache (stale but non-empty)
   - TTL: 5 min → acceptable staleness

---

## 2. Upload Service Failure

### Scenario A: Upload Service Pod Crash (mid-upload)

**What fails:**
- Client's HTTP connection drops mid-chunk
- Chunks 1-3 already uploaded to S3, chunks 4-10 lost

**Solution: Resumable upload protocol**
```
Client: GET /upload/{upload_id}/status
Server: returns { last_received_chunk: 3, total_chunks: 10 }
Client: resume from chunk 4
```
- Upload state in Postgres: `upload_chunks(upload_id, chunk_no, s3_etag, created_at)`
- S3 multipart upload ID is stored → can be resumed across server restarts
- TTL: incomplete uploads auto-aborted after 24h (S3 lifecycle policy + Postgres cleanup job)

### Scenario B: S3 Unavailable

**What fails:**
- Cannot store raw video
- Upload Service returns 503

**Solutions:**
1. **S3 regional failover**
   - Upload to us-east-1 primary; if unavailable → upload to us-west-2
   - S3 cross-region replication ensures Transcoding Service can pick up from either region

2. **Queue uploads locally** (risky, short-term)
   - Local disk buffer on Upload Service: accept upload, respond 200, retry S3 async
   - Risk: if Upload pod dies before S3 write → lost video
   - Mitigation: use EBS (persistent disk) not ephemeral
   - Max buffer: 50GB per pod (50 concurrent uploads × 1GB average raw)

3. **Multi-cloud storage provider**
   - Primary: AWS S3. If S3 unavailable → failover to Google Cloud Storage (GCS) or Azure Blob Storage
   - Upload Service uses a pluggable storage backend; health-check each provider before write; route to healthy one
   - Trade-off: cross-cloud egress costs (~$0.08/GB) + operational complexity of maintaining two SDKs, two IAM configs, two billing accounts — justified only when S3's 99.99% availability SLA is insufficient
   - Real-world: most hyperscale companies keep S3 as primary and GCS as cold warm-standby (replicated async), rather than active-active dual writes (doubles ingress cost + dual-write consistency complexity)

4. **User-facing:** Show spinner / retry message — do not confirm upload until S3 durably written

---

## 3. Transcoding Service Failure

### Scenario A: Transcoding Workers Down

**What fails:**
- Videos uploaded but not processed
- `video.status = PROCESSING` indefinitely
- Creator cannot see their video (never PUBLISHED)

**Solutions:**
1. **Kafka as durable queue** (primary)
   - `video.uploaded` events stay in Kafka indefinitely (7-day retention)
   - When workers recover: replay from last committed offset
   - Zero work lost — Kafka is the source of truth for pending jobs

2. **Dead Letter Queue (DLQ)**
   - If transcoding fails 3 times (malformed video, codec unsupported):
     → Move to `video.transcoding.dlq` Kafka topic
     → Alert ops team + notify creator "video could not be processed"
   - Do not retry infinitely (corrupted input won't fix itself)

3. **Job timeout**
   - Worker claims job: `SET transcoding_lock:{video_id} worker_id EX 300`
   - If worker dies mid-job: key expires after 5 min → another worker picks up
   - Idempotent: transcoding same video twice → same output (deterministic)

### Scenario B: Transcoding Queue Backlog (flash upload event)

**What fails:**
- Creator event (challenge launches): 10M uploads in 1 hour
- Transcoding queue builds up → videos take 6+ hours to publish

**Solutions:**
1. **Auto-scaling transcoding workers**
   - K8s HPA on Kafka consumer lag metric
   - Lag > 10K messages → scale out workers (up to 500 GPU pods)
   - AWS Spot instances for GPU (70% cheaper, acceptable for async jobs)

2. **Priority queue**
   - Large creators (>1M followers): higher transcoding priority → faster publishing
   - Normal users: best-effort FIFO

3. **Creator feedback**
   - Show progress: "Your video is in queue, position 4,523"
   - Set expectation: "Usually published within 2 hours during peak"

---

## 4. CDN Failure

### Scenario A: CDN Node Failure (Regional PoP Down)

**What fails:**
- Users in region see video buffering / failed loads

**Solutions:**
1. **Multi-CDN routing**
   - Primary: ByteDance CDN (cheapest, most capacity)
   - Secondary: Akamai or CloudFront
   - DNS-based failover: if primary CDN health check fails → switch to secondary (30-60s propagation)
   - Anycast routing: BGP withdraws the failed PoP's IP → traffic re-routes automatically

2. **Origin fallback**
   - If CDN miss → request falls through to S3 origin
   - S3 has high availability (11 nines durability)
   - Latency: much higher (100-300ms vs 5ms CDN), but functional

3. **Client retry logic**
   - Video player: if first CDN URL fails → retry with secondary CDN URL
   - Pre-signed URLs for both CDNs embedded in API response

### Scenario B: Viral Video CDN Stampede

**What fails:**
- A video goes viral: 10M concurrent viewers → CDN edge cache cold → all requests go to origin simultaneously → origin overloaded

**Solutions:**
1. **Request coalescing** (see Concepts Index #45)
   - CDN edge: on cache miss, only ONE request goes to origin; all concurrent requests wait for that one response
   - Thundering herd reduced from 10M to 1 per edge node

2. **Proactive CDN warming**
   - Monitoring: if view velocity > 10K/min → push to all edge nodes immediately
   - Pre-computed: trending detection runs every 5 min → push top 1K videos to all edges

3. **S3 origin rate limiting**
   - S3 Transfer Acceleration + Cloudfront Origin Shield (single point between CDN and S3)
   - Origin Shield absorbs coalesced requests → protects S3 from stampede

---

## 5. Kafka Failure (Watch Events)

### What fails:
- Watch events not flowing → ML signals lost → FYP degrades
- Counter Service not receiving likes → stale counts

### Solutions:
1. **Kafka cluster HA** (3 brokers, replication factor = 3)
   - One broker down → other two continue (quorum maintained)
   - Leader election < 10s for each partition

2. **Client-side buffering**
   - Client buffers events locally for 30s if Kafka unavailable
   - On reconnect → flush buffered events
   - If client disconnects before flush → events lost (acceptable: small % of interactions)

3. **Impact of ML signal loss**
   - FYP uses yesterday's model (already trained) → stays accurate until next retrain
   - 1 hour of missing training signals in a 100B daily events dataset = ~0.001% degradation
   - Acceptable: retrain daily anyway

---

## 6. Interaction Service (Likes) Failure

### Scenario: Like API Down

**What fails:**
- Users cannot like videos
- Like count not updated

**Solutions:**
1. **Client-side retry**
   - Like is stored locally on device
   - Retried every 30s until success (with exponential backoff)
   - If not synced after 24h → drop (user can re-like)

2. **Dedup on retry — two layers**

   Layer 1 — Redis (hot path, blocks immediate retries):
   - `Redis SET NX like:{user_id}:{video_id} 1 EX 60` — short TTL, not 24h
   - 24h TTL costs ~750GB RAM at scale (5B likes/day × ~150 bytes/key). 60s is enough; retries happen in seconds, not hours.

   Layer 2 — Postgres (correctness, catches anything that slips through):
   - `INSERT INTO likes (user_id, video_id) ON CONFLICT (user_id, video_id) DO NOTHING`
   - UNIQUE constraint is the safety net, not the primary dedup gate

3. **Like write path — async batch inserts via Kafka**
   - Hot path: `Redis INCR video:{id}:likes` + emit `like.event` to Kafka (both sub-ms)
   - Kafka consumer accumulates events in memory, flushes to Postgres every 500ms or 1000 events (whichever first):
   ```sql
   INSERT INTO likes (user_id, video_id, created_at)
   VALUES (101,999,NOW()), (102,999,NOW()), ...   -- up to 1000 rows
   ON CONFLICT (user_id, video_id) DO NOTHING;
   ```
   - Why batch: single-row inserts pay parse + lock + WAL + commit overhead per row (~5K rows/sec). Bulk insert amortizes it (~200K rows/sec same hardware).
   - Trade-off: up to 500ms before like is durable in Postgres. Acceptable — user already sees it via Redis counter.

4. **Like count inconsistency**
   - Redis counter: updated instantly on every like tap
   - Postgres: up to 500ms behind (Kafka consumer batch lag)
   - Display: always read from Redis (`"1.2M likes"`) — slight inaccuracy is fine for UX
   - Reconciliation: hourly job compares Redis counter vs `COUNT(*)` from Postgres; corrects drift

---

## 7. Full Region Failure

### Scenario: AWS us-east-1 goes down

**What fails:**
- Upload API, Feed API (primary region)
- Video metadata DB (if primary)
- CDN origin (if S3 is there)

**Solutions:**
1. **Active-active across regions**
   - us-east-1 (primary), us-west-2 (hot standby), eu-west-1 (active)
   - API Gateway: GeoDNS routes to nearest healthy region
   - Data: Cassandra cross-region replication (async, ~50ms lag)

2. **Trade-off: data consistency in multi-region**
   - Upload in us-east-1 crashes → cross-region replica may be 50ms behind
   - Video appears unavailable for 50ms → then appears globally
   - Acceptable: uploads tolerate eventual consistency

3. **RTO/RPO targets**
   - RTO (recovery time): < 60s (DNS propagation)
   - RPO (data loss): < 50ms (async replication lag)

---

## Failure Coverage Summary

| Component | Primary Defense | Fallback | Data Risk |
|-----------|----------------|----------|-----------|
| Recommender | Redis FYP cache | Trending feed | Low (stale personalization) |
| Redis cache | Sentinel/Cluster | Cassandra DB | Low |
| Upload Service | Resumable chunks | Local buffer (risky) | Low if S3 committed |
| Transcoding | Kafka durability | Replay on recovery | None (Kafka retains) |
| CDN | Multi-CDN + Anycast | S3 origin | None (origin intact) |
| Kafka | 3-broker HA | Client buffer | Minimal (30s events) |
| Like API | Client retry + dedup | Async reconcile | None (idempotent) |
| Full region | Active-active | GeoDNS failover | < 50ms RPO |
