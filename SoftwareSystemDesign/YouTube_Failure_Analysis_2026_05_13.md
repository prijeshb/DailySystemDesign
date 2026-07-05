# YouTube System — Failure Analysis & Resilience Playbook
**Date:** 2026-05-13 | **System:** Video Streaming Platform (YouTube-style)

> For every component: what happens if IT fails, what happens if the component BEFORE it fails, what happens if the component AFTER it fails. Then: detection, prevention, and recovery.

---

## Failure Analysis Map

```
Creator → [Upload Service] → [Object Store] → [Kafka] → [Transcoding Workers]
                                                              ↓
Viewer  → [CDN] → [API Gateway] → [Video Service] → [PostgreSQL]
                                                   → [Redis Cache]
                                                   → [Elasticsearch]
```

---

## 1. Upload Service Failure

### 1a. Upload Service itself crashes mid-upload

**What breaks:**
- Creator's upload is interrupted
- Partially uploaded chunks are in object store
- No VideoUploaded event is published to Kafka
- Video record may be stuck in `UPLOADING` status permanently

**Detection:**
- Health check on Upload Service pods fails → load balancer stops routing
- Monitoring: videos in `UPLOADING` status for > 30 minutes → alert
- Client receives 502/503 and must detect incomplete upload

**Prevention / Solution:**
- Multipart upload state stored in object store AND in Upload DB — not in memory
- Client saves upload progress locally (part numbers + ETags already uploaded)
- Resumable upload: client polls `GET /uploads/{id}` to discover which parts are missing, re-uploads only those
- Idempotency: each presigned URL is tied to a specific (upload_id, part_number) pair — re-uploading the same part is safe (object store deduplicates by ETag)
- Upload Service is stateless — any instance can serve the resume request

**Recovery:**
- Upload Service restarts (Kubernetes restarts pod within 30s)
- Creator resumes from last completed chunk — no data loss
- Zombie cleanup job: scan `UPLOADING` videos older than 2 hours → notify creator, delete partial uploads from object store

---

### 1b. Component BEFORE Upload Service fails (API Gateway / Load Balancer)

**What breaks:**
- Creator cannot initiate upload (cannot get presigned URLs)
- Existing in-progress uploads continue (chunks go directly to object store — they don't pass through gateway after initiation)

**Solution:**
- Multiple API Gateway instances behind Anycast/DNS
- Circuit breaker: if gateway fails, failover to secondary region within 60s
- Presigned URLs are already issued → in-flight chunk uploads are unaffected
- Creator UX: show "Connection lost, retrying..." with backoff

---

### 1c. Component AFTER Upload Service fails (Object Store)

**What breaks:**
- Presigned PUT requests fail — creator's chunks cannot be stored
- Even if Upload Service is healthy, it can't complete uploads
- Upload Service returns 503 after detecting object store failures

**Solution:**
- Object store (S3/GCS) has 99.999999999% (11 nines) durability by design — it's multi-AZ
- If region-level object store outage: pre-signed URLs generated for secondary region bucket
- Upload Service: try primary object store, fall back to secondary region on 3 consecutive failures
- Circuit breaker pattern: stop issuing presigned URLs for primary region, switch to fallback
- Alert SRE within 5 minutes of object store error rate > 1%

---

## 2. Kafka (Event Bus) Failure

### 2a. Kafka itself fails

**What breaks:**
- VideoUploaded events not published → transcoding never starts
- Videos stuck in `PROCESSING` status indefinitely
- VideoReady events not published → creator notifications don't fire
- Analytics events dropped

**Detection:**
- Kafka broker health metrics (under-replicated partitions > 0 → alert)
- Transcoding queue depth falls to zero while upload count stays high → alert

**Prevention / Solution:**
- Kafka with replication factor = 3; min in-sync replicas = 2 (writes acknowledged by 2+ brokers)
- Producer uses acks=all + retry with idempotent producer (same event not duplicated on retry)
- If Kafka is fully down: Upload Service falls back to writing events to a durable outbox table in PostgreSQL
  ```
  outbox_events table:
    event_id, event_type, payload, created_at, published_at (nullable)
  ```
  - Outbox poller runs every 5 seconds: reads unpublished events → publishes to Kafka → marks published
  - This is the Transactional Outbox Pattern — guarantees at-least-once delivery

**Recovery:**
- Kafka auto-recovers from broker crash (leader election within seconds for surviving brokers)
- On full cluster restart: producers reconnect, consumers resume from last committed offset — no events lost

---

## 3. Transcoding Worker Failure

### 3a. Worker crashes mid-transcoding

**What breaks:**
- Partial transcoded segments are in object store (garbage)
- The transcoding job appears to be claimed but never completes
- Video stuck in `PROCESSING`

**Detection:**
- Worker heartbeat fails → job marked as `STALE` after 2-minute timeout
- Monitoring: jobs in `TRANSCODING` state for > 30 min → alert

**Prevention / Solution (Idempotency is key here):**
- Jobs are idempotent: if worker crashes and restarts, it:
  1. Checks which segments are already uploaded (object store list operation)
  2. Resumes from the first missing segment
  3. Uses segment naming: `seg_{video_id}_{quality}_{n}.m4s` → deterministic path
- Orchestrator re-queues stale jobs (atomic CAS: `UPDATE jobs SET status=PENDING WHERE status=TRANSCODING AND last_heartbeat < now()-2min AND claimed_by=dead_worker`)
- New worker picks up the re-queued job
- Partial segment files are cleaned up before re-run (idempotent delete + re-create)

**Why not just re-download raw video and restart?**
For large videos (10+ GB), re-transcoding from scratch is expensive. Instead, workers save a checkpoint of the last completed segment to a progress file in object store every 60s. On restart, the worker reads the checkpoint and continues.

---

### 3b. Component BEFORE workers fails (Kafka — workers can't receive jobs)

**What breaks:**
- Workers starve: no new jobs to consume
- In-flight jobs continue processing (Kafka failure doesn't affect already-claimed jobs)
- New uploads queue up in Kafka (on disk, durable) until Kafka recovers

**Solution:**
- Kafka is durable — once VideoUploaded event is written with acks=all, it survives broker restarts
- Workers resume consuming when Kafka recovers; they resume from their last committed offset
- No manual intervention needed for transient outages

---

### 3c. Component AFTER workers fails (Object Store — can't upload segments)

**What breaks:**
- Workers complete transcoding but cannot upload segments
- Workers fail with upload error → job marked FAILED → retried
- Retry storm possible if object store is degraded (not fully down)

**Solution:**
- Exponential backoff with jitter on segment upload failures (1s, 2s, 4s, 8s... max 5 min between retries)
- Circuit breaker in worker: after 5 consecutive segment upload failures → pause job → release claim → re-queue for later
- Object store multi-region: if primary bucket unavailable, upload to secondary bucket (same CDN origin can point to either)
- Alerting: if job failure rate > 5% → page on-call

---

## 4. CDN Failure

### 4a. CDN PoP (Point of Presence) fails

**What breaks:**
- Viewers in that geographic region get higher latency (routed to next-nearest PoP)
- Not a total failure — Anycast DNS automatically reroutes to healthy PoP

**Solution:**
- Anycast DNS: client doesn't pick the PoP explicitly — the network routes to the nearest healthy PoP
- CDN providers (Cloudflare, Akamai, Fastly) SLA: single PoP failure is transparent
- Monitor: CDN error rate by region → alert if error rate > 0.1% in any region

---

### 4b. CDN origin (Object Store) unreachable from CDN

**What breaks:**
- CDN cannot refresh expired cached segments
- Cache HITs continue serving → no immediate impact for cached content
- Cache MISSes (new/unpopular videos) → 504 error to viewer

**Detection:**
- CDN origin error rate rises → CDN dashboard alert + PagerDuty

**Solution:**
- Serve stale content (Cache-Control: stale-while-revalidate=3600) — segments are immutable, stale is safe
- Segments have 1-year TTL at edge → popular content is unaffected even if origin is down for hours
- Only long-tail content (not yet cached at edge) affected
- Cross-region failover: CDN can be configured with secondary origin in another region

---

### 4c. Full CDN failure (global outage)

**What breaks:**
- All video streaming fails globally — clients cannot fetch segments
- This is catastrophic — CDN is the most critical SPoF for streaming

**Solution:**
- Multi-CDN strategy: primary CDN (Cloudflare), secondary CDN (Akamai)
- DNS failover: if primary CDN health check fails, switch DNS to secondary CDN within 60s
- Client-side fallback: if CDN URL fails, client retries with direct object store URL (slower, no geo-distribution)
- Geo-distributed object stores: GCS multi-region / S3 cross-region replication
- Accept degraded experience: stream at lower quality from non-CDN origin

---

## 5. PostgreSQL (Video Metadata DB) Failure

### 5a. Primary DB crashes

**What breaks:**
- Video metadata reads fail (video title, status, manifest URLs)
- New video publishes blocked
- Comments, likes blocked

**Detection:**
- DB connection errors in Video Service → Prometheus alert within 30s

**Prevention / Solution:**
- PostgreSQL primary-replica replication (synchronous for writes, async for read replicas)
- Automatic failover via Patroni or AWS RDS Multi-AZ: standby promoted within 30–60 seconds
- During failover window (30–60s): Video Service returns cached metadata from Redis
- Redis cache hit rate for popular videos is >95% — most viewers unaffected during brief failover

**Read path with cache:**
```
Video Service:
  1. Check Redis (TTL: 60s)
  2. Cache HIT → return immediately
  3. Cache MISS → query PostgreSQL → populate cache → return
  4. If PostgreSQL unavailable → return stale cache (extend TTL) or 503
```

**Consistency risk:**
- If Redis serves stale data during DB failover, video status might show `PROCESSING` even after it's `READY`
- Acceptable: creator sees delay in status update; viewer can retry in 60s
- Unacceptable stale: video showing as PUBLIC after creator set to PRIVATE → mitigated by lower cache TTL for privacy-sensitive fields (5s for visibility field)

---

### 5b. Write amplification / hot shard

**Scenario:** Viral video gets 10M views in 1 hour → view_count update storms on one shard

**Solution:** Counter shard pattern (see main design doc)
- 16 shards per video → 16x write distribution
- Read path: `SELECT SUM(count)` across 16 rows (fast, single shard query within one node)
- Acceptably stale: view counts shown to creator are ±1 minute; viewer-facing counts updated every 5 minutes (batch aggregation)

---

## 6. Redis Cache Failure

### 6a. Redis cluster node fails

**What breaks:**
- Cache misses increase → more load on PostgreSQL
- Response times increase from ~5ms to ~50ms for affected keys

**Solution:**
- Redis Cluster: 6 nodes (3 primary + 3 replica); node failure → replica promoted automatically within seconds
- Video Service: if Redis is unavailable → bypass cache, go direct to PostgreSQL
- PostgreSQL read replicas handle 10x normal read load temporarily
- Monitoring: cache hit rate drop > 10% → alert

### 6b. Redis data wiped (full cluster restart)

**What breaks:**
- Cold cache → thundering herd on PostgreSQL
- All 50M concurrent viewers suddenly query DB simultaneously

**Solution (Cache Stampede Prevention):**
- Probabilistic early expiration: instead of all cached items expiring at the same TTL, add random jitter (TTL + random(0, 30s))
- Mutex lock on cache population: only one request populates the cache for a given key; others wait or return stale
- Read from DB replicas (horizontal scaling) + connection pooling (PgBouncer)
- Circuit breaker: if DB error rate > 20%, return 503 with Retry-After header to shed load

---

## 7. Search Service (Elasticsearch) Failure

### 7a. Elasticsearch cluster fails

**What breaks:**
- Search returns errors
- Video discovery via search broken

**Solution:**
- Search is not in the critical streaming path — a viewer already watching a video is unaffected
- Video Service: if ES returns error → return 503 with message "Search temporarily unavailable"
- Fallback: basic PostgreSQL full-text search (`tsvector`) for degraded-mode search (lower quality results)
- ES data is derived from PostgreSQL → fully rebuildable; no data loss

---

## 8. Analytics Ingestion Failure

### 8a. WatchEvents Kafka topic full / Kafka down

**What breaks:**
- View counts and watch-time stats are delayed
- Creator analytics dashboard shows stale data

**Solution:**
- WatchEvents are fire-and-forget from the client — losing some events is acceptable (not financially critical)
- Client batches events locally (every 30s) before sending → on failure, retries up to 3 times
- Kafka consumer lag monitoring: if analytics consumer is > 10 minutes behind → scale out ClickHouse consumers
- ClickHouse is append-only — no consistency requirements; eventual delivery is fine

---

## 9. Network Partition Scenarios

### 9a. Creator upload succeeds but VideoUploaded event not published (Kafka partition)

**Scenario:** Upload Service writes video to object store, then crashes before publishing to Kafka.

**Without Outbox Pattern:** Video stuck in UPLOADING forever — transcoding never starts.

**With Transactional Outbox:**
```sql
BEGIN;
  INSERT INTO videos (video_id, status) VALUES (?, 'UPLOADING');
  INSERT INTO outbox_events (event_type, payload) VALUES ('VideoUploaded', ?);
COMMIT;
-- Outbox poller reads unpublished events and publishes to Kafka
-- Both insert or neither — no partial state
```
- Even if Upload Service crashes after DB commit, the outbox poller (separate process) picks up the event and publishes it
- This pattern guarantees at-least-once delivery across service crash

### 9b. Transcoding completes but VideoReady event not published

**Same problem, same solution:** Transcoding Orchestrator uses outbox table to publish VideoReady event. If Orchestrator crashes after marking `status=READY` in DB, the outbox poller publishes the event. Notification Service then fires creator/subscriber notifications.

---

## 10. Idempotency Reference Table

| Operation | Idempotency Key | Risk Without It | Solution |
|---|---|---|---|
| Upload chunk PUT | (upload_id, part_number) | Duplicate chunks | Object store deduplicates by ETag |
| Transcoding job claim | (video_id, quality, codec) | Double transcoding | DB unique constraint + row lock |
| Segment upload | Deterministic path (video_id + quality + seq) | Duplicate segments | Object store PUT is idempotent (overwrite) |
| VideoUploaded publish | video_id | Double transcoding triggered | Orchestrator checks if job already exists before creating |
| View count increment | (user_id, video_id, session_id) | Inflated counts | Deduplicate in ClickHouse materialized view |
| Subscription fan-out | (video_id, subscriber_id) | Duplicate notifications | Notification dedup table with (video_id, user_id) unique key |

---

## Summary: Failure Severity Matrix

| Component | Failure Impact | Recovery Time | Viewer Impact |
|---|---|---|---|
| Single CDN PoP | Minimal — rerouted | Automatic, <5s | None (latency bump) |
| Redis cache node | Low — DB fallback | Automatic, <30s | Slight latency increase |
| Upload Service pod | Low — stateless restart | 30s | Upload retried by client |
| Kafka broker (1 of 3) | Low — replicas absorb | Automatic, <10s | None |
| PostgreSQL primary | Medium — failover needed | 30–60s | Served from cache |
| Transcoding worker | Low — job re-queued | Minutes | No viewer impact |
| Full CDN region | High — failover CDN | 60s DNS TTL | Buffering / quality drop |
| Kafka cluster (full) | High — uploads queue, no transcoding | Minutes (outbox) | Delayed new videos |
| Object Store (primary region) | High — uploads fail | Minutes (failover region) | New uploads blocked |
| PostgreSQL cluster (all) | Critical — reads fail | Minutes | Metadata errors |
