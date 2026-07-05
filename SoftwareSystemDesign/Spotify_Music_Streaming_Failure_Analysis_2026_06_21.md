# Spotify Music Streaming — Failure Analysis
**Date:** 2026-06-21

---

## Failure Taxonomy

```
                    ┌─── CDN Node Failure
                    ├─── Audio Upload / Transcoder Failure
         External ──┤
                    ├─── Payment / Subscription Failure
                    └─── Third-party Licensing API Down

                    ┌─── Audio Service Crash
                    ├─── Recommendation Service Crash
         Internal ──┤
                    ├─── Redis Failure (Play State)
                    ├─── Kafka Consumer Lag / Failure
                    ├─── Elasticsearch Cluster Down
                    └─── Postgres Primary Failure

                    ┌─── Client Cache Corruption (Offline)
         Client ────┤
                    └─── DRM License Expiry on Disconnect
```

---

## Failure 1: CDN Edge Node Failure

**Scenario:** CDN edge node serving Asia-Pacific goes down mid-stream.

**Impact without mitigation:**
- All streams to affected region buffer → stall → users see spinner
- 10M users in region → mass churn event if >5s outage

**Failure Chain:**
```
CDN Node A (Tokyo) → DOWN
    ↓
Client requests chunk → 504 Gateway Timeout (10s)
    ↓ (without mitigation)
Playback stalls at current buffer position
User sees loading spinner after buffer exhausts (~20s)
```

**Component Before Fails (Origin / S3):**
If S3 origin goes down, all CDN cache misses fail too. CDN edge caches valid data but can't serve cache misses.
- Mitigation: Multi-region S3 replication (S3 Cross-Region Replication). CDN misses fail over to backup origin region.

**Component After Fails (Client app crashes):**
- Mid-stream state lost. On restart: client reads play state from Redis → resumes from last synced position (within 5s accuracy)

**Solutions:**

**Prevention:**
1. Multi-CDN strategy: Akamai + Cloudflare + own edge. Client has a CDN preference list; on error, tries next.
2. Hot-standby CDN: DNS failover (30s TTL) or client-side retry with alternate CDN URL.

**Detection:**
- Synthetic monitors: canary requests from each region every 30s
- CDN health APIs: auto-detect node degradation
- Alert: latency p99 > 500ms for audio chunk fetch → PagerDuty

**Recovery:**
```
Client retry logic:
  Attempt 1: original CDN URL
  Attempt 2: alternate CDN URL (secondary CDN)
  Attempt 3: direct S3 pre-signed URL (higher latency but works)
  
Each retry: exponential backoff (200ms, 400ms, 800ms)
10s chunks = 20s buffer = 30s to recover before user hears silence
```

---

## Failure 2: Audio Service Crash

**Scenario:** Audio Service pods crash (OOM, bad deploy) — no pre-signed URL generation.

**Impact:** Users cannot start new songs. Currently playing songs continue (client has URL already). New streams fail.

**Failure Chain:**
```
User clicks Play → POST /api/audio/{track_id}
    ↓
Audio Service pods: CRASHED (0/5 running)
    ↓
API Gateway: 503 Service Unavailable
    ↓
Client: "Can't play right now" error toast
    ↓ (without mitigation)
No music for duration of outage
```

**Component Before (API Gateway) Fails:**
If API Gateway itself is down, no requests reach Audio Service at all — broader outage. Mitigation: multi-AZ API Gateway deployment.

**Component After (CDN) Fails:**
Once pre-signed URL is issued, CDN is independent. CDN failure doesn't affect Audio Service health.

**Solutions:**

**Prevention:**
1. Pod disruption budgets: Kubernetes ensures minimum 3/5 pods always running during deploys
2. Horizontal pod autoscaler: scale out before OOM
3. Memory limits + heap profiling to prevent OOM in the first place

**Mitigation during outage:**
```
Circuit Breaker at API Gateway:
  5 consecutive 503s from Audio Service → circuit OPEN
  Fallback: return 503 with Retry-After: 30 header
  Client shows: "Having trouble connecting, retrying in 30s"

Partial degradation:
  Cache last-issued URL per user per track (Redis, 15min TTL)
  If Audio Service down: serve cached URL from Redis
  This covers the most common case (user replaying same songs)
```

**Recovery:**
- Kubernetes restarts pods automatically (liveness probe)
- Health check: GET /health → if 200, add back to load balancer pool
- Zero-downtime: rolling restart, 1 pod at a time

---

## Failure 3: Redis Failure (Play State + Session)

**Scenario:** Redis primary fails. Sentinel election takes 30 seconds.

**Impact:** 
- Play state sync lost during failover
- Session tokens can't be validated → users logged out
- Recommendation cache miss → falls back to expensive DB calls

**Failure Chain:**
```
Redis PRIMARY → DOWN
    ↓
Redis Sentinel: detects failure (10s) → starts election (20s) → promotes replica
    ↓ (during 30s window)
GET playstate:{user_id} → ECONNREFUSED
POST /session/validate → can't read session token → 401 Unauthorized
GET user:{id}:recs → MISS → falls through to Postgres (100ms instead of 2ms)
```

**Component Before (App Servers) Fails:**
If app servers can't reach Redis at all (network partition, not just Redis down): same behavior but longer. Mitigation: local in-memory session cache (short TTL).

**Component After (Postgres) Fails — During Redis Fallback:**
If Postgres is also degraded: session validation chain breaks completely. Mitigation: session JWTs with embedded claims (don't require DB lookup for every request).

**Solutions:**

**Play State:**
```
Best-effort sync: play position is non-critical (approximate resume is fine)
During Redis outage:
  Client continues playback locally
  Periodically retries Redis write (exponential backoff)
  On reconnect: flush buffered position updates
  
Worst case: user resumes 30s early/late. Acceptable.
```

**Session Tokens:**
```
Switch to JWT with embedded user_id + subscription_tier + exp
  Verify signature only (HMAC-SHA256, secret in env) → no Redis lookup needed
  TTL: 1h, refresh token: 30 days (in Postgres, can tolerate Redis miss)
  
During Redis outage: JWT validation still works from signature alone
  Caveat: can't revoke sessions until Redis recovers (logout doesn't work)
  Mitigated by short JWT TTL (1h)
```

**Recommendation Cache:**
```
Circuit breaker on Redis call:
  On miss → check: is Redis degraded? (3 consecutive failures = degraded)
  If degraded: serve genre-based popular tracks from Postgres (pre-computed daily, small table)
  Accept stale/generic recommendations during outage
```

**Prevention:**
- Redis Cluster (3 primary + 3 replica shards): one node failure doesn't take down all keys
- Redis Sentinel with 3 sentinels: quorum-based election, no split-brain

---

## Failure 4: Kafka Consumer Lag (Royalty Calculator)

**Scenario:** Royalty Calculator service falls behind on play_events topic. Topic lag grows.

**Impact:** Royalties temporarily uncounted. Financial consequence if not corrected.

**Failure Chain:**
```
Play events: 10K/sec produced to Kafka
Royalty Calculator: normally 10K/sec consumed
    ↓
Royalty Calculator: slow query to ClickHouse (index degraded)
Processing drops to 2K/sec
Lag grows: 8K events/sec × 3600s = 28.8M unprocessed events/hour
    ↓ (without mitigation)
Month-end royalty report: undercounted plays → incorrect payouts → legal risk
```

**Component Before (Kafka) Fails:**
Kafka broker failure → partitions become unavailable. If ISR (in-sync replicas) = 2: one broker failure is tolerated. Mitigation: replication.factor=3, min.insync.replicas=2.

**Component After (ClickHouse) Fails:**
Royalty Calculator writes to ClickHouse. If ClickHouse is down, royalty calculator crashes or backs up. Mitigation: write to Kafka → ClickHouse consumer (double-hop, adds durability).

**Solutions:**

**Kafka's Durability is the Safety Net:**
```
play_events topic:
  retention.ms = 604800000  (7 days)
  
If Royalty Calculator falls behind → events are NOT lost (still in Kafka)
It can catch up when the bottleneck is resolved
Time to catch up: lag / (processing_rate - produce_rate)
```

**Alerting:**
```
Metric: consumer_group lag (topic offset - consumer offset)
Alert: lag > 1M events → PagerDuty (means >100s behind at normal rate)
Dashboard: Kafka consumer group lag per service, per partition
```

**Scaling:**
```
Royalty Calculator: 10 consumer instances × 10 partitions
If lag grows: spin up more consumer instances (Kubernetes HPA on lag metric)
  → More parallelism → faster catch-up
  
BUT: each play event must be processed exactly once (deduplication by event_id)
```

**Prevention:**
- ClickHouse query optimization: MATERIALIZED VIEW pre-aggregates counts per track per hour
- Royalty Calculator: batch writes (1000 events per ClickHouse INSERT instead of 1-by-1)
- Idempotent consumer: SET processed:{event_id} NX before processing

---

## Failure 5: Transcoder Failure (New Song Upload)

**Scenario:** Transcoder service crashes. New tracks can't be processed.

**Impact:** New songs unavailable for playback. Existing catalog unaffected.

**Failure Chain:**
```
Label uploads new track → S3 raw bucket → triggers SQS message
Transcoder Service (consumes SQS):
    ↓ CRASHES mid-transcode
SQS message: not acknowledged → returns to queue after visibility timeout (5min)
    ↓ (retried 3× by default)
After 3 failures → DLQ (Dead Letter Queue)
Track status: remains "processing" indefinitely
```

**Solutions:**

**SQS Dead Letter Queue (DLQ):**
```
SQS config:
  maxReceiveCount: 3          → 3 retry attempts
  VisibilityTimeout: 600s     → 10min to transcode before SQS assumes failure
  DLQ: transcode-failures     → manual review queue

Alert: DLQ message count > 0 → ops team notified
Ops: inspect failed track, retry manually or fix transcoder bug
```

**Partial Failure (Transcoder crashes mid-job):**
```
Problem: 4 of 5 quality tiers transcoded, then crash
         → partial files in S3

Solution:
  1. Transcode to temp path: songs/tmp/{job_id}/{quality}.mp3
  2. Only after ALL tiers complete: atomic rename to songs/{track_id}/{quality}.mp3
  3. Update AudioFile DB records in single transaction
  4. If any step fails: cleanup temp files, SQS message retried from scratch
```

**Preventing Duplicate Transcoding:**
```
Idempotency key: {track_id} + {quality}
Check S3: does songs/{track_id}/320.mp3 already exist?
  Yes → skip this tier, continue with others
  No → transcode
```

---

## Failure 6: Recommendation Service Down

**Scenario:** Recommendation Service pods crash or are overloaded. User opens app.

**Failure Chain:**
```
User opens app → GET /api/feed
    ↓
API Gateway → Recommendation Service → TIMEOUT (10s)
    ↓ (without mitigation)
User sees blank home screen for 10 seconds
→ frustrating UX; user may close app
```

**This is NOT a critical path failure** — user can still play music directly (search works, playlists work). But home screen blank is bad UX.

**Solutions:**

**Circuit Breaker + Fallback:**
```
Circuit Breaker wraps Recommendation Service call:
  Timeout: 500ms (not 10s!)
  After 5 timeouts: circuit OPEN

Fallback strategy (ordered by quality):
  1. Redis cache: user:{id}:recs still has yesterday's recommendations → stale but personal
  2. Postgres: "Recently played" (last 20 tracks) + "New from followed artists" (simple SQL)
  3. Static: "Top tracks this week" from Trending Service (always available)
  
User sees relevant content within 500ms even during outage
```

**Why 500ms timeout (not 10s):**
Home screen load budget is 1s total. Recommendation can use at most 500ms. A 10s timeout blocks the entire page.

---

## Failure 7: Offline Download — DRM License Expiry Mid-Use

**Scenario:** User on airplane (no internet). Their subscription expired yesterday. They tap a downloaded song.

**Failure Chain:**
```
User opens app (offline)
App checks: is there a network connection? NO → offline mode
    ↓
User taps downloaded track
    ↓
App checks local license validity:
  License has: expires_at = yesterday
  Current time > expires_at → LICENSE EXPIRED
    ↓ (without mitigation)
Song refuses to play: "Subscription required"
User has no internet to renew
```

**This is intentional DRM behavior, but UX matters:**

**Design Decisions:**
```
Grace period: 30-day offline license window
  Even if subscription expires day 1, offline works for 30 more days
  License renewed every time app goes online
  
Clear user communication:
  Before trip: "Your offline license expires in 3 days" notification
  On airplane: if expired, show: "Offline license expired. Connect to internet to renew."
  NOT just "Can't play" (confusing) but explanatory message
  
Emergency: 7-day grace past license expiry (silent, server-side configurable)
  Prevents edge case: user's payment fails for 1 day, card re-charges next day
  Without grace: user loses offline access even if they fix billing in 24h
```

---

## Failure Summary Matrix

| Component | Failure | User Impact | Recovery Time | Mitigation |
|-----------|---------|-------------|--------------|------------|
| CDN edge node | DOWN | Streams buffer in region | ~30s (failover to next CDN) | Multi-CDN, client retry |
| Audio Service | All pods crash | Can't start new songs | 1-3min (pod restart) | Kubernetes auto-restart, cached URLs |
| Redis | Primary fails | Play state stalls 30s, possible logout | 30-60s (Sentinel election) | JWT sessions, Redis Cluster |
| Kafka lag | Consumer slow | Royalty undercounting (invisible to user) | Minutes-hours | 7-day retention, consumer scaling |
| Transcoder | Crashes | New tracks delayed | Minutes (SQS retry) | DLQ, idempotent re-run |
| Recommendation Service | Down | Generic home screen | 500ms (fallback) | Circuit breaker, cached recs |
| Postgres primary | Fails | Can't edit playlists | 1-2min (RDS Multi-AZ) | Read replicas for catalog reads |
| Elasticsearch | Cluster down | Search broken | 5-10min (cluster recovery) | Fallback: Postgres LIKE search (degraded) |

---

## Real-World Incidents

**Spotify Outage Dec 2023:**
- Cause: DNS/infrastructure issue
- Impact: App loading failures globally for ~2 hours
- Learning: DNS TTL was too long; CDN failover was slow

**Spotify Podcast Feed Delays (2022):**
- Cause: Transcoder queue overwhelm after large podcast upload batch
- Impact: New podcast episodes delayed 2-6 hours
- Fix: Queue priority (paid/popular channels get dedicated queue)

**Royalty Calculation Errors (Multiple providers, industry-wide):**
- Cause: Play events double-counted due to retry logic without idempotency
- Impact: Artists overpaid in some cases, underpaid in others
- Fix: Industry moved to event_id-based deduplication (Spotify uses this)
