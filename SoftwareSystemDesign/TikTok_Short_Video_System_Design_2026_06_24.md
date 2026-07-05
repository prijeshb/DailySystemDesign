# TikTok Short Video Platform — System Design
> Date: 2026-06-24 | Focus: Recommendation-first feed, short video pipeline, CDN cost economics

---

## First Principles: Do We Even Need This?

**Why short video vs long video (YouTube)?**
- Discovery mechanism is fundamentally different: algorithm-first (FYP), not search-first
- Users don't choose what to watch next — the algorithm does
- Core metric: watch time completion rate, not click-through rate
- Consequence: recommendation engine IS the product, not just a feature

**What must we optimize for?**
1. Feed load latency < 200ms (user sees first video instantly on open)
2. Video start time < 500ms (no buffering spinner)
3. FYP relevance (correct recommendation = user keeps scrolling)
4. Upload → published < 60s for creators (fast feedback loop)

---

## Scale Estimates (Back of Envelope)

```
DAU:             1B users
Videos watched:  avg 60 videos/session, 2 sessions/day = 120 videos/day/user
Total views:     1B × 120 = 120B views/day = 1.4M views/sec

Videos uploaded: 50M/day = 578/sec
Avg video:       30s, 720p, ~15MB after compression (raw: ~100MB)

Storage ingress: 50M × 15MB = 750TB/day new storage
Total storage:   750TB × 365 = ~274PB/year

CDN bandwidth:   120B views × 15MB = 1.8EB/day (too high)
                 Fix: pre-buffer 3 videos only → 3 × 15MB = 45MB pre-loaded
                 Active stream: 2Mbps for 30s = 7.5MB/view
                 Realistic CDN: 120B × 7.5MB = 900PB/day
```

### AWS Budget (CDN is the Monster)

| Component | Estimate | AWS Cost/day |
|-----------|----------|-------------|
| CDN bandwidth | 900PB/day | $0.008/GB × 900M GB = **$7.2M/day** |
| S3 storage | 750TB new | $0.023/GB = $17K/day |
| Transcoding (GPU) | 50M vids × $0.03/job | $1.5M/day |
| EC2 (services) | ~50K instances | $1M/day |
| **Total** | | **~$9.7M/day** |

**Budget constraint reality:** At $9.7M/day, CDN is 74% of cost. This is why:
1. TikTok operates its own CDN (ByteDance CDN) → reduces CDN cost to ~$0.002/GB
2. Uses P2P content delivery for viral content (viewers seed to nearby viewers)
3. Aggressively pre-warms popular content at edges, caches only top-5% of videos (long tail has very low hit rate)

**Budget constraint + limitation:**
> If budget constrains CDN to $500K/day (~62PB/day bandwidth), we can only serve 8.3B views/day instead of 120B.
> Mitigation: reduce pre-buffer from 3 videos to 1, compress 720p down to 480p for lower-tier devices, implement P2P CDN for viral content.
> Trade-off: worse perceived quality + playback smoothness vs. serving more users.

---

## Entities

```
User:       user_id, username, follower_count, following_count, region
Video:      video_id, creator_id, s3_key, duration_ms, width, height, 
            status (PROCESSING | PUBLISHED | DELETED), 
            view_count, like_count, comment_count, share_count,
            hashtags[], music_id, created_at
Interaction: user_id, video_id, type (LIKE|COMMENT|SHARE|SKIP|WATCH), 
             watch_duration_ms, created_at
Follow:     follower_id, followee_id, created_at
Comment:    comment_id, video_id, user_id, text, like_count, reply_to_id
Music:      music_id, title, artist, duration_ms, s3_key
```

---

## Actions & Data Flow

```
1. Upload     → Upload Svc → S3 (raw) → Transcoding Queue → CDN (processed)
2. Watch      → Feed Svc → Recommendation Engine → Video metadata + CDN URLs
3. Interact   → Interaction Svc → Kafka → Update counts + feed signals
4. Search     → Search Svc (Elasticsearch) → video/user results
5. Follow     → Follow Svc → Graph DB → affects FYP candidate pool
```

---

## High Level Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Client (Mobile App)                         │
└──────────┬──────────────────┬────────────────────┬───────────────────┘
           │                  │                    │
    ┌──────▼──────┐   ┌───────▼───────┐   ┌───────▼────────┐
    │ Upload API  │   │  Feed API     │   │ Interaction API│
    └──────┬──────┘   └───────┬───────┘   └───────┬────────┘
           │                  │                    │
    ┌──────▼──────┐   ┌───────▼───────┐   ┌───────▼────────┐
    │  S3 (raw)   │   │ Recommender   │   │  Kafka Events  │
    └──────┬──────┘   │  Engine       │   └───────┬────────┘
           │          └───────┬───────┘           │
    ┌──────▼──────┐           │             ┌─────▼──────────┐
    │ Transcoding │   ┌───────▼───────┐     │ Counter Svc    │
    │  Service    │   │  Video DB     │     │ (Redis atomic) │
    └──────┬──────┘   │  (Cassandra)  │     └────────────────┘
           │          └───────────────┘
    ┌──────▼──────┐
    │  S3 (CDN)   │
    │  processed  │
    └──────┬──────┘
           │
    ┌──────▼──────┐
    │    CDN      │
    │  (ByteDance │
    │   / CF)     │
    └─────────────┘
```

---

## Low Level Design

### 1. Upload Pipeline

```
Phase 1: Client-side chunking
  Video → split into 5MB chunks
  Client: PUT /upload/init → upload_id
  Client: PUT /upload/{upload_id}/chunk/{n} → parallel (5 concurrent)
  Client: PUT /upload/{upload_id}/complete → triggers processing
  
  Why chunked? Resume interrupted uploads. Mobile connections drop.
  State: upload_chunks table (upload_id, chunk_no, s3_etag, received_at)

Phase 2: Upload Service
  Validates chunk checksums
  Assembles via S3 multipart upload API (no extra copy needed)
  Publishes: video.uploaded event to Kafka

Phase 3: Transcoding Service (Kafka consumer)
  Consumes video.uploaded event
  Spawns GPU jobs in parallel:
    - 1080p @ 4Mbps (30fps)
    - 720p  @ 2Mbps (30fps)  ← default for most users
    - 480p  @ 800Kbps
    - 360p  @ 400Kbps        ← cellular fallback
    - Thumbnail extraction (frame at 1s, 5s, mid-point)
  
  Note: Unlike YouTube, TikTok videos are SHORT (< 3 min).
  No HLS segmentation needed for most videos:
    ≤ 60s: serve as single progressive MP4 (simpler, faster to start)
    > 60s: use HLS with 2s segments (allows seek, ABR)
  
  Why this distinction matters:
    HLS splits a video into many small 2s .ts segments + a manifest file (.m3u8).
    The player fetches the manifest first, then each segment on demand.
    This adds 2-3 extra HTTP round trips before playback can start (~300-500ms extra).
    
    Progressive MP4 is a single file. The player requests byte range 0-N and
    starts playing as soon as the first ~500KB buffer fills — ~200ms to first frame.
    
    For a 15s TikTok, the user would sit through a 400ms black screen instead of
    200ms — a 2× startup penalty that visibly hurts the "swipe and instantly play"
    feel. HLS's benefits (adaptive bitrate switching, mid-video quality changes)
    simply don't matter for a 15s clip where the whole thing fits in 2-3 segments anyway.
    
    For videos > 60s, the math flips: HLS lets the player downgrade from 1080p to 480p
    mid-video if the user's connection degrades — essential for a 3min video, not
    worth the complexity for a 15s one.

Phase 4: Quality check + publish
  Upload processed variants to CDN origin (S3 with CloudFront or ByteDance CDN)
  Update video.status = PUBLISHED in Cassandra
  Push notification to creator
  Enqueue for recommendation indexing

Total: raw upload → published ≈ 30-90s depending on video length
```

### 2. Feed (For You Page) — Core System

#### Why FYP is hard
- 1B users, each needs a personalized feed
- Cannot compute at request time (too slow, too expensive)
- Must pre-compute candidates, then rank at request time

#### Multi-Stage Pipeline (industry standard)
```
Stage 1: Candidate Retrieval (offline, pre-computed)
  For each user, run Two-Tower model:
    User Tower: encode user features → 128-dim embedding
    Item Tower: encode video features → 128-dim embedding
    FAISS ANN search: top-500 candidate videos for user
  
  Also pull candidates from:
    - Following feed: new videos from accounts user follows
    - Trending pool: top 1000 videos by region in last 24h
    - Hashtag match: videos from hashtags user engaged with
  
  Merge: ~1000 candidates per user
  Stored in Redis: FYP:{user_id} = [video_id_1, ..., video_id_1000]  EX 1800

Stage 2: Re-ranking (online, per request)
  User opens app → fetch FYP candidates from Redis
  Run lightweight scoring model over each candidate:
    Features: user_embedding × video_embedding + context (time of day, device, wifi/cellular)
    Score = p(watch_completion) × weight_1 + p(like) × weight_2 + p(share) × weight_3
  Sort by score → return top 20 for initial buffer
  
  Latency budget: < 50ms total (Redis fetch 5ms + scoring 30ms + metadata 10ms)

Stage 3: Diversity injection
  After scoring: ensure top 20 don't have > 3 consecutive same-creator videos
  Inject 1 "exploration" video per 10 (exploitation-exploration balance)
```

#### Two-Tower Model Details
```
User Tower inputs:
  - Demographic (age bucket, region)
  - Recent watch history (last 100 videos, weighted by completion rate)
  - Liked hashtags
  - Time-of-day embedding

Video Tower inputs:
  - Visual features (extracted by CNN at upload time)
  - Audio features (MFCC spectrum, music beat)
  - Textual (caption, hashtags, comments)
  - Engagement signals (like rate, completion rate, share rate)
  - Freshness (decay function: older = lower)

Training: contrastive learning (watched video = positive, skipped = negative)
Retrained: daily (offline batch on GPU cluster)
Embeddings: refreshed daily for users, on-publish for videos
```

### 3. Watch Signal Processing (Critical for ML)

```
Client sends events every 2s while watching:
  {video_id, user_id, watch_duration_ms, action: "WATCHING"}
  {video_id, user_id, total_watched_ms, action: "SKIP" | "COMPLETE"}

Backend:
  Kafka topic: watch.events (partitioned by user_id)
  Flink consumer:
    Per video: compute completion_rate = total_watched / video_duration
    Bucket: 0-25%, 25-50%, 50-75%, 75-100%, replayed

Signal weight in ranking:
  Completion rate 75-100%: strong positive signal
  Like: moderate positive
  Share: strong positive (user values this enough to show others)
  Comment: moderate
  Skip < 25%: strong negative signal
  
Note: Completion rate >> likes as signal quality
  Reason: Likes require action (friction). Completion is passive but honest.
  A "like" from someone who watched 10% is noise.
  A "watch 95%" without like is a real positive signal.
```

### 4. Interaction Service

```
Like:
  Redis: INCR video:{id}:likes          → atomic increment
  Kafka: like.event → fan-out to creator notification
  Postgres: likes table (user_id, video_id, created_at) — for "did I like this?" check
  
  Dedup: Redis SET NX like:{user_id}:{video_id} EX 86400
    Prevents double-like on network retry

Comment:
  Postgres: comments table (tree structure via parent_comment_id)
    Schema: (comment_id, video_id, user_id, text, like_count,
             parent_comment_id, created_at)
    
    parent_comment_id = NULL → top-level comment
    parent_comment_id = X   → reply to comment X (one level deep on TikTok)
    
    Why Postgres (not Cassandra)?
      Comments need: ORDER BY like_count DESC (top comments first),
      JOIN to user table (for username/avatar), COUNT(replies).
      These are relational queries — Postgres handles them cleanly.
      Comment write volume is low (~1K/sec globally) — no need for Cassandra scale.
    
    Why NOT a dedicated graph/nested-set structure?
      TikTok only allows 1 level of replies (comment → reply, not reply → reply).
      parent_comment_id on a flat table is sufficient. A nested-set or closure
      table adds complexity only justified for deeply recursive trees (e.g., Reddit).
  
  Redis cache: top 10 comments per video (refreshed on new comment)
    Key: comments:top:{video_id}  →  JSON array of top 10 by like_count  EX 300
    
    Why cache at all?
      When 5M users open the same viral video simultaneously, 5M Postgres
      queries for "top 10 comments on video X" would collapse the DB.
      Redis serves all 5M from a single cached result.
    
    Why only top 10?
      The app shows top comments first; users load more on scroll (paginated
      from Postgres). Caching the full comment tree is wasteful — 99% of reads
      only ever see the top 10.
    
    Refresh trigger: on any new comment → ZADD comments:scores:{video_id}
      like_count comment_id, then rebuild top-10 JSON → SET comments:top:{video_id}.
      Short TTL (300s) as backstop in case refresh event is missed.

Share:
  Kafka event: share.event → attribution, analytics
  Generates short link: https://vm.tiktok.com/{7-char-id} → resolved to video_id
```

### 5. Database Design

```
Video metadata → Cassandra (write-heavy, partitioned by video_id)
  Partition key: (video_id)
  Clustering key: created_at DESC

User profile → Postgres (structured, joins needed for follow graph)

Follow graph → PostgreSQL + Redis cache for hot users
  follows (follower_id, followee_id, created_at)
  For users with 1M+ followers (celebrities): pre-computed in Redis

Search → Elasticsearch
  Index: video caption, hashtags, creator username
  Suggest: autocomplete on hashtag/username search

Analytics (views, completion rates) → ClickHouse
  100B events/day, retention 90 days
  Queried for ML training data, creator analytics dashboard
```

### 6. Creator Analytics Dashboard

```
Metrics: views, unique viewers, completion rate (by day/week/30d), follower change
Pipeline: 
  Watch events → Kafka → Flink (aggregate per video per hour)
  → ClickHouse (OLAP)
  Dashboard query: SELECT ... WHERE video_id = $1 GROUP BY DATE_TRUNC('hour', ts)

Refresh: near-real-time (< 5 min lag acceptable for creators)
Creator follows trend but doesn't need sub-second accuracy for their own stats.
```

---

## Key Trade-offs

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| Pre-computed FYP vs real-time | Pre-computed (Redis cache) | Stale if user tastes change rapidly in session; saves 200ms+ per request |
| Progressive MP4 vs HLS for short vids | Progressive for ≤60s | Simpler encoding, faster start; no seek granularity |
| Completion rate vs likes for ranking | Completion rate | More honest signal but requires client to stream events; like is cheaper to collect |
| Redis sorted set for likes | Redis INCR (approximate) | Sub-ms, but may lose counts on crash; restore from Kafka replay |
| Single CDN vs multi-CDN | Multi (ByteDance + Akamai) | Redundancy + geo coverage; added routing complexity |
| Cassandra for videos | Cassandra | Scales writes horizontally; no JOINs (acceptable since video is self-contained doc) |

---

## Sharding Strategy

```
Videos DB (Cassandra): partition by video_id (UUID) → natural distribution
Users DB (Postgres): shard by user_id once > 100M users
  Shard 0: user_id 0..999M
  Shard 1: user_id 1B..1.999B
  etc.

FYP cache (Redis): consistent hash on user_id
  32 Redis nodes, ~31M users per node
  Each node: ~31M × 1000 video_ids × 8 bytes = ~248GB → needs 512GB RAM node
  Or: store only top 50 candidates in Redis, fetch rest from Cassandra on demand

Watch events (Kafka): partition by user_id
  50K partitions for 50M events/sec / 1000 events/partition/sec
```

---

## Real-World Reference
- **ByteDance engineering blog** (2021): "Monolith" system — online training feature store that updates recommendation model in real-time based on watch events (not daily batch)
- **Meta Reels** engineering blog: Two-Tower candidate retrieval + sequential recommendation model
- **Netflix** (2016): "Why we use HLS for short previews" — progressive download is faster for < 60s clips
- **Akamai** case study: TikTok pre-warms top 10K trending videos to all edge nodes every 15 min → 95% cache hit rate for popular content
