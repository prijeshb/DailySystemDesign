# Video Streaming Platform System Design (YouTube)
**Date:** 2026-05-13 | **Difficulty:** Hard | **Category:** Distributed Storage + Media Pipeline

---

## 1. Problem Framing & Scope

Design a video streaming platform where creators upload videos, the system processes and stores them at scale, and viewers can watch on-demand with adaptive quality across devices and networks globally.

### Functional Requirements
- Creators can upload videos (any size, up to 50 GB)
- System transcodes uploads into multiple resolutions and codecs
- Viewers can search, browse, and stream videos
- Adaptive bitrate streaming — quality adjusts to network conditions in real-time
- Videos are globally available with low latency
- Creators can see view counts, watch time, engagement metrics
- Viewers can like, comment, and subscribe to channels
- Video feed / home page shows personalized recommendations

### Non-Functional Requirements
- **Upload throughput:** 500 hours of video uploaded every minute (YouTube real stat)
- **Streaming scale:** 2.5 billion monthly active users; peak ~50M concurrent viewers
- **Latency:** stream start < 2 seconds; upload acknowledged < 500ms
- **Availability:** 99.99% for streaming; 99.9% for uploads (brief upload retries acceptable)
- **Durability:** zero video loss after upload confirmation
- **Storage:** petabytes of video data; each raw video stored in 8+ transcoded variants
- **CDN coverage:** global, < 50ms to nearest edge for 95% of users

### Out of Scope
- Live streaming (separate real-time pipeline)
- Monetization / ad insertion
- Creator Studio analytics deep dives
- Copyright / content-ID matching internals

---

## 2. Entities

```
User
├── user_id (UUID)
├── email, display_name, handle
├── avatar_url
├── created_at
└── role: VIEWER | CREATOR | BOTH

Channel
├── channel_id (UUID)
├── owner_user_id
├── name, description, banner_url
├── subscriber_count (denormalized, eventually consistent)
└── created_at

Video
├── video_id (UUID, also used as object-store key prefix)
├── channel_id
├── title, description, tags[]
├── status: UPLOADING | PROCESSING | READY | FAILED | DELETED
├── raw_object_key         ← location in object store before transcoding
├── duration_seconds
├── thumbnail_url
├── view_count (approx, counter shard)
├── like_count (approx)
├── uploaded_at, published_at
└── visibility: PUBLIC | UNLISTED | PRIVATE

VideoVariant  (one row per transcoded file)
├── variant_id
├── video_id
├── resolution: 144p | 240p | 360p | 480p | 720p | 1080p | 1440p | 4K
├── codec: H264 | VP9 | AV1
├── bitrate_kbps
├── object_key             ← path in object store
├── manifest_key           ← HLS/DASH manifest file
└── size_bytes

Segment  (not stored in DB — files in object store)
├── segment_000.ts / segment_000.m4s
├── ...N segments per variant
└── duration: ~4 seconds each (DASH) / 6–10 seconds (HLS)

Comment
├── comment_id (UUID)
├── video_id, user_id
├── parent_comment_id (for threads)
├── body (text, max 10,000 chars)
├── like_count
└── created_at

Subscription
├── subscriber_id (user_id)
├── channel_id
└── subscribed_at

WatchEvent  (analytics, write-heavy, append-only)
├── event_id
├── user_id (nullable for anon)
├── video_id
├── session_id
├── watch_duration_seconds
├── quality_played (720p, etc.)
└── timestamp
```

---

## 3. Core Actions & APIs

### Upload API
```
POST /uploads/initiate
  Body: { filename, file_size_bytes, content_type, title, description }
  Returns: {
    upload_id,
    upload_urls: [{ part_number, presigned_url }],   ← multipart S3-style
    chunk_size_bytes: 10_485_760  (10 MB per chunk)
  }
  → Creates Video record with status=UPLOADING

PUT {presigned_url}
  → Creator uploads each chunk directly to object store (bypasses app servers)

POST /uploads/{upload_id}/complete
  Body: { parts: [{ part_number, etag }] }
  Returns: { video_id, status: "PROCESSING" }
  → Object store assembles chunks; triggers transcoding pipeline
```

### Streaming API
```
GET /videos/{video_id}
  Returns: { video metadata, manifest_url, thumbnail_url, available_qualities[] }

GET /manifests/{video_id}/master.m3u8        ← HLS master manifest
GET /manifests/{video_id}/master.mpd         ← DASH manifest
  → Returns list of variant streams with bandwidth hints
  → Client player picks appropriate quality

GET /segments/{video_id}/{quality}/seg_{n}.ts   ← served from CDN edge
```

### Feed & Discovery API
```
GET /feed?page_token=...
  Returns: { videos: [...VideoCard], next_page_token }

GET /search?q=...&filter=...&sort=relevance|date|views
  Returns: { videos: [...VideoCard], total_hits }

POST /videos/{video_id}/views          ← fire-and-forget watch event
POST /videos/{video_id}/likes
POST /videos/{video_id}/comments
GET  /videos/{video_id}/comments?sort=top|new
```

---

## 4. Data Flow

### 4a. Upload & Processing Flow

```
Creator Browser / App
        │
        │ 1. POST /uploads/initiate
        ▼
   API Gateway ──► Upload Service
        │               │
        │               │ 2. Create Video(status=UPLOADING)
        │               │    Generate multipart presigned URLs
        │               │
        │        ◄──────┘
        │
        │ 3. PUT chunks directly to Object Store (S3/GCS)
        │    (bypasses app servers — direct upload)
        ▼
   Object Store (Raw Bucket)
        │
        │ 4. POST /uploads/{id}/complete
        ▼
   Upload Service
        │ 5. Trigger: publish VideoUploaded event to Kafka
        │    { video_id, raw_object_key, duration_hint }
        ▼
      Kafka
        │
        │ 6. Transcoding Orchestrator consumes event
        ▼
 Transcoding Orchestrator
        │ 7. Spawns N parallel transcoding jobs
        │    One job per (resolution × codec) combination
        │    e.g., 8 resolutions × 3 codecs = up to 24 jobs
        │    In practice: H264 for all, VP9 for 720p+, AV1 for modern clients
        ▼
 Transcoding Worker Pool (stateless, horizontally scaled)
        │ 8. Worker pulls raw video from object store
        │    Runs FFmpeg: decode → filter → encode → segment
        │    Segments: ~4s each (DASH) / ~6s each (HLS)
        │    Uploads segments + manifest to CDN-origin bucket
        ▼
   Object Store (Processed Bucket)
        │
        │ 9. Worker publishes TranscodingJobComplete to Kafka
        ▼
 Transcoding Orchestrator
        │ 10. When all critical variants done (e.g., 360p, 720p):
        │     Update Video(status=READY)
        │     Publish VideoReady event
        ▼
   Video Metadata DB (PostgreSQL)
        │
        │ 11. Notification Service consumes VideoReady
        │     Notifies subscribers via push/email
        ▼
   Subscribers' Feeds (Fan-out Service)
```

### 4b. Streaming / Playback Flow

```
Viewer Browser / App
        │
        │ 1. GET /videos/{video_id}  → metadata + manifest URL
        ▼
   API Gateway → Video Service → PostgreSQL (metadata)
                               → Cache (Redis) [TTL: 60s]
        │
        │ 2. Player fetches master manifest
        ▼
   CDN Edge (nearest PoP)
        │  Cache HIT? → serve immediately
        │  Cache MISS? → fetch from CDN Origin → Object Store
        │
        │ 3. Player reads manifest, picks initial quality
        │    (based on network speed estimate)
        │
        │ 4. Player fetches segments sequentially
        ▼
   CDN Edge (same or nearby)
        │  Segments are pre-cached at edge (popular videos)
        │  Each segment: ~500KB–3MB depending on quality
        │
        │ 5. Player buffers 3–5 segments ahead
        │    ABR algorithm adjusts quality every segment
        │    If bandwidth drops: step down quality
        │    If buffer > 20s: step up quality
        ▼
   Rendered to screen
        │
        │ 6. Client fires WatchEvent every 30s (batch)
        ▼
   Analytics Ingestion Service → Kafka → ClickHouse / BigQuery
```

---

## 5. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                                 │
│   Browser (DASH)     iOS/Android (HLS)     Smart TV (HLS/DASH)     │
└──────────────┬──────────────────┬─────────────────────┬─────────────┘
               │                  │                     │
               ▼                  ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GLOBAL CDN (3,000+ PoPs)                          │
│   Edge Cache: video segments, manifests, thumbnails                  │
│   Anycast DNS → nearest PoP                                          │
│   Cache-Control: segments = 1 year, manifests = 60s                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ cache miss
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      API GATEWAY / LOAD BALANCER                     │
│   TLS termination, rate limiting, auth token validation              │
│   Routes: /api/* → App Services    /segments/* → CDN Origin         │
└──┬──────────────┬──────────────┬────────────────┬────────────────────┘
   │              │              │                │
   ▼              ▼              ▼                ▼
Upload        Video/Feed     Search           Analytics
Service       Service        Service          Ingestion
   │              │              │                │
   │              │              │                ▼
   │         ┌────┴───┐    Elasticsearch      Kafka
   │         │ Redis  │    (video index)         │
   │         │ Cache  │                          ▼
   │         └────┬───┘                      ClickHouse
   │              │
   ▼              ▼
Object        PostgreSQL          ┌──────────────────────┐
Store ──────► (video metadata,   │  TRANSCODING PIPELINE │
(Raw +         users, comments)   │  Orchestrator + Workers│
Processed)         │              │  FFmpeg / cloud jobs   │
                   │              └──────────┬───────────┘
                   └──────── Kafka ──────────┘
                         (event bus)
```

---

## 6. Low-Level Design

### 6a. Video Upload: Resumable Chunked Upload

**Problem:** 50 GB videos cannot be uploaded in a single HTTP request. Network drops will lose everything.

**Solution: Multipart Upload (S3-compatible)**

```
1. Client calls POST /uploads/initiate
   → Server calls object-store CreateMultipartUpload API
   → Gets upload_id from object store
   → Pre-generates N presigned PUT URLs (one per 10 MB chunk)
   → Stores { upload_id, video_id, expected_parts } in Upload DB

2. Client uploads chunks in parallel (8 concurrent connections)
   → Each PUT goes directly to object store
   → Object store returns ETag per part
   → Client stores { part_number → etag } locally (for resume)

3. If upload interrupted:
   → Client resumes: fetches which parts are missing
   → Re-uploads only missing parts

4. Client calls POST /uploads/{id}/complete with all { part, etag } pairs
   → Object store reassembles into single object atomically
   → Upload Service updates Video status, publishes VideoUploaded to Kafka
```

**Why presigned URLs?**
Creators upload directly to object store. App servers never touch raw video bytes. At 500 hours/minute of uploads, this avoids routing gigabytes/second through app tier.

---

### 6b. Transcoding Pipeline

**Input:** Raw video (any codec, any container: .mp4, .mov, .mkv, .avi)
**Output:** Segmented MPEG-DASH + HLS manifests at multiple qualities

**Resolution ladder (YouTube-style):**
```
Resolution   Bitrate (H264)  Bitrate (VP9)   Segment Duration
─────────────────────────────────────────────────────────────
144p          80 Kbps         70 Kbps         4s (DASH)
240p         200 Kbps        170 Kbps         4s
360p         500 Kbps        430 Kbps         4s
480p        1000 Kbps        870 Kbps         4s
720p        2500 Kbps       1800 Kbps         4s
1080p       5000 Kbps       3500 Kbps         4s
1440p       8000 Kbps       6000 Kbps         4s
4K         16000 Kbps      12000 Kbps         4s
```

**Transcoding Job States (stored in DB):**
```
PENDING → CLAIMED → TRANSCODING → UPLOADING_SEGMENTS → DONE
                                                     → FAILED (retryable)
                                                     → DEAD (manual review)
```

**Worker Design:**
- Workers are stateless VMs / containers; pull jobs from queue
- Each worker claims one (video_id, quality, codec) job atomically via DB row lock
- Worker downloads raw video, runs FFmpeg, uploads segments to object store
- Uses exponential backoff + 3 retries on failure
- Heartbeat every 30s: if worker dies, Orchestrator re-queues job after 2 min

**Orchestrator Logic:**
```python
def on_job_complete(video_id, quality, codec):
    mark_variant_done(video_id, quality, codec)
    
    # Critical path: enable playback as soon as 360p + 720p done
    if has_minimum_variants(video_id, required=['360p_h264', '720p_h264']):
        set_video_status(video_id, READY)
        publish(VideoReady, video_id)
    
    # Non-critical: 4K, AV1 variants continue in background
    if all_variants_done(video_id):
        publish(VideoFullyProcessed, video_id)
        delete_raw_file(video_id)   # save storage, raw no longer needed
```

**Why delete raw after transcoding?**  
Raw video is 5–20x larger than transcoded output. At 500 hours/min upload rate, storing raw files would cost ~$50M+/year in storage alone.

---

### 6c. CDN and Adaptive Bitrate Streaming

**CDN Strategy:**
- Popular videos: pre-warmed at edges (top 0.1% of videos = 90% of traffic)
- Long-tail videos: pulled to edge on first request, cached for 24h
- Thumbnails + manifests: served from CDN (short TTL: 60s)
- Segments: immutable (URL contains content hash) → TTL = 1 year

**HLS Master Manifest (`.m3u8` example):**
```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=640x360
360p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p/index.m3u8
```

**ABR (Adaptive Bitrate) Algorithm — client-side:**
1. Player maintains a throughput estimate (EWMA of last 5 segment download times)
2. Player maintains buffer level (seconds of video buffered ahead)
3. Decision every segment:
   - If buffer < 10s AND throughput < current_bitrate × 1.2 → step down
   - If buffer > 30s AND throughput > next_bitrate × 1.5 → step up
   - Otherwise: hold
4. Hysteresis prevents oscillation (requires sustained improvement before stepping up)

**Trade-off:** More frequent segments (2s) = faster quality switches, but more HTTP overhead. Less frequent (10s) = fewer requests, but slower adaptation. YouTube uses ~4s for DASH, 6s for HLS.

---

### 6d. Video Metadata & Feed

**Database Choice: PostgreSQL with Vitess sharding**
- Videos sharded by `video_id` (hash)
- Channels and Users replicated (small tables)
- Comments sharded by `video_id` (keeps all comments for a video on same shard)

**View Count Problem:**  
`UPDATE videos SET view_count = view_count + 1` at 50M concurrent viewers = millions of writes/sec to hot rows.

**Solution: Counter Shards**
```
view_count_shards table:
  video_id | shard_id | count
  ──────────────────────────────
  vid_123  |    0     | 48,291
  vid_123  |    1     | 51,004
  vid_123  |    2     | 49,827
  ...      |   ...    |   ...
  (16 shards per video)

Write: UPDATE shard WHERE shard_id = rand(0,15)
Read:  SELECT SUM(count) FROM view_count_shards WHERE video_id = ?
```

**Feed / Recommendation:**  
- Home feed generated by ML ranking service (not designed here, out of scope)
- Subscription feed: fan-out on read (pull model)
  - When user opens app: fetch latest N videos from subscribed channels
  - Sorted by published_at, merged, ranked
  - Creator with 100M subscribers → fan-out on write would queue 100M events on each upload (push model is too expensive for mega-channels)

---

### 6e. Search

- Video metadata indexed into **Elasticsearch**
- Index fields: title (boosted), description, tags, channel_name, transcript (speech-to-text output)
- On video publish: Video Service publishes VideoPublished to Kafka → Search Indexer consumes and upserts into ES
- Search ranking: BM25 text score × engagement signal (views, likes, recency)
- Autocomplete: prefix trie index on Elasticsearch with `search_as_you_type` mapping

---

## 7. Key Trade-offs

| Decision | Choice | Benefit | Cost |
|---|---|---|---|
| Presigned URLs for upload | Direct-to-object-store | No bandwidth through app servers | Presigned URL expiry complexity; client must handle multi-part logic |
| Fan-out on read for subscriptions | Pull per channel on open | No write fan-out storm for mega-creators | Feed generation latency; heavier reads at open time |
| Counter shards for view counts | 16 DB rows per video | 16x write throughput; no hot row | Reads are slower (SUM query); slightly stale between shards |
| CDN immutable segments (1yr TTL) | Hash-in-URL scheme | Zero origin load for cached segments | Must re-encode to change a segment (can't patch in place) |
| Delete raw after transcoding | Raw deleted post-processing | ~80% storage saving | Cannot re-transcode for new codecs (must re-upload; or keep archival copy) |
| Segment size 4s | Short segments | Fast ABR switching, low startup latency | More HTTP requests, more manifest entries |
| Postgres + Vitess sharding | Relational + horizontal scale | ACID transactions, familiar | Vitess adds operational complexity; cross-shard joins impossible |
| Elasticsearch for search | Dedicated search engine | Rich text search, fast aggregations | Eventual consistency; separate infra to maintain |

---

## Real-World References

- YouTube processes **500 hours of video per minute** — all transcoded, stored, and distributed within minutes
- YouTube's **Vitess** (originally built at YouTube) is now open-source and used by Slack, GitHub, and PlanetScale
- **AV1 codec** saves ~30% bitrate vs VP9 at same quality — YouTube rolled out AV1 in 2019 for supported devices
- **Google's CDN** has 3,000+ edge PoPs; segment cache-hit rates exceed 95% for popular content
- YouTube uses **Bigtable** for some high-throughput write paths (watch history, analytics events)
- Adaptive bitrate pioneered by Apple (HLS, 2009) and later standardized as MPEG-DASH (2012)
