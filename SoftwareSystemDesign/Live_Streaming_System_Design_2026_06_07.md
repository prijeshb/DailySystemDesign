# Live Streaming System Design (Twitch)
**Date:** 2026-06-07  
**Difficulty:** Hard  
**Real-world refs:** Twitch, YouTube Live, Instagram Live, Kick.com

---

## 0. First Principles — Do We Even Need This?

**What is live streaming?** A broadcaster sends video in real time → millions of viewers watch with ~5–30s delay.

**Why not just use regular video upload?**
- Upload = full file exists before viewers see it → can't do real-time
- Live streaming = encode-as-you-go, chunk into small segments, serve as they're generated

**Why not WebRTC for delivery?**
- WebRTC = peer-to-peer, designed for <500ms latency (video calls)
- At 100K viewers, broadcaster can't maintain 100K direct connections — CPU/bandwidth dies
- CDN can't cache WebRTC (it's peer-to-peer)
- Trade-off: HLS/DASH via CDN = ~5-15s delay but scales to millions. WebRTC = <1s but max ~10K viewers

**Do we need exact viewer count?**
- No — "1.2M watching" is fine if off by 5%. HyperLogLog over exact SET.
- Billing uses session records, not viewer count display

---

## 1. Scope & Requirements

### Functional
- Streamer broadcasts live video from desktop (OBS) or browser
- Viewers watch with ≤15s latency (acceptable for gaming/events)
- Multiple quality options: 1080p60, 720p60, 480p, 360p (adaptive bitrate)
- Live chat alongside stream
- Stream recorded → available as VOD after broadcast ends
- Discover live streams (browse by game/category)

### Non-Functional
- **Scale:** 10M concurrent viewers, 100K concurrent streamers
- **Availability:** 99.99% for playback (viewers drop → revenue drop)
- **Ingest availability:** 99.9% (streamer disconnect is recoverable)
- **Latency to first frame:** ≤3s for viewer
- **Recording:** All streams stored as VOD, 90-day retention

### Out of Scope
- Ads insertion (SSAI — separate system)
- Subscription/payment
- Recommendation ML

---

## 2. Entities

```
Streamer
  id              UUID
  username        string
  stream_key      string (secret, authenticates RTMP push)
  channel_id      UUID

Stream (one per broadcast)
  id              UUID
  channel_id      UUID
  title           string
  game_id         UUID
  status          enum(LIVE, ENDED, PROCESSING_VOD)
  started_at      timestamp
  ended_at        timestamp
  thumbnail_url   string
  viewer_peak     int

StreamSegment (internal, never user-facing)
  stream_id       UUID
  sequence_num    int
  quality         enum(1080p60, 720p60, 480p, 360p)
  s3_key          string
  duration_ms     int
  created_at      timestamp

VOD
  id              UUID
  stream_id       UUID
  duration_sec    int
  playlist_url    string   (HLS master playlist on S3)
  status          enum(PROCESSING, READY, DELETED)

User (viewer)
  id              UUID
  username        string

Game
  id              UUID
  name            string
  box_art_url     string

ChatMessage
  id              UUID
  stream_id       UUID
  user_id         UUID
  text            string
  sent_at         timestamp
```

---

## 3. Actions

| Actor | Action | Key Concern |
|-------|--------|-------------|
| Streamer | Go live (RTMP push) | Auth stream_key, assign ingest server |
| Streamer | End stream | Finalize VOD, update status |
| Viewer | Watch stream | Low latency, adaptive quality |
| Viewer | Send chat message | Real-time, ordered |
| Viewer | Browse discover page | Cached, sorted by viewer count |
| System | Transcode segments | Multi-bitrate, real-time |
| System | Generate HLS playlist | Updated every ~2s |
| System | Aggregate viewer count | Approximate, HyperLogLog |
| System | Archive VOD | Concatenate segments after stream ends |

---

## 4. Data Flow

### 4A. Ingest (Streamer → System)

```
OBS / Browser
    │  RTMP (tcp:1935)
    ▼
Ingest Server (nginx-rtmp or SRS)
    │  On-connect: validate stream_key → auth DB lookup
    │  On-publish: create Stream record, status=LIVE
    │  On-receive-video: pipe to Transcoder via FFMPEG
    ▼
Transcoder Cluster (FFmpeg workers)
    │  Encode to: 1080p60, 720p60, 480p, 360p
    │  Segment size: 2 seconds each (.ts files)
    │  Generate updated .m3u8 playlist per quality
    ▼
S3 (segment storage)
    │  Path: s3://vod/{stream_id}/{quality}/{seq_num}.ts
    │        s3://vod/{stream_id}/{quality}/live.m3u8
    ▼
CDN (pull from S3)
```

### 4B. Playback (Viewer → CDN)

```
Viewer Player (HLS.js or native)
    │  GET https://cdn.twitch.tv/{stream_id}/master.m3u8
    ▼
CDN Edge (nearest PoP)
    │  Cache miss → pull from S3
    │  Cache hit  → serve directly
    │  master.m3u8: lists all quality playlists
    │
    │  Player selects quality based on bandwidth estimate
    │  GET live.m3u8  (quality-specific, updated every 2s)
    │  GET {seq}.ts   (2s segment, ~400KB–2MB each)
    │
    │  TTL for segments: 86400s (immutable, never change)
    │  TTL for playlist: 1–2s  (always near-current)
    ▼
Viewer sees video
```

### 4C. Chat

```
Viewer → WebSocket → Chat Service → Redis pub/sub (channel:{stream_id})
                                  → All chat WS servers subscribed
                                  → Fan-out to viewers on same stream

Persistence: Cassandra (append-only, never joined, massive write volume)
  chat_messages (stream_id, sent_at, message_id, user_id, text)
  Partition key: (stream_id, time_bucket)  ← prevents hot partition
```

### 4D. Viewer Count

```
Every viewer client: heartbeat POST /heartbeat {stream_id, user_id} every 30s
    ▼
Viewer Count Service
    PFADD stream:{stream_id}:viewers user_id   ← Redis HyperLogLog
    EXPIRE stream:{stream_id}:viewers 60        ← auto-expire on 2 missed heartbeats

GET /streams/{stream_id}/viewers
    PFCOUNT stream:{stream_id}:viewers   ← ~0.81% error, O(1)
```

---

## 5. High-Level Architecture

```
                    ┌─────────────────────────────────┐
Streamer (OBS) ─RTMP─►  Ingest Cluster               │
                    │   (nginx-rtmp, geo-distributed) │
                    │              │                  │
                    │         FFmpeg Workers          │
                    │   (multi-bitrate transcoding)   │
                    │              │                  │
                    │         S3 (segments)           │
                    └─────────────┼───────────────────┘
                                  │ CDN pull
                    ┌─────────────▼───────────────────┐
Viewer ◄── HLS ────  CDN (CloudFront / Fastly)        │
                    │  Edge PoPs globally             │
                    └─────────────────────────────────┘

                    ┌─────────────────────────────────┐
API Gateway         │  Stream API (Postgres)          │
  ├─ /streams       │  - metadata, LIVE/ENDED status  │
  ├─ /discover      │  - game catalog, thumbnails     │
  └─ /viewer-count  │  Redis: viewer HyperLogLog      │
                    └─────────────────────────────────┘

                    ┌─────────────────────────────────┐
Chat               │  Chat Service                   │
Viewer ─WS─►       │  Redis pub/sub per stream       │
                    │  Cassandra for persistence      │
                    └─────────────────────────────────┘
```

---

## 6. Low-Level Design

### 6A. RTMP Ingest → Transcoder Pipeline

```
Ingest server receives RTMP stream from streamer.

On connect:
  1. Extract stream_key from URL (rtmp://ingest.twitch.tv/live/{stream_key})
  2. Lookup in DB: SELECT streamer_id WHERE stream_key = $1
  3. If not found → reject connection (auth failure)
  4. Create Stream record: INSERT streams (id, status='LIVE', started_at=now())
  5. Publish event to Kafka: stream.started {stream_id, streamer_id}

On receiving video (continuous):
  Ingest server → fork FFmpeg:
    ffmpeg -i rtmp://localhost/live/{stream_key}
      -map 0 -vf scale=1920:1080 -b:v 6000k -s3_key {stream_id}/1080p60/{n}.ts
      -map 0 -vf scale=1280:720  -b:v 4000k -s3_key {stream_id}/720p60/{n}.ts
      -map 0 -vf scale=854:480   -b:v 1500k -s3_key {stream_id}/480p/{n}.ts
      -map 0 -vf scale=640:360   -b:v 800k  -s3_key {stream_id}/360p/{n}.ts
      -hls_time 2 -hls_list_size 10

On each segment (.ts) written:
  1. Upload to S3 (multipart, async)
  2. Update HLS playlist (.m3u8) on S3
  3. Playlist has sliding window: last 10 segments (20s of buffer)
     → older segments still in S3 (for VOD), just removed from live playlist

On disconnect:
  1. Kafka event: stream.ended {stream_id}
  2. Update stream status = ENDED
  3. Trigger VOD processing job
```

### 6B. HLS Playlist Structure

```
Master playlist (master.m3u8) — static, cached long:
  #EXTM3U
  #EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080
  1080p60/live.m3u8
  #EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1280x720
  720p60/live.m3u8
  #EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360
  360p/live.m3u8

Quality playlist (720p60/live.m3u8) — sliding window, refreshed every 2s:
  #EXTM3U
  #EXT-X-VERSION:3
  #EXT-X-TARGETDURATION:2
  #EXT-X-MEDIA-SEQUENCE:847        ← current seq, viewer skips old segments
  #EXTINF:2.000,
  720p60/847.ts
  #EXTINF:2.000,
  720p60/848.ts
  ...
  720p60/856.ts                    ← last 10 segments = 20s window
```

**CDN TTL strategy:**
- `{seq}.ts` segments → `Cache-Control: max-age=86400` (immutable — never changes)
- `live.m3u8` → `Cache-Control: max-age=2, s-maxage=1` (near-real-time)
- `master.m3u8` → `Cache-Control: max-age=60` (changes rarely)

### 6C. VOD Processing (Post-Stream)

```
Trigger: stream.ended event from Kafka

VOD Worker:
  1. List all segments: S3.list_objects(prefix={stream_id}/)
     → sorted by sequence_num
  2. Generate VOD playlist (full, not sliding window):
     #EXT-X-ENDLIST tag added → signals "this is complete VOD, not live"
  3. Upload to S3: s3://vod/{stream_id}/vod_master.m3u8
  4. Update VOD record: status=READY, playlist_url=...
  5. Kafka: vod.ready {stream_id, vod_id}

No re-encoding needed — segments already on S3, just stitch playlists.
VOD is ready within 1–2 min of stream ending.
```

### 6D. Discover Feed

```
Postgres: streams table, sorted by viewer_count
  Freshness: viewer_count column updated every 60s from Redis HyperLogLog

Redis cache: discover:{game_id} sorted set
  ZADD discover:fortnite {viewer_count} {stream_id}
  TTL: 30s

GET /discover?game=fortnite&limit=20
  1. Check Redis: ZREVRANGE discover:fortnite 0 19 WITHSCORES
  2. Cache miss → Postgres: SELECT * FROM streams WHERE game_id=... AND status='LIVE' ORDER BY viewer_count DESC LIMIT 20
  3. Populate Redis, return

Trade-off: 30s stale discover data is fine. Viewer watching doesn't need real-time discover.
```

### 6E. Ingest Server Selection (Geo-routing)

```
Streamers connect to nearest ingest PoP (DNS-based geo-routing):
  ingest.twitch.tv → Anycast → nearest ingest cluster

Problem: ingest server selected, transcoder may be far → adds latency
Solution: ingest server IS the transcoder (or transcoder co-located in same DC)
  Segment upload to S3 from same DC → S3 region closest to ingest → CDN pulls

Ingest clusters: us-east, us-west, eu-west, ap-southeast
```

---

## 7. Storage Decisions

| Data | Store | Why |
|------|-------|-----|
| Video segments (.ts) | S3 | Durable, cheap, CDN-friendly, unlimited scale |
| HLS playlists (.m3u8) | S3 | Same CDN path as segments |
| Stream metadata | Postgres | Structured, JOIN with games/users, low volume |
| Chat messages | Cassandra | Append-only, massive writes, partition by (stream_id, time_bucket) |
| Viewer count | Redis HyperLogLog | Approximate unique count, O(1), 12KB fixed memory |
| Discover cache | Redis Sorted Set | ZADD/ZREVRANGE, O(log N) |
| Active heartbeats | Redis (with TTL) | Auto-expire viewers who disconnect |
| Chat real-time | Redis pub/sub | Fan-out per stream, not persistent |

---

## 8. Key Trade-offs

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| HLS vs WebRTC for delivery | HLS | +CDN scale (millions), -latency (5-15s vs <1s). Right for 1-to-many |
| RTMP vs WebRTC for ingest | RTMP | +OBS ecosystem, stable protocol. WebRTC = browser ingest only, higher complexity |
| 2s segments vs 6s | 2s | +lower latency (less buffer), -more S3 PUT requests, more playlist updates |
| Sliding playlist (10 segs) vs full | Sliding | +smaller file = faster poll, -viewer can't seek live. VOD playlist = full |
| Exact viewer count vs HyperLogLog | HyperLogLog | +O(1), 12KB memory, -~1% error. "1.2M viewers" doesn't need to be exact |
| Chat in Cassandra vs Postgres | Cassandra | +massive append throughput, -no complex queries (acceptable, we only read by stream+time) |
| CDN pull vs push | CDN pull | +only popular segments get cached, -first viewer triggers cache miss. For live, nearly all segments are popular → pull is fine |
