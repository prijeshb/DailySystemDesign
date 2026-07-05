# Live Streaming Platform (Twitch) — System Design
**Date:** 2026-06-23

---

## First Principles: Do We Even Need This?

**Problem being solved:**
Creators want to broadcast video in real-time. Viewers want to watch and chat simultaneously. The interaction loop (chat reaction → streamer reacts) is what separates live from VOD.

**Why is this hard?**
1. Video ingest: One stream = 6–10 Mbps of sustained data, 24/7
2. Transcoding: Must produce multiple quality ladders within seconds of capture
3. Distribution: 3M concurrent viewers × 3 Mbps = 9 Tbps egress
4. Chat at scale: Popular channels get 10K+ messages/min; must fanout to all viewers
5. Latency tension: Lower latency → smaller segments → more CDN requests → more cost
6. Streamer reconnect: RTMP drops happen; stream must resume without losing audience

**Simplest version that works:**
Single RTMP ingest server + FFmpeg transcoder + S3 segments + CloudFront + WebSocket chat server. Works for <1K concurrent viewers before CDN cost and single points of failure matter.

---

## Entities & Actions

### Entities
```
User         id, username, email, role(streamer/viewer), created_at
Channel      id, owner_id, title, category_id, is_live, viewer_count, stream_key
Stream       id, channel_id, started_at, ended_at, peak_viewers, vod_url
Segment      id, stream_id, sequence_num, s3_key, duration_ms, created_at
ChatMessage  id, channel_id, user_id, body, created_at
Category     id, name, box_art_url
Clip         id, stream_id, creator_id, start_offset_sec, duration_sec, s3_key
Subscription id, subscriber_id, channel_id, tier, expires_at
Donation     id, sender_id, channel_id, amount_cents, message, stream_id
```

### Actions
| Actor    | Action                          | Frequency |
|----------|---------------------------------|-----------|
| Streamer | Go live (RTMP connect)          | Low       |
| Streamer | End stream                      | Low       |
| Viewer   | Watch stream (HLS playlist poll)| Very High |
| Viewer   | Send chat message               | High      |
| Viewer   | Subscribe / donate              | Medium    |
| Platform | Transcode ingest → segments     | Continuous|
| Platform | Update m3u8 manifest            | Every 2s  |
| Platform | Fanout chat to channel viewers  | High      |

---

## Scale Estimates (Back of Envelope)

**Assumptions:**
- 100K simultaneous live streams
- 3M concurrent viewers; avg 30 viewers/stream (long tail: top 100 streams get 80% of traffic)
- Avg bitrate delivered: 3 Mbps (mix of 1080p, 720p, 480p viewers)
- Segment duration: 2 seconds (LL-HLS) → 500 segments/hour per stream
- Chat: top 1K channels avg 5K msgs/min = 5M msgs/min total

**Ingest bandwidth:**
- 100K streams × 6 Mbps (source) = 600 Gbps ingest
- Transcoder output per stream: 6 + 3 + 1.5 + 0.8 Mbps ≈ 11.3 Mbps → 1.13 Tbps to segment storage

**CDN egress:**
- 3M viewers × 3 Mbps = 9 Tbps peak

**Storage:**
- Segment: 2s × 6 Mbps = 1.5 MB per segment (source quality)
- Total per stream/hour: 1.5 MB × 1800 = 2.7 GB/hr (source only)
- 100K streams × 2.7 GB = 270 TB/hr → segments expire after 24h (VOD separately)
- Live rolling window (12h): ~3.2 PB

**AWS Cost Snapshot (peak load):**
| Component         | Unit Cost             | Est. Monthly      |
|-------------------|-----------------------|-------------------|
| CDN egress (CF)   | $0.085/GB (list)      | ~$1.9B (list)     |
| CDN egress (CF)   | ~$0.005/GB (committed)| ~$110M            |
| EC2 transcoding   | c6i.8xlarge $1.36/hr  | ~$5M (5K nodes)   |
| S3 storage        | $0.023/GB             | ~$74M (segments)  |
| Ingest EC2        | m6i.4xlarge $0.77/hr  | ~$750K (1K nodes) |

> **Real Twitch:** ~$100–120M/yr infrastructure cost (2022 leak). CDN is negotiated at $0.002–0.005/GB.

### Budget-Constrained Startup Example

**Budget: $5,000/month**

Constraints:
- CDN: $3K/month → at $0.085/GB = 35 TB/month
- Transcoding: $1K → ~30 c5.2xlarge hours/day
- Storage: $500 → ~22 TB

Limits:
- Max ~10 concurrent streams at 720p, ~200 viewers each
- Must disable 1080p transcoding; single quality ladder
- Chat: self-hosted WebSocket on same EC2 (no Redis cluster)

**What you sacrifice:** redundancy, 1080p, VOD storage beyond 7 days, global CDN PoPs

---

## Data Flow

```
Streamer OBS/App
    │  RTMP (TCP)
    ▼
Ingest Server (RTMP edge, assigns stream key → channel)
    │  validates stream_key → Channel DB
    │  raw H.264/AAC
    ▼
Transcoding Queue (Kafka topic: raw_segments)
    │
    ├──► FFmpeg Worker Pool (one job per quality ladder)
    │         1080p / 720p / 480p / 360p
    │         Output: 2-second .ts segments
    ▼
Segment Store (S3: /streams/{stream_id}/{quality}/{seq}.ts)
    │
    ├──► Manifest Updater (appends to .m3u8 every 2s)
    │         /streams/{stream_id}/index.m3u8  (master)
    │         /streams/{stream_id}/720p.m3u8   (variant)
    ▼
CDN (CloudFront / Fastly)
    │  Viewer polls .m3u8 every 2s
    │  Fetches .ts segments on cache miss
    ▼
Viewer HLS Player (auto quality selection by ABR logic)

Chat Path:
Viewer → HTTPS POST /chat/{channel_id} → Chat API
    → Kafka topic: chat_messages
    → Chat Fanout Workers (per channel shard)
    → Redis Pub/Sub (channel:{channel_id})
    → WebSocket servers subscribed to that channel
    → Push to all connected viewers
```

---

## High-Level Design

```
                    ┌─────────────────────────────────────────────┐
                    │             INGEST LAYER                     │
 Streamer ─RTMP──►  │  Ingest LB → Ingest Nodes (stateful, sticky)│
                    └───────────────┬─────────────────────────────┘
                                    │ raw video
                    ┌───────────────▼──────────────────┐
                    │        TRANSCODING LAYER          │
                    │  Kafka → FFmpeg Workers (auto-scale)│
                    │  Output: .ts segments → S3        │
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │       MANIFEST SERVICE            │
                    │  Polls new segments → updates     │
                    │  m3u8 in S3 + invalidates CDN     │
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │              CDN                  │
                    │  CloudFront/Fastly with short TTL │
                    │  (2s for .m3u8, 300s for .ts)     │
                    └───────────────┬──────────────────┘
                                    │
                              Viewers (HLS)

                    ┌─────────────────────────────────────┐
                    │          CHAT LAYER                  │
 Viewers ─WS──────► │  Chat API → Kafka → Fanout Workers  │
                    │  → Redis Pub/Sub → WS Servers        │
                    └─────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │       DISCOVERY / METADATA           │
                    │  Channel Service, Search, Recommend  │
                    │  PostgreSQL + Elasticsearch          │
                    └─────────────────────────────────────┘
```

---

## Low-Level Design

### 1. Stream Ingest & Key Validation
```
RTMP connect:
  client sends: rtmp://ingest.twitch.tv/live/{stream_key}
  Ingest node calls Auth Service: GET /validate?key={stream_key}
  → returns channel_id or 403
  → opens Stream record in DB: status=live
  → publishes to Kafka: stream_started event

Sticky routing: same streamer reconnects to same ingest node
  (Consistent hash on stream_key → ingest node)
  If node is dead → reassign to next node in ring
```

### 2. Transcoding Pipeline
```
Ingest node slices raw video into ~2s GOP-aligned chunks
→ publishes to Kafka: raw_segments topic (partitioned by stream_id)

FFmpeg Worker (one per quality per stream):
  - Consumes raw segment
  - Transcodes: H.264 video + AAC audio
  - Outputs: {stream_id}/{quality}/{seq:08d}.ts to S3
  - Publishes: segment_ready event → Manifest Service

Scaling: 1 FFmpeg worker per quality ladder per stream
  → 100K streams × 4 qualities = 400K workers
  → Each worker: ~2 vCPU, ~2 GB RAM
  → Kubernetes HPA on queue depth
```

### 3. HLS Manifest (m3u8)
```
Master manifest (static, cached long):
  #EXTM3U
  #EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080
  1080p.m3u8
  #EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720
  720p.m3u8

Variant manifest (updated every 2s):
  #EXTM3U
  #EXT-X-TARGETDURATION:2
  #EXT-X-MEDIA-SEQUENCE:4821
  #EXTINF:2.000,
  seg004819.ts
  #EXTINF:2.000,
  seg004820.ts
  #EXTINF:2.000,
  seg004821.ts   ← latest

Viewer player polls every 2s; on cache hit: instant
On cache miss: CDN fetches from S3 → ~50ms
```

### 4. Chat Fanout
```
Channel with 50K viewers:
  - All 50K WebSocket connections land on ~50 WS servers (1K viewers each)
  - Each WS server subscribes to Redis channel: channel:{channel_id}
  - Message arrives → Kafka → Fanout Worker → Redis PUBLISH
  - Each WS server's subscriber callback → push to 1K connections

Scale math:
  50K msgs/min on top channel = 833 msgs/sec
  Each WS server handles 833 PUBLISH deliveries → fans to 1K connections
  = 833K message-sends/sec per channel across 50 servers = ~16.7K/server
  Redis Pub/Sub: single-threaded; shard by channel_id if needed

Chat rate limiting: token bucket per user, per channel (Redis)
  Default: 1 msg/30s for new accounts, 1 msg/3s for subscribers
```

### 5. Viewer Count (HyperLogLog)
```
See Concepts_Index #19
Viewer connects WS → PFADD hll:viewers:{channel_id} {user_id}
Every 10s: PFCOUNT hll:viewers:{channel_id} → update channel.viewer_count
Error: ±0.81% — acceptable for display

On disconnect: hard to decrement HLL; use expiring heartbeat instead:
  Viewer sends heartbeat every 30s → SETEX viewer:{channel_id}:{user_id} 60 1
  Background job: SCAN + count active keys (expensive)
  Better: use sliding window counter in Redis Sorted Set
    ZADD viewers:{channel_id} {timestamp} {user_id}
    ZREMRANGEBYSCORE ... 0 (now-60s)
    ZCARD → live count
```

### 6. Clip Creation
```
User clicks "Clip" → sends {channel_id, end_offset, duration=30s}
Clip Service:
  - Identifies last 30s of segments from Segment table
  - Enqueues async clip job
Clip Worker:
  - Downloads segments from S3
  - FFmpeg concat + transcode → MP4
  - Uploads to S3 under /clips/
  - Inserts Clip record
  - Returns clip URL to user

Idempotency: clip_key = hash(channel_id + user_id + timestamp_bucket)
  → UPSERT on duplicate key (prevent double-click duplicates)
```

### 7. Database Schemas
```sql
-- Channels (PostgreSQL)
CREATE TABLE channels (
  id UUID PRIMARY KEY,
  owner_id UUID NOT NULL REFERENCES users(id),
  stream_key TEXT UNIQUE NOT NULL,  -- hashed
  title TEXT,
  category_id INT,
  is_live BOOLEAN DEFAULT FALSE,
  viewer_count INT DEFAULT 0,
  started_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);
CREATE INDEX idx_channels_live ON channels(is_live, viewer_count DESC);

-- Streams
CREATE TABLE streams (
  id UUID PRIMARY KEY,
  channel_id UUID NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  peak_viewers INT DEFAULT 0,
  vod_s3_key TEXT
);

-- Segments (time-series, partition by day)
CREATE TABLE segments (
  id UUID,
  stream_id UUID NOT NULL,
  quality TEXT NOT NULL,  -- '1080p','720p','480p','360p'
  sequence_num INT NOT NULL,
  s3_key TEXT NOT NULL,
  duration_ms INT,
  created_at TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (created_at);
-- Expire partitions older than 24h (keep only for DVR rewind)

-- Chat (Cassandra — high write, time-series)
CREATE TABLE chat_messages (
  channel_id UUID,
  created_at TIMEUUID,
  user_id UUID,
  body TEXT,
  PRIMARY KEY ((channel_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND default_time_to_live = 86400;  -- keep 24h
```

---

## Trade-offs

| Decision | Chose | Why | Cost |
|----------|-------|-----|------|
| RTMP ingest | RTMP over WebRTC | Stable, OBS native, proven at scale | 5–7s latency |
| LL-HLS vs standard HLS | LL-HLS (2s segments) | ~3–5s latency (vs 15–30s standard) | 3× more CDN requests |
| Redis Pub/Sub for chat | Yes | Sub-ms fanout, simple | Single-threaded; must shard at very high scale |
| Cassandra for chat | Yes | High write throughput, TTL native | No ad-hoc queries; schema must match access pattern |
| CDN push vs pull | Pull | Segments are unpredictable; push wastes bandwidth | Cache miss on first viewer per PoP |
| FFmpeg workers on K8s | Yes | Auto-scale, isolated per stream | Cold start ~10s; brief quality drop on worker restart |
| HyperLogLog for viewers | Yes | O(1) memory, ±0.81% error | Cant get exact count; cant decrement |

---

## Real-World References

- **Twitch engineering blog**: Describes moving from HLS to LL-HLS; segment duration reduced from 8s → 2s
- **Mux.com blog**: "How live video transcoding pipelines work" — per-GOP FFmpeg workers
- **AWS re:Invent 2019**: Twitch real-time messaging at scale — Redis Pub/Sub sharding
- **Netflix tech blog (CMAF)**: Common Media Application Format for reducing latency
- **Akamai**: CDN pull-through caching for live HLS reduces origin load by 90%+ at popular events
