# Spotify / Music Streaming — System Design
**Date:** 2026-06-21

---

## First Principles: Do We Even Need This?

**Problem being solved:**
People want to listen to any song, instantly, on any device, anywhere — without downloading a 1.2PB catalog locally.

**Why is this hard?**
1. 100M+ songs × multiple quality tiers = petabytes of audio to serve globally
2. Music must start in <200ms (people leave if it buffers)
3. Royalties: every play must be tracked accurately (legal requirement)
4. Cross-device sync: pause on phone, resume on laptop from same position
5. Discovery: user doesn't know what they want — recommendation matters

**What's the simplest version?**
S3 bucket + signed URLs + a song list in Postgres. Works up to ~100K users. Everything beyond that is a scaling decision.

---

## Scale / Back-of-Envelope

| Metric | Estimate |
|--------|----------|
| Total users | 600M registered |
| DAU | 30M |
| Avg songs per user per day | 30 |
| Plays per day | 900M → ~10K plays/sec |
| Concurrent streams (peak) | 10M |
| Audio bitrate (standard) | 128kbps = 16KB/s per stream |
| Peak bandwidth | 10M × 16KB/s = **160GB/s** |
| Catalog size | 100M songs × avg 3.5MB (128kbps) = **350TB per quality tier** |
| Tiers (96/128/256/320kbps + FLAC) | ~1.8PB total storage |
| Playlist writes/sec | ~5K/sec (adds, reorders) |
| Search queries/sec | ~20K/sec |

### AWS Cost Estimate (at scale)

```
Audio storage (S3):
  1.8PB × $0.023/GB = ~$41K/month

CDN data transfer (CloudFront):
  160GB/s × 86,400s/day × 30 days = ~415PB/month
  At $0.008/GB blended = ~$3.3M/month  ← CDN is the dominant cost

EC2 (API, transcoding, recommendation):
  ~$200K/month

RDS (Postgres + read replicas):
  ~$30K/month

Elasticsearch (search cluster):
  ~$50K/month

Redis (ElastiCache):
  ~$20K/month

Total rough estimate: ~$3.6M/month
```

**Reality check:** Spotify doesn't use CloudFront at full price. They peer directly with ISPs, use their own CDN infrastructure, and negotiate volume discounts. Their actual CDN cost is ~30% of list price.

---

## Budget Constraint Example

**Constraint: $500K/month (startup building a regional streaming service)**

| Cut | Impact |
|-----|--------|
| Single bitrate only (128kbps) | No high-quality tier; alienates audiophiles |
| No lossless | Can't compete with Tidal/Apple Music HiFi users |
| Offline download limit: 500 songs | vs. Spotify's 10K; frustrates commuters |
| CDN only in top 3 regions | High latency for tier-2 cities; >500ms start time |
| Cold storage for songs with <100 plays/month | 200-400ms latency on first play of obscure songs |
| No real-time recommendation | Serve editorial playlists only; no personalization |

**Architectural consequence:** Long-tail songs (99% of catalog, <0.1% of plays) stored on S3 Glacier Instant Retrieval. Cache hit ratio only needs to be ~1% to cover 99% of plays (Zipf distribution).

---

## Entities

```
User            → id, name, email, subscription_tier, country
Artist          → id, name, bio, genres, follower_count
Album           → id, artist_id, title, release_date, cover_art_url
Track           → id, album_id, artist_id, title, duration_ms, isrc, explicit
AudioFile       → track_id, quality, format, file_key (S3), size_bytes
Playlist        → id, owner_id, name, description, is_public, track_count
PlaylistTrack   → playlist_id, track_id, position, added_at
PlayEvent       → id, user_id, track_id, device_id, started_at, duration_played_ms, context
Device          → id, user_id, device_type, last_seen
Download        → user_id, track_id, quality, expires_at, encrypted_key
PlayState       → user_id, track_id, position_ms, device_id, updated_at
```

---

## Actions

| Action | Who | Notes |
|--------|-----|-------|
| Search tracks/artists/albums | User | Full-text + phonetic |
| Play track | User | Returns pre-signed CDN URL |
| Pause / seek / skip | User | Updates play state in Redis |
| Create/edit playlist | User | Postgres write |
| Follow artist | User | Postgres, powers recommendations |
| Download for offline | Premium user | Encrypted, device-bound |
| Sync play state | Device→Server | Redis, ~1s interval |
| Upload new track | Label/Artist | Triggers transcoder pipeline |
| Calculate royalties | System | Batch, per-play event |

---

## Data Flow

```
[User taps "Play Track"]
    ↓
API Gateway → Auth check → Audio Service
    ↓
Audio Service:
  1. Check user subscription (can they play 320kbps?)
  2. Fetch AudioFile record: track_id + quality → S3 key
  3. Generate pre-signed CDN URL (15-min TTL)
  4. Return URL + chunk manifest to client
    ↓
Client fetches first 2 chunks (0-20s) from CDN immediately
    ↓
Client sends PlayEvent to Analytics Ingestor:
  {user_id, track_id, device_id, started_at, context: playlist/search/radio}
    ↓
Analytics Ingestor → Kafka topic: play_events
    ↓ (fan-out)
  ├─ Royalty Calculator Service    ← tracks 30-sec threshold per play
  ├─ Recommendation ML Pipeline    ← updates user preference model
  ├─ Analytics Store (ClickHouse)  ← for dashboards, artist analytics
  └─ Trending Service              ← updates real-time trending tracks
```

---

## High Level Design

```
                  ┌─────────────────────────────────────┐
                  │          Client Apps                  │
                  │  iOS / Android / Web / Desktop        │
                  └──────────────┬──────────────────────┘
                                 │ HTTPS
                  ┌──────────────▼──────────────────────┐
                  │   API Gateway + Load Balancer        │
                  │   (Auth, Rate Limiting, Routing)     │
                  └──┬─────┬──────┬──────┬─────┬────────┘
                     │     │      │      │     │
              ┌──────▼─┐ ┌─▼────┐ ┌▼───┐ ┌▼──┐ ┌▼────────────┐
              │ User   │ │Search│ │Play│ │Play│ │Recommendation│
              │Service │ │Svc   │ │list│ │back│ │Service       │
              └──────┬─┘ └─┬────┘ └┬───┘ │Svc│ └──────┬───────┘
                     │     │       │     └─┬──┘        │
              ┌──────▼─────▼───────▼────────▼──────────▼──┐
              │         Databases / Stores                  │
              │  Postgres  │  Elasticsearch  │  Redis       │
              │  (users,   │  (search index) │  (play state,│
              │  playlists,│                 │  session,    │
              │  catalog)  │                 │  rate limit) │
              └────────────────────────────────────────────┘
                                 │
              ┌──────────────────▼──────────────────────────┐
              │          Audio Pipeline                       │
              │  Upload → Transcoder → S3 → CDN Edge Nodes  │
              └─────────────────────────────────────────────┘
                                 │
              ┌──────────────────▼──────────────────────────┐
              │          Kafka Event Bus                      │
              │  play_events │ upload_events │ artist_events │
              └──┬───────────┬──────────────┬───────────────┘
                 ▼           ▼              ▼
          Royalty Calc  Rec ML Pipeline  Analytics
```

---

## Low Level Design

### 1. Audio Serving Pipeline

```
Track uploaded by label (SFTP / API):
    ↓
Upload Service → S3 raw: songs/raw/{track_id}.wav
    ↓ publishes to Kafka: upload_events
Transcoder Service (consumes event):
    Transcode in parallel:
      songs/{track_id}/96.mp3    (free + mobile data saving)
      songs/{track_id}/128.mp3   (standard free)
      songs/{track_id}/256.aac   (premium)
      songs/{track_id}/320.mp3   (high quality)
      songs/{track_id}/flac      (lossless HiFi tier)
    Update AudioFile table: status = READY
    ↓
CDN Pre-warm:
    Top 1M tracks (by play count, refreshed daily) → pushed to all CDN edge nodes
    Long-tail tracks → pulled on-demand, cached at edge for 24h
```

**Pre-signed URL generation:**
```python
def get_stream_url(track_id, quality, user):
    check_subscription(user, quality)        # 256/320 = premium only
    s3_key = f"songs/{track_id}/{quality}.mp3"
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': 'audio-storage', 'Key': s3_key},
        ExpiresIn=900   # 15 minutes
    )
    # CDN URL fronts the same key — presigned works through CloudFront too
    return url.replace('s3.amazonaws.com', 'audio.cdn.example.com')
```

### 2. Chunked Streaming + Gapless Playback

```
Song stored as one file (not pre-chunked on server).
Client uses HTTP Range requests to fetch chunks.

Client behavior:
  1. Start: GET /audio/{track_id}?q=128 → Range: bytes=0-163839 (10s @ 16KB/s)
  2. On each chunk load: pre-fetch next chunk (2-chunk buffer = 20s)
  3. At track position T - 15s: pre-fetch first chunk of NEXT track
     (uses queued tracks or radio predictions)

Gapless: next track's first chunk ready before current track ends
  → seamless playback, no 200ms gap between songs

Seek:
  byte_offset = (seek_position_ms / 1000) × (bitrate_bps / 8)
  GET /audio/{track_id} Range: bytes={byte_offset}-{byte_offset+163839}
```

### 3. Play State Sync (Cross-Device)

```
Redis key: playstate:{user_id}
Value (JSON):
  {
    "track_id": "abc123",
    "position_ms": 87400,
    "device_id": "device_xyz",
    "is_playing": true,
    "updated_at": 1718956800000,
    "context": {"type": "playlist", "id": "pl_456"}
  }
TTL: 7 days

Update frequency: every 5s while playing (to Redis) + on pause/skip

Cross-device load:
  User opens new device → GET /me/playstate → reads from Redis
  Shows: "Resume 'Bohemian Rhapsody' at 1:27?"

Conflict: two devices active simultaneously
  Last-write-wins (latest updated_at wins)
  Design choice: Spotify allows one active playback at a time
    → playing on new device pauses old device via WebSocket push
```

### 4. Spotify Connect (Remote Playback)

```
Two roles:
  Speaker  → the device actually playing audio
  Controller → the device sending commands (phone controls desktop)

Flow:
  Desktop (Speaker) → connects WebSocket to Presence Service
  Phone (Controller) → sends command: {action: PLAY, track_id: X}
  Presence Service → routes command to Desktop's WebSocket
  Desktop → starts streaming from CDN

  Server is the relay. NOT P2P (avoids NAT traversal).
  Commands: PLAY, PAUSE, SEEK, SKIP_NEXT, SET_VOLUME, QUEUE_ADD

Presence Service:
  Redis: SET device:{user_id}:speaker {device_id, ws_connection_id} EX 3600
  All active devices for user fetched for "Available Devices" UI
```

### 5. Recommendation Service

```
Two-tier (Online + Offline):

Offline (nightly Spark job):
  Input: play_events last 90 days
  Collaborative filtering (ALS matrix factorization):
    user × track matrix → latent factor embeddings
    Similar users → surfaced tracks they played but user hasn't
  Content-based (audio features via Essentia):
    BPM, key, energy, danceability per track
    Compute track embeddings → cosine similarity
  Output:
    → Write to Feature Store (Redis HSET):
        user:{user_id}:recs → [track_id, score] × 100 tracks
    → Write to Offline store (BigQuery) for analysis

Online (real-time):
  User opens app → fetch user:{user_id}:recs from Redis (<2ms)
  Merge with:
    - Editorial playlists (manually curated)
    - New releases from followed artists
    - Context-aware boost (time of day: morning→energetic, night→chill)
  
  Redis cache TTL: 24h → refreshed by next nightly run
  If cache miss (new user): cold start → serve genre-based popular tracks
```

### 6. Royalty Calculation

```
Rule: Royalty owed when user plays >= 30 seconds of a track.
  (This is approximate; actual is more complex per licensing agreement)

Flow:
  Kafka consumer (Royalty Service) reads play_events
  For each event:
    IF duration_played_ms >= 30_000:
      INCREMENT royalty_counter:{track_id}:{month} BY 1
      (Redis INCR, batch flush to ClickHouse every 60s)

Month-end batch:
  SELECT track_id, SUM(play_count) FROM royalty_counters WHERE month = $1
  JOIN tracks → artist_id, label_id
  Calculate payout per label contract (per-stream rate × play_count)
  Generate payout reports → wire transfers / royalty statements

Idempotency:
  play_event_id unique per event
  Before processing: SET processed:{event_id} NX EX 86400
  → prevents double-counting on Kafka redelivery
```

### 7. Offline Downloads (DRM)

```
User taps "Download":
  1. Backend validates: user is premium, device_download_count < 5 devices
  2. Issue device-bound encryption key (AES-256):
       key = HMAC-SHA256(device_fingerprint, master_secret)
       Stored in HSM; key valid while subscription active
  3. Client downloads encrypted chunks → local SQLite tracks download metadata

Playback (offline):
  Decrypt using device-bound key (never leaves device)
  App checks license validity every 30 days (phone-home check)
  If subscription cancelled → key invalidated → decryption fails → locked

Why device-bound:
  User cannot copy encrypted file to another device (different fingerprint → different key)
  Prevents one account downloading and sharing files widely
```

### 8. Search

```
Elasticsearch index: tracks, artists, albums (separate indices)

Track document:
  {
    "track_id": "abc",
    "title": "Bohemian Rhapsody",
    "artist": "Queen",
    "album": "A Night at the Opera",
    "genres": ["rock", "classic rock"],
    "release_year": 1975,
    "play_count": 5000000000,  ← used for relevance boost
    "explicit": false
  }

Search query flow:
  1. Autocomplete (edge n-gram on title/artist, <50ms)
  2. Full search: BM25 + popularity boost (function_score)
  3. Phonetic match (Double Metaphone): "Kueen" → Queen
  4. Typo correction (fuzziness=1): "Boehmian" → Bohemian

Index updates:
  New track uploaded → Kafka consumer → Elasticsearch index
  Play count updated → async every 1h (not per play; too many writes)
  Zero-downtime reindex: alias pattern (see Concept #53)
```

---

## Trade-off Analysis

| Decision | Option A | Option B | Choice | Why |
|----------|----------|----------|--------|-----|
| Audio chunk size | 2s (HLS-style) | 10s via Range requests | **Range requests** | Simpler server, less overhead; clients handle buffering natively |
| Play state sync | WebSocket (push) | Polling every 5s | **Polling + on-event push** | 30M users × WebSocket = too many connections; polling every 5s is fine for position sync |
| Royalty counting | Exact SQL per play | Redis counter + batch flush | **Redis + batch** | 10K plays/sec → Postgres can't take that write volume; Redis INCR is atomic |
| Recommendation | Real-time inference | Pre-computed batch | **Pre-computed (nightly)** | 30M users × <2ms read is impossible with real-time inference; batch + cache is practical |
| CDN for audio | Pull only | Pull + push for popular | **Hybrid** | Top 1M tracks cover 99% of plays; push them to all edges, pull the rest |
| Offline DRM | Device-bound key | Server-side stream DRM | **Device-bound** | No streaming = no server contact; must be client-side decryption |

### Cache Trade-off
> Adding Redis cache for recommendations reduces load on ML pipeline but serves potentially stale picks for up to 24h. A user who just played 20 new songs will get recommendations based on yesterday's listening until next nightly run. Trade-off: freshness vs. cost. Acceptable for music; unacceptable for real-time fraud scoring.

### Gapless Playback Trade-off
> Pre-fetching the next song's chunks reduces buffer time to 0ms but increases bandwidth usage by ~15% (partial pre-fetches that may be abandoned on skip). This is acceptable for WiFi; on mobile data, limit pre-fetch to first 1 chunk only.

---

## Database Schema (Key Tables)

```sql
-- Core catalog
tracks (id PK, album_id FK, artist_id FK, title, duration_ms, isrc UNIQUE,
        explicit BOOL, play_count BIGINT, status ENUM(processing,active,removed))

audio_files (id PK, track_id FK, quality ENUM(96,128,256,320,flac),
             format ENUM(mp3,aac,flac), s3_key TEXT, size_bytes BIGINT)

-- User data
playlists (id PK, owner_id FK, name, is_public BOOL, version INT)

playlist_tracks (playlist_id FK, track_id FK, position INT, added_at TIMESTAMP,
                 PRIMARY KEY (playlist_id, track_id))

-- Append-only event log (ClickHouse for analytics)
play_events (id UUID, user_id, track_id, device_id, started_at TIMESTAMP,
             duration_played_ms INT, context_type, context_id)
```

---

## Sharding Strategy

| Data | Sharding | Why |
|------|----------|-----|
| `tracks` | No sharding — 100M rows fits Postgres with read replicas | Catalog is read-heavy, rarely updated |
| `playlists` | Shard by `owner_id` | Each user's playlists accessed together |
| `play_events` | Partition by month in ClickHouse | Time-series, only recent data queried hot |
| Redis play states | Redis Cluster, hash by `user_id` | Each user's state accessed independently |
| Audio files on S3 | Natural partitioning by `track_id` prefix | S3 handles it internally |

---

## Real-World Reference

- **Spotify Engineering Blog**: "The Story Behind the New Spotify.com" — React + server-side rendering decisions
- **Spotify's Event Delivery** (2016 blog post): Kafka for 2B+ events/day; batching at edge to reduce network
- **Spotify's Podcast Architecture**: Separate pipeline from music (RSS ingest, no royalty tracking, different licensing)
- **Netflix's Adaptive Streaming**: Same ABR principles apply to audio (though Netflix uses DASH for video, Spotify uses custom HLS-like chunking)
- **Apple Music vs Spotify**: Apple uses AAC at 256kbps; Spotify uses Ogg Vorbis (own format, not MP3) for efficiency
