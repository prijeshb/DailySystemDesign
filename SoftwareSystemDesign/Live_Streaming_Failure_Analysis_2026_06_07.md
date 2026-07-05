# Live Streaming — Failure Analysis
**Date:** 2026-06-07

> Failure-first approach: for every component, ask what happens if it fails, what upstream/downstream impact is, and how to prevent/recover.

---

## Component Map

```
Streamer → [Ingest Server] → [Transcoder] → [S3] → [CDN] → Viewer
                                                      ↑
                                              [Playlist Service]

[Chat Service] → [Redis pub/sub] → Viewer (WebSocket)
                → [Cassandra]

[Viewer Count Service] → [Redis HyperLogLog]
[Stream API] → [Postgres]
```

---

## 1. Ingest Server Fails (Mid-Stream)

### What fails
- Streamer's RTMP connection drops
- No more segments being produced
- Transcoder loses its input

### Downstream impact
- Transcoder idles, no new .ts files written to S3
- Live playlist on S3 stops updating
- Viewers: playlist poll returns same last segment → player stalls → "Buffering…"
- After ~30s of no new segments, player shows "Stream offline"

### Why it happens
- OOM / process crash on ingest server
- Network partition between streamer and ingest PoP
- Machine hardware failure

### Prevention
- Ingest cluster behind load balancer; streamer connects to a VIP, not specific host
- But: RTMP is stateful — can't mid-stream switch servers (connection is persistent)
- So: health-check ingest servers; if server crashes, streamer OBS auto-reconnects (OBS retries every 3s)

### Recovery
```
Streamer: OBS reconnects after drop → hits same VIP → LB routes to healthy ingest server
System: new stream_key connection = new stream? No — same stream_key → resume existing stream_id
  → Ingest server: on reconnect, look up existing LIVE stream for stream_key
  → Resume segment sequence numbering from last_seq + 1 (fetched from DB)
  → Viewers: playlist resumes updating → gap in timestamps covered by player buffering
```

### Viewer experience
- ~10–60s outage (reconnect time + OBS retry)
- Short gap in VOD (playlist has sequence gap, player skips gracefully)

### What if ingest cluster itself goes down (full DC outage)?
- DNS failover: ingest.twitch.tv → next PoP (60s TTL)
- Streamers reconnect to different region; add ~20-50ms latency
- S3 data from that region lost only if not yet uploaded (in-memory buffer, max 2s loss)

---

## 2. Transcoder Fails (Mid-Segment)

### What fails
- FFmpeg process crashes on a transcoder worker
- In-flight segment partially written or never uploaded to S3

### Downstream impact
- Current .ts segment corrupt or missing
- HLS playlist points to a segment that doesn't exist → player 404 → skip to next segment (small glitch)

### Recovery
```
Transcoder orchestrator (Kubernetes) restarts FFmpeg pod:
  - Ingest server still has RTMP stream buffered (short ring buffer)
  - New FFmpeg process starts, resumes from ingest input
  - Writes next segment with new seq_num (skipping the lost one)
  - Player detects sequence gap → skips segment → continues
```

### Prevention
- Transcoder workers: 3 per stream (in parallel)? Too expensive at 100K streams
- Instead: fast restart (<5s) + player handles gap gracefully
- Monitor: alert if segment write latency > 3s (should be near real-time)

### What if transcoder falls behind (overloaded)?
```
Symptom: new segment arrives but previous one not yet encoded
  → HLS playlist not updated → latency grows (10s → 30s → ...)
  → viewers get increasing delay

Fix:
  - Drop frames in encoder (accept quality drop, never accept latency growth)
  - Auto-scale transcoder pool: if queue depth > 5 segments, add worker
  - Rate limit: if stream quality > server capacity, force down to 720p60 max
```

---

## 3. S3 Unavailable

### What fails
- Segment upload from transcoder fails
- Playlist update fails
- CDN pulls return 503/errors

### Downstream impact
- No new segments → viewers see buffering → stream appears offline within 20-30s

### Why it matters more here than other systems
- S3 is in the critical path for EVERY viewer's every segment request
- Unlike a database (cached), video segments can't be served from cache after S3 fails (new segments don't exist in CDN cache yet)

### Recovery
```
Short outage (<60s):
  - Transcoder: buffer segments in local disk (ring buffer, last 60s)
  - When S3 comes back: replay buffered segments to S3 in order
  - Playlist resumes; viewers see slightly increased latency buffer

Long outage (>5min):
  - Ingest continues, transcoder buffers to disk (bounded: 30s max per stream × 100K streams = 3TB ... too much)
  - Realistic: pause transcoding, queue segment work, drain when S3 recovers
  - Viewers see stream as offline
  - Alert streamer; they may re-start after recovery

Prevention:
  - Multi-region S3 replication (us-east + us-west mirrors)
  - Upload to primary, async replicate; CDN serves from nearest region
  - Use S3 Transfer Acceleration (edge upload, reduced S3 failure surface)
```

---

## 4. CDN Edge PoP Fails

### What fails
- One geographic CDN node goes down
- Viewers in that region lose access to segments

### Downstream impact
- Player requests timeout → triggers CDN failover to next PoP
- Higher latency for affected region; slight buffering spike
- Other regions unaffected

### Recovery
```
CDN failover (built-in):
  - CloudFront/Fastly: automatic failover to origin (S3) or next edge PoP
  - TTL on segment cache is 86400s → other PoPs still have cached segments
  - New edge PoP: cache miss → pull from S3 → serve
  - Recovery transparent to viewer within ~5s (player buffers enough)
```

### What if CDN origin (S3) is behind CDN but CDN says it's down?
- CDN has health checks on origins; falls back to secondary origin if configured
- Set up: primary origin = S3 us-east, fallback origin = S3 eu-west replica

---

## 5. Playlist Service Fails (M3U8 Updates Stop)

### What happens
- Transcoder writes segments to S3
- But: who updates the live.m3u8 file on S3?
  - Option A: FFmpeg writes it directly (common, simpler)
  - Option B: Separate playlist service reads segment events and writes playlist

### If FFmpeg writes directly (Option A):
- FFmpeg crash = both segment production AND playlist update fail
- Single point of failure, but single recovery point too

### If separate playlist service (Option B):
- Transcoder writes segments, emits Kafka event: segment.ready
- Playlist service consumes, updates m3u8 on S3
- Playlist service fails → segments still uploaded, just not indexed
- Recovery: playlist service restarts, reads last N segment events from Kafka, rebuilds playlist

**Choice:** Option A (FFmpeg writes) for simplicity. Option B only if playlist logic is complex (e.g., multi-rendition sync, DRM).

---

## 6. Chat Service Fails

### Impact
- Viewers can't send/see chat
- Video playback unaffected (completely decoupled)

### Recovery
```
Chat WS server crashes:
  - Client detects disconnect → retry WS connection (exponential backoff)
  - New server: read last 100 messages from Cassandra, replay to reconnecting clients
  - Redis pub/sub: new server subscribes to channel:{stream_id}

Redis pub/sub node fails:
  - Chat messages in-flight lost (pub/sub is fire-and-forget)
  - Recovery: reconnect → new messages flow; lost ~10s of chat messages
  - Acceptable: chat is ephemeral, no one audits missed chat

Cassandra node fails:
  - Cassandra: replication factor 3, quorum writes (W=2, R=1)
  - One node loss: no impact (still 2 replicas available for quorum)
  - Two node loss: writes degrade to eventual consistency
```

---

## 7. Viewer Count Redis Fails

### Impact
- PFADD/PFCOUNT fails → viewer count unavailable
- Displayed as "— viewers" or last known count

### Recovery
- Redis sentinel / cluster: auto-failover in ~30s
- HyperLogLog data is in-memory; failover replica has last checkpoint (if AOF enabled)
- Worst case: start fresh count from 0, rebuilds in 60s from heartbeats

### Why it's low-stakes
- Viewer count is display-only; no billing, no routing depends on it
- Eventual consistency is fine

---

## 8. Streamer Key Compromised / Stream Hijacking

### Scenario
- Attacker gets stream_key → can publish to streamer's channel

### Prevention
```
1. stream_key is shown ONCE in dashboard, stored hashed (like password) in DB
2. On ingest: compare provided key against stored hash → match = allow
3. Streamer can rotate stream_key instantly → old key immediately invalid
4. Rate limit: max 3 concurrent RTMP publishes per stream_key
5. IP allowlist (optional): streamer can pin their ingest IP
```

---

## 9. Segment Latency Spike (Transcoding Falls Behind Real-Time)

### Symptom
- Viewer latency grows: from 10s → 30s → 60s → player gives up

### Root cause
- Transcoder CPU saturated (too many streams, complex game scenes)
- Segment encode takes >2s for a 2s segment → pipeline can't keep up

### Detection
```
Monitor: segment_production_lag = current_time - last_segment_uploaded_at
Alert if lag > 5s
```

### Fix
```
Short term: 
  - Drop to lower bitrate profiles only (720p max, skip 1080p60)
  - Hardware encoding: NVENC (Nvidia GPU) instead of software x264 → 10× faster

Long term:
  - Auto-scale transcoder pool on queue depth
  - Reserve GPU workers for high-viewer streams (priority queue)
  - Limit: one transcoder per 5 concurrent streams on same host
```

---

## Failure Summary Table

| Component | Failure | Viewer Impact | Recovery Time | Priority |
|-----------|---------|---------------|---------------|----------|
| Ingest server | crash | Buffering → offline | 10–60s (OBS reconnect) | High |
| Transcoder | crash | 1 segment gap + glitch | <5s (pod restart) | Medium |
| S3 | unavailable | Offline after buffer drains | Depends on outage | Critical |
| CDN PoP | down | Region latency spike | ~5s (CDN failover) | Medium |
| Chat service | crash | Chat down, video fine | ~30s (WS reconnect) | Low |
| Viewer count Redis | fail | Wrong count shown | 30s (sentinel failover) | Low |
| Playlist not updated | stale | Player loops last segment | Immediate on restart | High |
| Transcoder lag | overload | Growing latency | Reduce quality profile | High |
