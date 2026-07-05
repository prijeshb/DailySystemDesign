# Spotify Music Streaming — Interview Q&A
**Date:** 2026-06-21

---

## Opening / Scoping

**Q: Design a music streaming service like Spotify.**

Walk through clarifying questions:
- How many users? (sets scale; answer: 600M registered, 30M DAU)
- Functional scope: just play + search, or also recommendations, offline, social?
- Global or regional? (affects CDN strategy)
- Free + premium tiers? (affects bitrate gating)
- Mobile + web + desktop? (affects cross-device sync)

Strong opening: "Before diving in — do we care about audio upload/transcoding, or just the playback path? And should I include recommendations or focus on core streaming?"

---

## High-Frequency Interview Questions

---

**Q: How would you store 100M songs?**

Don't say "S3 and done." Show the layers:

1. Multiple quality tiers per song (96/128/256/320kbps + FLAC). 1.8PB total.
2. CDN as primary serving layer. S3 as origin (not served directly to clients).
3. Hot/cold tiering: top 1M tracks (99% of plays) pre-pushed to CDN edge. Long-tail stored on S3 Glacier Instant, fetched on-demand.
4. Pre-signed URLs (15-min TTL) for access control — user's subscription tier gates which quality they can request.
5. Content-addressed storage: S3 key = `songs/{track_id}/{quality}.{format}`. Immutable once written (tracks don't change). Perfect for caching.

Follow-up: "What if two different labels upload the same song?" → ISRC (International Standard Recording Code) deduplication at ingest. Same ISRC = same master file, just different licensing metadata. De-duplicate and link.

---

**Q: How does music start playing in under 200ms?**

Three parts:

1. **Pre-signed URL generation is fast (<10ms):**
   Audio Service: check subscription → fetch S3 key from AudioFile table (Redis cache hit) → generate presigned URL → return. No actual audio data served yet.

2. **CDN proximity:**
   Popular tracks pre-cached at CDN edge near user. No origin fetch needed. First chunk returned in <50ms for most users.

3. **Small first chunk:**
   Client requests first 10 seconds (160KB at 128kbps). CDN returns first chunk in 50-100ms. Playback starts immediately while rest buffers in background.

Total: URL generation (10ms) + network (50ms) + first chunk (50ms) ≈ 110ms.

---

**Q: How would you implement cross-device sync (pause on phone, resume on desktop)?**

```
Device A plays → every 5s: POST /me/playstate {track_id, position_ms, device_id}
  → Redis: SET playstate:{user_id} {json} EX 604800

Device B opens app → GET /me/playstate → read from Redis
  → Shows: "Resume 'Song X' at 1:27?"

On resume: client issues new audio chunk request from byte_offset(position_ms)
```

Conflict (two devices both active):
- Spotify's model: one active player at a time
- Playing on Device B → sends PAUSE command to Device A via Presence Service
- Presence Service: routes command over Device A's WebSocket connection

Follow-up: "What if device B doesn't have a WebSocket connection open?" → Fall back to next-time-app-opens: check Redis for current state → show resume prompt.

---

**Q: How does gapless playback work?**

Gapless = zero silence between consecutive songs in an album/playlist.

Client-side solution:
1. While current track is playing, at T-15s before end: pre-fetch first 2 chunks of next track
2. Client uses two audio decoders: current track plays in decoder A, next track loads in decoder B
3. At track boundary: seamlessly switch decoders (< 1ms gap)

Server impact:
- Just additional CDN requests (pre-fetched chunks)
- Backend doesn't know about gapless — it's entirely client-handled
- Trade-off: 15% extra CDN bandwidth from pre-fetches that get abandoned on skip

---

**Q: How do you calculate and pay royalties?**

```
Every play event → Kafka
Royalty Calculator consumes events:
  IF duration_played_ms >= 30_000: count as a paid play
  ZINCRBY royalties:{track_id}:{month} 1  [Redis]
  
Monthly batch:
  SELECT track_id, sum(plays) GROUP BY track_id → per-stream rate × plays
  → payout reports per label → wire transfers
```

Key subtlety: deduplication. Kafka at-least-once → same event may arrive twice.
```
SET processed:{event_id} NX EX 86400
→ only first delivery is counted; duplicates ignored
```

Real number: Spotify pays ~$0.003-0.005 per stream. 900M plays/day × $0.004 ≈ $3.6M/day in royalties.

---

**Q: How does the recommendation system work?**

Two-tier:
1. **Offline (nightly Spark job):** Collaborative filtering (ALS) on play history. "Users who listened to X also listened to Y." Output: top 100 track recommendations per user → stored in Redis (Feature Store).
2. **Online (real-time, <2ms):** Read from Redis, apply context boosts (time of day, current session genre), merge with new releases from followed artists.

Cold start (new user): no play history → fall back to genre-based popular tracks. If user selected genres during signup → use that. "Taste profile" built after first 5 songs played.

Follow-up: "How do you avoid the filter bubble (only recommending what user already knows)?"
→ Exploration vs. exploitation: 80% similar to past listening (exploitation), 20% new tracks from similar-genre artists user doesn't follow (exploration). "Discover Weekly" style.

---

**Q: How would you handle a viral song that suddenly gets 10x normal traffic?**

Normal: CDN caches popular songs. Cache hit ratio ~99% for top 1M songs.

Viral event (new Taylor Swift album drop):
1. CDN already has the tracks (pre-pushed to all edges for popular artists)
2. Surge in simultaneous plays → CDN serves from cache → origin not hit
3. Presigned URL generation: Audio Service may be bottleneck

Mitigation:
- Audio Service: horizontally scaled (Kubernetes HPA), can scale 10x in ~2min
- Presigned URLs: cache in Redis (key: `{user_id}:{track_id}:{quality}`, TTL 14min). Same user playing same track multiple times → return cached URL. Reduces Audio Service load by ~80%.
- Kafka play events: consumer groups scale independently. No cascading load to royalty calculator.

Real example: Beyoncé's "Lemonade" surprise release caused Tidal to crash (2016). Spotify survived similar events by pre-positioning CDN content after internal "high-anticipation" signals from social/pre-save data.

---

**Q: How does Spotify Connect (phone controls desktop speakers) work?**

Two roles: Speaker (plays audio) and Controller (sends commands).

```
Desktop (Speaker):
  Opens WebSocket to Presence Service
  Registers: {user_id, device_id: "desktop_xyz", role: SPEAKER}

Phone (Controller):
  GET /me/devices → lists user's active devices (from Redis)
  Sees: "Desktop (Active)"
  User taps: "Play on Desktop"
  
  POST /me/player/command:
    {action: PLAY, track_id: X, target_device: "desktop_xyz"}
  
Presence Service:
  Looks up WebSocket connection for "desktop_xyz"
  Forwards command over that WebSocket
  
Desktop:
  Receives command → makes its own audio stream request → plays
  
Key: all media flows Desktop→CDN. Phone only sends lightweight commands.
     No P2P (avoids NAT traversal, works through firewalls).
```

---

**Q: How would you design the search feature?**

Elasticsearch with three indices: tracks, artists, albums.

Key features:
- **Autocomplete:** Edge n-gram tokenizer on title/artist. "Bohemi" → "Bohemian Rhapsody"
- **Typo tolerance:** fuzziness=1 on query. "Boehmian" → matches "Bohemian"
- **Phonetic:** Double Metaphone filter. "Kueen" → matches "Queen"
- **Popularity boost:** function_score boosts tracks with higher play_count. Known songs surface first.

```json
{
  "query": {
    "function_score": {
      "query": { "multi_match": { "query": "bohemian", "fields": ["title^3", "artist^2", "album"] }},
      "functions": [{ "field_value_factor": { "field": "play_count", "modifier": "log1p", "factor": 0.1 }}]
    }
  }
}
```

Keeping index fresh:
- New tracks: Kafka → Elasticsearch indexer consumer (~5s delay after upload)
- Play count: batch update every 1h (too high-write to do per-play)
- Zero-downtime reindex: alias pattern (new index built in background, alias swapped)

---

**Q: How would you handle offline downloads with DRM?**

1. **Device fingerprint:** hash of device hardware IDs (Android ID, iOS UUID) → unique per device
2. **Key derivation:** `HMAC-SHA256(device_fingerprint, master_secret)` → device-bound encryption key
3. **Download:** server encrypts audio chunks with this key → stored locally
4. **Playback:** app decrypts using device-derived key (never transmitted)
5. **License validity:** app checks license expiry locally. Goes online every 30 days to renew. Subscription cancel → server marks key as revoked; next online check fails → locked.

Why device-bound: encrypted file on Device A is useless on Device B (different key). Prevents sharing.

Max devices: 5 per account (Spotify actual limit). Backend tracks active device keys.

---

## Deeper Follow-ups

**Q: Your Redis for play state has 30M entries. How do you size it?**

```
30M users × 200 bytes per entry = 6GB
Redis instance: 16GB → comfortable headroom
TTL: 7 days → stale entries auto-expire (users who don't open app for a week)
At scale: Redis Cluster with 3 shards → 2GB per shard, easy to expand
```

---

**Q: How do you handle multi-market licensing? Song available in US but not in Germany.**

```
Track has a `license_regions` field: ["US", "CA", "AU"] or ["*"] (worldwide)
Audio Service:
  User requests track → look up user's country (from profile + IP check)
  Check: user.country IN track.license_regions
  If NO → return 451 (Unavailable For Legal Reasons), not 403
  Client shows: "This track is not available in your region"
  
Geo-IP: IP-based country detection for users who don't set profile
```

Regional catalog diffs are small (~2-5% of catalog blocked per country). Not a performance problem.

---

**Q: How would you prevent a single user from downloading the entire catalog?**

```
Rate limits on download API:
  - Max 10K downloads per account (Spotify actual)
  - Max 5 devices
  - Download rate: 100 songs/hour per device (prevents scripted bulk download)
  
Monitoring:
  Download velocity anomaly: ZADD downloads:{user_id}:{hour} → count per hour
  Alert: any user exceeds 200 downloads/hour → flag for review
  
DMCA protection: even if user downloads, file is encrypted with device-bound key
  → file is useless without the device that generated the key
  → sharing the file doesn't help (can't decrypt)
```

---

**Q: How do you handle podcast episodes differently from music tracks?**

| | Music | Podcasts |
|--|-------|---------|
| Licensing | Label deals, royalties | RSS feed, creator ownership |
| Royalty tracking | Per-play, 30s threshold | No per-play royalty |
| Storage | Encoded to multiple qualities | Single MP3/AAC (creator provides) |
| Transcoding | Full quality ladder | Validate + re-host (virus scan) |
| Offline download | Premium only, encrypted | Available free, unencrypted |
| Length | 2-5 min avg | 20-120 min avg |

Separate pipeline: Podcast Ingester subscribes to RSS feeds, downloads episodes, validates, hosts on different S3 bucket with looser caching rules. Recommendation system is also separate (episode freshness matters; old episodes less recommended).

---

## Common Mistakes to Avoid

1. **"Use WebSocket for all 30M users' play state sync"** → Too many connections. Use polling (every 5s). Reserve WebSocket for Spotify Connect (device commands) which has far fewer concurrent sessions.

2. **"Store audio files in the database"** → Never. Files in S3/object storage. DB stores metadata (S3 key, duration, quality). Always.

3. **"Track royalties with SELECT COUNT(*)"** → Can't run COUNT on 900M plays/day in real-time against Postgres. Use Kafka → Redis counter → batch flush to ClickHouse.

4. **"Cache the pre-signed URL forever"** → Pre-signed URLs expire (15min). If cached in client beyond that, user gets 403 on CDN. Cache them but respect the expiry and re-fetch when approaching expiry.

5. **"Recommendation is a simple lookup"** → Cold start problem, filter bubble, context-awareness (time of day), new user (no history), followed artist new release priority — all must be addressed.

6. **"Search is just LIKE '%query%' on Postgres"** → Doesn't scale to 20K search QPS on 100M tracks. Elasticsearch with proper tokenizers, fuzziness, and popularity scoring is the answer.
