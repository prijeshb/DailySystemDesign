# Live Streaming Platform — Interview Q&A
**Date:** 2026-06-23

---

## Opening / Scoping Questions (Interviewer → You)

**Q: Design a live streaming platform like Twitch.**

*Your clarifying questions:*
- Scale target? (100K streams, 3M viewers — I'll assume Twitch scale)
- Latency requirement? (Low-latency interactive <5s, or broadcast-quality 15–30s?)
- Chat included? Monetization? (Include chat, skip payments unless asked)
- VOD/replay needed? (Yes — 7-day replay)
- Read-heavy or write-heavy focus? (Both: read-heavy for viewing, write-heavy for ingest)

---

## Section 1: Video Ingest

**Q: How does a streamer's video reach your platform?**

A: Via RTMP (Real-Time Messaging Protocol) — industry standard, supported by all streaming software (OBS, Streamlabs). Streamer configures RTMP endpoint + stream key. Our ingest load balancer routes TCP connection to nearest ingest node. Ingest node validates stream key against auth service, creates a Stream record, and begins forwarding raw video to the transcoding pipeline via Kafka.

**Q: Why RTMP and not WebRTC?**

A: RTMP pros: stable, battle-tested, supported by every streaming tool, tolerates variable-quality networks well. WebRTC gives <500ms latency but requires custom encoder support, complex NAT traversal (STUN/TURN), and isn't natively supported by OBS — the tool 90%+ of streamers use. Tradeoff: RTMP adds ~1–2s at ingest.

**Q: What happens if a streamer's connection drops?**

A: RTMP is TCP — connection drop is detected immediately. We:
1. Mark stream as `reconnecting` (not `offline`) for 60s grace period
2. Store session metadata in Redis (stream_id, last sequence number) keyed by stream_key
3. Ingest LB routes reconnect to same node (consistent hash on stream_key)
4. If that node is dead, any node picks up using Redis session
5. Viewers: HLS player keeps playing buffered segments; sees freeze if reconnect takes >buffer duration (~10s)

**Q: How do you prevent someone from using another person's stream key?**

A: Stream keys are long random tokens (UUID + HMAC signature). They're hashed in DB; never stored plaintext. Ingest validates signature + checks channel ownership. Additional: RTMP connection IP is logged; anomalous geographic shifts alert the user.

---

## Section 2: Transcoding

**Q: Why do you need to transcode at all?**

A: Streamers send high-quality source (1080p60, 6 Mbps). Viewers have varying bandwidth. We produce multiple "quality ladders" — 1080p, 720p, 480p, 360p — so HLS ABR can pick the right one per viewer. Also: re-encode ensures consistent codec settings regardless of what OBS sends.

**Q: How does transcoding scale to 100K streams?**

A: Each stream gets its own set of FFmpeg workers — one per quality tier. Workers pull from Kafka partitioned by stream_id, so same stream always goes to same consumer group. Workers run in Kubernetes, scaled by Kafka consumer lag. At 100K streams × 4 qualities = 400K workers — we use lightweight containers (~2 vCPU each), auto-scaling based on active stream count.

**Q: What if transcoding can't keep up with real-time?**

A: Graceful degradation:
1. Disable highest quality tier (1080p) first — halves CPU load
2. Increase segment duration from 2s to 6s — reduces context switches
3. If still behind: prioritize top N streams by viewer count; queue others with brief delay
Monitor: Kafka consumer lag alert at 5+ segments behind (10s).

**Q: How do you handle a corrupt stream from the encoder?**

A: Ingest node validates NAL unit headers before forwarding. FFmpeg worker validates output with FFprobe before publishing `segment_ready`. Corrupt output triggers retry from the Kafka message (idempotent because S3 key is deterministic: `{stream_id}/{quality}/{seq}.ts`).

---

## Section 3: Delivery / CDN

**Q: How does a viewer start watching?**

A: Player fetches master manifest (m3u8 URL from our API). Master manifest lists quality variants. Player's ABR algorithm picks variant based on bandwidth estimate, fetches variant manifest. Variant manifest lists last ~3 segments. Player fetches segments in order. Polls variant manifest every 2s for new segments.

**Q: How does CDN help here?**

A: Segments are immutable (`.ts` files don't change once written). CDN caches them indefinitely — TTL=300s for segments (they're often fetched multiple times per viewer session). Manifests have TTL=2s (must reflect new segments quickly). For 3M viewers: CDN absorbs >99% of segment reads; S3 only gets cache-miss requests (typically 1 per PoP per new segment).

**Q: What's the latency for a viewer? How do you reduce it?**

A: Standard HLS: 15–30s (player buffers 3 × 6s segments = 18s). Low-Latency HLS (LL-HLS): 3–5s achieved by:
- 2s segment duration
- "Partial segments" — server pushes segment before it's complete
- Playlist delta updates — only sends new entries, not full manifest

Tradeoff: LL-HLS triples CDN request rate (30 requests/minute vs 10 at 6s segments) → higher CDN cost.

---

## Section 4: Chat

**Q: How does chat work at scale?**

A: 
1. Viewer sends message via HTTP POST → Chat API validates, rate-limits, stores in Cassandra, publishes to Kafka
2. Fanout worker consumes → Redis PUBLISH to channel:{channel_id}
3. All WS servers subscribed to that channel receive → push to their connected viewers

For 50K concurrent viewers on 1 channel: ~50 WS servers (1K viewers each), each subscribed to Redis. Message arrives → 50 Redis deliveries → 50 × 1K WS pushes = 50K deliveries in <100ms.

**Q: What's the bottleneck in chat at very high scale?**

A: Redis single-threaded PUBLISH. At ~100K msgs/sec, Redis saturates. Solution:
1. Shard by channel_id: `SHARD = channel_id % N_shards` → each shard handles subset of channels
2. Top channels get dedicated Redis instance (identified by viewer count threshold)
3. Alternative: Cassandra-backed polling with 1s TTL (lower resource, 1s latency penalty)

**Q: How do you prevent chat spam?**

A: Layered:
1. Rate limit: token bucket per (user_id, channel_id) in Redis — 1 msg/3s default
2. Content filter: word blocklist + ML classifier (inline, p99 <5ms)
3. Channel moderators: `/ban`, `/timeout` commands → blocklist in Redis, checked on publish
4. Follower-only / subscriber-only mode: check subscription status before allowing publish

**Q: How do you handle chat message history / replay?**

A: Cassandra with TTL=24h. Partition key = channel_id, clustering key = created_at (TIMEUUID). On page load: fetch last 50 messages with `SELECT ... ORDER BY created_at DESC LIMIT 50`. Efficient because Cassandra stores within partition in sorted order.

---

## Section 5: Scale & Estimation Follow-ups

**Q: How much does this cost to run?**

A: At Twitch scale (~3M concurrent viewers):
- CDN is dominant cost: 9 Tbps × $0.005/GB (negotiated) ≈ $30M+/month
- Transcoding: ~$5M/month (GPU/CPU fleet)
- Storage: ~$2M/month (segments + VoD)
- Total infra: ~$100–120M/year (consistent with leaked 2022 figures)

For a startup: must accept much higher per-GB CDN rates ($0.085/GB on AWS list price). At $5K/month CDN budget: max ~10 concurrent streams with limited viewers. Must use peer-assisted delivery (WebTorrent) or regional CDN resellers (BunnyCDN at ~$0.01/GB).

**Q: How do you count live viewers?**

A: Two approaches:
1. WebSocket connection count (exact) — but WS server must aggregate across all servers → gossip or centralized counter
2. Redis Sorted Set heartbeat: `ZADD viewers:{channel_id} {timestamp} {user_id}` every 30s; count = `ZCOUNT ... (now-60s, +inf)`. Approx. update every 10s. Error: 1 heartbeat interval (up to 60s stale).
3. For display only: HyperLogLog (±0.81%) is memory-efficient but can't decrement.

**Q: How does the "Top Streams" / Discovery page work?**

A: `SELECT id, title, viewer_count FROM channels WHERE is_live=TRUE ORDER BY viewer_count DESC LIMIT 20`

Problem: reads this frequently (every user who loads the homepage). Solution:
- Cache in Redis with 10s TTL: `SETEX top_streams 10 <json>`
- Single DB query per 10s regardless of concurrent homepage loads
- PostgreSQL index: `(is_live, viewer_count DESC)` for fast sort

---

## Section 6: Architecture Deep-Dives

**Q: How do you handle the "thundering herd" when a top streamer goes live?**

A: Multiple triggers simultaneously:
1. 500K followers get push notifications → 500K browser tabs open → CDN hit
2. CDN has no cached segments yet → origin hit storm
3. Transcoding just started → first segments not ready yet

Mitigation:
1. Pre-warm CDN: manifest service publishes a "stream started" event → CDN fetcher pre-fetches first 5 segments across PoPs
2. Push notification staggering: notifications sent over 30s window, not all at once
3. Viewer connection rate limiting per channel (leaky bucket: max 10K new connections/s)

**Q: How do you implement DVR / rewind while watching?**

A: Extend HLS manifest to include historical segments:
- Standard live manifest: last 3 segments
- DVR manifest: last 1800 segments (1hr of 2s segments)
- Viewer player switches to DVR playlist, seeks to any point
- Segments must remain in S3 for DVR window (extra storage cost ~$0.02/GB-hr)

**Q: How do Clips work technically?**

A: Clip = short highlight cut from live stream.
1. User clicks "Clip" → send {channel_id, duration=30s}
2. Clip service: identify last 30s of segments from Segment table
3. Enqueue async job (Clip Worker)
4. Worker: download segments from S3, FFmpeg concat → MP4, upload to S3
5. Return shareable clip URL

Idempotency: hash(user_id + channel_id + floor(timestamp/5)) as idempotency key → prevent duplicate clips from double-click. Async processing: clip URL returned immediately (pending), resolves when done.

---

## Common Follow-up Patterns

**Interviewer: "What if we want sub-1-second latency?"**
- Drop HLS; use WebRTC for delivery (not just ingest)
- Each viewer establishes WebRTC data channel to edge server
- Edge server receives RTMP → decodes → re-encodes → sends RTP to viewers
- Cost: 10× more edge server capacity; works for small audiences (<10K/stream)
- Twitch's "Squad Stream" uses this for creator-to-creator viewing

**Interviewer: "What about stream moderation / NSFW detection?"**
- Async: every 30s, extract a keyframe from stream → send to Vision API (AWS Rekognition) → flag if NSFW score > threshold → auto-suspend stream + notify moderators
- Synchronous would add latency to the stream; async is fine for policy enforcement

**Interviewer: "How would you handle a DDOS on the ingest servers?"**
- Ingest LB: SYN cookies, connection rate limit per IP
- Anycast ingest IPs: flood distributed across global PoPs
- Stream key validation as first step — invalid key drops connection immediately
- AWS Shield Advanced: ~$3K/month but absorbs large volumetric attacks

**Interviewer: "Difference between Twitch and YouTube Live architecture?"**
- Twitch: lower latency focus (LL-HLS), chat interaction is core feature
- YouTube Live: higher latency tolerable, optimized for max quality and global scale
- YouTube reuses existing CDN (same infrastructure as regular video)
- Twitch ingest: global ingest edge nodes close to streamers; YouTube: fewer ingest PoPs
