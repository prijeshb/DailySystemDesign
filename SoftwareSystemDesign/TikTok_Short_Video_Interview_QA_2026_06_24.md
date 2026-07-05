# TikTok Short Video — Interview Q&A
> Date: 2026-06-24 | Real interviewer questions + follow-ups

---

## Opening Questions

**Q: Design TikTok (or a short video platform).**

Strong opening response structure:
1. Clarify scope: "Are we focusing on the FYP feed, upload pipeline, or the full system?"
2. State assumptions: "1B DAU, 50M uploads/day, video length 15s - 3min"
3. Back of envelope: CDN bandwidth is the constraint → leads to architecture decisions
4. Identify the core insight: "Unlike YouTube, the recommendation engine IS the product"

---

## Feed / Recommendation Questions

**Q: How does TikTok's For You Page work?**

Multi-stage pipeline:
1. **Candidate retrieval** (offline): Two-Tower model generates top-1000 candidate videos per user. Stored in Redis pre-computed.
2. **Re-ranking** (online, <50ms): Lightweight scoring model ranks candidates using real-time context (device, time of day, recent session behavior).
3. **Diversity injection**: No more than 3 consecutive same-creator videos. 10% exploration (new content types).

**Follow-up: Why pre-compute instead of computing at request time?**
At 1B users × 2 feed requests/day = 2B requests/day. Running Two-Tower model per request = impossible (model inference ~500ms, need <50ms). Pre-computation shifts cost to offline batch (cheap) and serves results from Redis (fast).

---

**Q: What signals does TikTok use for ranking?**

Ordered by signal quality (highest to lowest):
1. **Completion rate** (most valuable): Passive, honest. Watching 90% = strong interest.
2. **Share**: High-friction action = strong positive signal.
3. **Like**: Medium signal (many people like without watching fully).
4. **Comment**: Interest signal, but adds noise from controversy.
5. **Replay**: Very strong positive signal.
6. **Skip < 25%**: Strong negative signal.
7. **Follow from FYP**: Strongest engagement possible.

**Follow-up: Why is completion rate more valuable than likes?**
Likes require deliberate action (friction). A user who watches 95% but doesn't like still found the video compelling. Conversely, a user might like while distracted and only watch 10% — that's low-quality signal. Completion is a more honest behavioral signal.

---

**Q: How do you handle cold start for new users?**

New user has no watch history → Two-Tower model has no user embedding.

Strategies:
1. **Onboarding interest selection**: Ask user to pick 5 categories (dance, cooking, gaming...). Seed FYP with trending content from those categories.
2. **Fallback to regional trending**: Top 200 videos in user's country/language that new users tend to enjoy.
3. **Quick warm-up**: After first 10 interactions, re-run candidate retrieval with lightweight user embedding built from those interactions.
4. **Cold start user tower**: Train a separate "new user" model that bootstraps from demographic features (age, region, device type).

**Follow-up: How do you handle cold start for new videos?**
New videos have no engagement data → video embedding only from content features (visual/audio/text analysis done at upload time). Inject new videos into 1-2% of FYP slots for a diverse set of users. Collect engagement data for 4 hours → if completion rate > threshold → promote to wider distribution (viral amplification loop).

---

**Q: How do you prevent the FYP from creating a "filter bubble"?**
- Reserve 10% of slots for exploration (different content type than user's usual)
- Diversity constraints: cap any single creator at 3/20 videos in a session
- Decay: inject novelty signals into ranking — new content types get a boost
- Interviewers rarely expect a full answer; the key is showing you're aware of the problem

---

## Upload Questions

**Q: A user uploads a video from a slow mobile connection. How do you make it reliable?**

Chunked upload with resume:
```
Client: splits video into 5MB chunks
GET  /upload/init → { upload_id, presigned_urls[] }
PUT  /upload/{upload_id}/chunk/1  (retry-able, idempotent)
PUT  /upload/{upload_id}/chunk/2
...
POST /upload/{upload_id}/complete
```
On network drop: client resumes from last acknowledged chunk. Server tracks which chunks are received (Postgres upload_chunks table). S3 multipart upload maps 1:1 with chunk tracking.

**Follow-up: What if the server crashes mid-upload?**
State is in Postgres + S3 multipart upload ID. Any server in the Upload Service pool can resume the upload — the client retries `/upload/{upload_id}/complete` and the new server finds the persisted state.

---

**Q: How long does it take from upload to the video being visible?**

Target: < 60s for normal videos.

Pipeline:
- Client upload: 5-30s (depends on video size + connection speed)
- S3 write: near-instant
- Kafka event: < 1s
- Transcoding: 10-30s for a 30s video on GPU (real-time factor ~1x)
- CDN push: parallel to transcoding for 720p, available ~5s after transcoding
- Total: ~30-90s

**Follow-up: What if there's a spike in uploads (viral challenge)?**
Auto-scale transcoding workers via Kubernetes HPA on Kafka consumer lag. Pre-warm spot GPU instances during anticipated peaks (e.g., Friday evening). Show creators a queue position estimate. Priority lane for large creators to maintain their trust.

---

## Scale / Storage Questions

**Q: How much storage does TikTok need?**

```
50M videos/day × 15MB compressed (720p, 30s avg) = 750TB/day
Multiple quality variants (~4x) = 3PB/day ingested
After 1 year = ~1 exabyte

CDN bandwidth = 120B views/day × 7.5MB/view = 900PB/day
```

**Follow-up: What's the single biggest cost driver?**
CDN bandwidth — at $0.008/GB (volume discount), 900PB/day = $7.2M/day. This is why TikTok operates its own CDN. They also use P2P delivery for viral content, reducing CDN bandwidth by ~15% on popular videos.

---

**Q: How do you shard the video database?**

Cassandra with `video_id` (UUID) as partition key. UUID is randomly distributed → natural even distribution. No hot partitions.

Why not shard by `creator_id`? A creator with 100M followers makes all their videos a hot partition. UUID avoids this.

For the follow/social graph: Postgres sharded by `user_id` once > 100M users.

---

**Q: How do you scale the like counter to handle 1M likes/second on a viral video?**

**Wrong answer:** One Postgres row with `UPDATE videos SET like_count = like_count + 1` — this serializes all increments.

**Right answer:**
1. Redis `INCR video:{id}:likes` — atomic, in-memory, handles 100K ops/sec per key easily
2. For extreme viral cases: shard into 10 Redis shards `INCR video:{id}:likes:{shard_no}` → aggregate on read (`MGET` + sum)
3. Async write-back to Postgres every 30s (eventual consistency for the DB count is fine)
4. Kafka: each like event also goes to Kafka for ML signals (separate from the counter)

---

## Architecture Trade-off Questions

**Q: Would you use HLS or progressive download for TikTok videos?**

It depends on video length:
- **≤ 60s: Progressive MP4** — simpler, faster start time (no playlist file round-trip), no need for seek (short video, user won't seek)
- **> 60s: HLS with 2s segments** — enables adaptive bitrate based on connection quality, allows seeking

Trade-off: HLS adds ~2 extra HTTP requests (manifest + segment playlist) before first frame. For a 15-second TikTok, that overhead is unacceptable. For a 3-minute video it's fine.

**Follow-up: What's HLS's disadvantage for short videos?**
Latency to first frame: client must fetch `.m3u8` manifest, then segment playlist, then first segment. That's 3 round trips minimum. For progressive MP4, client starts playing from byte 0. For sub-1s start time, progressive wins.

---

**Q: How would you design the video recommendation to avoid over-indexing on engagement signals from power users?**

Power users (top 1% of engagers) can skew the training data — they watch, like, and comment far more than average users. Models trained on this data optimize for power-user behavior, not the median user.

Solutions:
1. **Weight capping**: cap each user's contribution to training signal at a max weight
2. **User sampling**: stratified sampling in training data — oversample casual users
3. **Separate model segments**: different model for casual vs. power users
4. **Metrics**: track engagement metrics split by user cohort (new users, casual, power)

---

## Failure Scenario Questions

**Q: The recommendation system goes down. What does the user see?**

Graceful degradation:
1. Feed Service reads pre-computed FYP from Redis (already there, independent of Recommender)
2. If Redis also unavailable: serve `trending:{region}` feed (always available, pre-computed every 5 min)
3. Circuit breaker: after 5 Recommender failures → open circuit → no calls to Recommender → serve trending automatically
4. User sees: slightly less personalized feed, but not an error. "Trending in your area" feed.

**Follow-up: How long before the personalized feed is restored?**
Recommender comes back → circuit breaker probes every 30s → success → close circuit → FYP candidates start refreshing in Redis for each user as they request feeds. Full personalization restored within 30 min as Redis FYP keys are rebuilt for active users.

---

**Q: A video goes viral globally. How does your CDN handle it?**

Problem: 50M simultaneous viewers → CDN cold → requests cascade to origin.

Solutions:
1. **Viral detection**: view velocity monitoring (Flink, 1-min windows). If video exceeds 10K views/min → trigger CDN warm-up job → push video to all ~200 edge nodes worldwide.
2. **Request coalescing at CDN edge**: if cache miss → only 1 origin request per edge per unique video. Other concurrent requests wait (stale-while-revalidate).
3. **Origin Shield**: intermediary layer between CDN edges and S3. Edges hit Origin Shield (single region), not S3 directly. Reduces S3 origin load from N-edge×M-concurrent to single-origin×M.
4. **P2P for ultra-viral**: Users who finish watching seed the video to nearby users. Reduces CDN bandwidth by 10-20% for top 1000 videos.

---

**Q: How do you ensure a video is not served if it was flagged/deleted?**

**Problem:** Video URL is on CDN → even after deletion from DB, CDN still serves it from cache.

Solutions:
1. **Signed URLs with expiry**: every video URL is a pre-signed S3/CDN URL with 6h TTL. After deletion → no new URLs issued → existing URLs expire in ≤ 6h.
2. **CDN purge API**: on deletion → immediately call CDN purge for that video's URLs → takes effect in ~5s.
3. **Token-based access**: CDN verifies a short-lived access token per video per user → server-side revocation invalidates the token → CDN request fails → client gets 403.

Trade-off: signed URL approach has a 6h window where a leaked URL still works. Token-based revocation adds latency on every video load (needs a token check). TikTok likely uses CDN purge + signed URL combination for balance.

---

## Deeper Follow-up Questions

**Q: How does the Two-Tower model handle new interest signals in real-time?**

**Standard approach** (used by TikTok competitor, ByteDance): Batch retrain daily. Slow to adapt.

**TikTok's actual approach** (from their "Monolith" paper, 2022):
- Online learning: embedding tables updated continuously as watch events arrive
- Non-embedding model weights: still updated in daily batch (stable parameters)
- Result: user embedding adapts within minutes of a new interest signal
- Challenge: online updates require collision-free embedding tables + careful learning rate scheduling

For an interview, explain both approaches and note the trade-off: online learning is more complex to build/maintain but significantly improves FYP quality for users who just discovered a new interest.

---

**Q: How do you measure if your recommendation system is working?**

Online metrics (real-time):
- **Session length**: avg time spent per session
- **Completion rate per FYP slot**: are users finishing videos? Is it decreasing over time?
- **Day-7 retention**: percentage of new users still active 7 days later
- **Follow rate from FYP**: creator discovery effectiveness

Offline metrics (before deploying new model):
- **AUC on held-out data**: can the model predict which videos a user will complete?
- **NDCG@20**: quality of top-20 recommendations vs. ground truth

A/B testing: 5% of users get new model, compare session length and D7 retention. Ship if positive (significance threshold: p < 0.05, minimum 7-day run to capture weekly patterns).

**Red flag metric**: watch time can be gamed (auto-play loops). Always pair with explicit engagement (likes, shares, follows) and survey satisfaction scores.

---

## Rapid-Fire Questions

**Q: Why Cassandra for video metadata?**
Write-heavy (50M new videos/day), partitioned by video_id (UUID) for even distribution, no JOINs needed (video is self-contained document), horizontal scale.

**Q: Why not Postgres for video metadata?**
Fine at small scale but single-node Postgres write throughput = ~10K writes/sec. 578 videos/sec is manageable, but Cassandra buys runway for 10× growth without re-architecture.

**Q: How do you prevent a competitor from scraping all your videos?**
Rate limiting per IP, require auth token for video URL, signed pre-signed URLs that expire, CDN CAPTCHA on anomalous request patterns, user-agent fingerprinting, per-account download limits.

**Q: How do you handle copyright (a video with copyrighted music)?**
Content ID fingerprinting at upload time (similar to YouTube Content ID). Audio fingerprint matched against music rights database. If match: monetize for rights holder, or mute/reject depending on policy. Processing is asynchronous (fingerprint check runs during transcoding pipeline).

**Q: What database would you use for the social graph (follows)?**
Small-to-medium scale: Postgres with (follower_id, followee_id) index on both columns.
Large scale: Neo4j or Amazon Neptune (graph DB) for traversal queries like "mutual follows" or "users who follow who I follow."
TikTok's scale: likely a custom in-house graph service + caching in Redis for hot influencer follower lists.
