# Video Streaming Platform — Interview Q&A
**Date:** 2026-05-13 | **System:** YouTube-style Video Streaming

> Mirrors real interview patterns. Study the *shape* of answers as much as the content — interviewers reward structured thinking, explicit trade-off reasoning, and confident depth on one sub-system.

---

## How Interviewers Structure This Problem

**Opening (5 min):** Scope and clarification — what features, what scale?
**Design (25 min):** High-level → components → interviewer picks one to deep-dive
**Probing (10 min):** "What if X fails?", "How would you scale Y?", "Why not Z instead?"
**Wrap-up (5 min):** Trade-offs you'd revisit, what you'd build next

Common deep-dives interviewers choose for this system:
- Upload pipeline (chunked upload, resumability)
- Transcoding pipeline (parallelism, failure, job states)
- CDN and adaptive streaming
- View count at scale (hot key problem)
- Feed/recommendation architecture

---

## Part 1: Scoping Questions

---

### Q1: "Before you draw anything — what would you clarify?"

**Strong answer:**

> "A few things I'd want to nail down:
>
> **Features:** Are we building the full platform — upload, transcode, stream, search, recommendations? Or should I focus on a specific slice? I'll assume the core loop: upload, process, stream on demand.
>
> **Scale:** Are we targeting YouTube-scale — 2.5 billion MAUs, 500 hours of video uploaded per minute? Or a smaller platform with, say, millions of users?
>
> **Content types:** On-demand video only, or also live streaming? I'll assume on-demand for now.
>
> **Consistency:** Is it okay if a viewer sees stale view counts (eventually consistent), or must counts be real-time? I'll assume eventual consistency for counts is fine — most platforms work this way.
>
> **Geography:** Global distribution required? I'll assume yes.
>
> I'll target: global scale, 50M concurrent viewers, 500 hours/min upload, on-demand streaming, eventual consistency for engagement metrics."

**Why this lands:** Shows you know what drives architecture decisions. Scale and consistency requirements are the two most important axes for this problem.

---

### Q2: "What are the main entities in this system?"

**Expected answer:**

> "Four core entities:
>
> **User** — account, subscriptions. **Video** — metadata: title, description, status, duration. **VideoVariant** — one per (resolution, codec) combination; the actual transcoded files. **WatchEvent** — the analytics stream: who watched what, for how long, at what quality.
>
> The key insight is that 'video' is actually two separate things: the metadata record (small, relational, needs strong consistency for privacy settings) and the media files (huge, immutable, optimized for read throughput). They live in completely different storage systems."

**Interviewer follow-up:** *"Why separate metadata from media files?"*

> "Metadata is kilobytes — stored in PostgreSQL. Media files are gigabytes — stored in object storage like S3. They have opposite characteristics: metadata needs ACID transactions (you can't have a video visible while its privacy setting is being updated), media files are append-only and immutable once written. Mixing them in one system would be like putting your JPEG files in MySQL."

---

## Part 2: Upload Pipeline Deep-Dive

---

### Q3: "How would you handle a creator uploading a 20 GB video file?"

**This is the most common opening question for upload. Interviewer wants to see you know chunked/multipart upload.**

> "A single HTTP request for 20 GB is a non-starter — any network hiccup loses everything and you'd have to start over. Here's the approach:
>
> **Step 1 — Initiate:** Creator calls `POST /uploads/initiate`. Server creates a multipart upload in the object store and returns N presigned PUT URLs, one per 10 MB chunk. So a 20 GB video = 2,000 presigned URLs.
>
> **Step 2 — Upload:** Creator's client uploads chunks in parallel — say 8 concurrent connections. The chunks go directly to the object store, bypassing app servers entirely. This is critical: at YouTube's scale, routing gigabytes through app servers would require enormous bandwidth.
>
> **Step 3 — Resume:** If the upload is interrupted, the client locally tracks which ETags it received for completed chunks. On resume, it queries which parts the object store has, re-uploads only the missing ones.
>
> **Step 4 — Complete:** Client calls `POST /uploads/{id}/complete` with the list of (part_number, etag) pairs. Object store assembles them atomically. We then publish a VideoUploaded event to trigger transcoding."

**Interviewer follow-up:** *"What if the creator's client crashes at step 4 — after all chunks uploaded but before calling complete?"*

> "The multipart upload is left open but uncommitted. Object stores have a configurable lifecycle rule: incomplete multipart uploads older than 7 days are automatically deleted. For better UX, we store the upload state (upload_id + all ETags) in local storage on the client. On app restart, the client detects the incomplete upload and calls complete directly — no re-upload needed."

---

### Q4: "Why use presigned URLs? Why not route uploads through your API?"

> "Two reasons: bandwidth and cost.
>
> At 500 hours/minute of video, that's roughly 5–50 GB/minute flowing through upload endpoints. Routing that through app servers means you need enormous network capacity there — and you're paying for it twice (in to app server, then out to object store).
>
> With presigned URLs, the data flows directly from the creator's browser to the object store. App servers only handle the small initiate and complete requests. The object store (S3, GCS) is built for exactly this load — it's infinitely scalable. Your app servers stay cheap and focused on business logic.
>
> The downside: presigned URLs have an expiry (e.g., 12 hours). If the creator takes longer than that to upload, URLs expire. Solution: chunk upload — each chunk's presigned URL needs to be valid only for that chunk's upload time, not the whole session."

---

## Part 3: Transcoding Pipeline Deep-Dive

---

### Q5: "Walk me through how a video goes from raw upload to playable for viewers."

**Core flow question — mirror the data flow in your design:**

> "Once the raw file lands in object storage, we publish a VideoUploaded event to Kafka. The Transcoding Orchestrator picks this up and spawns multiple parallel jobs — one per (resolution, codec) combination. A 1080p video in 3 codecs at 8 resolutions could mean up to 24 parallel jobs.
>
> Each Transcoding Worker claims a job, downloads the raw file, runs FFmpeg to transcode and segment the video into ~4-second MPEG-DASH segments, then uploads segments + manifest back to object storage.
>
> The Orchestrator tracks completion. When the critical variants are done — say, 360p and 720p in H264 — it marks the video READY and notifies subscribers. Higher-quality and alternative-codec variants (4K, AV1) continue processing in the background.
>
> The key design choice is 'critical path first': viewers can start watching 360p while 4K is still transcoding. This gets the video live in minutes even for large files."

**Follow-up:** *"How do you handle a transcoding worker crashing mid-job?"*

> "Workers heartbeat to a jobs table every 30 seconds. If the Orchestrator sees a job has been CLAIMED for more than 2 minutes with no heartbeat, it marks it STALE and re-queues it.
>
> The recovered job needs to be idempotent: the new worker checks which segments are already uploaded to object storage, then resumes from the first missing segment. Segment paths are deterministic — `video_id/quality/seg_N.m4s` — so the worker knows exactly what exists.
>
> We also have a maximum retry limit — say 3. After 3 failures, the job moves to DEAD_LETTER for manual investigation. The creator gets notified: 'We had trouble processing your video, please try uploading again.'"

---

### Q6: "Should you transcode eagerly or lazily?"

**Trade-off question — shows system thinking:**

> "Eager transcoding is YouTube's model: transcode all variants immediately on upload. The benefit is that any viewer on any device or network gets their optimal quality immediately.
>
> Lazy transcoding — transcode on first request — sounds appealing but has serious problems. If a video suddenly goes viral, the first 10,000 viewers trigger 10,000 simultaneous transcoding requests. You'd need to either queue them (viewers wait) or have massive standby capacity (expensive).
>
> My recommendation: **eager for critical variants** (360p, 720p), **lazy for niche variants** (8K, older codecs that <1% of clients use). For the long tail of videos that get zero views, you've paid transcoding cost unnecessarily — but that's a small fraction of total cost, and you avoid the 'viral spike' problem entirely."

---

## Part 4: Streaming & CDN Deep-Dive

---

### Q7: "How does adaptive bitrate streaming work? Why does it matter?"

> "ABR is how the video player automatically adjusts quality as network conditions change.
>
> Instead of one video file, we serve many small segments — typically 4 seconds each — in multiple quality levels. The player requests segments one by one. Before each request, it estimates current network throughput based on how fast the last segment downloaded.
>
> Decision logic each segment:
> - If throughput >> current quality bitrate and buffer is healthy → try stepping up
> - If throughput < current quality bitrate or buffer is getting thin → step down
> - Hysteresis prevents flapping: you need sustained improvement to step up, but can step down immediately
>
> Why it matters: without ABR, you pick one quality for the whole video. A 1080p video on a mobile device that hits a tunnel just buffers until connectivity returns. With ABR, it gracefully degrades to 144p during poor connectivity and recovers automatically. This is the single biggest contributor to watch time — people abandon videos that buffer."

**Follow-up:** *"How does the CDN fit in here?"*

> "Segments are served from CDN edges, not directly from your object store. An edge server in Mumbai serves Mumbai viewers — round-trip for a segment is 5ms instead of 200ms from a US datacenter.
>
> Segments are immutable — the content of a segment never changes after transcoding. So CDN TTL can be set to 1 year. This means 95%+ of segment requests are cache HITs at the edge; origin load is minimal.
>
> Manifests — the index files that tell the player which segment URLs to request — have a short TTL (60 seconds) because they might change as new quality variants become available."

---

### Q8: "If a video is 10 seconds into playback and the CDN goes down, what happens?"

> "The player has pre-buffered 3–5 segments ahead — typically 12–20 seconds of video. During a brief CDN outage (< 20 seconds), the player keeps playing from its buffer with no visible impact.
>
> For a longer outage, the player can fall back to alternate CDN URLs — we embed multiple CDN hostnames in the manifest. The player tries the next one on failure.
>
> If all CDNs are unreachable, the player can be configured to fall back to the origin URL — the object store's direct URL. Slower (no geo-distribution), but functional.
>
> On the infrastructure side: we run multiple CDN providers (e.g., primary Cloudflare, secondary Akamai). DNS failover switches within 60 seconds of detecting primary CDN failures. Segments cached in the primary CDN are independently cached in the secondary, so failover doesn't cause a cache cold-start problem for popular content."

---

## Part 5: Scale & Hot Key Problems

---

### Q9: "A video goes viral — 50 million viewers watch simultaneously. Where does your system break?"

**Stress test question — interviewer probes specific bottlenecks:**

> "Let me trace through the pressure points:
>
> **CDN segments:** This is fine. CDN is designed for this — 50M viewers fetching the same segments is the ideal CDN use case. Popular segment cache-hit rate approaches 100%.
>
> **Video metadata DB:** When 50M people open the video page, they read video metadata (title, description, view count). This read hits Redis first. A popular video's metadata is in every Redis node — cache hit rate is near 100%. PostgreSQL barely notices.
>
> **View count writes:** This is the real problem. 50M views in an hour = ~14,000 writes/second to view_count. A single row update at that rate causes lock contention. Solution: counter shards — 16 rows per video, each worker writes to a random shard. 14,000 / 16 = ~875 writes/second per shard — manageable.
>
> **Subscription fan-out:** If the viral video is from a creator with 100M subscribers, you can't write 100M feed entries on publish. Solution: pull-based subscription feed — read fan-out happens when subscribers open the app, not when the video is published. The video service queries each subscribed channel's latest videos on feed open.
>
> **Transcoding:** The video was transcoded before going viral — not a scaling issue. But a load spike might occur if the CDN needs to simultaneously serve the video from cold cache across many PoPs. Pre-warming the CDN for predicted-viral content is an option (e.g., based on early engagement signals)."

---

### Q10: "How would you design the view count system to be accurate to within 1%, real-time?"

> "Pure real-time accurate counts at 50M concurrent viewers is fundamentally hard — you'd need distributed atomic increments across every replica simultaneously. That's slow and expensive.
>
> I'd design a two-tier system:
>
> **Tier 1 — Fast approximate count:** WatchEvents are streamed to Kafka and consumed by a ClickHouse or Redis stream consumer that maintains a running count. This is accurate within seconds, but only reads a single consumer's accumulated state — may miss cross-region events temporarily.
>
> **Tier 2 — Precise batch count:** Every 5 minutes, a batch job reads all WatchEvents from ClickHouse since the last run, deduplicates by (user_id, video_id, session_id) to prevent inflated counts from network retries, and writes the accurate count to PostgreSQL via counter shards.
>
> For the viewer-facing count: display the fast count. For creator analytics (revenue-critical): use the batch-accurate count.
>
> To hit 1% accuracy: the deduplication must handle the same user watching on 3 devices simultaneously, ad-blockers that fire events twice, and network retries. The session_id dedup key covers all of these."

---

## Part 6: Trade-off Questions

---

### Q11: "Why not just store all transcoded videos in one bucket and serve directly, without a CDN?"

> "Because object storage is not designed to serve video at scale.
>
> Object stores (S3, GCS) are optimized for durability and eventual consistency — not low-latency, high-concurrency reads from geographically distributed clients. Serving 50M concurrent viewers from a single-region object store would cause:
> 1. High latency for non-US viewers (200ms+ round trips)
> 2. Egress costs: 50M viewers × 1 Mbps average = 50 Tbps. Object store egress is expensive ($0.09/GB). CDN egress is 10x cheaper.
> 3. S3 throttling: S3 supports 5,500 GET requests/second per prefix without explicit configuration — popular videos would get throttled.
>
> CDN solves all three: geo-distribution cuts latency, caching cuts origin requests 95%+, and CDN providers have peering agreements that dramatically reduce egress cost."

---

### Q12: "Fan-out on write vs. fan-out on read for the subscription feed — when would you choose each?"

> "Fan-out on write: when you publish a video, you immediately push it into every subscriber's feed. Reads are O(1) per user. Writes are O(subscribers_count) per publish.
>
> Fan-out on read: when a user opens their feed, you fetch the latest videos from each subscribed channel. Reads are O(subscriptions_count) per user. Writes are O(1) per publish.
>
> **Choose fan-out on write when:** Most creators have < 10K subscribers (most of Twitter's users). Reads are far more frequent than writes. You can afford the write amplification.
>
> **Choose fan-out on read when:** Some creators have 100M+ subscribers. Publishing one video would trigger 100M DB writes — that's a multi-minute operation that would overload your write capacity.
>
> **YouTube's actual approach:** Hybrid. For small channels (< 1M subscribers): fan-out on write — pre-populate feeds for fast reads. For mega-channels (1M+): fan-out on read — fetch on demand. The cutoff is configurable and tuned based on load.
>
> The interesting question is: how do you handle a creator who crosses the threshold? You gradually migrate them from write-fan-out to read-fan-out as their subscriber count grows."

---

### Q13: "Your interviewer says: 'The transcoding pipeline is a black box. How would you design the storage for transcoded segments?'"

> "The core constraint: segments are immutable once written, referenced by manifests, and need to be globally distributed. Here's the object store layout:
>
> ```
> s3://video-cdn-origin/
>   {video_id}/
>     manifest/
>       master.m3u8           ← HLS master manifest
>       master.mpd            ← DASH manifest
>     360p_h264/
>       index.m3u8            ← variant manifest
>       seg_000.ts
>       seg_001.ts
>       ...
>     720p_h264/
>       index.m3u8
>       seg_000.m4s
>       ...
>     1080p_vp9/
>       index.m3u8
>       seg_000.m4s
>       ...
> ```
>
> Why this layout?
> - video_id as root prefix: allows prefix-based ACLs, lifecycle rules, and deletion
> - quality + codec as subpath: player constructs segment URLs from manifest without DB lookup
> - Segment filenames sequential: CDN can predictively prefetch seg_N+1 when seg_N is requested
> - Manifests separate from segments: short TTL on manifests (60s), long TTL on segments (1yr)
>
> For deletion (e.g., creator deletes video): S3 lifecycle rule to expire all objects under `{video_id}/`. CDN purge API to invalidate cached manifests immediately. Segments expire from CDN naturally after 1 year (or use CDN bulk purge)."

---

## Part 7: Follow-Up & Gotcha Questions

---

### Q14: "How would you ensure a deleted video stops playing immediately for viewers who already started watching?"

> "This is a great consistency edge case.
>
> The viewer's client has already fetched the manifest and is fetching segments. Segments are cached in CDN with 1-year TTL — so even after deleting from object store, CDN edges serve from cache.
>
> Two mechanisms:
>
> 1. **Manifest TTL:** Manifests have a 60s TTL. After deletion, the manifest becomes a 404. Player fetching the next manifest refresh gets a 404 → stops playback. Within 60 seconds, all active viewers are cut off.
>
> 2. **CDN purge:** Use CDN purge API to immediately invalidate the cached manifests and (optionally) segments for the deleted video. This cuts off active viewers instantly.
>
> For privacy-critical deletions (DMCA, CSAM): immediate CDN purge is mandatory. For creator deleting their own content: 60s TTL drain is acceptable UX."

---

### Q15: "How would you handle the 'thundering herd' when a very popular video's cache expires?"

> "Cache stampede: when a popular item's CDN cache expires, thousands of simultaneous requests all miss the cache and hammer the origin.
>
> Three solutions:
>
> 1. **Stale-while-revalidate:** CDN serves stale content while async-fetching a fresh copy from origin. The viewer sees a 1-year-old cached segment — but segments are immutable, so 'stale' = the correct content. This is the cleanest solution: `Cache-Control: max-age=31536000, stale-while-revalidate=86400`.
>
> 2. **Probabilistic early refresh:** Before the segment expires, a small % of cache nodes start refreshing it proactively. This spreads the origin request over time instead of all at once.
>
> 3. **Request coalescing (CDN-side):** Modern CDNs coalesce simultaneous misses for the same URL into a single origin request. The CDN fetches once, responds to all waiters. No special configuration needed — most major CDNs do this by default (called 'request collapsing' in Varnish, 'shield' in Fastly).
>
> In practice: for video segments with 1-year TTL and immutable content, this problem barely exists. The real stampede risk is on metadata/manifests with 60s TTL — request coalescing at the CDN handles that."

---

### Q16: "If you had to choose between VP9 and AV1 for all your transcoded videos, which would you pick and why?"

> "This is a trade-off between compute cost and bandwidth savings.
>
> **AV1** is technically superior: ~30% better compression than VP9 at the same quality, meaning 30% less bandwidth per stream and 30% cheaper CDN egress at scale. For YouTube's egress volume, that's hundreds of millions of dollars per year.
>
> **But:** AV1 encoding is 10–50x slower than H264 and 3–5x slower than VP9. At 500 hours/minute of uploads, that means 5x more transcoding compute for AV1 than VP9 — significant infrastructure cost.
>
> **My answer:** Transcode to both. Serve AV1 to clients that support it (modern Chrome, Android, smart TVs), fall back to VP9 or H264 for others. The manifest negotiates the codec. This is exactly what YouTube does.
>
> For a new platform: start with H264 only (widest compatibility, fastest encoding). Add VP9 when you have budget and scale. Add AV1 only when your viewer devices support it — check your analytics for client capability distribution first."

---

## Common Mistakes Interviewers Watch For

1. **Not splitting raw storage from processed storage** — raw video in the same bucket as segments adds cost and complexity (different lifecycle policies, ACLs, retention)

2. **Synchronous transcoding** — calling a transcoding API synchronously during the upload request means the creator waits hours for large files. Always async with job queue.

3. **Storing view counts in a single row** — "UPDATE videos SET view_count = view_count + 1" at 50M concurrent viewers creates a hot key. Counter shards or Redis approximate counters are the answers.

4. **Fan-out on write for mega-creators** — 100M subscriber channels would generate write storms on every upload. Hybrid fan-out (write for small channels, read for large) is the real answer.

5. **Forgetting CDN invalidation on video delete** — segments cached at CDN with 1-year TTL continue being served after deletion unless you explicitly purge them.

6. **Not considering upload resumability** — any design that doesn't handle partial upload recovery will fail for large files over unreliable connections.

7. **Single CDN provider** — treating CDN as a reliable black box. Multi-CDN with failover is production-grade.
