# Instagram System Design
**Date:** 2026-06-02  
**Pattern:** Photo/video sharing + social feed + CDN-heavy reads  
**Real-world:** Instagram, Pinterest, Flickr, 500px

---

## First Principles Check

**Do we need a custom system?**  
Photos/videos are large binary blobs — can't store in relational DB efficiently.  
Feed requires aggregating posts from N followed accounts — can't do naive JOIN at read time for 500M users.  
→ Yes: object storage for media, separate feed generation, CDN for delivery.

**Core tension:** Write fanout (post → push to all followers) vs. Read fanout (pull at read time).  
A user with 50M followers can't push to 50M feeds on every post. Need hybrid.

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| User | user_id, username, profile_pic_url, follower_count, is_celebrity |
| Post | post_id, user_id, media_url, caption, created_at, media_type (photo/video/reel) |
| Follow | follower_id, followee_id, created_at |
| Like | post_id, user_id, created_at |
| Comment | comment_id, post_id, user_id, body, created_at |
| Feed | user_id, post_id, score, inserted_at (pre-computed timeline) |
| Story | story_id, user_id, media_url, expires_at (24h TTL) |

---

## Actions

- Upload photo/video
- Follow/unfollow user
- View home feed
- Like/comment on post
- View profile (grid of posts)
- View stories
- Search users/hashtags
- View explore (discovery)

---

## Data Flow

### Upload
```
Client → API Gateway → Upload Service
                           ↓
                    Object Storage (S3)
                           ↓
                    CDN (CloudFront) ← invalidate/pre-warm
                           ↓
                    Media Processing Queue
                           ↓
                    Transcoding Service (resize, compress, generate thumbnails)
                           ↓
                    Post DB write → Feed Fanout Queue
                           ↓
                    Feed Service (push to follower feeds)
```

### Feed Read
```
Client → API Gateway → Feed Service
                           ↓
                    Read pre-computed feed (Redis sorted set or Cassandra)
                           ↓
                    Hydrate post details (Redis cache → Post DB)
                           ↓
                    Return ranked/paginated feed
```

---

## High Level Design

```
                         ┌─────────────┐
                         │   Clients   │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │ API Gateway │  (auth, rate limiting)
                         └──────┬──────┘
            ┌──────────┬────────┼────────┬──────────┐
            ▼          ▼        ▼        ▼          ▼
      ┌──────────┐ ┌───────┐ ┌──────┐ ┌──────┐ ┌────────┐
      │  Upload  │ │ Feed  │ │ User │ │Social│ │ Search │
      │ Service  │ │Service│ │  Svc │ │  Svc │ │  Svc   │
      └────┬─────┘ └───┬───┘ └──┬───┘ └──┬───┘ └───┬────┘
           │           │        │         │          │
      ┌────▼─────┐ ┌───▼──────┐ ├─────────┘          │
      │    S3    │ │  Redis   │ ▼                     ▼
      │(media)   │ │ (feeds)  │ MySQL              Elasticsearch
      └────┬─────┘ └──────────┘ (users/posts)
           ▼
       CloudFront
        (CDN)
```

---

## Low Level Design

### 1. Media Upload Pipeline

**Why not store directly to DB?**  
Binary blobs inflate DB size, no CDN integration, no compression pipeline.

```
POST /upload
1. Client requests pre-signed S3 URL (avoids server as proxy)
2. Client uploads directly to S3 (reduces server load)
3. S3 triggers SNS event → SQS queue
4. Transcoding workers pull from queue:
   - Resize to 1080px, 720px, 320px
   - Generate thumbnail
   - Extract dominant colors (for placeholder blur)
5. Write Post record to DB with final CDN URLs
6. Publish post_created event → Feed Fanout
```

**Trade-off: Pre-signed URL vs server-side upload**  
| | Pre-signed | Server Proxy |
|---|---|---|
| Server load | Low | High |
| Progress tracking | Client-side | Easy |
| Virus scanning | Harder | Easy |
→ Use pre-signed for scale, add async virus scan via S3 events.

### 2. Feed Generation — Push vs Pull vs Hybrid

**Option A: Push (fanout-on-write)**  
On post, write post_id to each follower's feed list in Redis.  
✅ O(1) read | ❌ Celebrity with 50M followers → 50M writes per post (fanout storm)

**Option B: Pull (fanout-on-read)**  
At read time, fetch posts from all followed users, merge, rank.  
✅ No fanout cost | ❌ User follows 1000 people → 1000 DB queries per feed load

**Option C: Hybrid (Instagram's actual approach)**  
- Normal users (< ~10K followers): push fanout to Redis sorted set  
- Celebrity users (> threshold): skip push; pull their posts at read time and merge  
- Feed read = pre-computed feed + pull from celebrity accounts + rank/merge

```python
def get_feed(user_id):
    # Pre-computed feed from Redis
    feed = redis.zrevrange(f"feed:{user_id}", 0, 50)
    
    # Pull recent posts from celebrities user follows
    celebrities = get_celebrity_followees(user_id)
    for celeb_id in celebrities:
        recent = db.query("SELECT * FROM posts WHERE user_id=? ORDER BY created_at DESC LIMIT 10", celeb_id)
        feed.extend(recent)
    
    # Rank by recency + engagement score
    return rank_and_dedupe(feed)[:20]
```

**Feed stored in Redis:**  
`ZADD feed:{user_id} {timestamp} {post_id}` — sorted set by score  
TTL: 7 days; inactive users get cold feed rebuilt on next login.

### 3. Post & User Storage — Sharding Strategy

**Posts table** sharded by `user_id` (keep user's posts co-located)  
→ Profile grid query = single shard  
→ Feed fanout must scatter to N shards (acceptable for writes)

**Follower graph** in separate service:  
- MySQL: `(follower_id, followee_id)` — can shard by follower_id  
- For large graphs: consider graph DB or dedicated social graph service

**Hot shard problem:** Celebrity's user shard gets hammered  
→ Read replicas per shard  
→ Cache celebrity posts in Redis with short TTL

### 4. CDN Strategy

```
Upload → S3 (origin) → CloudFront (edge)
                            ↑
                     Cache-Control: max-age=31536000 (immutable media)
                     URL includes content hash → cache forever
```

- Profile pics / thumbnails: aggressive CDN cache (content-addressed URLs)
- Stories: 24h TTL, shorter CDN cache + background delete after expiry
- Videos: HLS streaming, segment-level CDN caching

### 5. Stories

**Key difference from posts:** 24h expiry  
- Store in Redis with TTL = 24h (fast expiry, no DB cleanup job needed)  
- Also write to Cassandra for audit/recovery with TTL column  
- Story views: HyperLogLog in Redis (approximate unique viewers, O(1) space)  
- On expiry: S3 objects moved to cold storage (not deleted — legal/ML reasons)

### 6. Likes & Comments

**Likes:** High write volume, exact count less critical  
- Write to Redis counter: `INCR likes:{post_id}`  
- Async sync to DB every 30s (eventual consistency acceptable)  
- "Did I like this post?" → Redis Set: `SISMEMBER liked:{user_id} {post_id}`

**Comments:** Ordered by time, need persistence  
- Write to Cassandra (wide column, partition by post_id, cluster by created_at)  
- Load first 3 comments inline, paginate rest

### 7. Search

- Users/hashtags indexed in Elasticsearch  
- On post creation: async index hashtags from caption  
- On follow: no index change needed  
- Explore feed: ML-ranked, personalized; separate recommendation service

---

## Trade-off Summary

| Decision | Choice | Why | Cost |
|----------|--------|-----|------|
| Media storage | S3 + CDN | Scale, geo-distribution | CDN cost, cache invalidation complexity |
| Feed | Hybrid push/pull | Balance write cost vs read latency | Complexity of celebrity detection |
| Post sharding | By user_id | Co-locate user's posts | Hot shards for celebrities |
| Likes | Redis + async DB | Write throughput | Slight inconsistency (±few likes) |
| Stories expiry | Redis TTL | Automatic cleanup | Redis memory cost |
| Feed storage | Redis sorted set | O(log N) insert, O(1) read | Memory cost, must rebuild for cold users |

---

## Failure Analysis

### Scenario 1: Upload Service crashes mid-upload

**What breaks:** Client uploaded to S3, but Post DB write never happened. Media orphaned.

**Failure chain:**  
`Client → S3 ✅ → Upload Service crashes → Post DB ❌ → Feed fanout ❌`

**Solutions:**
- S3 event triggers a "pending upload" reconciler (checks for S3 objects without corresponding Post records)
- Client retries with same idempotency key (pre-signed URL tied to idempotency token)
- Saga pattern: compensating transaction deletes orphaned S3 object after timeout
- Upload Service writes intent to DB *before* returning pre-signed URL (2-phase commit lite)

### Scenario 2: Feed Fanout Queue backs up (celebrity posts at scale)

**What breaks:** User posts to 50M followers; fanout queue has 50M write tasks. Queue depth explodes. Feed Service workers overwhelmed.

**Failure chain:**  
`Post created → Fanout Queue overloaded → Workers fall behind → Followers see stale feed for hours`

**Solutions:**
- Celebrity threshold detection: skip push fanout, use pull at read time
- Rate-limit fanout worker throughput to protect downstream Redis
- Backpressure: if queue depth > threshold, reject new non-critical fanout tasks
- Separate queues per tier (celebrity vs normal) with different worker pools
- Fan-out to Redis in batches with pipeline commands (reduce round trips)

### Scenario 3: Redis feed cache dies

**What breaks:** All pre-computed feeds lost. Every feed read falls through to DB. DB gets 100x normal load. Cascading failure.

**Failure chain:**  
`Redis down → Feed Service → DB fallback → DB overloaded → DB down → total outage`

**Solutions:**
- Redis Cluster with 3 replica nodes (automatic failover in ~30s)
- Circuit breaker: if Redis latency > 500ms, switch to pull-from-DB mode with aggressive rate limiting
- Feed rebuild on cache miss (lazy rebuild, not all at once — avoid thundering herd)
- Staggered rebuild: rebuild feeds sorted by user last-active-time (active users first)
- Persistent Redis (AOF or RDB snapshots) to survive restarts

### Scenario 4: CDN cache miss storm (new viral post)

**What breaks:** Post goes viral. All CDN edges simultaneously miss cache for thumbnail. 10M requests hit S3 origin in seconds. S3 throttled, high latency.

**Failure chain:**  
`Viral post → CDN miss across all edges → S3 origin hammered → S3 throttled → image 503s globally`

**Solutions:**
- CDN cache stampede prevention: Origin Shield (single point to S3, others cache from it)
- Pre-warm CDN: on post creation, proactively push to top N CDN edge nodes
- Request coalescing at CDN: multiple miss requests for same object → only 1 origin fetch
- S3 request rate limits: prefix-based sharding of S3 keys (`{shard}/{post_id}/img.jpg`)

### Scenario 5: Post DB shard becomes unavailable

**What breaks:** All posts for users mapped to that shard are unreadable. Feed hydration fails. Profile pages 404.

**Failure chain:**  
`Shard 3 down → Feed hydration for 1/N users fails → Feed Service returns partial/empty feeds`

**Solutions:**
- Each shard has 1 primary + 2 read replicas across AZs
- Read from replica on primary failure (may see slightly stale data — acceptable)
- Graceful degradation: return cached CDN thumbnails even if metadata unavailable
- Post metadata also cached in Redis (short TTL) — serve stale on DB failure
- Shard rebalancing: consistent hashing minimizes resharding impact on failures

### Scenario 6: Media transcoding service crashes

**What breaks:** Post uploaded, raw media in S3, but no thumbnails/compressed versions. Feed shows broken images.

**Failure chain:**  
`S3 upload ✅ → SNS event → SQS → Transcoding Service crashes → thumbnails never generated`

**Solutions:**
- SQS visibility timeout: message reappears after 30s, another worker picks it up (at-least-once)
- Idempotency: transcoding job checks if output already exists in S3 before reprocessing
- Dead letter queue (DLQ): after 3 retries, move to DLQ → alert on-call
- Show original unprocessed image temporarily while transcoding in progress (flag `is_processed=false`)
- Multi-AZ transcoding worker pool

---

## Capacity Estimates

**Scale:** 500M DAU, 100M posts/day, 5B feed reads/day

| Metric | Estimate |
|--------|----------|
| Post writes | ~1,200/sec |
| Feed reads | ~58,000/sec |
| Media storage | 100M posts × 5MB avg = 500TB/day → use S3 Glacier for older media |
| Redis feed memory | 500M users × 20 post_ids × 8 bytes = ~80GB (only active users in memory) |
| Fanout writes | 1,200 posts/sec × avg 200 followers = 240K Redis writes/sec |

---

## Real-World References

- Instagram Engineering: [Scaling Instagram Infrastructure](https://engineering.fb.com/2012/05/02/web/the-architecture-behind-instagrams-infrastructure/)
- Instagram switched from PostgreSQL → Cassandra for some workloads at scale
- Uses TAO (Facebook's distributed graph store) for social graph
- Vacuum system to clean up orphaned S3 objects
- "Thundering herd" protection for celebrity posts is well-documented

