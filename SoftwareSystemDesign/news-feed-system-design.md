# System Design: News Feed (Twitter/Facebook-style)

> **Daily System Design — 2026-05-17**
> Topic: Personalized News Feed | Concepts: Fan-out, Caching, Sharding, Idempotency, Failure-first

---

## 0. First Principles — Do We Even Need This?

**Problem:** Users follow hundreds of accounts. Showing them relevant, ordered posts in real-time without scanning all followed users' posts on every page load.

**Why it's hard:**
- Read-heavy: millions of users refreshing feed constantly
- Write amplification: 1 celebrity post → fan-out to millions of followers
- Ordering: recency + relevance ranking
- Freshness vs. latency trade-off

**Decision:** Yes — we need a pre-computed feed system (push model) with fallback pull for celebrity accounts.

---

## 1. Entities

| Entity | Key Attributes |
|--------|---------------|
| User | user_id, username, follower_count |
| Post | post_id, author_id, content, media_url, created_at |
| Follow | follower_id, followee_id, created_at |
| Feed | user_id, post_id, score, seen |
| Like/Comment | post_id, user_id, created_at |

---

## 2. Actions

- User publishes a post
- User follows/unfollows someone
- User loads their feed (paginated)
- User likes/comments on a post
- User scrolls (infinite scroll → pagination token)

---

## 3. Capacity Estimation (Back-of-Envelope)

- 500M DAU, avg 5 feed loads/day = **2.5B reads/day** → ~29K reads/sec
- 50M posts/day → ~580 writes/sec
- Avg 200 followers per user → fan-out: 50M × 200 = **10B feed insertions/day**
- Post size ~1KB, feed entry ~100 bytes
- Feed storage: 500M users × 200 feed items × 100B = **10TB** (hot feed in Redis)

---

## 4. Data Flow

```
[User publishes post]
        │
        ▼
   Post Service → writes to Post DB (primary)
        │
        ▼
   Message Queue (Kafka topic: new_posts)
        │
        ▼
   Fan-out Service
   ├── Fetch followers from Follow DB
   ├── For each follower → push post_id into their Feed Cache (Redis sorted set)
   └── For celebrity (>1M followers) → skip push, mark as "pull-on-read"
        │
        ▼
   [User loads feed]
        │
        ▼
   Feed Service
   ├── Reads pre-computed feed from Redis (ZREVRANGE by score/timestamp)
   ├── For followed celebrities → fetches their latest posts from Post DB (pull)
   ├── Merges + ranks
   └── Returns paginated result
```

---

## 5. High-Level Design

```
Client
  │
  ▼
API Gateway / Load Balancer
  │
  ├── Post Service        → Post DB (Cassandra, sharded by post_id)
  │                       → CDN for media
  │
  ├── Fan-out Service     ← Kafka (new_posts topic)
  │   └── writes to Feed Cache (Redis Cluster)
  │
  ├── Feed Service        → Redis (read pre-built feed)
  │                       → Post DB (hydrate post details)
  │                       → Ranking Service (ML score)
  │
  ├── Follow Service      → Follow DB (MySQL, sharded by user_id)
  │
  └── Notification Svc   ← Kafka (new_posts topic)
```

**Key Tech Choices with Trade-offs:**

| Component | Choice | Why | Trade-off |
|-----------|--------|-----|-----------|
| Post storage | Cassandra | High write throughput, partition by post_id | No joins; complex queries hard |
| Feed cache | Redis Sorted Set | O(log n) insert/read, TTL expiry | Memory-expensive; stale data risk |
| Fan-out queue | Kafka | Durable, replay, decouple producer/consumer | Added latency (~seconds) |
| Follow graph | MySQL (sharded) | ACID for follow/unfollow | Sharding adds ops complexity |
| Media | S3 + CloudFront CDN | Offload bandwidth, edge caching | CDN cache invalidation lag |

---

## 6. Low-Level Design

### 6a. Feed Cache Structure (Redis)

```
Key:   feed:{user_id}
Type:  Sorted Set
Score: timestamp (or ranking score)
Value: post_id

# Add to feed
ZADD feed:123 1716000000 post_456

# Read top 20 (newest first)  
ZREVRANGE feed:123 0 19

# Trim to last 1000 posts (memory control)
ZREMRANGEBYRANK feed:123 0 -1001
```

### 6b. Fan-out Service (Pseudo-logic)

```python
def fan_out(post_id, author_id):
    followers = follow_db.get_followers(author_id)  # paginated
    
    if len(followers) > CELEBRITY_THRESHOLD:  # e.g., 1M
        mark_as_celebrity(author_id)  # pull-on-read
        return
    
    pipe = redis.pipeline()
    for follower_id in followers:
        pipe.zadd(f"feed:{follower_id}", {post_id: timestamp})
        pipe.zremrangebyrank(f"feed:{follower_id}", 0, -1001)  # keep 1000
    pipe.execute()
```

### 6c. Post DB Schema (Cassandra)

```cql
CREATE TABLE posts (
    post_id   UUID,
    author_id UUID,
    content   TEXT,
    media_url TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (post_id)
);

-- For author's own timeline
CREATE TABLE posts_by_user (
    author_id  UUID,
    created_at TIMESTAMP,
    post_id    UUID,
    PRIMARY KEY (author_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
```

### 6d. Pagination (Cursor-based, not offset)

```
GET /feed?cursor=<encoded_timestamp>&limit=20

Response:
{
  "posts": [...],
  "next_cursor": "<encoded_timestamp_of_last_item>"
}
```

**Why cursor, not offset?**
- Offset = COUNT(*) + SKIP → expensive at scale
- Cursor = resume from exact position → O(log n) in sorted set
- Stable: new posts don't shift offsets

### 6e. Sharding Strategy

| Data | Shard Key | Why |
|------|-----------|-----|
| Posts | post_id (hash) | Even distribution |
| Feed cache | user_id (hash) | Isolates hot users |
| Follow graph | follower_id | Fan-out reads co-located |
| User data | user_id (hash) | Even distribution |

**Hotspot problem:** Celebrity user_id → millions of follow rows on one shard.
**Fix:** Shard follow table by (followee_id % N) for reads, (follower_id % N) for writes → dual-shard or denormalize.

---

## 7. Failure-First Analysis

### 7a. Fan-out Service Fails Mid-way

**Scenario:** Service crashes after updating 500K of 2M followers.

**Problem:** Inconsistent feed — some users see the post, others don't.

**Solution:**
- Kafka consumer commits offset only after batch succeeds
- Idempotent writes: `ZADD` is idempotent (same post_id → same score)
- Retry from last committed Kafka offset → safe to re-process
- Use consumer group with DLQ (Dead Letter Queue) for poison messages

### 7b. Redis Cache Fails / Eviction

**Scenario:** Redis node goes down; user's pre-built feed is lost.

**Solution:**
- Redis Cluster with replication (1 primary + 2 replicas)
- On cache miss → fall back to Pull model: query Follow DB + Post DB
- Lazily rebuild cache on miss (write-back)
- Set TTL = 7 days; inactive users don't need pre-built feeds

### 7c. Kafka Consumer Lag (Fan-out Slowdown)

**Scenario:** Spike in posts → fan-out workers can't keep up → feeds are stale.

**Impact:** User sees old feed.

**Solution:**
- Horizontal scale fan-out workers (Kafka consumer group auto-rebalances)
- Prioritize: process non-celebrity posts first (smaller fan-outs)
- Circuit breaker: if Redis write latency > threshold, batch writes or drop + rely on pull
- Monitor lag with Kafka consumer group lag metric; alert at >30s lag

### 7d. Post DB Primary Fails

**Scenario:** Primary Cassandra node crashes mid-write.

**Solution:**
- Cassandra quorum writes (W=QUORUM): majority of replicas must acknowledge
- Write is durable even if one node fails
- Hinted handoff: coordinator stores the write hint and replays when node recovers
- Read repair: on read, if replica has stale data, it's updated

### 7e. Fan-out for Celebrity Unfollows

**Scenario:** User unfollows a celebrity. Their feed still shows celebrity posts (pull-on-read includes them).

**Solution:**
- Follow Service publishes `unfollow` event to Kafka
- Feed Service checks follow status at read time (cached in Redis with 60s TTL)
- No need to retroactively remove posts — just filter at read time

### 7f. Duplicate Posts in Feed

**Scenario:** Fan-out retries insert same post_id twice into feed cache.

**Solution:**
- `ZADD` is idempotent → duplicate insert with same score is a no-op
- Post IDs are UUIDs generated at write time (server-side, not client)
- Idempotency key on Post Service: client sends `idempotency_key` header → dedupe at API gateway level

### 7g. CDN / Media Unavailable

**Scenario:** S3 outage or CDN POP failure.

**Solution:**
- Multi-region S3 with cross-region replication
- CDN fallback: origin shield → if edge fails, go to S3 directly
- Graceful degradation: show post text even if image fails (lazy load images)

---

## 8. Caching Strategy Summary

| Cache | What | TTL | Invalidation |
|-------|------|-----|-------------|
| Redis Sorted Set | Pre-built feed (post_ids) | 7 days | Append-only; trim to 1000 |
| Redis Hash | Post metadata | 1 hour | On post edit/delete |
| Redis Set | Follow list (for pull) | 5 min | On follow/unfollow event |
| CDN | Media (images/videos) | 30 days | Versioned URLs (no invalidation needed) |
| App cache (in-memory) | User profile | 60 sec | On profile update |

**Trade-off — Cache vs. Freshness:**
- Longer TTL = faster reads, staler data
- Shorter TTL = fresher data, more DB load
- **Decision:** Feed cache is append-only (new posts added in real-time via fan-out), so staleness only applies to edits/deletes (rare) → acceptable trade-off

---

## 9. Real-World References

- **Facebook:** "TAO" graph store for social graph; "Multifeed" for fan-out
- **Twitter:** Migrated from pull to push+pull hybrid (Flashbird); Redis for timelines
- **Instagram:** Uses Memcached + async fan-out workers; celebrity accounts use pull
- **LinkedIn:** "Galene" feed indexing; Kafka for event streaming

**Engineering Blogs:**
- Twitter: https://blog.twitter.com/engineering/en_us/topics/infrastructure/2022/rebuilding-timeline-at-twitter
- Instagram: https://instagram-engineering.com/making-instagram-faster-part-1-62cc0c327538
- Facebook: https://research.facebook.com/publications/tao-facebooks-distributed-data-store-for-the-social-graph/

---

## 10. Interview Q&A

### Clarification Questions (Interviewers ask YOU to ask these)

> "What's the scale? DAU, posts/day, avg followers?"
> "Read-heavy or write-heavy?"
> "Do we need real-time or is slight delay (seconds) OK?"
> "What's the feed ordering — chronological or ranked?"

---

### Common Interviewer Follow-ups

**Q: How would you handle a celebrity with 100M followers posting?**
A: Don't fan-out to all 100M — use pull-on-read. Fan-out service detects follower count > threshold (e.g., 1M), skips Redis push. Feed service fetches celebrity's latest posts separately and merges at read time.

**Q: What if user changes their feed algorithm preference (chronological vs. ranked)?**
A: Store preference in user profile. Feed Service applies ranking at read time using pre-built feed (post_ids). Ranking is a post-processing step — we don't need to rebuild the feed.

**Q: How do you handle deleted posts?**
A: Post Service soft-deletes (sets `deleted_at`). Feed Service filters out deleted post_ids when hydrating. Redis feed may still contain the post_id — filtered at hydration time. Eventually, cleanup job removes from Redis.

**Q: Pagination — what happens if new posts come in while user is scrolling?**
A: Cursor-based pagination. The cursor is a timestamp/score. New posts added to top of feed don't affect cursor position → stable pagination.

**Q: What if Kafka goes down entirely?**
A: Fan-out stops, feeds become stale. Mitigation: Kafka has replication factor ≥ 3, ZooKeeper/KRaft for leader election. If Kafka is fully unavailable, fall back to synchronous fan-out (slower) or accept staleness with pull-on-read fallback.

**Q: How do you ensure no post is shown twice in infinite scroll?**
A: Cursor-based pagination + seen set. Client tracks shown post_ids; Feed Service uses cursor to start from last position. Duplicates possible only on retry (handled by client dedup).

**Q: How would you add a "stories" feature (24hr ephemeral posts)?**
A: Separate Stories Service. Stories use TTL in Redis (86400s = 24hr). Separate Kafka topic for story events. Feed Service conditionally fetches stories based on user preference.

**Q: How would you shard the Follow table for a user with 100M followers?**
A: Shard by `followee_id % N` for "get all followers of X" queries. Use async replication to a denormalized table sharded by `follower_id` for "get all accounts user follows" queries.

**Q: How do you measure feed quality?**
A: Metrics: CTR (click-through rate), dwell time, scroll depth, explicit feedback (like/hide). A/B test ranking algorithms. Log impression events → feed quality pipeline.

---

## 11. System Design Checklist

- [x] Functional requirements scoped
- [x] Non-functional: scale, latency, availability
- [x] Entities and schema defined
- [x] Data flow: write path + read path
- [x] High-level components
- [x] Low-level: cache structure, sharding, pagination
- [x] Failure scenarios: each component + upstream/downstream
- [x] Trade-offs documented
- [x] Real-world references
- [x] Interview Q&A

---

*Next topics to cover: Google Drive, URL Shortener, Rate Limiter, Distributed Job Scheduler*
