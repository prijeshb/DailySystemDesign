# Twitter / X Feed System Design
**Date:** 2026-05-14 | **Difficulty:** Hard | **Category:** Social Graph + Feed Generation + Fan-Out

---

## 1. Problem Framing & Scope

Design a Twitter-like social feed platform where users post short messages (tweets), follow other users, and see a personalized feed of tweets from people they follow — at massive global scale.

### First-Principles Check: Do We Even Need This?

Before diving in, ask: *What problem does a feed solve?*

> A user follows 300 accounts. Without a pre-computed feed, "show me the latest posts from my follows" = 300 DB queries merged and sorted. At 300M DAU, that's 90 billion real-time fan-in operations per feed open. That's clearly untenable — so yes, we need a pre-computed or partially cached feed system.

The core architectural tension: **push (fan-out on write) vs. pull (fan-out on read)**. This single decision cascades into every storage, caching, and infrastructure choice.

### Functional Requirements
- Users can post tweets (text up to 280 chars, optional media)
- Users can follow/unfollow other users
- Home timeline: user sees latest tweets from people they follow (reverse-chronological, with optional ranking)
- Tweets can have replies, likes, retweets, quote-tweets
- Search tweets and users
- Trending topics / hashtags
- Notifications (likes, follows, mentions, replies)

### Non-Functional Requirements
- **Scale:** 400M MAU, 200M DAU, ~6,000 tweets/second peak
- **Read/write ratio:** ~100:1 — reads dominate massively (feed scrolls >> posts)
- **Timeline latency:** home feed load < 200ms at p99
- **Write latency:** tweet post acknowledged < 100ms
- **Availability:** 99.99% (social media tolerates brief degradation better than fintech, but users notice)
- **Eventual consistency:** a tweet may take a few seconds to appear in all follower feeds — acceptable
- **Fanout celebrities:** some accounts have 50M+ followers — needs special handling

### Out of Scope
- Twitter Spaces (audio rooms)
- Ad insertion internals
- Full recommendation ranking model (ML)
- DM/direct messaging (covered in Chat System design)
- Verification / trust & safety

---

## 2. Entities

```
User
├── user_id        (UUID, immutable)
├── handle         (@username, unique, indexed)
├── display_name
├── bio, avatar_url, banner_url
├── follower_count   (denormalized counter, eventually consistent)
├── following_count
├── is_celebrity     (boolean flag: follower_count > 1M — changes fan-out strategy)
├── created_at
└── verified_status

Tweet
├── tweet_id       (Snowflake ID — encodes timestamp + shard, sortable)
├── author_id      (user_id)
├── body           (text, max 280 chars — stored as UTF-8)
├── media_keys[]   (array of media_id refs)
├── reply_to_tweet_id   (null if original tweet)
├── quote_tweet_id      (null if not a quote tweet)
├── retweet_of_id       (null if not a retweet)
├── like_count          (approximate counter)
├── retweet_count
├── reply_count
├── view_count
├── hashtags[]     (extracted at write time, indexed separately)
├── mentions[]     (user_ids mentioned)
├── created_at
└── visibility: PUBLIC | FOLLOWERS_ONLY | DELETED

Follow
├── follower_id    (user_id)
├── followee_id    (user_id)
├── created_at
└── INDEX: (follower_id), (followee_id)   ← both directions needed

Media
├── media_id
├── uploader_id
├── tweet_id
├── type: IMAGE | GIF | VIDEO
├── object_key     (CDN/object store path)
├── width, height
└── duration_seconds (video only)

FeedEntry  (pre-computed, stored in Redis)
├── user_id        (whose feed this belongs to)
├── tweet_id       (injected tweet)
├── score          (timestamp or ranking score)
└── source: FOLLOW | TOPIC | ADS (for ranking)

Hashtag
├── hashtag_id
├── tag (e.g. "systemdesign", case-normalized)
├── tweet_count    (approximate, for trending)
└── hourly_buckets (sliding window counter for trending calculation)

Notification
├── notification_id
├── recipient_id
├── type: LIKE | FOLLOW | MENTION | REPLY | RETWEET
├── actor_id       (who caused this notification)
├── target_tweet_id (nullable)
├── read: bool
└── created_at
```

**Key design choice:** `tweet_id` uses Snowflake format (Twitter's own invention):
```
[41 bits: millisecond timestamp] [10 bits: machine ID] [12 bits: sequence]
= 63-bit integer, ~4096 tweets/ms per machine, globally sortable by time
```
This is critical — it enables feed entries to be sorted by tweet_id alone without a separate `created_at` sort.

---

## 3. Actions & API Design

### Core Write APIs
```
POST /tweets
  body: { text, media_ids[], reply_to, quote_of }
  → tweet_id
  → triggers: fan-out job, notification job, hashtag indexing

DELETE /tweets/{tweet_id}
  → soft delete (visibility = DELETED), cascade fan-out removal

POST /tweets/{tweet_id}/likes        → like a tweet
DELETE /tweets/{tweet_id}/likes      → unlike

POST /tweets/{tweet_id}/retweets     → retweet
DELETE /tweets/{tweet_id}/retweets   → undo retweet

POST /follows
  body: { followee_id }
  → triggers: fan-out backfill job (inject followee's recent tweets into follower's feed)

DELETE /follows/{followee_id}
  → remove followee's tweets from follower's cached feed
```

### Core Read APIs
```
GET /timelines/home
  query: { cursor, limit=20 }
  → paginated list of tweet objects
  → served from pre-computed Redis feed cache

GET /timelines/user/{user_id}
  → user's own tweets (simpler: just query Tweet table by author_id)
  → served from DB/cache, no fan-out needed

GET /tweets/{tweet_id}
  → single tweet with engagement counts

GET /tweets/{tweet_id}/replies
  → threaded replies (paginated)

GET /search
  query: { q, filter: LATEST|TOP|MEDIA, since_id, until_id }
  → Elasticsearch-backed full-text search

GET /trends
  query: { woeid }  (where-on-earth-id for geo trends)
  → top 10 trending hashtags from sliding-window counters
```

---

## 4. Data Flow

### 4a. Tweet Creation (Write Path)

```
User
 │
 ▼ POST /tweets
API Gateway (auth + rate limit)
 │
 ▼
Tweet Service
 ├── Validate: length, media ownership, rate limit (no more than 300 tweets/day)
 ├── Write tweet to TweetDB (PostgreSQL / Cassandra)  ← source of truth
 ├── Enqueue fan-out event: Kafka topic "tweet.created"
 └── Return tweet_id to user (immediate ack, fan-out is async)

Kafka: "tweet.created"
 │
 ├──▶ Fan-Out Service
 │     ├── Fetch author's follower list from Social Graph DB
 │     ├── IF author.is_celebrity:
 │     │     Push tweet_id to small "celebrity queue" only — NOT to all feeds
 │     │     (fan-out on READ for celebrities — explained below)
 │     └── ELSE (normal user, <1M followers):
 │           For each follower_id: LPUSH feed:{follower_id} tweet_id
 │           Trim each feed list to last 800 entries (LTRIM)
 │
 ├──▶ Notification Service
 │     ├── Find mentioned users → create MENTION notifications
 │     ├── If reply: create REPLY notification for original author
 │     └── Push to notification delivery pipeline (APNs, FCM, email)
 │
 ├──▶ Search Indexer
 │     └── Index tweet body + hashtags in Elasticsearch
 │
 └──▶ Trending Service
       └── Increment hashtag counters in Redis sorted set (sliding window)
```

### 4b. Feed Read Path

```
User
 │
 ▼ GET /timelines/home?cursor=...
API Gateway
 │
 ▼
Timeline Service
 ├── LRANGE feed:{user_id} 0 19   ← Redis: get top 20 tweet_ids from pre-built feed
 │
 ├── Celebrity Hydration (fan-out on read for celebs):
 │     For each celebrity user A follows:
 │       ZREVRANGEBYSCORE tweets:{celebrity_a} NOW-48h → latest N tweet_ids
 │     Merge + re-sort celebrity tweets with the regular feed by tweet_id
 │
 ├── Fetch tweet objects:
 │     Multi-get tweet IDs from Redis tweet cache (tweet:{tweet_id})
 │     Cache miss → fetch from TweetDB → cache with TTL 1hr
 │
 ├── Fetch user hydration (display names, avatars):
 │     Multi-get from Redis user cache
 │     Cache miss → UserDB
 │
 └── Return ordered list of hydrated tweet objects

Cache miss cascade:
  Redis miss → Postgres (or Cassandra) → cache the result
  Redis miss rate target: < 5% for active feeds
```

### 4c. Follow / Unfollow

```
POST /follows/{followee_id}
 │
 ├── Write to Follow table: (follower_id, followee_id)
 ├── Increment followee's follower_count (async, eventually consistent)
 └── Enqueue "follow.created" event to Kafka
      │
      └──▶ Fan-Out Backfill Job
            Fetch followee's recent tweets (last 24h, up to 50)
            Inject into follower's feed Redis list
            (if followee is celebrity: no injection, handled at read time)
```

---

## 5. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              CLIENTS                                      │
│             iOS / Android / Web / Third-party API                        │
└──────────────────┬───────────────────────────────────────────────────────┘
                   │ HTTPS
                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          API GATEWAY LAYER                                │
│   • Auth (JWT / OAuth)   • Rate limiting   • TLS termination             │
│   • Request routing      • DDoS protection                               │
└────┬──────────────┬───────────────┬──────────────────┬───────────────────┘
     │              │               │                  │
     ▼              ▼               ▼                  ▼
 Tweet         Timeline          User              Search
 Service       Service           Service           Service
     │              │               │                  │
     │         ┌────┘               │                  │
     ▼         ▼                    ▼                  ▼
 ┌──────────────────────────────────────────────────────────┐
 │                     DATA STORES                           │
 │  TweetDB       │  UserDB         │  Social Graph DB       │
 │  (Cassandra)   │  (PostgreSQL)   │  (PostgreSQL / Neo4j)  │
 │                │                 │                        │
 │  Tweet Cache   │  User Cache     │  Feed Cache            │
 │  (Redis)       │  (Redis)        │  (Redis Cluster)       │
 │                                                           │
 │  Media: S3 + CloudFront CDN                              │
 │  Search: Elasticsearch Cluster                           │
 │  Analytics: Kafka → Flink → ClickHouse                   │
 └──────────────────────────────────────────────────────────┘
     │              │
     ▼              ▼
 ┌──────────────────────────────────────────────────────────┐
 │                  ASYNC PIPELINE (Kafka)                    │
 │   Fan-Out Service  │  Notification Service  │  Indexer    │
 │   Trending Service │  Analytics Consumers   │             │
 └──────────────────────────────────────────────────────────┘
```

---

## 6. Deep Dives

### 6a. The Fan-Out Problem & Celebrity Solution

**The problem:**
- Katy Perry has 50M followers. She tweets. Fan-out service must write tweet_id to 50M Redis lists.
- At 10K Redis writes/second, that takes **83 minutes**. Followers would see the tweet an hour later. Unacceptable.
- At the same time, normal users with 500 followers: fan-out takes milliseconds.

**Two strategies:**

| Strategy | Fan-Out on Write (Push) | Fan-Out on Read (Pull) |
|----------|------------------------|----------------------|
| How | At tweet time, push tweet_id to all follower feed caches | At read time, query each followed account's recent tweets and merge |
| Read speed | O(1) — just read pre-built feed | O(following_count) — merge N sorted lists |
| Write cost | O(follower_count) — can be huge | O(1) — just write the tweet |
| Best for | Normal users (<1M followers) | Celebrities (50M+ followers) |
| Stale risk | Low (pushed immediately) | Low (query is real-time) |

**Twitter's actual hybrid approach:**
1. **Normal users (<1M followers):** Fan-out on write. Async push to Redis feeds.
2. **Celebrities (>1M followers, flagged as `is_celebrity=true`):** Fan-out on read. At timeline read time, for each celebrity the user follows, query that celebrity's recent tweet_ids from a small per-celebrity Redis sorted set. Merge into feed at read time.
3. **Inactive users:** Don't maintain a pre-built feed cache at all. When they log in, compute their feed on-demand (lazy computation), then cache it.

**Why this hybrid is correct:**
- Celebrities have many followers but users typically follow only 1-5 celebrities. So the "fan-out on read" cost at read time is small (merge 5 celebrity sorted sets ≠ 300 normal-user feed lists).
- Normal users have few followers but many followed accounts. Fan-out on write is efficient for them (write once, read many times).

```
Feed assembly algorithm at read time:
  regular_tweets  = LRANGE feed:{user_id} 0 200   // pre-built, fast
  celebrity_tweets = []
  for celebrity in user.followed_celebrities:
    celebrity_tweets += ZREVRANGEBYSCORE celebrity_feed:{celebrity_id} now now-48h LIMIT 50

  all_tweets = merge_sort(regular_tweets, celebrity_tweets) by tweet_id (timestamp-encoded)
  return first 20
```

### 6b. Storage Choices

**Why Cassandra for Tweet storage?**
- Tweets are append-only (rarely updated), write-heavy (6K/s peak), and partitioned cleanly by tweet_id.
- Wide-column model: `partition key = tweet_id`, allows shard-local reads.
- Cassandra's LSM-tree is optimized for write throughput — no page locking.
- Trade-off: no ACID joins, eventual consistency. Acceptable for tweets (you don't need transactional integrity across tweets the way you do for payments).

**Why Redis for feed cache?**
- Feed is a sorted list of tweet_ids (integers). Redis `LPUSH/LTRIM/LRANGE` is O(1) per push, O(N) per read — exactly the right primitives.
- In-memory → sub-millisecond. A 200-entry feed list fits in ~2KB. 
- Feed is ephemeral: can always be rebuilt from source of truth if evicted.
- Trade-off: expensive per GB vs. disk stores. Mitigated by storing only tweet_ids (not full tweet data) in the feed list.

**Feed size limit:** Store only last 800 tweet_ids per user. If a user hasn't opened their feed in a week, we stop pre-computing it (cold user optimization).

**Why PostgreSQL for Social Graph (Follow table)?**
- Follow relationships need strong consistency: if you unfollow someone, their tweets must stop appearing immediately.
- Follow counts need atomic increments.
- Graph is queried in both directions (followers of X, following of Y) — relational joins are fine at this size.
- Alternative for very large graphs: Neo4j or purpose-built graph DB. Twitter originally used MySQL here.

### 6c. Sharding Strategy

**Tweet Table (Cassandra):**
- Partition by `tweet_id` (Snowflake) — natural distribution, no hot spots.
- Time-based Snowflake means recent tweets go to the "latest" shard, which could create a write hotspot. Solution: use the machine_id bits to spread writes across machines even within the same millisecond.

**Social Graph (Follow table):**
- Shard by `follower_id` for "get followers of user X" queries.
- Replicate a "reverse index" sharded by `followee_id` for "get following list" queries.
- Two separate tables with opposite shard keys → double storage, but both queries are single-shard.

**Feed Cache (Redis):**
- Consistent hashing across Redis Cluster nodes.
- `feed:{user_id}` — the user_id naturally distributes across nodes.
- Hot users (celebrities) have their feed on one shard — but they only have ONE feed (their own outgoing tweet list is tiny, their incoming feed is not pre-computed for them due to high follower count).

**Trending Counters:**
- Per-hashtag Redis sorted sets. Potential hotspot: a viral hashtag gets 100K increments/sec.
- Solution: probabilistic counting (HyperLogLog) or counter sharding (20 Redis keys per hashtag, randomly pick one to increment, sum all 20 at read time).

### 6d. Idempotency

**Tweet creation:**
- Client sends an `idempotency_key` (UUID generated client-side) with POST /tweets.
- Tweet Service checks a short-lived idempotency store (Redis, TTL 24h): `idempotency:{key}` → tweet_id.
- If key exists: return existing tweet_id (do not create duplicate).
- If not: create tweet, store mapping, return tweet_id.
- This handles the case where client retries due to network timeout but the tweet was actually created.

**Fan-out idempotency:**
- Fan-out jobs are consumer-group consumers from Kafka. Kafka guarantees at-least-once delivery.
- Inserting the same tweet_id into a Redis list twice is not idempotent by default — it creates duplicates.
- Solution: use Redis Set for feeds instead of List? No — sets don't preserve order.
- Better: before LPUSH, check if tweet_id already exists in the list (LPOS command). If yes, skip.
- Or: accept rare duplicates and deduplicate at read time (check tweet_id seen set in the response).

**Like / Unlike:**
- `POST /tweets/{id}/likes` is idempotent: implemented as upsert into Likes table. Double-like does nothing.
- Like count increment: use Redis INCR with a Lua script that checks if the user already liked before incrementing.

### 6e. Caching Strategy

```
Layer 1 — CDN (CloudFront):
  - Static assets: profile images, media thumbnails
  - Cache-Control: max-age=86400 (images), max-age=3600 (thumbnails)
  - Invalidated via versioned URLs (avatar?v=xyz) when user updates profile picture

Layer 2 — Redis (Application Cache):
  - tweet:{tweet_id}     TTL: 1 hour (tweet content rarely changes)
  - user:{user_id}       TTL: 15 min (profile can change)
  - feed:{user_id}       TTL: 24 hours (actively maintained by fan-out service)
  - trending:{woeid}     TTL: 60 seconds (recomputed frequently)

Layer 3 — DB Read Replicas:
  - User profile queries that miss Redis go to Postgres read replica
  - Tweet queries that miss Redis go to Cassandra (Cassandra distributes reads across replicas)
```

**Cache invalidation:**
- Tweet deleted: `DEL tweet:{tweet_id}` + async fan-out removal from feeds.
- Profile updated: `DEL user:{user_id}` → next read rebuilds from DB.
- Engagement counts (likes/retweets): use Redis INCR directly (counter is the cache, not a DB-backed value). Periodically flush counters to DB (every 60 seconds via cron job). Accept that counts in DB may lag by 60s — this is fine (Twitter doesn't guarantee real-time counts).

**Trade-off:** Adding caching reduces read latency dramatically (sub-ms vs. 10ms DB query) but introduces stale data windows. For tweets, this is acceptable — a tweet body doesn't change. For engagement counts, 60-second staleness is fine. For "is this account suspended" — NOT fine. Suspension checks must always hit the auth service / source of truth.

### 6f. Trending Topics Algorithm

**Goal:** Identify hashtags trending in the last hour, not all-time popular tags.

**Sliding window approach:**
```
Every tweet: for each hashtag in tweet:
  ZINCRBY trending:{woeid}:{minute_bucket} 1 hashtag
  EXPIRE trending:{woeid}:{minute_bucket} 7200   // keep 2 hours of buckets

Every minute (cron): Trending Service computes:
  score = Σ count(hashtag) across last 60 minute buckets
  Velocity bonus: weight recent buckets more (bucket at T-1 counts 2x vs T-60)
  Output: ZADD trending_results:{woeid} score hashtag  // top N results

GET /trends: read from trending_results:{woeid}  (TTL 60s, refreshed by cron)
```

**Trade-off vs. simple all-time counter:** Sliding window is more complex but catches viral moments. A hashtag with 1M total tweets that had its peak 2 years ago should not trend over one with 10K tweets in the last 5 minutes.

---

## 7. Low-Level Design: Fan-Out Service

```python
class FanOutService:
    FEED_MAX_SIZE = 800
    CELEBRITY_THRESHOLD = 1_000_000

    def handle_tweet_created(self, event: TweetCreatedEvent):
        tweet_id = event.tweet_id
        author_id = event.author_id
        
        author = user_service.get(author_id)
        
        # Store tweet in per-author sorted set (for celebrity fan-out-on-read)
        redis.zadd(f"author_tweets:{author_id}", {tweet_id: tweet_id_to_timestamp(tweet_id)})
        redis.zremrangebyrank(f"author_tweets:{author_id}", 0, -501)  # keep last 500
        
        if author.is_celebrity:
            # Don't fan-out to followers — they'll query at read time
            # Just update the author's tweet set (done above)
            return
        
        # Normal user: push to all follower feeds
        follower_ids = social_graph.get_followers(author_id)  # batched, paginated
        
        pipeline = redis.pipeline()
        for follower_id in follower_ids:
            if user_service.is_active(follower_id):  # skip inactive users
                pipeline.lpush(f"feed:{follower_id}", tweet_id)
                pipeline.ltrim(f"feed:{follower_id}", 0, self.FEED_MAX_SIZE - 1)
        pipeline.execute()
    
    def handle_tweet_deleted(self, event: TweetDeletedEvent):
        # Remove from all feeds — this is hard for high-follower users
        # Solution: don't remove from Redis (expensive); instead mark deleted in DB
        # Timeline Service filters out DELETED tweet_ids when hydrating
        tweet_service.mark_deleted(event.tweet_id)
        redis.delete(f"tweet:{event.tweet_id}")
        # Feed cleanup happens lazily as feeds are read
```

---

## 8. Scale Estimates

| Metric | Value |
|--------|-------|
| DAU | 200M |
| Tweets/day | 600M (assuming 3 tweets/active user avg) |
| Tweets/second (peak) | ~14,000 (3x average) |
| Feed reads/second | 1.4M (200M * 50 feeds/day / 86400 * peak factor) |
| Fan-out writes/second (normal users) | 14K tweets/s * avg 200 followers = 2.8M Redis writes/s |
| Storage: Tweet table | 600M tweets/day * 365 * 5 years * 500 bytes ≈ 550 TB |
| Storage: Media | 600M tweets/day * 15% with media * 500KB avg = 45 TB/day |
| Redis feed storage | 200M users * 800 tweet_ids * 8 bytes = 1.28 TB (fits in Redis cluster) |

---

## 9. Trade-Off Summary

| Decision | Choice | Trade-Off |
|----------|--------|-----------|
| Fan-out strategy | Hybrid (push for normal, pull for celebs) | Complexity vs. consistency and cost |
| Tweet storage | Cassandra | Write throughput + partition tolerance vs. no ACID joins |
| Social graph | PostgreSQL | Strong consistency vs. vertical scale limits |
| Feed storage | Redis Lists (tweet_ids only) | Sub-ms reads vs. cost, ephemeral (need rebuild on eviction) |
| Engagement counts | Redis counters (async flush) | High write throughput vs. 60s staleness |
| Trending | Sliding-window minute buckets | Captures viral moments vs. more complex than simple counter |
| Tweet IDs | Snowflake | Time-sortable, no central sequencer vs. clock skew risk |
| Idempotency | Client-side key + Redis dedupe | Handles retries vs. 24h storage cost for keys |
