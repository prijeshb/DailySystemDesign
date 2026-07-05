# Instagram — Interview Q&A
**Date:** 2026-06-02

---

## Opening Questions (Scoping)

**Q: Design Instagram.**

Good answer structure:
1. Clarify scale: DAU, upload volume, read/write ratio
2. Clarify features: just photo feed? Stories? Reels? DMs?
3. State assumptions: global, ~500M DAU, read-heavy (10:1 read:write)
4. Start with entities → data flow → HLD → LLD

---

## Feed Design

**Q: How would you design the home feed?**

Walk through push vs pull vs hybrid. Land on hybrid.  
Key insight: "For celebrities with millions of followers, push fanout is impractical — one post would trigger millions of writes. So we skip fanout for them and merge their posts at read time."

**Q: What's the trade-off between push and pull fanout?**

| | Push | Pull |
|---|---|---|
| Read latency | O(1) — pre-computed | O(N) — N followees queried |
| Write cost | O(followers) per post | O(1) per post |
| Celebrity problem | Catastrophic | Fine |

**Q: How do you determine if someone is a "celebrity" for fanout purposes?**

- Threshold on follower_count (e.g., > 1M)
- Flag in User table updated by background job
- Edge case: account that just went viral — short delay acceptable

**Q: What happens if a user follows 5000 accounts at read time? Isn't pull still slow?**

- Batch read from DB with `IN (user_id_1, ... user_id_5000)` — one query, not 5000
- Or: pre-shard followers by user_id → parallel reads
- In practice, Instagram limits follows to 7500

---

## Storage

**Q: Why not store images in the database?**

DB not designed for large binary objects. No CDN integration. Can't compress/resize efficiently. S3 offers 99.999999999% durability, CDN integration, lifecycle policies (move old media to Glacier).

**Q: How do you shard the posts table?**

Shard by `user_id`. Keeps all posts from one user co-located — profile grid query hits one shard.  
Follow-up: hot shard for celebrities → read replicas + cache celebrity posts in Redis.

**Q: What database would you use for comments?**

Cassandra. Partition key = `post_id`, clustering key = `created_at DESC`.  
One query returns all comments for a post, ordered by time. Wide-column model fits this access pattern perfectly.

---

## Media Pipeline

**Q: Walk me through what happens when a user uploads a photo.**

1. Client requests pre-signed S3 URL from Upload Service
2. Client uploads directly to S3 (bypasses server — reduces bandwidth cost)
3. S3 event → SQS → Transcoding Service
4. Transcode: 1080px, 720px, 320px + thumbnail + extract placeholder color
5. Write Post record to DB with CDN URLs (`is_processed = true`)
6. Publish `post_created` event → Feed Fanout Service

**Q: What if transcoding fails?**

- SQS visibility timeout: message reappears → another worker picks it up
- Show original unprocessed image while transcoding in progress
- After 3 retries → DLQ → alert on-call
- Idempotent: worker checks if output S3 key already exists before reprocessing

---

## Caching

**Q: What do you cache and where?**

| What | Where | TTL | Why |
|------|-------|-----|-----|
| Feed (list of post_ids) | Redis sorted set | 7 days | O(1) read |
| Post metadata | Redis hash | 1 hour | Reduce DB reads |
| Like counts | Redis counter | Sync to DB async | High write rate |
| Celebrity posts | Redis | 5 min | Avoid DB fan-in on every read |
| Media (images/video) | CloudFront CDN | 1 year (immutable URL) | Global low latency |

**Q: How do you handle cache invalidation for media?**

Use content-addressed URLs: `cdn.example.com/{hash_of_content}/photo.jpg`  
Content never changes → cache forever. If user updates profile pic → new hash → new URL → old URL naturally expires from CDN.

---

## Failure Scenarios

**Q: What happens if Redis goes down?**

1. Circuit breaker opens → stop sending feed reads to Redis
2. Fallback: pull-from-DB mode (fan-in query for each user's followees)
3. Rate limit fallback reads to protect DB
4. Redis recovers → lazy rebuild feeds on next request (don't rebuild all at once — thundering herd)

**Q: What happens if S3 goes down?**

- CDN has cached copies — most reads unaffected (CDN cache hit rate ~99%+)  
- New uploads buffer in queue; retry when S3 recovers  
- CDN serves stale images until S3 returns  
- Multi-region S3 replication (S3 Cross-Region Replication) for DR

**Q: How do you handle a post going viral and causing a CDN cache miss storm?**

- CloudFront Origin Shield: all CDN POPs fetch from one regional cache, not all hitting S3 directly
- S3 key prefix sharding: `{random_prefix}/{post_id}/img.jpg` — distributes request load across S3 partitions
- Pre-warm CDN on posts from accounts with large followings (>1M followers)

---

## Scale & Deep-Dives

**Q: How would you scale the like system to handle millions of likes per second?**

- Write likes to Redis counter only: `INCR likes:{post_id}`
- User-liked check: Redis Set `SADD liked:{user_id} {post_id}` (or Bloom filter for space efficiency)
- Async batch: every 30s, flush Redis counters to DB
- Consistency: exact count less important than throughput — "±50 likes" acceptable

**Q: How do stories work differently from posts?**

- 24h TTL: Redis sorted set with score = expiry_timestamp; background job prunes expired
- View counting: HyperLogLog per story (`PFADD views:{story_id} {user_id}`) — O(1) space, ~1% error
- On expiry: don't delete from S3 (legal/ML reasons), just stop serving; move to cold storage
- Story order: sorted by (has_unseen_story, last_active_time, mutual_interaction_score)

**Q: How would you add an "Explore" page?**

- Separate recommendation service (ML-based)
- Offline: train collaborative filtering model (users who liked X also liked Y)
- Online: real-time features (trending hashtags, location-based posts, engagement velocity)
- Serve via dedicated Explore cache, refreshed every 15 min per user segment
- Not personalized per user in real-time — segment-level personalization scales better

**Q: How do you handle GDPR / user deletion?**

- Soft delete: mark user as `deleted`, stop serving content immediately
- Async hard delete: queue job to delete S3 objects, DB records, CDN purge
- Feed entries: lazy purge (filter out deleted user's posts at read time; async cleanup)
- Right to erasure: 30-day SLA for full deletion — log all storage locations in audit table

---

## Common Follow-ups

**Q: Why Cassandra for comments over MySQL?**

Access pattern: always read by post_id, ordered by time. Cassandra's partition key = post_id → all comments for a post on one node. MySQL would need secondary index + JOIN. Cassandra also handles write-heavy workload at scale without locking.

**Q: How do you prevent duplicate likes?**

Unique constraint on `(user_id, post_id)` in DB. Redis: `SADD liked:{user_id} {post_id}` returns 0 if already exists — idempotent. API returns 200 on duplicate (not error).

**Q: What's your monitoring strategy?**

Key metrics:
- Upload latency P99 (target < 2s)
- Feed read latency P99 (target < 200ms)
- Fanout queue depth (alert > 100K)
- Redis memory usage (alert > 80%)
- CDN cache hit rate (alert < 95%)
- Transcoding DLQ depth (alert > 0)

