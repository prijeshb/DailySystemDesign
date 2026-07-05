# Reddit — Interview Q&A
**Date:** 2026-06-22

---

## Opening Questions (Scoping)

**Q: Design Reddit.**
Strong open. Ask first:
- Read-heavy or write-heavy focus? (Read 95%, vote writes are the interesting part)
- Which features? (Feed, voting, comments, search — confirm scope)
- Scale? (50M DAU, 580 votes/sec)
- Any specific constraints? (Real-time vote counts? Budget limit?)

---

## Core Design Questions

**Q: How do you handle 580 votes/sec without locking the `posts` table?**

Naive approach (wrong): `UPDATE posts SET upvotes = upvotes + 1 WHERE id = $1` — row-level lock on hot posts. 100 votes/sec on same post = serialized queue.

Better: Redis write-behind buffer.
```
INCR vote_buffer:post:{id}:up
Flush worker every 30s: batched UPDATE posts SET upvotes = upvotes + delta
```
30s of votes → 1 DB write per post per flush interval, regardless of traffic.

Follow-up: "What if Redis crashes before flush?"
→ Redis AOF (`appendfsync everysec`) limits loss to 1s. Also dual-write to Kafka as recovery path.

---

**Q: How do you prevent a user from voting 1000 times on a post?**

Vote deduplication:
```
Redis: SET vote:user:{uid}:post:{id}  "+1"  NX EX {long_ttl}
NX = only set if not exists → atomic, no race condition
```
If key exists → vote rejected. Change vote: Lua script to atomically delete old and set new.

Also: rate limit vote endpoint per user_id (token bucket, 100 votes/min).

Also: server-side re-verify user account age, karma thresholds before votes count toward hot score (new accounts' votes have reduced weight for 24h).

---

**Q: How do you rank comments in a post thread?**

Three sort modes:
1. **Top** (default): by `(upvotes - downvotes)` at each depth level, maintaining tree structure
2. **New**: by `created_at DESC`
3. **Controversial**: by `(upvotes + downvotes)` with high ratio of either

Implementation: fetch all comments for a post ordered by `path` (materialized path). Client receives flat list, reconstructs tree using `parent_id`. Sort within siblings in-memory (post has at most ~5K comments — manageable client-side).

Alternative: sort in SQL using `ROW_NUMBER() OVER (PARTITION BY parent_id ORDER BY score DESC)` then sort flat list.

---

**Q: r/AskReddit has 42M subscribers. How do you handle a new post?**

Naive: fan-out write to 42M user feeds on post creation = 42M Redis writes × post frequency. Infeasible.

Hybrid fan-out:
1. Identify online subscribers (active last 30 min → Redis set, ~500K users)
2. Intersect online users with subreddit subscribers → push to their feed caches
3. Offline users: pull on demand (query subreddits they follow, merge and rank)

This is the same principle as Twitter feed. Trade-off: offline users' feeds don't update until they open the app. Acceptable.

---

**Q: How does the "hot" feed work?**

Reddit's actual algorithm:
```python
hot_score = sign * log10(max(abs(score), 1)) + epoch_seconds(created_at) / 45000
```

Time decay is baked in: every 45,000 seconds (~12.5 hours), posts need 10× more votes to maintain rank.

A post with 1 vote at midnight beats a post with 10 votes from yesterday.

Cache in Redis sorted set: `ZADD subreddit:{id}:hot {score} {post_id}`. Update on every vote flush.

---

**Q: How do you store nested comments efficiently?**

Materialized path:
```
Top comment:   path = "0001"
Reply:         path = "0001.0004"
Reply to reply: path = "0001.0004.0009"
```

`SELECT * FROM comments WHERE post_id = $1 ORDER BY path` → returns all comments in correct depth-first display order in one query.

Trade-off: fast read, write requires generating path. Moving comments is expensive (update all children). Reddit doesn't allow moving comments → trade-off accepted.

Alternative (adjacency list): simple writes, but fetching a subtree requires recursive CTE → multiple round trips or complex query. Worse for Reddit's read-heavy workload.

---

**Q: How do you handle search at scale?**

Elasticsearch with BM25 + score boost. Posts indexed asynchronously via Debezium CDC → Kafka → indexer (~1s lag).

Hot queries cached in Redis (top searches in last 5 min → pre-executed results).

Degraded fallback: if ES down → Postgres `to_tsvector` full-text search. Slower (300ms vs 50ms) but functional.

---

## Follow-up / Deep Dive Questions

**Q: You mentioned vote fuzzing — isn't that misleading users?**

It's intentional anti-manipulation, confirmed by Reddit staff. The noise is small (±2% for large posts) and consistent per session (same user sees same number on refresh). The goal: bot operators can't tell if their upvote farms are working because displayed count doesn't reflect actual writes.

Trade-off: users can't get exact counts (power users notice). Reddit decided integrity >> precision.

---

**Q: What if your vote flush worker gets behind — can a post with 10,000 cached votes in Redis show 0 on the site?**

Yes, if worker dies. Prevention:
1. Multiple worker instances (leader election via Redis lock)
2. Watchdog: if heartbeat key expires → restart worker
3. Redis INCR buffer is additive → even if worker restarts, it reads current accumulated delta, not losing votes

The "show stale" window is bounded by Redis durability (1s with AOF) + worker restart time (10-30s).

---

**Q: How would you add real-time vote count updates (like Reddit's live counter)?**

Server-Sent Events (SSE) per post:
```
Client opens SSE: GET /posts/{id}/vote_stream
Server maintains SSE connection per post_id
On each vote flush, pushes { post_id, score_delta } to all open SSE connections for that post
```

Scale: only needed for "active" posts (being viewed right now). 99% of posts have 0 concurrent viewers.

Rate limit updates to 1 push/second per post. Batch vote deltas: if 50 votes happen in 1s, push once with total delta.

---

**Q: How do you handle a coordinated spam attack — 10K accounts spamming upvotes on one post?**

Layered defense:
1. Account age threshold: votes from accounts <24h old count as 0 toward hot score
2. IP rate limiting: >50 votes/min from same /24 subnet → block
3. Karma weight: accounts with <10 karma → votes have 0.1× weight
4. Pattern detection: Kafka consumer analyzes vote patterns → if user_ids vote same post within 5s of each other → flag for review
5. Vote audit trail: every vote stored with IP, user_agent, timestamp → post-hoc reversal if manipulation detected

Reddit calls this "vote fuzzing" at the display layer and "vote manipulation detection" at the write layer. Both work together.

---

**Q: How would you implement subreddit moderation tools?**

Moderation actions: remove post, ban user, approve post, set flair.

These are write events → go to Postgres with moderation log table (append-only, like order_events):
```sql
moderation_actions (id, subreddit_id, mod_user_id, target_type, target_id, action, reason, created_at)
```

Automod (rule engine):
- Rules stored in Redis (hot path) and Postgres (source of truth)
- On post creation: evaluate rules synchronously before post is visible
- On comment creation: async (brief visible window before automod removes)

Rule examples: "remove if title contains [list of words]", "require flair in r/science", "minimum account age 30 days to post"

---

## Common Mistakes to Avoid in Interview

| Mistake | Better Approach |
|---------|----------------|
| Direct DB write per vote | Redis write-behind buffer |
| Fetch all 42M subscriber feeds on post | Hybrid push (online) / pull (offline) |
| Recursive query for comment tree each request | Materialized path, single query |
| Store exact vote counts publicly | Vote fuzzing is intentional |
| Ignore read/write ratio | Reddit is 95% read → optimize for reads |
| Elasticsearch as primary store | ES as search-only index; Postgres is source of truth |
| Ignore cache stampede on hot subreddit feeds | Mutex lock + jittered TTL |

---

## System Design Signal Checklist

- [ ] Identified vote contention as the core write problem
- [ ] Proposed Redis write-behind buffer with durability (AOF + Kafka)
- [ ] Explained hybrid fan-out for large subreddits
- [ ] Covered materialized path for comments
- [ ] Explained hot score algorithm (not just "sorted by upvotes")
- [ ] Mentioned vote fuzzing as deliberate design
- [ ] Covered failure: Redis crash → AOF or Kafka replay
- [ ] Covered failure: DB failover during peak
- [ ] Covered cache stampede prevention
- [ ] Separated search (ES) from primary data (Postgres)
