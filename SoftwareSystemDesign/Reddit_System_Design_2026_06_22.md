# Reddit — System Design
**Date:** 2026-06-22

---

## First Principles: Do We Even Need This?

**Problem being solved:**
People want to discuss topics organized by community, vote content up/down, and see a ranked feed of what's most relevant — not just newest.

**Why is this hard?**
1. Voting at scale: 50M votes/day on shared counters = high write contention
2. Feed ranking: must compute a time-decayed score across unlimited posts per subreddit
3. Nested comments: trees with 1000s of nodes, rendered in sorted order
4. Fan-out: r/AskReddit has 42M subscribers — you can't push to all on every post
5. Vote integrity: easy to manipulate without countermeasures

**Simplest version that works:**
Postgres + a single `hot_score` column recalculated on every vote. Works up to ~10K DAU. Everything beyond is a scaling decision.

---

## Entities & Actions

### Entities
```
User         id, username, karma, created_at
Subreddit    id, name, description, subscriber_count
Post         id, author_id, subreddit_id, title, body/media_url, type(text/link/image/video)
             upvotes, downvotes, hot_score, created_at
Comment      id, post_id, parent_comment_id, author_id, body, upvotes, downvotes, path
Vote         id, user_id, target_id, target_type(post/comment), direction(+1/-1), created_at
Subscription user_id, subreddit_id, created_at
```

### Actions
```
Write (low freq): create post, create comment, subscribe/unsubscribe, create subreddit
Write (high freq): vote on post, vote on comment
Read (high freq):  load feed (frontpage, subreddit), load post+comments, search
```

---

## Data Flow

```
Vote:
  Client → API → Vote Service → Redis (INCR buffer) → async flush → Postgres
                                                    → Kafka (vote event)
                                                    → Score Updater → Redis sorted set

Post creation:
  Client → API → Post Service → Postgres (post row)
                              → Kafka (new_post event)
                              → Notification Service (fan-out to online subscribers)
                              → Search Indexer → Elasticsearch

Feed request:
  Client → Feed Service → Redis sorted set (subreddit:{id}:hot) → post IDs
                        → Cache miss → Postgres (score query) → populate cache
                        → Fetch post details → Redis or Postgres
```

---

## Scale / Back-of-Envelope

| Metric | Estimate |
|--------|----------|
| MAU | 1.6B |
| DAU | 50M |
| Posts/day | 1M → ~12/sec |
| Comments/day | 10M → ~116/sec |
| Votes/day | 50M → ~580/sec |
| Feed loads/day | 250M → ~2,900/sec |
| Peak multiplier | 5× → ~15K reads/sec, 3K vote writes/sec |
| Media uploads/day | 200K posts with media × avg 3MB = 600GB/day |
| Post content storage | 1M × 1KB = ~365GB/year text |
| Media storage | 600GB/day × 365 = ~219TB/year |
| Search index | ~500GB for post titles + bodies |

### AWS Cost Estimate

```
S3 (media + cold storage):
  219TB/year = ~18TB/month new uploads
  Total stored: ~500TB × $0.023/GB = ~$11.5K/month

CDN (CloudFront) — dominant cost:
  50M DAU × 4 media views × 150KB avg = 30TB/day = 900TB/month
  At $0.008/GB blended = ~$7.2M/month
  (Reality: Reddit peers directly with ISPs, ~40% discount → ~$4.3M/month effective)

EC2 (API servers, workers, recommendation):
  ~250K/month

RDS Postgres (primary + 3 replicas):
  db.r6g.8xlarge × 4 = ~$60K/month

ElastiCache Redis (feed cache, vote buffers):
  ~$40K/month

Elasticsearch (search):
  ~$80K/month

Kafka (MSK):
  ~$20K/month

Total rough: ~$7.7M/month
```

**Reality check:** Reddit was acquired by Condé Nast, then spun out. They migrated to AWS and use aggressive CDN caching + Fastly. Their actual spend is lower via enterprise agreements.

---

## Budget Constraint Example

**Constraint: $200K/month (niche community platform with 500K DAU)**

| Cut | Impact |
|-----|--------|
| Single CDN region (US only) | 300-800ms latency for EU/Asia users |
| No video hosting — link to YouTube | No native video; worse UX |
| Vote flush interval: 5 min (not 30s) | Vote counts stale for 5 min; may confuse users |
| No Elasticsearch — use Postgres full-text search | Search slower, no fuzzy/relevance ranking |
| Replicas: 1 read replica only | Read capacity limited; feed queries slower at peak |
| No recommendation engine | Feed = raw chronological per subscribed subreddits |
| Cold storage for subreddits inactive >30 days | Obscure subreddits load in 2-3s instead of 200ms |

**Hard limit hit at ~2M DAU:** Vote write contention overwhelms Postgres even with batching. At that point, Cassandra for vote storage becomes necessary — can't avoid it.

---

## High-Level Design

```
                    ┌─────────────────────────────────────────────────────┐
                    │                   API Gateway                        │
                    └─────────────┬────────────────────┬──────────────────┘
                                  │                    │
              ┌───────────────────┼──────────────────┐ │
              ▼                   ▼                  ▼ ▼
        ┌──────────┐      ┌──────────────┐    ┌──────────────┐
        │   Feed   │      │  Post/Comment│    │ Vote Service │
        │  Service │      │   Service    │    │              │
        └────┬─────┘      └──────┬───────┘    └──────┬───────┘
             │                   │                   │
    ┌────────▼───────────────────▼──────────────────▼────────┐
    │                       Redis                             │
    │  sorted sets (hot feeds)   vote buffers   post cache    │
    └────────────────────────────────────────────────────────┘
             │                   │                   │
    ┌────────▼───────┐  ┌────────▼───────┐  ┌───────▼───────┐
    │   Postgres     │  │ Elasticsearch  │  │     Kafka     │
    │ posts comments │  │    search      │  │ vote/post     │
    │ votes users    │  │    index       │  │ events        │
    └────────────────┘  └────────────────┘  └───────┬───────┘
                                                    │
                                    ┌───────────────┼──────────────────┐
                                    ▼               ▼                  ▼
                             Score Updater   Notification Svc   Search Indexer
```

---

## Low-Level Design

### 1. Vote System (Core Complexity)

**Problem:** 580 writes/sec avg, 3K/sec peak, all on a small set of hot post_ids. Direct Postgres `UPDATE` = lock contention.

**Solution: Redis write-behind buffer**
```
Client votes up on post 42:
  HSETNX vote:user:{uid}:post:{post_id}  → prevents double-vote (atomic)
  INCR   vote_buffer:post:{post_id}:up

Vote Flush Worker (every 30s):
  For each dirty post_id:
    delta_up   = GETDEL vote_buffer:post:{id}:up
    delta_down = GETDEL vote_buffer:post:{id}:down
    UPDATE posts SET upvotes = upvotes + delta_up,
                     downvotes = downvotes + delta_down,
                     hot_score = calculate_hot(upvotes+delta_up, downvotes+delta_down, created_at)
    WHERE id = {id}
```

**Vote deduplication:**
```
Key: vote:user:{uid}:post:{id}  → value: +1 or -1
SET vote:user:{uid}:post:{id}  NX EX 86400×365
→ NX: only set if not exists
→ Changing vote: DEL old key, HSET new key (two-step, idempotency via Lua script)
```

**Vote fuzzing (anti-manipulation):**
Reddit intentionally adds ±noise to displayed vote counts. User sees "1,284 upvotes" but actual might be 1,267. This prevents bots from verifying if their vote rings are working.
```python
def display_score(actual_score):
    if actual_score < 100:
        return actual_score  # small posts: exact
    noise = random.randint(-int(actual_score * 0.02), int(actual_score * 0.02))
    return actual_score + noise
```
Noise is deterministic per-session so users don't see different numbers on refresh.

### 2. Hot Score Algorithm

Reddit's actual formula (Gawker algorithm):
```python
import math
from datetime import datetime

EPOCH = datetime(2005, 12, 8, 7, 46, 43)  # Reddit epoch

def hot_score(ups, downs, created_at):
    score = ups - downs
    order = math.log10(max(abs(score), 1))
    sign = 1 if score > 0 else (-1 if score < 0 else 0)
    age_seconds = (created_at - EPOCH).total_seconds()
    return sign * order + age_seconds / 45000
```

**Key insight:** Every 45,000 seconds (~12.5 hours), one unit of age equals one order of magnitude of votes. A post with 1 upvote after 12.5 hours equals a 10-upvote post at creation. Old posts sink naturally.

**Feed cache (Redis sorted set):**
```
ZADD subreddit:{id}:hot  {hot_score}  {post_id}
ZREVRANGE subreddit:{id}:hot 0 24    → top 25 post IDs
TTL: 5 minutes (stale-while-revalidate acceptable — hot feed changes slowly)
```

**"Top" feed (weekly/monthly):**
```sql
SELECT id, (upvotes - downvotes) as score
FROM posts
WHERE subreddit_id = $1
  AND created_at > now() - interval '7 days'
ORDER BY score DESC
LIMIT 25
```
Index: `(subreddit_id, created_at, upvotes)` — covers the query.

### 3. Nested Comments

**Problem:** Comments nest arbitrarily deep. Need to:
- Display sorted (top comments first, sorted within each subtree)
- Efficiently retrieve entire thread

**Schema — Materialized Path:**
```sql
comments (
  id          UUID,
  post_id     UUID,
  parent_id   UUID NULL,
  author_id   UUID,
  body        TEXT,
  upvotes     INT DEFAULT 0,
  downvotes   INT DEFAULT 0,
  path        TEXT,  -- e.g. "0001.0003.0007" (sortable depth-first)
  depth       INT,   -- 0 = top-level
  created_at  TIMESTAMPTZ
)

-- Index for fetching all comments in a thread, sorted:
CREATE INDEX idx_comments_thread ON comments(post_id, path);
```

**Path generation:**
```
Top-level comment:   path = lpad(seq::text, 4, '0')  → "0001"
Reply to 0001:       path = "0001." + lpad(seq::text, 4, '0') → "0001.0004"
Reply to 0001.0004:  path = "0001.0004." + ...       → "0001.0004.0009"
```

**Fetching thread:**
```sql
SELECT * FROM comments
WHERE post_id = $1
ORDER BY path ASC   -- depth-first, preserves nesting order
```

One query returns all comments in correct display order. Client rebuilds tree from `parent_id`.

**Trade-off vs Adjacency List:**
| | Adjacency List | Materialized Path |
|--|--|--|
| Fetch subtree | Multiple queries or recursive CTE | Single range scan |
| Insert | Simple | Requires path generation |
| Move comment | Trivial | Must update all child paths |
| Sort within subtree | Needs recursive sort | Built into path string |

Reddit comments are never moved → materialized path wins.

### 4. Fan-out for New Posts

**Problem:** r/AskReddit has 42M subscribers. You can't write to 42M user feeds when a post is created.

**Hybrid approach:**
```
On new post in subreddit:
  1. Kafka publishes: { post_id, subreddit_id, author_id, created_at }
  
  2. Fan-out Worker:
     a. Identify users online in last 30 min (Redis SET: active_users → ~500K users)
     b. Intersect with subreddit subscribers:
        SINTERSTORE temp:fanout:{post_id}  active_users  subreddit:{id}:subscribers
     c. For each online subscriber:
        ZADD user:{uid}:feed {hot_score} {post_id}  (cap at 500 entries per user)
        
  3. Offline users: pull on demand.
     When user opens app: query top posts from their subscribed subreddits
     SELECT post_id FROM posts WHERE subreddit_id IN (...user's subs...)
     ORDER BY hot_score DESC LIMIT 50
```

**Why not push to everyone:**
- 42M ZADD operations per post × Kafka lag = minutes to propagate
- Most subscribers won't log in that day anyway
- Pull for inactive users is cheaper and produces same result

**Why not pull for everyone:**
- Pull = N subreddit queries merged + ranked on every feed load
- At 50 subscriptions × 2,900 loads/sec = 145K queries/sec → DB killer

**Threshold:** Push to users active in last 30 min (small set). Pull for everyone else. Fan-out for post karma >1000 only (viral content worth pre-distributing).

### 5. Search

**Elasticsearch index:**
```json
POST /posts/_doc/{id}
{
  "title": "What's the best programming language for...",
  "body_preview": "First 500 chars...",
  "subreddit": "learnprogramming",
  "author": "user123",
  "score": 1284,
  "created_at": "2026-06-22T10:00:00Z",
  "post_type": "text"
}
```

**Indexing pipeline:** Postgres WAL → Debezium → Kafka → Search Indexer → Elasticsearch (async, ~1s lag).

**Query (BM25 + score boost):**
```json
{
  "query": {
    "function_score": {
      "query": { "multi_match": { "query": "...", "fields": ["title^3", "body"] }},
      "functions": [
        { "field_value_factor": { "field": "score", "modifier": "log1p", "factor": 0.1 }}
      ]
    }
  }
}
```
High-score posts rank higher for same text relevance.

---

## Component Trade-offs

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| Vote buffer in Redis | Write-behind | Fast votes, risk: votes lost if Redis crashes before flush. Mitigation: Redis AOF + flush on graceful shutdown |
| Comment storage | Materialized path | Fast tree retrieval, path strings grow with depth. Max depth 10 = manageable |
| Hot feed | Redis sorted set | O(log N) insert/read, stale up to 5 min. Acceptable: hot feed doesn't need second-level freshness |
| Vote counts in cache | Redis INCR | ~30s lag on counts. Reddit intentionally fuzzes anyway — this aligns |
| Fan-out | Hybrid push/pull | Online users see post immediately; offline users see it on next load. Small consistency gap |
| Nested comments query | Single path-ordered query | Full thread fetched regardless of user reading depth 1. Mitigation: cursor pagination on first load |

---

## Real-world References

- **Reddit engineering blog (2017):** migrated from Python 2 monolith to Python 3 services. Comment tree stored with path encoding.
- **Reddit r/changelog (2020):** "New Reddit" moved to React + GraphQL. Feed service became separate microservice.
- **Cloudflare case study:** Reddit uses Cloudflare + Fastly for CDN, significantly reducing origin load.
- **Redis Labs:** Reddit uses Redis extensively for session storage, rate limiting, vote buffers. Cited in Redis case studies.
- **Vote fuzzing:** Confirmed by Reddit staff (u/kemitche) in AMA threads as deliberate anti-manipulation.
