# Twitter Feed System — Interview Q&A
**Date:** 2026-05-14 | **System:** Twitter/X Social Feed

> Study the *shape* of answers: structured thinking, explicit trade-off reasoning, first-principles justification, and confident depth on one sub-system. Interviewers reward "why" more than "what."

---

## How Interviewers Structure This Problem

**Opening (5 min):** Scope and clarification — what features, what scale?
**Design (25 min):** High-level → components → interviewer picks one to deep-dive
**Probing (10 min):** "What if X fails?", "How would you scale Y?", "Why not Z instead?"
**Wrap-up (5 min):** Trade-offs you'd revisit, what you'd build next

Common deep-dives interviewers choose for this system:
- Fan-out strategy (celebrity problem) ← most common
- Feed cache design (Redis)
- Sharding the tweet table
- Trending algorithm
- What happens when a user with 50M followers posts

---

## Part 1: Scoping & First Principles

---

### Q1: "Before you draw anything — what would you clarify?"

**Strong answer:**

> "A few things that dramatically change the architecture:
>
> **Features:** Full Twitter — post, follow, feed, search, trends? Or one specific slice? I'll scope to core: post a tweet, follow users, and read home timeline.
>
> **Scale:** Are we targeting Twitter-scale — 200M DAU, ~6K tweets/second? Or a smaller system? The answer drives whether we need distributed caching, sharding, and complex fan-out at all.
>
> **Consistency model:** Does every follower need to see a tweet within 1 second? Or can we accept eventual consistency — tweet appears in feed within 5-30 seconds? I'll assume eventual consistency is fine, which is what every major social platform uses.
>
> **Read vs. write ratio:** Feeds are read-heavy (~100:1). This means we should optimize read path aggressively, and can absorb write complexity to make reads fast.
>
> I'll design for: 200M DAU, 600M tweets/day, eventual consistency, global distribution."

**Why this lands:** Identifies the single most important question for this problem (consistency + scale) before drawing a single box.

---

### Q2: "Do we even need a pre-computed feed? Can't we just query on the fly?"

**First-principles check — interviewers love this question.**

> "Let's check the math. If I follow 300 accounts, a naive approach is:
> SELECT * FROM tweets WHERE author_id IN (...300 IDs...) ORDER BY created_at DESC LIMIT 20
>
> At 200M DAU each loading their feed 10 times/day, that's 2 billion feed-load queries/day — about 23K queries/second.
>
> Each query scans indexes for 300 user_ids, merges, sorts. Even with perfect indexes, this is 10-50ms per query on a well-tuned DB. At 23K/s, you'd need thousands of DB replicas, and the fan-in merge at the application layer adds latency.
>
> So no, we can't query on the fly at this scale. We need a pre-computed feed where tweet_ids are already sorted and cached per user. The cost shifts from read time to write time (fan-out), but reads happen 100x more often than writes, so this trade-off is strongly in our favor."

---

### Q3: "What are the entities?"

**Expected answer:**

> "Four core entities:
>
> **User** — account info, follower/following counts. The key attribute for architecture: `is_celebrity` flag (follower_count > 1M), which changes how we handle fan-out.
>
> **Tweet** — content, author, engagement counts, timestamps. Critical design choice: tweet_id uses Snowflake format — it encodes a millisecond timestamp so tweets are globally sortable by ID alone, no separate timestamp sort needed.
>
> **Follow** — the social graph edge. Sharded in two ways: by follower_id (for 'who do I follow?') and by followee_id (for 'who follows me?') — two indexes with opposite shard keys.
>
> **FeedEntry** — the pre-computed per-user sorted list of tweet_ids. Stored in Redis, not in a relational DB. This isn't really a first-class entity — it's a derived, cached view. It can be thrown away and rebuilt."

---

## Part 2: Fan-Out Deep Dive (Most Common)

---

### Q4: "How does a tweet get to a follower's feed? Walk me through the whole flow."

**This is the core question. Structure your answer as a numbered flow.**

> "When user A (200 followers) tweets:
>
> 1. Tweet Service writes the tweet to Cassandra (source of truth) and an outbox record atomically.
> 2. Outbox reader publishes a `tweet.created` event to Kafka — this ensures the event is published even if the service crashes after the write.
> 3. Fan-Out Service consumes the Kafka event, fetches A's 200 follower IDs from the Social Graph DB.
> 4. For each follower ID, it does LPUSH feed:{follower_id} tweet_id in Redis — pushing the tweet_id onto that user's pre-built feed list.
> 5. LTRIM limits each feed to the last 800 tweet_ids (garbage collection).
> 6. When a follower opens their feed, Timeline Service does LRANGE feed:{follower_id} 0 19 — instant, O(N) Redis read.
> 7. Timeline Service multi-gets the full tweet objects from a tweet cache (Redis), falls back to Cassandra on miss.
>
> The whole path from tweet creation to fan-out completion: 1-5 seconds for normal users, completely async."

**Interviewer follow-up: "What if A has 50 million followers?"**

> "That's the celebrity problem. At 10K Redis writes/second, fanning out to 50M followers takes 83 minutes — followers would see the tweet an hour later. Unacceptable.
>
> Solution: hybrid strategy. Celebrities use fan-out on READ instead of fan-out on write:
>
> - Celebrity's tweet_id goes into a small per-celebrity Redis sorted set: `author_tweets:{celebrity_id}`.
> - We do NOT push to 50M follower feeds.
> - At read time, Timeline Service checks which celebrities the requesting user follows, queries each celebrity's sorted set (maybe 3-5 celebrities), and merges those results with the user's pre-built regular feed.
> - Merge cost: sorting 5 lists of 50 items each — negligible.
>
> For the user, the experience is identical. The cost shifts from write-time (expensive: 50M writes) to read-time (cheap: 5 sorted set reads)."

---

### Q5: "How does following a new account work? When do their tweets appear in my feed?"

> "Two things happen when I follow @alice:
>
> 1. A Follow record is written to the Social Graph DB: (my_id, alice_id, timestamp).
> 2. A `follow.created` event is published to Kafka.
> 3. Fan-Out Backfill Job consumes this event, fetches Alice's recent tweets (last 24 hours, up to 50), and injects them into my feed Redis list.
>
> Within a few seconds, Alice's recent tweets appear in my feed. Future tweets fan out normally because Alice's follower list now includes me.
>
> Edge case: Alice is a celebrity. No backfill needed — at my next feed load, Timeline Service sees I follow a celebrity, queries `author_tweets:{alice_id}`, and her tweets appear.
>
> Edge case: Alice has no recent tweets. Nothing to backfill. Her new tweets will appear going forward."

---

### Q6: "What if the fan-out service is slow and builds up lag?"

> "Fan-out is async and its consumer lag is the key health metric. A few mitigations:
>
> **Scale horizontally:** Kafka partitions are the unit of fan-out parallelism. If we have 100 partitions and each Fan-Out consumer handles 1 partition, scaling to 100 instances means 100x throughput. We auto-scale consumer count based on lag metric.
>
> **Prioritize active users:** If lag is building, skip injecting into feeds of users who haven't logged in for 7+ days. They'll get a cold rebuild when they return.
>
> **Emergency: drop normal fan-out, route to celebrity-style pull:** If fan-out can't keep up during a massive spike (e.g., a major breaking news event), temporarily treat ALL users as 'celebrities' — all feeds computed at read time from DB queries. This is slower per read but prevents the fan-out backlog from growing unboundedly.
>
> In practice: Twitter had a major fan-out lag incident during Michael Jackson's death in 2009. The entire platform slowed because the tweet storm overwhelmed the fan-out pipeline. The solution was the hybrid strategy now in use."

---

## Part 3: Storage & Caching Deep Dive

---

### Q7: "Why Cassandra for tweets? Why not PostgreSQL?"

> "Three reasons:
>
> First, write throughput. At 14K tweets/second peak, Cassandra's LSM-tree storage is optimized for high-write workloads — writes go to an in-memory memtable and sequential WAL, then are flushed to disk. PostgreSQL's B-tree index updates are more write-amplified under this load.
>
> Second, natural partitioning. Tweets are partitioned by tweet_id (Snowflake). Each tweet is independent — there are no cross-tweet joins needed. Cassandra's partition model is perfect: each tweet lives on its shard, and we always fetch tweets by tweet_id.
>
> Third, horizontal scale. Cassandra is designed to scale linearly by adding nodes. PostgreSQL vertical scaling hits a ceiling; sharding Postgres manually is complex and operationally painful.
>
> Trade-offs I'm accepting: no ACID transactions, no joins, eventual consistency. For tweets, this is fine — I don't need tweet A and tweet B to be created atomically. I do need individual tweet writes to be durable, which Cassandra provides with RF=3 QUORUM writes."

**Interviewer follow-up: "When would you choose PostgreSQL over Cassandra?"**

> "PostgreSQL wins when: (1) you need ACID transactions — like financial records or follow-count updates that must be consistent, (2) you have complex relational queries — the Social Graph (follow table) benefits from joins and foreign keys, (3) your dataset fits on a single machine with replicas — Postgres is far simpler to operate. Cassandra's complexity is only justified at write-heavy, horizontally-scaled workloads."

---

### Q8: "Walk me through the feed cache design. What's stored in Redis and why?"

> "Redis stores only tweet_ids in the feed list, not full tweet objects. Here's why:
>
> A tweet_id is an 8-byte integer. A tweet object with text, author info, engagement counts, media URLs is 2-5 KB. If we stored full tweet objects in feed lists:
> - 200M users × 800 tweets × 3 KB = 480 TB of Redis. Prohibitively expensive.
>
> By storing only tweet_ids:
> - 200M users × 800 IDs × 8 bytes = 1.28 TB of Redis. Affordable.
>
> Feed read path is then two steps:
> 1. LRANGE feed:{user_id} 0 19 → get 20 tweet_ids. O(1) per ID.
> 2. Multi-get tweet objects: GET tweet:{id} for each. Parallel, ~0.5ms per batch.
>
> Tweet objects are cached separately in Redis with a 1-hour TTL. Cache hit rate is high because popular tweets (viral ones) are read millions of times. Cache miss falls back to Cassandra.
>
> This two-level approach keeps Redis memory manageable while still achieving sub-10ms feed loads."

---

### Q9: "How do you handle engagement counts (likes, retweets) at scale?"

> "The naive approach — SELECT COUNT(*) FROM likes WHERE tweet_id = X — is a full table scan. At 600M tweets and billions of likes, this is catastrophic.
>
> Instead, we maintain counters directly in Redis: INCR tweet_like_count:{tweet_id} is atomic, sub-millisecond, and handles 1M operations/second per Redis instance.
>
> Periodically (every 60 seconds), a flush job reads Redis counters and writes them back to the tweet record in Cassandra. This means DB counts may lag by up to 60 seconds.
>
> The trade-off: a tweet that just went viral might show '1,000 likes' in Cassandra when it actually has '1,200 likes' in Redis. We serve the Redis counter to users — so what users see is current, but the durable store lags.
>
> Hot key problem: a viral tweet like may hit 100K increments/second — far beyond one Redis key's capacity. Solution: counter sharding — 100 Redis keys (tweet_like_count:{tweet_id}:shard_{0-99}), randomly pick one to increment, sum all 100 at read time. Sums cached in a separate Redis key with 5-second TTL."

---

## Part 4: Trending & Search

---

### Q10: "How would you build the trending topics feature?"

> "The naive approach — SELECT hashtag, COUNT(*) FROM tweet_hashtags ORDER BY COUNT DESC — is an all-time counter. That would make '#ChristmasDay' permanently trending. We want to surface what's trending NOW.
>
> Solution: sliding window with minute-buckets.
>
> On every tweet with a hashtag:
> ZINCRBY trending:global:{minute_bucket} 1 'hashtag_name'
> EXPIRE trending:global:{minute_bucket} 7200   ← keep 2 hours of buckets
>
> Every minute, Trending Service computes:
> For each hashtag: sum counts across last 60 minute buckets.
> Apply velocity weighting: recent buckets count more than older ones.
> Output: top N hashtags sorted by weighted score.
>
> The velocity weighting catches breakout moments: a hashtag with 100 tweets/minute for the last 5 minutes ranks higher than one with 10 tweets/minute steadily for 60 minutes — even if the total is similar.
>
> Geo-trending: same structure but per WOEID (Where On Earth ID). Most cities and countries have their own trending topics. Store trending:{woeid}:{minute_bucket} keys.
>
> Scale concern: a viral hashtag during a breaking news event gets 50K increments/second. Solution: same counter sharding as engagement counts."

---

## Part 5: Failure & Edge Cases

---

### Q11: "What happens if a user deletes a tweet? It's already in 200M follower feeds."

> "Deletion is hard. There are three approaches:
>
> 1. **Lazy deletion (recommended):** Mark the tweet as DELETED in Cassandra. Don't touch Redis feeds. When Timeline Service hydrates tweet objects for a feed, it skips tweet_ids where status = DELETED. Cost: one extra status check per tweet object hydration — cheap. Trade-off: deleted tweet's ID stays in Redis feed lists, wasting a tiny bit of space.
>
> 2. **Eager deletion:** For each of the 200M followers, remove the tweet_id from their Redis feed. At 10K Redis ops/second: 200M / 10K = 20,000 seconds = 5 hours. Way too slow for a 'deleted' operation. Only feasible for small follower counts.
>
> 3. **Hybrid:** Eager deletion for the author's own profile timeline (one Redis key). Lazy deletion for follower feeds.
>
> My recommendation: lazy deletion. It's O(1) at delete time and has minimal overhead at read time. Twitter actually does this."

---

### Q12: "How do you prevent abuse — someone spamming 10,000 tweets in a minute?"

> "Rate limiting at multiple layers:
>
> **API Gateway level:** Per-user rate limit stored in Redis: INCR rate:{user_id}:{minute} with EXPIRE 60. If count > 5, return 429. This is the first line of defense.
>
> **Tweet Service level:** Even if the gateway is bypassed, Tweet Service has a secondary check against the same Redis counter.
>
> **Product limits:** 300 tweets/day per account (Twitter's actual limit). Enforced at the DB level: check count before insert.
>
> **IP-level limiting:** For unauthenticated spam bots, rate limit by IP at the gateway.
>
> **Why not just trust the database?** DB-level enforcement (COUNT + INSERT in a transaction) is expensive at scale. Redis INCR is O(1) and handles millions of ops/second — it's the right tool for rate counters.
>
> Beyond rate limiting: ML-based spam detection runs async, analyzing patterns. Suspicious accounts get their tweets shadow-banned (tweet exists but not fanned out) while under review."

---

### Q13: "The system needs to support both reverse-chronological feed AND a ranked/algorithmic feed. How does your design change?"

> "Reverse-chronological is solved: feed lists are sorted by tweet_id (Snowflake = timestamp-encoded). LRANGE returns them in insert order.
>
> Algorithmic ranking requires a score per tweet that accounts for: engagement (likes, retweets), recency, relationship strength (do I interact with this author often?), content affinity (topics I engage with). This score changes over time as engagement grows.
>
> Two options:
>
> **Option A — Re-rank at read time:**
> Feed list stays as tweet_ids sorted by time. At read time, Timeline Service fetches the top 200 tweet_ids, asks a Ranking Service to score each one, returns top 20 by score.
> Trade-off: requires a ranking API call at every feed load. Adds 10-50ms latency. Score is always fresh.
>
> **Option B — Pre-rank during fan-out:**
> Fan-Out Service assigns an initial score at insertion. Uses Redis sorted sets instead of lists (ZADD with score instead of LPUSH). Feed read = ZREVRANGEBYSCORE.
> Trade-off: score is set at write time, misses engagement that happens after insertion. Cheaper reads.
>
> **My recommendation:** Option A (re-rank at read time) for better relevance. Cap the candidate set to 200 tweets to bound ranking latency. This is close to how Twitter's 'For You' timeline works."

---

## Part 6: Interviewer Follow-Up Bank

These are the most common unexpected follow-ups. Prepare a 1-2 sentence answer for each.

---

**"What's a Snowflake ID?"**
> "A 63-bit integer that encodes: 41 bits of millisecond timestamp + 10 bits of machine ID + 12 bits of sequence number per millisecond. It's globally unique, roughly sortable by creation time without a database lookup, and was invented by Twitter specifically for this use case."

---

**"Why not use UUIDs for tweet IDs?"**
> "UUIDs are random — they don't encode time, so sorting by tweet_id to get timeline order requires a separate timestamp column and index. Snowflakes let us sort by ID = sort by time. Also, 63-bit integers have better index performance than 128-bit UUIDs in Cassandra and PostgreSQL B-tree indexes."

---

**"How does Twitter handle the 'first tweet' from a brand new account?"**
> "No feed pre-exists. The feed is computed cold on first login by pulling recent tweets from people they follow. This is the 'cold start' case. It's also why Twitter shows suggestions / trends on signup — to seed the follow graph so the feed has something to show."

---

**"What happens to feeds of users who haven't logged in for 6 months?"**
> "We stop maintaining their feed cache — no more fan-out writes to that user's Redis list. When they log in, we detect cache miss, trigger an on-demand feed rebuild: fetch their recent follows' tweets from TweetDB, build and cache the feed. This is slower (1-2 seconds) but acceptable since this is a rare, infrequent event. It also saves significant Redis memory — inactive users can be 50% of all accounts."

---

**"How would you design the 'Who to follow' recommendation?"**
> "Out of scope for this design, but briefly: graph-based 'friends of friends' (users followed by people you follow, ranked by overlap), content-based similarity (users who tweet about topics you engage with), and collaborative filtering (users similar to you followed X). These run as batch jobs (Spark on the full follow graph), output ranked recommendations per user, stored in a separate recommendations table, surfaced by a Recommendation Service."

---

**"Your Redis cluster is 1.28 TB. Is that cost-efficient?"**
> "Redis on EC2: r6g.16xlarge has 512 GB RAM, costs ~$3.5/hr. For 1.28 TB you need 3 nodes (~$10.5/hr). Monthly: ~$7,500 for feed cache alone. At Twitter's revenue scale (~$4B/year), that's 0.002% of revenue. The alternative — computing feeds from DB on every load — would require dozens of database clusters at far higher cost. So yes, Redis is extremely cost-efficient here."

---

**"What's the CAP theorem implication of your design?"**
> "The feed is AP (Available + Partition Tolerant). During a network partition, users can still read their cached feeds (available) even if fan-out is delayed (consistency is sacrificed). We accept eventual consistency: a tweet may take 1-30 seconds to appear in all follower feeds. The tweet itself (source of truth in Cassandra) is CP for writes — we require quorum acknowledgment before confirming success to the user. So: tweet creation is CP (strong write consistency), feed reads are AP (eventual consistency)."

---

## Scoring Rubric (What Interviewers Look For)

| Criterion | Green | Yellow | Red |
|-----------|-------|--------|-----|
| Problem framing | Clear scope, identifies scale, consistency trade-off | Jumps to design without asking | Designs without knowing requirements |
| Fan-out | Explains hybrid push/pull, celebrity problem | Knows push vs pull but not hybrid | Only knows one strategy |
| Storage choices | Justifies Cassandra vs Postgres with trade-offs | Picks right tools but can't explain why | Mismatches tools to problems |
| Failure handling | Covers component, before, after failures | Covers only "what if X crashes" | No failure discussion |
| Sharding | Explains Snowflake ID, feed shard by user_id | Mentions sharding without specifics | No sharding discussion |
| Trade-offs | Explicitly states trade-offs for every major choice | Some trade-offs | No trade-offs |
| First principles | Proves need for pre-computed feed with math | Assumes it's needed | Doesn't question assumptions |
