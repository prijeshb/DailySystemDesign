# Real-time Leaderboard — Interview Q&A
**Date:** 2026-06-15

---

## Opening Question

**Q: Design a real-time leaderboard for a mobile game with 10 million active players.**

Think out loud from first principles:
1. Can SQL handle this? → `ORDER BY score` on 10M rows, 50K writes/sec = no
2. What operations matter? → update score, get rank, get top-K
3. What data structure fits? → Sorted Set: O(log N) for all three
4. Core answer: Redis Sorted Set + async PostgreSQL persistence via Kafka

Response structure: clarify scale → entities → data flow → HLD → LLD

---

## Requirement Clarification (Ask These First)

- Is score additive (accumulate over games) or best-score (take highest)?
- What time windows? Daily, weekly, all-time?
- Do we need a friends leaderboard? Regional?
- What's the acceptable lag? Real-time (<1s) or near-real-time (<30s)?
- How many concurrent games/tournaments?
- What happens on tie (same score)? First-to-reach wins? Alphabetical?
- Is historical leaderboard needed (what was rank last Tuesday)?

*These questions show you understand the problem is underspecified and change the design.*

---

## Component Deep-Dives

**Q: Why Redis Sorted Set over a database with an index?**

Three operations define a leaderboard:
1. Update a player's score
2. Get rank of a specific player
3. Get top-N players by rank

SQL with B-tree index on score:
- UPDATE is O(log N) ✓
- `RANK() OVER (ORDER BY score DESC)` requires computing rank at query time — O(N) scan
- Even with materialized rank column: updating one score cascades rank changes to O(N) other rows

Redis Sorted Set:
- ZADD GT: O(log N) ✓
- ZREVRANK: O(log N) ✓  
- ZREVRANGE: O(log N + K) ✓

Purpose-built. Also: Redis lives in RAM → microsecond latency vs milliseconds for disk-based DB.

---

**Q: What's the ZADD GT flag and why use it here?**

`ZADD key GT score member` — only updates member's score if new score > existing score.

Use case: best-score leaderboard (player's rank reflects their personal best, not latest match).

```redis
ZADD lb:fortnite:alltime GT 7000 player123  # sets to 7000
ZADD lb:fortnite:alltime GT 9500 player123  # updates to 9500 (higher)
ZADD lb:fortnite:alltime GT 8000 player123  # no-op (8000 < 9500)
```

Benefits:
1. Idempotent retries — re-submitting the same score is a no-op
2. Network replays don't corrupt leaderboard
3. Out-of-order messages from Kafka don't cause rank regression

When NOT to use GT: games where score accumulates (total XP, total kills). Use ZINCRBY instead.

---

**Q: How do you implement a daily leaderboard that resets at midnight?**

Naive approach: scheduled job deletes and recreates the key. Problems: downtime window, thundering herd on first read of empty set.

Better approach: TTL-keyed sorted sets.
```
Key: lb:{game_id}:daily:{YYYYMMDD}
Set TTL on creation: EXPIRE lb:fortnite:daily:20260615 172800  # 2 days

On day rollover:
  - Client sends scores to lb:fortnite:daily:20260616 (new key, auto-created)
  - Old key lb:fortnite:daily:20260615 naturally expires in 2 days
  - No downtime, no thundering herd, history preserved for 2 days
```

Score Service always derives current key from `datetime.utcnow().strftime('%Y%m%d')`.

---

**Q: How do you handle ties?**

Option 1: Same rank for tied players (dense ranking). Show both as rank 42.

Option 2: Tie-break by timestamp — first to achieve score wins.
```python
# Composite score: actual_score in upper bits, timestamp-based tiebreak in lower
EPOCH_BASELINE = 2_000_000_000_000  # ms
composite = score * 1_000_000 + (EPOCH_BASELINE - submit_time_ms)
# Decoded: actual_score = composite // 1_000_000
```

Option 3: Alphabetical (last resort, bad UX).

**Interviewer follow-up:** What's the max safe score before composite overflows?
float64 precision: 2^53 ≈ 9×10^15. With composite = score × 10^6, max safe score = 9×10^9. For most games fine; for financial scores or XP in billions, use strings or store tiebreak separately.

---

**Q: How do you build a friends leaderboard?**

Approach 1 (read-time aggregation, recommended):
```
1. SMEMBERS friends:{player_id} → [friend1, friend2, ... friend200]
2. ZMSCORE lb:fortnite:alltime friend1 friend2 ... friend200 → [9500, 7200, ...]
3. Sort in application layer → return top-N
```
Cost: O(F) Redis calls (F = friend count). For 200 friends, trivial.

Approach 2 (pre-materialized):
Maintain a sorted set per user: `lb:fortnite:friends:{player_id}`.
On every score update, fan-out ZADD to all friends' sorted sets.

- Pro: O(log N) read
- Con: Each score update requires F writes (200 friends × 50K updates/sec = 10M Redis writes/sec). Too expensive.

**Verdict:** Use read-time for up to ~5K friends. Only pre-materialize for viral social apps where friends = 10K+, and even then — consider lazy materialization triggered by viewing leaderboard.

---

**Q: Your Redis cluster holds 10M sorted set members. A player asks "what's my global rank?" Walk me through the entire call.**

```
1. Client: GET /leaderboard/{game_id}/rank?player_id=player123

2. Leaderboard Service:
   ZREVRANK lb:fortnite:alltime player123
   → Redis: O(log N) skip-list traversal → returns 42 (0-indexed)
   → rank = 43

3. Fetch player metadata (display name, avatar):
   HGETALL player:player123:meta
   → {display_name: "xXSniper99Xx", region: "NA", avatar_url: "..."}

4. Fetch score:
   ZSCORE lb:fortnite:alltime player123 → 9500.0
   Decode if composite: actual_score = int(9500.0) // 1_000_000

5. Return: {rank: 43, score: 9500, display_name: "xXSniper99Xx"}
```

Total: 2-3 Redis round trips, ~1ms.

---

**Q: How do you scale when a single Redis instance can't hold 100M players?**

Shard sorted sets by player:
```
shard_id = murmur3_hash(player_id) % N_SHARDS  # consistent hash
key = lb:fortnite:alltime:shard_{shard_id}

# ZADD: goes to one shard — O(log(N/shards))
# ZREVRANK: must determine global rank:
#   1. Get player's score: ZSCORE ... player_id
#   2. For each shard (parallel): ZCOUNT shard_i (player_score +inf → count of players scoring higher
#   3. Sum + 1 = global rank

# Top-K: fetch top-K from each shard, merge-sort → true global top-K
# Cost: O(N_SHARDS × K) entries to merge-sort, acceptable for top-100
```

**Interviewer follow-up:** Why not shard by score range?
Score-range sharding creates hot shards (top scores get all reads). Player-hash sharding distributes load uniformly.

---

**Q: What happens if Redis loses all its data (hardware failure, datacenter outage)?**

Recovery strategy:
1. Redis AOF (Append-Only File) persistence: every write appended to disk. On restart, AOF replays to reconstruct sorted sets. Recovery time: ~minutes for 10M members.
2. If AOF also lost: replay Kafka `score.submitted` topic from beginning (or from last PostgreSQL checkpoint).
   - `ZADD GT` makes replay idempotent: out-of-order events are handled correctly
   - Full replay from start = slowest path, but guarantees correctness

**Interviewer follow-up:** How long does Kafka replay take?
Depends on retention period (30 days) and event rate (50K/sec). 30 days × 86400s × 50K = 129 billion events. Full replay impractical.

Better approach: Daily PostgreSQL snapshot as checkpoint. Kafka retains only last 24h. On failure: load yesterday's snapshot into Redis (ZADD for each row), then replay 24h of Kafka events. Total: minutes, not days.

---

## Rapid-Fire Follow-ups

**Q: How would you show rank change (↑↑ climbed 5 spots)?**

Store player's previous rank in Redis Hash: `HSET player:{id}:rank_snapshot {game_id} 48`. On each leaderboard request, compare current ZREVRANK vs snapshot. Update snapshot on read.

**Q: How would you implement a tournament bracket leaderboard (only active during event)?**

Tournament-scoped key: `lb:fortnite:tournament:{tournament_id}`. Created when tournament starts, deleted (or archived to S3) when it ends. No TTL — explicit lifecycle management.

**Q: How would you prevent a player from gaming the leaderboard (submitting fake scores)?**

1. Score Service validates: score must come from Game Server (mutual TLS), not directly from client
2. Game Server signs score event with a secret: `HMAC(match_id + player_id + score, server_secret)`
3. Score Service verifies signature before ZADD
4. Anomaly detection: alert if player's score jumps from 1K to 9M in one match

**Q: How would you serve the leaderboard to 1M concurrent users at match end (everyone checks rank simultaneously)?**

Thundering herd problem. Solutions:
1. Cache top-100 snapshot in Redis (separate key) with 2s TTL — most users only care about top-100
2. CDN-cache `/leaderboard/top100` response with 5s TTL
3. For individual rank: ZREVRANK is fast enough (~1ms) to absorb the load on a clustered Redis

**Q: What if you need the leaderboard from 1 week ago?**

Redis doesn't retain history (sorted sets are mutable). PostgreSQL `score` table has `submitted_at`. To reconstruct historical leaderboard:

```sql
SELECT player_id, MAX(value) as best_score
FROM scores
WHERE game_id = 'fortnite' AND submitted_at < '2026-06-08 00:00:00'
GROUP BY player_id
ORDER BY best_score DESC
LIMIT 100
```

Run on PostgreSQL read replica or ClickHouse (for analytics queries). Slow but acceptable for historical queries — not a real-time path.

---

## Common Mistakes to Avoid

1. **Using SQL for rank queries** — O(N) window function kills performance at 10M+ rows
2. **Forgetting idempotency** — without ZADD GT + idempotency key, retries inflate scores
3. **maxmemory-policy allkeys-lru** — silently corrupts leaderboard by evicting members
4. **Score-range sharding** — creates hot shards; always shard by player_id hash
5. **Not clarifying best-score vs additive** — ZADD GT vs ZINCRBY: completely different semantics
6. **Sync Kafka writes on hot path** — adding Kafka latency to the 50K/sec write path; always async
7. **Materializing friends leaderboard** — O(F) writes per score update is too expensive for large friend graphs
