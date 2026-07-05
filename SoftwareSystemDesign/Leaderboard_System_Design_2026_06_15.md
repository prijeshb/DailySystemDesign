# Real-time Leaderboard System — System Design
**Date:** 2026-06-15

---

## First Principles: Do We Need This?

**Problem:** Players want to know "where do I rank among 10 million others — right now, not tomorrow."

**Why not SQL `ORDER BY score DESC`?**
- 50K score updates/sec → B-tree index maintenance on every write → lock contention
- `SELECT rank FROM (SELECT player_id, RANK() OVER (ORDER BY score DESC)) WHERE player_id = ?` → full window function scan on 10M rows → 5-10s, unusable
- Even with materialized rank column: UPDATE to 1 row changes ranks of all players above/below → O(N) cascading updates

**What we actually need:**
1. O(log N) score update
2. O(log N) rank lookup for any player
3. O(log N) top-K range query
4. Multiple time windows: daily / weekly / all-time
5. Friends leaderboard (subset ranking)
6. Regional leaderboard

Answer: **Redis Sorted Set** — natively supports all these operations. ZADD O(log N), ZREVRANK O(log N), ZREVRANGE O(log N + K).

---

## Scale Assumptions (Clarify in Interview)

| Dimension | Value |
|-----------|-------|
| Active players per game | 10M |
| Score updates/sec | 50K |
| Leaderboard reads/sec | 200K (reads >> writes) |
| Top-K page size | 100 |
| Time windows | Daily, Weekly, All-time |
| Games on platform | 1,000 |
| Friends per player (avg) | 200 |

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| **Player** | id, display_name, region, avatar_url |
| **Score** | player_id, game_id, value, submitted_at |
| **ScoreEvent** | player_id, game_id, delta (for additive scores) or absolute_value, idempotency_key |
| **LeaderboardEntry** | rank, player_id, display_name, score, rank_change (since last view) |
| **Leaderboard** | game_id, window (daily/weekly/all-time), generated_at |
| **FriendRelation** | player_id, friend_id |

---

## Actions

1. **Submit score** → update player's score for a game (e.g. match ends)
2. **Get global top-N** → return ranked list with scores and player info
3. **Get my rank** → where does the requesting player stand globally?
4. **Get friends leaderboard** → top-N among friends only
5. **Get regional leaderboard** → top-N within player's region
6. **Window reset** → daily resets at midnight UTC, weekly on Monday midnight
7. **Get rank around me** → ±10 players around my rank (neighborhood view)

---

## Data Flow

### Write Path (Score Submission)

```
Game Server (match ends)
    ↓  POST /scores  {player_id, game_id, score, idempotency_key}
Score Service
    ↓ 1. Idempotency check (Redis SET NX, TTL 24h)
    ↓ 2a. Redis: ZADD lb:{game_id}:alltime GT player_id score
    ↓ 2b. Redis: ZADD lb:{game_id}:daily:{YYYYMMDD} GT player_id score (TTL 2 days)
    ↓ 2c. Redis: ZADD lb:{game_id}:weekly:{YYYY_WW} GT player_id score (TTL 14 days)
    ↓ 3. Kafka topic: score.submitted (async)
         ↓
    Score Consumer → PostgreSQL upsert (durable record)
    Analytics Consumer → ClickHouse (historical analytics)
```

**GT flag:** `ZADD ... GT` = only update if new score > existing. Prevents replays/retries from corrupting leaderboard.

### Read Path (Global Top-100)

```
Client
  ↓
API Gateway → Leaderboard Service
                   ↓
             Redis: ZREVRANGE lb:{game_id}:alltime 0 99 WITHSCORES  [O(log N + 100)]
                   ↓
             Player Service (batch): MGET player:{id}:meta for display names
                   ↓
             Cache: top-100 snapshot cached for 2s (reduce Redis load on viral games)
                   ↓
             Return: [{rank, player_id, display_name, score, rank_change}, ...]
```

### Read Path (Friends Leaderboard)

```
Client
  ↓
Leaderboard Service
  ↓ 1. Fetch friend list: SMEMBERS friends:{player_id}  [Redis Set]
  ↓ 2. Batch score fetch: ZMSCORE lb:{game_id}:alltime friend_id_1 ... friend_id_200
  ↓ 3. Sort in application layer (200 scores, negligible)
  ↓ 4. Return top-N with rank relative to friends group
```

---

## High Level Design

```
                         ┌──────────────────────────────────────┐
                         │           Game Servers               │
                         └──────────────┬───────────────────────┘
                                        │ POST /scores
                         ┌──────────────▼───────────────────────┐
                         │          Score Service                │
                         │  (idempotency → Redis → Kafka)        │
                         └──────────────┬───────────────────────┘
                                        │
              ┌─────────────────────────┼─────────────────────────┐
              ▼                         ▼                         ▼
    ┌──────────────────┐    ┌─────────────────────┐   ┌───────────────────┐
    │  Redis Cluster   │    │  Kafka              │   │  Score Consumer   │
    │  Sorted Sets     │    │  score.submitted    │   │  → PostgreSQL     │
    │  (leaderboards)  │    │                     │   │  → ClickHouse     │
    └────────┬─────────┘    └─────────────────────┘   └───────────────────┘
             │
    ┌────────▼─────────┐
    │ Leaderboard Svc  │◄── Client reads (top-N, rank, friends, region)
    │ + Player Svc     │
    └──────────────────┘
```

---

## Low Level Design

### Redis Key Schema

| Key Pattern | Type | Content | TTL |
|-------------|------|---------|-----|
| `lb:{game_id}:alltime` | Sorted Set | member=player_id, score=score | None |
| `lb:{game_id}:daily:{YYYYMMDD}` | Sorted Set | member=player_id, score=score | 2 days |
| `lb:{game_id}:weekly:{YYYY_WW}` | Sorted Set | member=player_id, score=score | 14 days |
| `lb:{game_id}:region:{region}:alltime` | Sorted Set | member=player_id, score=score | None |
| `friends:{player_id}` | Set | friend player IDs | None |
| `idem:{idempotency_key}` | String | "1" | 24h |
| `player:{id}:meta` | Hash | display_name, region, avatar_url | None |

### Core Redis Operations

```
# Submit score (score is highest ever — GT flag)
ZADD lb:fortnite:alltime GT 9500 player123
ZADD lb:fortnite:daily:20260615 GT 9500 player123
ZADD lb:fortnite:weekly:2026_25 GT 9500 player123

# Get rank (0-indexed from top, +1 for 1-indexed rank)
rank = ZREVRANK lb:fortnite:alltime player123  → 42 → "rank 43"

# Get top 100
ZREVRANGE lb:fortnite:alltime 0 99 WITHSCORES

# Get neighborhood (player at rank 42, show ±10)
start = max(0, rank - 10)
end = rank + 10
ZREVRANGE lb:fortnite:alltime start end WITHSCORES

# Friends leaderboard
friend_ids = SMEMBERS friends:player123
scores = ZMSCORE lb:fortnite:alltime friend_ids...  # O(F)
sort locally → return top-N

# Idempotent submit
SET idem:{key} 1 NX EX 86400  → if nil, already processed, skip
```

### Score Tie-Breaking

Problem: Two players both have score 9500 — who ranks higher?

**Solution:** Composite score encoding:
```
# Earlier submission wins ties (first to reach score ranks higher)
composite_score = score * 1e9 + (EPOCH_BASELINE - submit_timestamp_ms)

# submit_timestamp_ms decreasing term ensures earlier = higher composite
ZADD lb:fortnite:alltime GT composite_score player123
```

Decode: `actual_score = floor(composite_score / 1e9)`

### Window Reset (Daily/Weekly)

Option 1: TTL-based (preferred)
- Key `lb:fortnite:daily:20260615` auto-expires after 2 days
- New key `lb:fortnite:daily:20260616` starts fresh at midnight
- No explicit reset job needed
- Client always uses today's date key

Option 2: Explicit reset job
- Cron job at midnight: `DEL lb:fortnite:daily` — risky (thundering herd, downtime)

→ **Use Option 1 (TTL-based keys)**. No reset needed, history preserved for 2 days if needed.

### Sharding Redis for Extreme Scale (>50M players)

Single Redis instance: holds ~50M sorted set members (~3GB RAM) — fine for most games.

For 100M+ players or multiple large games:
```
shard_id = hash(player_id) % N_SHARDS

# Score update: always goes to correct shard
ZADD lb:fortnite:alltime:shard_{shard_id} GT score player_id

# Rank query: need cross-shard merge
# 1. Get player's score: ZSCORE lb:fortnite:alltime:shard_X player_id → 9500
# 2. Count players with score > 9500 across all shards:
#    for each shard: ZCOUNT lb:fortnite:alltime:shard_i (9500 +inf
# 3. Sum counts + 1 = global rank
# 
# This is O(N_SHARDS) Redis calls — use pipeline/parallel for speed

# Top-K query: fetch top-K from each shard, merge sort in app layer
# Top 100 from 10 shards = 10 sorted lists of 100 → merge → true top 100
```

**Trade-off:** Sharding adds complexity to rank/top-K queries. Accept O(N_SHARDS) overhead only when single instance can't hold the data.

---

## Trade-offs Summary

| Decision | Why | Trade-off |
|----------|-----|-----------|
| Redis Sorted Set | O(log N) for all leaderboard ops, microsecond latency | All in-memory — costly at extreme scale; need AOF/RDB for durability |
| `ZADD GT` flag | Idempotent: retries and replays safe — score never goes backward | Wrong for games where score accumulates (use ZINCRBY instead) — choose based on game type |
| TTL-based window keys | Zero downtime reset, no cron job risk, history available | Key proliferation — 1000 games × 365 days = 365K keys (fine, Redis handles millions) |
| Async Kafka → PostgreSQL | Keeps write path fast (Redis only) | Score in Redis but not yet in DB — on Redis total loss, replay Kafka from last DB checkpoint |
| Friends leaderboard in-app sort | Simple, no extra Redis memory | Doesn't scale past ~5K friends; for social platforms with 10K+ friends, use pre-computed friend sorted sets |
| Composite score for tie-breaking | Deterministic ordering, no extra fields | Reduces effective score precision — ensure score × 1e9 doesn't overflow float64 (max ~9×10^15 safe) |

---

## Real-world References

- **Riot Games (LoL ranked ladder):** Redis Sorted Sets per queue type, region-sharded, checkpoint to Cassandra every 5min
- **Steam Leaderboards:** Per-game sorted sets, daily/weekly/all-time separate tables, S3 snapshots for historical
- **DraftKings:** Real-time contest leaderboards during live sports; composite score encoding for tiebreakers; read-through cache for top-100 (2s TTL)
- **Fortnite (Epic):** Sharded Redis for 350M registered players; batch ZMSCORE for friends leaderboard; CDN-cached top-1000 snapshots refreshed every 30s
