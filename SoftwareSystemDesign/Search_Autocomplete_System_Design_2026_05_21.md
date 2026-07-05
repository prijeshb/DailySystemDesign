# Search Autocomplete (Typeahead) — System Design
*Date: 2026-05-21 | Series: Daily System Design*

---

## 0. First Principles — Do We Even Need It?

**Problem**: A user types "appl" — should the server respond to *every keystroke*?

- Without autocomplete → user must type full query → bad UX
- With autocomplete → suggestions appear in ~100ms → reduces typos, speeds search
- **Worth it?** Yes. Google reports autocomplete reduces keystrokes by ~25%. At scale it drives query volume and revenue.

**Key insight**: We trade *write complexity + storage* for *read speed + UX quality*.

---

## 1. Entities

| Entity | Attributes |
|--------|-----------|
| Query | `query_string`, `frequency`, `last_updated` |
| Suggestion | `prefix`, `top_k_results[]`, `score` |
| UserSession | `session_id`, `user_id`, `partial_input` |

---

## 2. Actions

| Action | Trigger | Latency Target |
|--------|---------|---------------|
| `getSuggestions(prefix)` | Every keystroke | < 100ms p99 |
| `recordQuery(query)` | User submits search | Async, best-effort |
| `updateFrequencies()` | Batch job (hourly/daily) | Minutes |
| `buildTrie()` | Offline pipeline | Hours (rebuild) |

---

## 3. Data Flow

```
User types keystroke
       │
       ▼
[Browser Cache] ── hit ──▶ Return cached suggestions
       │ miss
       ▼
[CDN Edge Cache] ── hit ──▶ Return cached suggestions
       │ miss
       ▼
[API Gateway]
       │
       ▼
[Autocomplete Service]
  ├── Check [Redis Cache] ── hit ──▶ Return top-k
  │         │ miss
  │         ▼
  └── Query [Trie Store (ZooKeeper/Custom)]
             │
             ▼
         Return top-k, write to Redis

[Search Submit Path — async]
User submits query
       │
       ▼
[Kafka Topic: raw-queries]
       │
       ▼
[Frequency Aggregator (Spark/Flink)]
       │
       ▼
[Query DB (Cassandra)]
       │
       ▼
[Trie Builder (batch)] ──▶ [Trie Store]
```

---

## 4. High-Level Design

```
                    ┌──────────────────────────────────────┐
Clients             │           CDN Edge Nodes             │
(browser/app)       │  (cache top 10k prefixes globally)   │
        │           └──────────────────────────────────────┘
        │                            │ miss
        ▼                            ▼
   [Load Balancer]         [Autocomplete API Servers]
                                     │
                       ┌─────────────┴─────────────┐
                       ▼                           ▼
                [Redis Cache]              [Trie Servers]
                (prefix→top-k)          (in-memory trie,
                 TTL: 5 min              sharded by prefix)
                                                   │
                                     ┌─────────────┘
                                     ▼
                              [Query Store]
                              (Cassandra: query frequencies)

Write Pipeline:
  [Kafka] → [Spark Streaming] → [Cassandra] → [Trie Builder] → [Trie Servers]
```

---

## 5. Low-Level Design

### 5.1 Core Data Structure: Trie

```
Trie Node:
{
  char: 'a',
  children: Map<char, TrieNode>,
  top_k: [(query, score), ...],  // precomputed top-k at every node
  is_end: bool
}
```

**Why precompute top-k at every node?**
- Naive trie traversal for top-k = O(n) DFS → too slow
- Precomputed top-k → O(1) lookup per prefix
- Trade-off: more memory, but read latency drops to microseconds

**Top-k update strategy:**
- Score = `frequency * recency_weight`
- Recency weight: `exp(-λ * days_since_last_seen)`
- Rebuild top-k arrays bottom-up during nightly batch

### 5.2 Sharding the Trie

**Problem**: Single trie for 1B+ queries won't fit in one machine's RAM.

**Option A: Shard by first character**
- `a-m` → Server 1, `n-z` → Server 2
- ❌ Uneven distribution ('s' queries dominate)

**Option B: Shard by prefix range**
- Consistent hash on prefix → assign ranges
- ✅ Better distribution
- Build a prefix→shard routing table (stored in ZooKeeper)

**Option C: Two-level trie**
- Level 1 (single node): root + first 2 chars
- Level 2 (sharded): full subtrees
- ✅ Routing lookup is O(1), subtrees manageable size

**Chosen: Option B + Option C hybrid**
- Route by first 2 chars → pre-mapped shard
- Each shard holds full subtree for its prefix range

### 5.3 Caching Strategy (Multi-Level)

| Layer | What's Cached | TTL | Invalidation |
|-------|--------------|-----|-------------|
| Browser | Last 50 prefixes typed this session | Session | On page close |
| CDN | Top 10k most-queried prefixes | 1 hour | Push on trie rebuild |
| Redis | All prefixes served in last 24h | 5 min | LRU eviction |
| Trie Server (in-memory) | Full trie subtree | Until rebuild | On rebuild completion |

**Trade-off**: CDN cache with 1hr TTL means trending queries (e.g. breaking news) won't appear in suggestions for up to 1 hour.
**Mitigation**: Maintain a "trending" bypass list — queries with >10x normal frequency in last 5 min skip CDN cache.

### 5.4 Frequency Aggregation

**Why not update trie in real-time per query?**
- 100k QPS of writes → trie rebuild on every write is impossible
- Trie is read-optimized; concurrent writes require locking

**Batch approach:**
1. Each search submit → Kafka topic `raw-queries`
2. Spark Streaming → aggregate counts per 5-min window
3. Write to Cassandra: `(query, window_start, count)`
4. Hourly Spark batch → compute rolling 7-day frequency
5. Nightly rebuild → regenerate trie with updated scores

**Idempotency**: Kafka consumer uses exactly-once semantics (transactions). Cassandra writes use `UPDATE ... IF NOT EXISTS` + CRDT counters to avoid double-counting on retry.

### 5.5 API Design

```
GET /v1/autocomplete?q={prefix}&limit=10&locale=en-US

Response:
{
  "prefix": "appl",
  "suggestions": [
    {"query": "apple", "score": 0.98},
    {"query": "apple store", "score": 0.87},
    ...
  ],
  "served_from": "cache|trie|edge",
  "latency_ms": 12
}
```

**Debouncing**: Client fires request only after 150ms of no typing. Reduces server QPS by ~60%.

---

## 6. Scale Estimates

- 100M DAU, avg 5 searches/day = 500M queries/day
- Each search = ~5 keystrokes → 2.5B autocomplete requests/day
- ~29k RPS average, ~87k RPS peak (3x)
- Top 1M prefixes = ~1M × 10 suggestions × 50 bytes = **500 MB** in Redis
- Trie size: ~10GB in-memory across cluster (manageable)

---

## 7. Trade-Offs Summary

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Data structure | Precomputed top-k trie | On-demand DFS | O(1) reads vs O(n), at cost of more RAM |
| Frequency update | Batch (hourly) | Real-time stream | Avoids write contention; accepts stale trending |
| Sharding | Prefix range | Consistent hash on full string | Locality of prefix queries; same shard serves all "app*" |
| Cache TTL | 5min Redis / 1hr CDN | Longer TTL | Balance freshness vs infrastructure cost |
| Personalization | None (global top-k) | Per-user top-k | 10x simpler; personalization adds massive storage |

---

## 8. Real-World References

- **Google**: Uses a multi-level cache + prefix-range sharding. Engineering blog: [Google Search Autocomplete](https://www.blog.google/products/search/how-google-autocomplete-works-search/)
- **LinkedIn Typeahead**: Cleo system — uses a custom inverted index + caching layer. [LinkedIn Engineering Blog](https://engineering.linkedin.com/open-source/cleo-open-source-technology-behind-linkedins-typeahead-search)
- **Elasticsearch**: Uses `completion` suggester — FST (Finite State Transducer) stored in memory, ~1/10th the size of a trie
- **Facebook**: Uses prefix trees with learned frequency weights, real-time trending layer on top
