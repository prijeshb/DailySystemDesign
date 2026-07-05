# Search Autocomplete — Interview Q&A
*Date: 2026-05-21 | Format: Interviewer asks → Strong answer*

---

## Opening Questions

**Q: Design a search autocomplete system.**

> Start by clarifying scope:
> - How many users? (I'll assume 100M DAU)
> - Global or single region?
> - Personalized suggestions or global top-k?
> - Real-time trending or batch updates acceptable?
> - Mobile + web?
>
> Then lead with first principles: "Before jumping to data structures, let me think about what we actually need — users type a prefix and want relevant suggestions in <100ms. The core trade-off is: how fresh do suggestions need to be vs. how complex is the write path?"

---

## Functional Requirements / Scoping

**Q: Should suggestions be personalized or global?**

> For V1, global top-k. Personalization multiplies storage by MAU (100M users × 10k prefixes = impossible). Global gives 80% of value at 1% of complexity. Personalization can be layered on later by blending global trie results with a per-user query history vector.

**Q: How fast do new trending queries need to appear?**

> The honest answer is: it depends on business requirements. For a general search engine, 1–2 hour lag is acceptable. For a news or social media search, you need a real-time trending layer on top of the batch trie. I'd build both: batch trie for stable queries, streaming trending layer for spikes.

---

## Data Structure Deep Dive

**Q: Why a trie instead of a database with LIKE 'prefix%' query?**

> Database LIKE queries with leading wildcard can't use B-tree indexes efficiently. Even with prefix-only LIKE (no leading %), at 87k RPS hitting a SQL DB → deadlock city. A trie gives O(len(prefix)) lookup, and with precomputed top-k at every node, it's O(1) to return suggestions. The trie lives in-memory — microsecond reads vs. millisecond DB roundtrips.

**Q: What if two queries have the same prefix and same score?**

> Tie-break by: (1) query length (shorter = more specific, rank higher), (2) alphabetical order for determinism. The key is consistency — same prefix always returns same order, so CDN cache is effective.

**Q: How big is the trie in memory?**

> Back-of-envelope: 1M unique queries × avg 20 chars × 50 bytes/node ≈ ~1GB raw. With top-k arrays (10 suggestions × 50 bytes each × 1M nodes) ≈ 500MB overhead. Total ~1.5GB — fits on a single machine. But for 1B queries you'd need ~15GB → shard across 3-5 servers.

**Q: Why not use an inverted index instead of a trie?**

> Good question. Elasticsearch's `completion` suggester uses an FST (Finite State Transducer) which is 5-10x more memory-efficient than a trie and still gives O(len(prefix)) lookup. For large-scale deployments, FST wins on memory. Trie is simpler to reason about and implement from scratch — correct choice for an interview. In production, I'd evaluate both.

---

## Scaling Follow-Ups

**Q: How do you shard the trie?**

> Shard by prefix range: maintain a routing table `{prefix_range → shard_id}` in ZooKeeper. The 26² = 676 possible two-char prefixes map to shards. Hotspot mitigation: 's' is 10x more common than 'x' — assign 's' prefix range to 3 shards, 'x' to 0.3 of a shard (or merge with 'y'). The routing table is small enough to cache in every API server's memory.

**Q: How do you handle a cache stampede when Redis goes down?**

> Two techniques: (1) Probabilistic early expiration — stagger TTLs with ±30s jitter so keys don't all expire at once. (2) Request coalescing — first request for a prefix acquires a distributed lock, fetches from Trie, populates Redis. Concurrent requests for the same prefix wait on the lock (50ms timeout). If lock times out, they query Trie directly (fallback, not failure).

**Q: Your system serves 29k RPS average, 87k peak. How many servers?**

> Trie server: each handles ~5k RPS (in-memory trie, fast). Need 18 servers at peak. Add 30% buffer → 24 servers. With 2 replicas per shard → 72 Trie server instances. Redis: 3 shards, each primary + replica = 6 Redis nodes. API layer: stateless, auto-scale with load.

---

## Failure Scenarios

**Q: What happens if the Trie server for 'a*' prefix goes down?**

> Read replicas are promoted automatically (Redis Sentinel model). Load balancer health check triggers within 10s. In that window: API servers fall back to Redis cache (which has 5min TTL) → users get slightly stale but valid suggestions. No empty responses. After 10s, replica takes over → fully transparent. MTTR < 30s.

**Q: What if your batch Spark job produces a corrupted trie?**

> Blue/green deployment of the trie. New trie deploys to 10% "canary" fleet first. Automated validation runs: does "google" return expected results? If validation fails, new trie is discarded and old trie keeps serving. Canary traffic is monitored for 5min before full rollout. Each server also keeps previous snapshot on disk for instant rollback.

**Q: How do you prevent bad actors from gaming autocomplete?**

> Input validation: filter non-printable chars, limit query length to 100 chars. Write path: anomaly detector flags IPs/accounts submitting >500 identical queries/hr → rate limited. Frequency aggregation: use median rather than sum across time windows to dampen spikes. Manual review queue for newly trending terms that hit a threshold before surfacing in production trie.

---

## Trade-Off Questions

**Q: You chose batch over real-time for trie updates. What's the downside?**

> Trending queries take hours to appear in autocomplete. If someone famous tweets about a product and it goes viral at 2pm, the trie won't reflect it until the next hourly rebuild. Mitigation: real-time trending layer that runs in parallel — Flink detects queries >5x baseline frequency in a 5-min window and injects them into suggestions, bypassing the trie. This gives you both: stable trie for 99% of queries + fresh trending for viral events.

**Q: Why not personalize suggestions?**

> Storage math: 100M users × avg 500 personal prefixes × 10 suggestions × 50 bytes = 25 TB. That's manageable with Cassandra — so *technically* feasible. But then cache hit rate drops dramatically (can't cache per-user in CDN). And it adds a per-user lookup on every keystroke. My call: global trie + blend top-2 personal history entries client-side (stored in browser localStorage). Best of both worlds.

**Q: Your Redis TTL is 5 minutes. Why not longer?**

> Longer TTL = better performance but staler suggestions. 5min is a sweet spot: at 87k RPS, most prefixes get re-cached within seconds of expiry. The risk of a thundering herd on expiry is mitigated by TTL jitter. If I wanted longer TTL, I'd implement proactive cache refresh: before TTL expires, a background job pre-fetches top 10k prefixes and refreshes their cache entries.

---

## Design Extension Questions

**Q: How would you add support for typo correction?**

> Two options: (1) Offline: build a phonetic index (Soundex/Metaphone) alongside the trie — if no results for prefix, check phonetic neighbors. (2) Online: use edit distance (Levenshtein) to find nearest matching prefixes — expensive at scale. Better: pre-generate a "typo map" (common typos → correct prefix) from historical query correction data, store in Redis. "iphone" → "iphone", "iphoe" → "iphone". Covers 80% of typos with O(1) lookup.

**Q: How would you support multi-language autocomplete?**

> Unicode-aware trie: each node key is a Unicode code point, not ASCII char. For CJK languages, consider character n-gram index instead of prefix trie (users may search by keyword within a phrase, not always from the start). Separate trie instances per language/locale — routing by `Accept-Language` header. Shared infrastructure, separate data.

**Q: How would you A/B test a new ranking algorithm?**

> Route 5% of traffic to "variant" trie servers built with new scoring formula. Compare: CTR on suggestions (did user click a suggestion?), time-to-query (did they type less?), null-result rate (did autocomplete help?). Trie servers tag responses with `experiment_id` → analytics pipeline joins with user action events → significance test after 24hrs.

---

## Concepts Tested in This Problem

| Concept | Where It Appears |
|---------|-----------------|
| Trie / prefix tree | Core data structure |
| Precomputed aggregates | Top-k at every trie node |
| Sharding | Prefix-range shard assignment |
| Caching (multi-level) | Browser → CDN → Redis → Trie |
| Cache stampede | Thundering herd on TTL expiry |
| Idempotency | Exactly-once Kafka processing |
| Batch vs. streaming | Frequency aggregation tradeoff |
| Blue/green deploy | Safe trie rebuild rollout |
| CAP theorem | AP system: availability > consistency |
| Rate limiting | Abuse prevention on write path |
