# E-Commerce Search — Interview Q&A
**Date:** 2026-06-14

---

## Opening Questions

**Q: Design a product search system for an e-commerce platform like Amazon.**

Think out loud from first principles:
1. Why can't we use SQL? → LIKE queries don't scale, no relevance ranking
2. What do we need? → Full-text match, typo tolerance, facets, ranking, <200ms
3. What's the scale? → Ask: 100M or 1B products? 10K or 100K RPS?
4. Core answer: Elasticsearch for search, MongoDB for product catalog, Kafka for index pipeline

Hit: entities → actions → data flow → HLD → LLD

---

## Requirement Clarification Questions (Ask These First)

- How many products? (100M vs 500M changes sharding strategy)
- What's acceptable search latency? (P50 vs P99)
- Is personalization in scope?
- How quickly must product updates appear in search? (30s vs real-time vs eventual)
- Is autocomplete in scope?
- Multi-language / multi-region?
- Do we need to support sellers adding custom product attributes?

---

## Component Deep-Dives

**Q: Why Elasticsearch over a regular database index?**

SQL B-tree index: binary — either matches exactly or doesn't. No concept of "how well" it matches.

ES inverted index: maps each term to documents containing it. After analysis (tokenization, stemming, synonyms), a query for "running shoes" matches docs containing "run", "shoe", "sneaker". Then BM25 scores each doc by term frequency × inverse document frequency × field length normalization.

Also: ES aggregations for facets are native, efficient (doc_values for numeric/keyword fields). SQL `GROUP BY` on 500M rows = full scan.

---

**Q: Walk me through how BM25 scoring works.**

BM25 improves on TF-IDF in two ways:
1. **Term frequency saturation** — doc with 10 occurrences of "shoe" is not 10× better than doc with 1 occurrence. BM25 applies diminishing returns.
2. **Length normalization** — a short product name mentioning "shoe" once is more relevant than a long description mentioning it once among 500 words.

Formula: `score = Σ [ IDF(term) × (TF × (k1+1)) / (TF + k1×(1-b+b×len/avg_len)) ]`

In practice, the key insight for the interview: common words ("the", "of") have low IDF (appear everywhere) so contribute little. Rare specific words ("ultraboost", "gore-tex") have high IDF and dominate the score.

Field boost (`name^3`) multiplies the IDF×TF contribution for name matches by 3 — because finding "Nike" in the product name is more relevant than in the description.

---

**Q: How do you handle typos in search queries?**

ES `fuzziness: "AUTO"`:
- 1-2 character strings: exact match only
- 3-5 characters: Levenshtein distance 1 (1 edit)
- 6+ characters: Levenshtein distance 2

"Nikie shoes" → distance 1 from "Nike" → matches Nike products.

**Trade-off:** Fuzzy search uses Levenshtein automaton under the hood — faster than naive O(m×n) but still slower than exact match. Too high fuzziness = irrelevant results (edit distance 2 matches unexpected words).

**Follow-up: What about phonetic matching?**
Soundex/Metaphone filters in ES analyzer: "phishing" vs "fishing". Useful for proper nouns (brand names). Add as a secondary analyzer, not primary.

---

**Q: How do synonyms work?**

ES synonym token filter in the analysis chain. Applied at index time and/or query time.

Query-time synonyms preferred:
- No need to re-index when synonym list changes
- Can update synonym file and reload analyzer without downtime

Synonym file: `shoes,sneakers,footwear => shoe,sneaker,footwear` (after stemming)

**Trade-off:** Synonyms expand the query → more candidates → slightly slower. Manage list carefully: too many synonyms = precision drops. "tv,television" is good. "apple,fruit" is bad if you sell both electronics and produce.

---

**Q: How do you implement faceted search (filters)?**

ES aggregations — run alongside the main query:
```json
"aggs": {
  "brands": { "terms": { "field": "brand", "size": 20 } },
  "price_ranges": { "range": { "field": "price", "ranges": [...] } }
}
```

**Important UX detail:** Post-filter vs filter

If user applies `brand=Nike` filter:
- **filter** in query: facet counts only show Nike products → other brand counts disappear from UI
- **post_filter**: main results filtered, but aggregations computed on unfiltered results → other brands still show counts (correct behavior)

Always use `post_filter` when the same field is used for both filtering results and showing facet counts.

---

**Q: How do you keep the search index in sync with product updates?**

Write path: MongoDB (source of truth) → Debezium CDC (tails oplog) → Kafka topic → Index Consumer → ES bulk upsert.

Why not write directly to ES from the product service?
- ES is secondary — MongoDB is truth. Tight coupling would mean product service must know about ES.
- If ES is down, product service would fail too. Decoupled = separate failure domains.
- Kafka buffers: if ES is overloaded, messages queue up in Kafka and drain when ES recovers.

**Follow-up: What if Debezium is down for 2 days and the oplog rotates?**
Must do a full re-sync: stream all MongoDB documents via a snapshot consumer → publish to same Kafka topic → ES consumers re-index. Debezium supports this with `snapshot.mode=initial`.

**Follow-up: How do you ensure no duplicate indexing during re-sync?**
ES upsert by product_id is idempotent. Indexing the same document twice → second write just overwrites with same data. Safe.

---

**Q: How does the sharding strategy affect query performance?**

6 primary shards → ES fans out each query to all 6 shards in parallel, collects top-K from each, merges and ranks.

**Why not shard by category?**
"Electronics" shard gets 5× traffic of "Books" shard → hotspot. Product_id hash → even distribution.

**Trade-off of more shards:**
More shards = more parallelism but more coordination overhead (more network round-trips to collect results). For 500M docs, 6 shards is sweet spot. Over-sharding causes "too many small shards" problem where overhead dominates.

**Too few shards:** Single shard can't be split without re-indexing. Start with more rather than fewer.

---

**Q: How do you do zero-downtime re-indexing when you change the ES mapping?**

Index alias pattern:
1. Create `products_v2` with new mapping
2. Start dual-write: all product updates go to both `products_v1` and `products_v2`
3. Backfill all existing docs from MongoDB → `products_v2`
4. Verify: run sample queries against both, compare results
5. Atomic alias flip: `products_alias → products_v2`
6. Stop dual-write to `products_v1`
7. Delete `products_v1`

Traffic always hits the alias. Step 5 is instant from user perspective.

---

**Q: How do you handle a sudden 10× traffic spike (flash sale)?**

1. **Pre-warming the cache:** Analytics predicts top queries → pre-compute and cache in Redis 30min before sale.
2. **ES query timeout:** All queries have `timeout: 200ms` → partial results returned rather than blocking.
3. **Circuit breaker on ES:** If ES slows down, serve cached results.
4. **Rate limiting:** API Gateway caps at 80K RPS. Excess → 429 with `Retry-After: 2s`.
5. **ES horizontal scaling:** Add more data nodes. ES auto-rebalances shards. Pre-scale if sale is known.

---

**Q: How do you rank search results beyond just text relevance?**

`function_score` in ES query:
- BM25 text relevance (base)
- Rating boost: `log1p(rating) × 1.2`
- Popularity boost: `log1p(sales_count) × 0.1`
- Freshness boost: exponential decay on `created_at` for new listings
- Personalization: post-ES re-rank based on user's brand/category affinity

**Trade-off of popularity boost:**
- Good: users trust bestsellers
- Bad: new products never rank → marketplace becomes a monopoly of old products
- Fix: separate "boost for new products in first 30 days" counter-acts this

**Follow-up: When would you use Learning to Rank (LTR)?**
When you have enough click/purchase signal to train a model. LTR uses gradient boosting (LambdaMART) on features like text relevance score, price, rating, click-through rate, brand affinity. Needs ~months of data and ML infra. BM25+function_score is the right starting point.

---

**Q: What's your caching strategy? What can and can't you cache?**

Cache: `Redis key = md5(normalized_query + sorted_filters + sort + page)`

**Can cache:**
- Popular queries (same query from many users)
- Category browse pages (no query text, just filters)
- Autocomplete suggestions

**Cannot cache:**
- User-personalized results (unique per user)
- Ultra-specific long-tail queries (cache miss rate ~100%, wastes memory)
- Results including real-time inventory ("only 2 left")

**Cache TTL:** 5min for popular queries. If a product is updated (new price), cache shows stale price for up to 5min — acceptable because product detail page (post-click) fetches real-time data.

---

**Q: What happens if Elasticsearch is completely down?**

Circuit breaker: 5 failures → OPEN → fail fast.

Fallback waterfall:
1. Redis has cached recent query results → serve stale (may be 5min old)
2. Redis also has "popular products" list (top-500 by sales, refreshed hourly) → serve as generic results
3. Neither available → return empty results + "Search is temporarily unavailable" message

**What we never do:**
- Show a 500 error page
- Expose stack traces
- Let requests queue up and time out after 30s (thread pool exhaustion → cascade)

---

## Advanced Follow-Ups

**Q: How would you add multi-language support?**

Per-language analyzer chain (different stemmers, stopword lists):
- English: standard tokenizer + english stemmer
- Hindi: indic tokenizer + hindi stopwords
- Japanese: kuromoji analyzer (completely different tokenization — no spaces between words)

Options:
1. **Separate field per language:** `name_en`, `name_hi`. Query routes to right field based on user locale. Clean, but bloats mapping.
2. **Separate index per language:** `products_en`, `products_hi`. More isolation, more infra.
3. **Single field with language detection:** Auto-detect query language → apply appropriate analyzer at query time. Risky (detection error = bad results).

For global e-commerce: option 2 (separate indices per language) is cleanest.

---

**Q: How would you detect that search quality degraded?**

Metrics to monitor:
- **Click-through rate (CTR):** If users stop clicking search results → relevance dropped
- **Zero-result rate:** % of queries returning 0 results → track and alert
- **Search abandonment:** User searches, sees results, leaves without clicking
- **Position of first click:** If average click position moves from 1.2 → 3.4 → results are worse
- **Purchase conversion from search:** Ultimate signal

When a deploy degrades search quality:
- These metrics drop within minutes for A/B test group
- Auto-rollback if zero-result rate > baseline + 5%

---

**Q: How do you handle a product that has variants (same shirt, different colors/sizes)?**

Two approaches:

**Parent-child model:** One parent doc (shirt), child docs per variant. ES join query. Pro: filter by variant attributes while searching for parent. Con: complex queries.

**Flat model (simpler, more common):** Each variant is a separate document. Deduplicate at display time by `parent_product_id`. Search returns any variant → show the parent with available variants.

For interviews: recommend flat model. Simpler, faster, ES handles it natively. Deduplication is a display concern, not a search concern.

---

## Common Interviewer Traps

**Trap 1:** "Why not just use PostgreSQL full-text search?"
- Fine for <10M rows
- At 500M rows: even with `tsvector` index, faceted aggregations + relevance ranking at 100K RPS is not achievable
- ES is horizontally scalable, Postgres full-text is not

**Trap 2:** "Can we write directly to ES instead of going through Kafka?"
- Can, but: what if ES is down when seller updates product? Update lost.
- MongoDB is source of truth. ES is derived. Never let ES be ahead of MongoDB.

**Trap 3:** "Should we cache ES results in the Search Service locally?"
- In-process cache (e.g., Caffeine): works for single instance. Doesn't share across N instances.
- Redis: shared, consistent cache across all Search Service instances. Always prefer.

**Trap 4:** Forgetting that filter ≠ post_filter in ES
- filter: reduces documents before aggregation → facet counts wrong
- post_filter: filters results but not aggregations → facet counts correct
- Always use post_filter when the same field is both filtered and aggregated
