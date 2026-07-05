# E-Commerce Product Search — System Design
**Date:** 2026-06-14

---

## First Principles: Do We Need This?

**Problem:** SQL `LIKE '%running shoes%'` on 500M products = full table scan = 30s+. Unusable.

**Why not just add DB indexes?**
- B-tree indexes work on exact/prefix matches, not full-text relevance
- Can't rank by "how well does this match?" — just binary match/no-match
- Can't handle synonyms (shoes = sneakers), typos, stemming (running = run)

**What we actually need:**
1. Sub-200ms search over 500M products
2. Relevance ranking (not just match)
3. Faceted filtering (price, brand, category)
4. Typo tolerance + synonyms
5. Real-time product index updates

Answer: **Elasticsearch** — purpose-built for this. Inverted index + BM25 scoring.

---

## Scale

- 500M products, avg document 5KB → 2.5TB raw data
- 100K searches/second (peak — sale days)
- 5K product updates/second (sellers updating prices, stock)
- Search P99 < 200ms
- Index lag: product update → searchable within 30s

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| **Product** | id, name, description, brand, category_path[], price, attributes{}, images[], seller_id, rating, rating_count, in_stock, created_at |
| **SearchQuery** | query_text, filters{price_range, brands[], categories[]}, sort, page, page_size |
| **SearchResult** | hits[]{product_id, score, highlights}, total, facets{}, took_ms |
| **IndexDocument** | ES representation of Product |
| **User** | id, purchase_history, click_history (for personalization) |

---

## Actions

1. **User searches** → query text + filters → ranked product list
2. **User filters** → apply facets (price range, brand, category)
3. **User autocompletes** → prefix → top suggestions
4. **Seller updates product** → propagates to search index within 30s
5. **Admin bulk-indexes** → full re-index without downtime

---

## Data Flow

### Write Path (Product Update → Indexed)
```
Seller API
    ↓
Product Service → MongoDB (source of truth)
                        ↓
                   Debezium CDC (tails MongoDB oplog)
                        ↓ Kafka topic: product.changes
                   Index Consumer Service
                        ↓
               Elasticsearch (upsert document)
                    index refresh every 1s → searchable
```

### Read Path (User Searches)
```
User
 ↓
API Gateway → Search Service
                   ↓
             Redis Cache (popular queries, TTL 5min)
                   ↓ miss
          Query Builder (parse text, apply filters, build ES DSL)
                   ↓
          Elasticsearch Cluster (BM25 + function_score)
                   ↓
          Personalization Layer (re-rank top-50 by user history)
                   ↓
          Product Enricher (fetch images/price from Redis/MongoDB)
                   ↓
              Response → User
```

---

## High Level Design

```
          ┌──────────────────────────────────┐
          │         Load Balancer            │
          └──────────────┬───────────────────┘
                         ↓
               ┌─────────────────┐
               │  Search Service │ (stateless, N replicas)
               └────┬────────────┘
                    │
         ┌──────────┼─────────────┐
         ↓          ↓             ↓
     Redis        Elasticsearch   MongoDB
     Cache        Cluster         (product details)
                    ↑
       ┌────────────┴───────────────┐
       │     Index Consumer         │
       │     (Kafka consumer)       │
       └────────────────────────────┘
                    ↑
               Kafka topic: product.changes
                    ↑
         MongoDB → Debezium CDC
```

---

## Low Level Design

### MongoDB Schema (Source of Truth)
```js
// Products collection
{
  _id: "prod_abc123",
  name: "Nike Air Zoom Running Shoes",
  description: "Lightweight responsive cushioning...",
  brand: "Nike",
  category_path: ["footwear", "shoes", "running"],
  price: 4999,
  currency: "INR",
  attributes: {          // variable schema — why MongoDB wins here
    size_us: [7, 8, 9, 10, 11],
    color: ["black", "white"],
    weight_grams: 280,
    gender: "unisex"
  },
  images: ["cdn.example.com/img/abc123_1.webp", ...],
  seller_id: "seller_xyz",
  rating: 4.3,
  rating_count: 1847,
  in_stock: true,
  updated_at: ISODate("2026-06-14T09:00:00Z")
}
```

**Why MongoDB over Postgres for product catalog:**
- Attributes are completely different per category (shoes have size, TVs have resolution, books have ISBN)
- Variable schema per document is MongoDB's core strength
- Self-contained document = single fetch for a product page
- **Trade-off:** No cross-product JOINs, no FK constraints (acceptable for catalog)

**Postgres still used for:** inventory counts, seller settlements, order management (transactional data).

---

### Elasticsearch Index Mapping
```json
{
  "settings": {
    "number_of_shards": 6,
    "number_of_replicas": 2,
    "analysis": {
      "analyzer": {
        "product_analyzer": {
          "tokenizer": "standard",
          "filter": ["lowercase", "stop", "product_synonyms", "english_stemmer"]
        }
      },
      "filter": {
        "product_synonyms": {
          "type": "synonym",
          "synonyms": ["shoes,sneakers,footwear", "tv,television", "laptop,notebook"]
        },
        "english_stemmer": { "type": "stemmer", "language": "english" }
      }
    }
  },
  "mappings": {
    "properties": {
      "product_id":      { "type": "keyword" },
      "name":            { "type": "text", "analyzer": "product_analyzer", "boost": 3 },
      "description":     { "type": "text", "analyzer": "product_analyzer" },
      "brand":           { "type": "keyword" },
      "category_path":   { "type": "keyword" },
      "price":           { "type": "scaled_float", "scaling_factor": 100 },
      "rating":          { "type": "float" },
      "rating_count":    { "type": "integer" },
      "in_stock":        { "type": "boolean" },
      "attributes":      { "type": "object", "dynamic": true },
      "updated_at":      { "type": "date" }
    }
  }
}
```

**Sharding (6 shards):**
- ES routes by `hash(product_id) % 6` → even distribution, no hotspot
- NOT by category — would create hotspot on popular categories (electronics)
- 6 primary shards × (1 primary + 2 replicas) = **18 total shard copies**.
  ES rule: no two copies of the same shard can live on the same node → need at least 3 nodes.
  But for even distribution: 18 shard copies across 18 data nodes = 1 shard/node.
  (Fewer nodes also work — e.g., 6 nodes each holding 3 shard copies — but 18 nodes = maximum parallelism + one node can die without losing any shard.)
  At 500M docs / 6 shards = ~83M docs/shard
- **Trade-off — why 6 shards specifically:**

  **More parallelism** (why more shards helps):
  Each query fans out to all primary shards simultaneously. 6 shards → 6 threads run the search in parallel on different slices of the data → faster than 1 big shard.

  **Coordination overhead** (why more shards hurts):
  For every query, the coordinating node must:
  1. Send request to all N shards
  2. Wait for all N to respond (slowest shard = your latency)
  3. Collect top-K from each shard (N × K results in memory)
  4. Merge + globally re-rank → return top-K

  With 100 shards: 100 network round-trips, 100 × K results merged. Master node manages 100 × 3 = 300 shard copies in cluster state → heavy for metadata operations (node joins, rebalance).

  **The math for 500M docs:**
  ```
  Target: ~50-100M docs/shard (ES recommendation)
  500M / 6 = ~83M docs/shard  ✓ in sweet spot
  
  Shard too large (e.g., 2 shards × 250M docs):
    → Single shard can't be split without full re-index
    → Segment merges slow (250M docs = huge SSTables)
    → JVM heap pressure on that node
  
  Too many shards (e.g., 50 shards × 10M docs):
    → 50 network calls per query instead of 6
    → Each shard has only 10M docs → merges finish in milliseconds but overhead dominates
    → ES cluster state: 50 × 3 replicas = 150 shard entries → slow master operations
    → Heap wasted: ES keeps per-shard metadata in master heap regardless of shard size
  ```

  **Rule:** Start with `ceil(total_docs / 50M)` shards. Pre-set higher if growth expected (can't split later without re-index). 6 shards for 500M gives 4× headroom to 2B docs before re-sharding.

---

### How the Inverted Index Works (ES Internal)

```
Products indexed:
  doc1: "Nike Air Zoom Running Shoes"
  doc2: "Adidas UltraBoost Running Shoes"
  doc3: "Puma Veloz Running Sneakers"

After analysis (lowercase + stemming + synonym expansion):
  doc1: [nike, air, zoom, run, shoe, sneaker, footwear]
  doc2: [adidas, ultraboost, run, shoe, sneaker, footwear]
  doc3: [puma, veloz, run, sneaker, shoe, footwear]

Inverted index:
  "nike"     → [doc1]
  "adidas"   → [doc2]
  "run"      → [doc1, doc2, doc3]
  "shoe"     → [doc1, doc2, doc3]
  "sneaker"  → [doc1, doc2, doc3]

Query "running shoes":
  tokens: [run, shoe, sneaker]  (after analysis)
  run matches:    [doc1, doc2, doc3]
  shoe matches:   [doc1, doc2, doc3]
  All match → scored by BM25
```

---

### BM25 Scoring + Function Score

```json
{
  "query": {
    "function_score": {
      "query": {
        "bool": {
          "must": [{
            "multi_match": {
              "query": "running shoes",
              "fields": ["name^3", "brand^2", "description"],
              "type": "best_fields",
              "fuzziness": "AUTO",
              "operator": "or"
            }
          }],
          "filter": [
            { "range":  { "price":    { "gte": 1000, "lte": 5000 } } },
            { "term":   { "in_stock": true } },
            { "terms":  { "brand":    ["Nike", "Adidas"] } }
          ]
        }
      },
      "functions": [{
        "field_value_factor": {
          "field": "rating",
          "factor": 1.2,
          "modifier": "log1p",
          "missing": 1
        }
      }, {
        "field_value_factor": {
          "field": "rating_count",
          "factor": 0.1,
          "modifier": "log1p",
          "missing": 0
        }
      }],
      "score_mode": "sum",
      "boost_mode": "multiply"
    }
  },
  "aggs": {
    "brands":       { "terms": { "field": "brand", "size": 20 } },
    "price_ranges": { "range": { "field": "price", "ranges": [
      { "to": 1000 }, { "from": 1000, "to": 3000 },
      { "from": 3000, "to": 7000 }, { "from": 7000 }
    ]}},
    "avg_rating":   { "avg": { "field": "rating" } },
    "categories":   { "terms": { "field": "category_path", "size": 10 } }
  },
  "highlight": {
    "fields": { "name": {}, "description": { "fragment_size": 150 } }
  },
  "from": 0,
  "size": 20
}
```

**BM25 formula intuition:**
```
score = Σ_terms [ IDF(term) × TF_sat(term, doc) ]

IDF(term) = log(1 + (N - df + 0.5) / (df + 0.5))
  High IDF = rare term = more informative ("ultraboost" > "shoes")

TF_sat = saturated TF (diminishing returns — doc with 10 matches ≠ 10× doc with 1 match)
  TF_sat = freq / (freq + k1 × (1 - b + b × len/avg_len))
  k1=1.2, b=0.75 (ES defaults)

name field boost ^3: multiplies IDF×TF by 3 for name matches
function_score (rating): multiplies final score by log(1 + rating × 1.2)
  → popular high-rated products rank above equal-text-relevance products
```

**Trade-off of rating boost:**
- Pro: better UX — users trust popular products
- Con: new products never surface even if better → new product penalty / freshness boost for recent listings

---

### Caching Layer

```
Key:   search:{md5(normalized_query + filters_sorted + sort + page)}
Value: {hits: [...], total, facets, timestamp}
TTL:   popular queries = 5min, others = 1min

Normalization before hashing:
  "Running SHOES" → "running shoes"
  filters sorted alphabetically (brands=[Nike,Adidas] = brands=[Adidas,Nike])

Cache hit rate target: ~40-60% (Pareto: top 20% queries = 80% traffic)

What NOT to cache:
  - Personalized results (user_id in query)
  - Queries with user-specific filters (saved addresses, etc.)
  - Single-product lookups (fetched from MongoDB directly)

Pre-warming on sale events:
  - Analytics predicts top 1000 queries for upcoming sale
  - Cron job pre-populates Redis 30min before sale start
```

**Trade-off:** Cache reduces read latency from 50ms → 2ms for popular queries, but serves results up to 5min stale. Acceptable for search (price updates are filtered server-side if critical).

---

### Autocomplete

**Two approaches:**

| | ES Completion Suggester | Separate Prefix Index |
|--|--|--|
| How | In-memory FST on indexed suggestions | Redis ZSET with lex scoring |
| Latency | ~2ms | ~1ms |
| Flexibility | Limited (sorted by weight only) | Full control |
| Memory | Part of ES heap | Separate Redis |
| Use | Production autocomplete | Simple or custom ranking |

**ES Completion Suggester setup:**
```json
{
  "suggest_field": {
    "type": "completion",
    "analyzer": "simple",
    "contexts": [{ "name": "category", "type": "category" }]
  }
}

// Index: add suggestion with weight = sales_count
"suggest_field": {
  "input": ["Nike Air Zoom", "Nike Air", "Nike"],
  "weight": 15000,
  "contexts": { "category": "running" }
}
```

**Query:** User types "nik" → return top-5 by weight → <5ms

---

### Personalization Layer

After ES returns top-50 results, re-rank before returning top-20:

```python
def personalize(user_id, es_hits):
    user_profile = Redis.get(f"user:{user_id}:profile")
    # profile: {preferred_brands: {Nike: 0.8, Adidas: 0.3}, categories: {...}}
    
    for hit in es_hits:
        brand_boost = user_profile.preferred_brands.get(hit.brand, 0) * 0.2
        category_boost = user_profile.categories.get(hit.category, 0) * 0.1
        hit.final_score = hit.es_score + brand_boost + category_boost
    
    return sorted(es_hits, key=lambda x: x.final_score, reverse=True)[:20]
```

**Trade-off:**
- Personalization improves conversion (user sees preferred brands first)
- Makes caching harder (can't cache per-user results at scale)
- Solution: cache ES results, personalize in-memory per user (fast, no cache miss)

---

### Index Update Pipeline (30s SLA)

```
MongoDB write
    ↓ (via Debezium, ~50ms)
Kafka topic: product.changes
    ↓ (Index Consumer, ~1s processing + batch)
ES bulk API (batch up to 500 docs per call)
    ↓ (ES index refresh interval = 1s)
Searchable

Total: ~2-5s typical. 30s SLA = headroom for consumer lag.
```

**Bulk indexing for efficiency:**
```python
# Index Consumer
async def consume_loop():
    buffer = []
    async for msg in kafka_consumer:
        buffer.append(build_es_doc(msg))
        if len(buffer) >= 500 or buffer_age > 2s:
            es.bulk(index="products", body=buffer)
            buffer.clear()
```

**Trade-off:** Batching reduces ES write load (fewer round-trips) but adds up to 2s latency. For product search, 2s is fine. For inventory counts ("only 2 left"), fetch real-time from MongoDB.

---

### Zero-Downtime Re-index (Blue-Green)

When schema changes require full re-index:
```
1. Create new index: products_v2 (new mapping)
2. Start dual-write: all updates go to products_v1 AND products_v2
3. Backfill: stream all docs from MongoDB → products_v2
4. Verify: compare search results between v1 and v2 on sample queries
5. Alias flip: products_alias → products_v2 (atomic, instant)
6. Drop products_v1

Traffic always hits alias, never the versioned index.
```

---

## Component Trade-Off Summary

| Choice | Picked | Alternative | Why |
|--------|--------|-------------|-----|
| Search engine | Elasticsearch | Solr, OpenSearch, Typesense | ES: widest ecosystem, best LTR support, aggregations |
| Product DB | MongoDB | Postgres | Variable per-category schema, document model |
| Cache | Redis | Memcached | Redis: rich data types, TTL, cluster mode |
| Index pipeline | Kafka+Debezium | Direct ES write from API | Decoupled: search index is secondary, DB is truth |
| Sharding key | product_id hash | category | Even distribution, no hotspot |
| Autocomplete | ES Completion | Redis ZSET | ES: context-aware, no extra infra |
| Relevance | BM25 + function_score | LTR ML model | BM25: interpretable, no infra; LTR for future |
