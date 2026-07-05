# System Design: News Aggregator Platform
## Interview Preparation Guide

**Date**: May 6, 2026  
**Scope**: Scalable news feed aggregation system  
**Author**: Interview Prep Task

---

## 1. REQUIREMENTS GATHERING

### Functional Requirements
- **Feed Aggregation**: Ingest articles from multiple sources (RSS, APIs, web scrapers)
- **Feed Ranking**: Personalized feed based on user preferences and engagement
- **Search**: Full-text search across archived articles
- **Bookmarking**: Users can save articles for later
- **Recommendations**: Content-based and collaborative filtering
- **Multi-platform**: Web, mobile, API access

### Non-Functional Requirements

| Metric | Target | Notes |
|--------|--------|-------|
| **Availability** | 99.9% (4.38 hours/month downtime) | SLA requirement |
| **Latency - Feed Load** | <200ms p99 | User-facing metric |
| **Latency - Search** | <500ms p99 | Acceptable for search |
| **Throughput** | 1M articles/day ingestion | From 5000+ sources |
| **Scale** | 50M users (10% DAU = 5M) | Growth target |
| **Data Retention** | 6 months hot, 2 years archived | Balance cost vs. search functionality |
| **Cost** | Optimize for per-user economics | Lean infrastructure |

### Constraints
- Team: 20-30 engineers
- Timeline: 6-month MVP → scale
- Tech Stack: Cloud-agnostic (multi-region capable)
- Compliance: GDPR, data privacy

---

## 2. HIGH-LEVEL ARCHITECTURE

```
┌─────────────────────────────────────────────────────────────┐
│                      EXTERNAL SOURCES                        │
│     (RSS Feeds, NewsAPI, Reddit, Twitter, Web Scrape)       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │  Feed Ingestion Service │  (Scale: 10-20 instances)
        │  - Deduplication       │  Rate: 10K articles/min
        │  - Validation          │
        └────┬───────────────────┘
             │
    ┌────────┼────────────┐
    │        │            │
    ▼        ▼            ▼
┌────────┐ ┌──────────┐ ┌──────────┐
│Kafka   │ │MongoDB   │ │Redis     │
│Topics  │ │Raw Docs  │ │Dedup Set │
└────────┘ └──────────┘ └──────────┘
    │
    ▼
┌─────────────────────────────┐
│ Processing Pipeline         │
│ - NLP/Classification        │
│ - Duplicate Detection       │
│ - Link Expansion            │
└────────┬────────────────────┘
         │
    ┌────┴────┐
    │          │
    ▼          ▼
┌─────────┐ ┌──────────────┐
│Postgres │ │Elasticsearch │
│Articles │ │Full-text idx │
└────┬────┘ └──────────────┘
     │
  ┌──┴──┐
  │     │
  ▼     ▼
┌──────────────────┐  ┌──────────────┐
│Ranking/Feed Svc  │  │Recommendation│
│- Personalization │  │  Engine      │
│- Timeline        │  │  (Collaborative│
└────┬─────────────┘  │   Filtering) │
     │                └──────────────┘
     │
     ▼
┌──────────────────┐
│API Gateway       │
│(Rate Limiting)   │
└─────────────────┘
     │
  ┌──┴──┬────┐
  ▼     ▼    ▼
 Web  Mobile Cache
                │
                ▼
            ┌─────────┐
            │CDN/Edge │
            └─────────┘
```

---

## 3. DETAILED COMPONENT DESIGN

### 3.1 Feed Ingestion Service

**Purpose**: Fetch articles from diverse sources at scale

**Design Choices**:
```
Sources Strategy:
├─ RSS Feeds (40% sources)
│  └─ Polling every 15 mins per feed
│  └─ Exponential backoff for dead feeds
├─ Webhook-based (20% sources)
│  └─ Real-time push from news providers
└─ Web Scraping (40% sources)
   └─ Headless browser pool
   └─ Intelligent scheduling based on publish frequency
```

**Implementation Details**:
- **Rate Limiting**: Token bucket per source
- **Retry Logic**:
  ```
  Attempt 1: Immediate
  Attempt 2: +5 seconds (exponential backoff)
  Attempt 3: +15 seconds
  Attempt 4: +60 seconds
  Max attempts: 4 (total window: ~80 seconds)
  
  Circuit breaker: Fail source after 10 consecutive failures
  ```

### 3.2 Message Queue: Kafka

**Why Kafka?**
- Durability: Articles buffered if downstream services slow
- Decoupling: Ingestion ≠ Processing
- Replay capability: Reprocess articles if logic changes
- Scaling: Multiple consumer groups for different tasks

**Topic Design**:
```
raw-articles
├─ Partitions: 100 (by source hash)
├─ Replication: 3
├─ Retention: 7 days
└─ Throughput: ~12 articles/second (1M/day ÷ 86,400s)

processed-articles (output)
enriched-articles (NLP, classification)
failed-articles (DLQ)
```

---

## 4. TRADE-OFF ANALYSIS

### 4.1 Cache Layer: Redis vs. Memcached vs. DynamoDB

| Decision | Redis | Memcached | DynamoDB |
|----------|-------|-----------|----------|
| **Use Case** | Personalized feed (hot path) | Session cache | Cold storage cache |
| **Latency** | <1ms | <1ms | 5-10ms |
| **Persistence** | Yes (RDB, AOF) | No | Yes (native) |
| **Cost** | Medium | Low | High (per request) |
| **Complexity** | High (replication, failover) | Low | Low (managed) |

**Decision**: **Redis for hot feed cache + Redis for dedup set during ingestion**

**Trade-offs**:
- ✅ Sub-millisecond response for user feed
- ✅ Can persist dedup state across restarts
- ❌ Operational complexity (sentinel/cluster mode)
- ❌ Memory cost at scale (5M DAU × 100KB feed = 500GB)

**Mitigation**:
- Use Redis Cluster (sharding by user_id)
- TTL of 12 hours per feed (refresh frequency)
- LRU eviction policy (remove least recently used)

---

### 4.2 Database: PostgreSQL vs. MongoDB vs. Cassandra

| Decision | PostgreSQL | MongoDB | Cassandra |
|----------|-----------|---------|-----------|
| **Consistency** | ACID | Eventual | Eventual |
| **Write Throughput** | ~10K/sec | ~50K/sec | ~100K/sec |
| **Query Flexibility** | Full SQL | Document queries | Limited (key-value) |
| **Cost** | Low | Medium | High |
| **Replication** | Streaming | Native | Native (P2P) |

**Decision**: **PostgreSQL for metadata + Cassandra for time-series articles**

**Rationale**:
- **PostgreSQL**: User accounts, bookmarks, preferences (ACID required)
- **Cassandra**: Articles by date (write-heavy, time-series access pattern)

**Schema**:
```sql
-- PostgreSQL
CREATE TABLE users (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR UNIQUE NOT NULL,
  created_at TIMESTAMP
);

CREATE TABLE bookmarks (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT REFERENCES users(id),
  article_id BIGINT,
  created_at TIMESTAMP,
  INDEX idx_user_created (user_id, created_at DESC)
);

-- Cassandra (time-series)
CREATE TABLE articles (
  source_id INT,
  published_date DATE,
  article_id UUID,
  title TEXT,
  content TEXT,
  url TEXT,
  PRIMARY KEY ((source_id, published_date), article_id)
) WITH CLUSTERING ORDER BY (article_id DESC);
```

**Trade-offs**:
- ✅ Separation of concerns (transactional vs. analytical)
- ✅ Cassandra scales horizontally for writes
- ❌ Complex join queries require application-level logic
- ❌ Data consistency between two databases

---

### 4.3 Search: Elasticsearch vs. PostgreSQL Full-Text

| Feature | Elasticsearch | PostgreSQL FTS |
|---------|---|---|
| **Relevance Ranking** | TF-IDF, BM25 | Basic scoring |
| **Faceted Search** | Native | Requires app logic |
| **Latency** | 50-100ms (w/ indexing) | 200-500ms |
| **Scaling** | Horizontal | Vertical |
| **Cost** | High (many nodes) | Low |
| **Complexity** | High (tuning, rebalancing) | Low |

**Decision**: **Elasticsearch for full-text search**

**Trade-offs**:
- ✅ Powerful ranking and faceting for user experience
- ✅ Can index billions of documents
- ❌ Operational overhead (cluster management)
- ❌ Duplicate storage (ES indexes + PostgreSQL data)

**Mitigation**:
- Use async indexing (ES lag acceptable: 30-60 seconds)
- TTL per index (keep only 6 months of searchable articles)
- Compression settings to reduce memory footprint

---

### 4.4 Deduplication Strategy

**Problem**: Same article published by multiple sources

**Options**:

1. **Hash-based (URL)**: Fast but unreliable
   ```
   hash(url) → Redis Set
   Dedup check: O(1)
   ```
   - ✅ Fast
   - ❌ Same article, different URLs = not detected

2. **Content Hash (Simhash)**: Fuzzy matching
   ```
   simhash(content) with Hamming distance < 3
   ```
   - ✅ Detects rephrased articles
   - ❌ Higher computational cost
   - ❌ Requires ML/NLP pipeline

3. **Hybrid**: URL first, then Simhash on miss
   ```
   IF url_hash in redis THEN SKIP
   ELSE compute simhash and check Cassandra
   ```
   - ✅ Best accuracy
   - ✅ Optimized path for exact duplicates
   - ❌ More complex

**Decision**: **Hybrid approach**

```
Ingestion Pipeline:
1. Extract URL from article
2. url_hash = sha256(normalize_url(url))
3. IF url_hash in Redis dedup-set → SKIP
4. ELSE:
   - Compute simhash(article_content)
   - Query Cassandra for similar hashes
   - If match found with similarity > threshold → SKIP
   - ELSE:
     - Add to Redis dedup-set (TTL: 30 days)
     - Insert into Cassandra
     - Publish to Kafka for downstream processing
```

**Cost Analysis**:
- Simhash computation: ~5-10ms per article
- Cassandra lookup: ~10-20ms
- Total: ~15-30ms per article (acceptable at 1M/day = 11.5 articles/sec)

---

## 5. FAILURE SCENARIOS & RESILIENCE

### 5.1 Failure Matrix

```
┌──────────────────────────────────┬──────────────────┬──────────────────┐
│ Component Failure                │ Impact           │ Mitigation       │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Feed Source (RSS down)           │ No new articles  │ Graceful degrade │
│                                  │ from source      │ Serve cached     │
│                                  │                  │ Return 204 for   │
│                                  │                  │ empty sources    │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Kafka Broker (1/3 down)          │ Degraded writes  │ Replication: 3   │
│                                  │ ~33% throughput  │ Automatic failover│
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Kafka Full (disk space)          │ Cannot ingest    │ Circuit breaker   │
│                                  │ articles         │ Alert ops team   │
│                                  │                  │ Manual cleanup   │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ PostgreSQL (primary down)        │ No user writes   │ Failover replica │
│                                  │ (bookmarks)      │ RTO: ~30 seconds │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Redis Cluster (quorum lost)      │ Feed cache miss  │ DB fallback      │
│                                  │ → DB hit         │ Latency: 100ms   │
│                                  │                  │ vs 1ms normal    │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Elasticsearch (node down)        │ Search degraded  │ Sharded indices  │
│                                  │ One shard × N    │ (n_replicas=2)   │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ Processing Service (memory leak) │ Slow processing  │ Health checks    │
│                                  │ → queue backup   │ Auto-restart     │
│                                  │                  │ every 24hrs      │
├──────────────────────────────────┼──────────────────┼──────────────────┤
│ API Gateway (rate limiter bug)   │ All users blocked│ Feature flag to  │
│                                  │                  │ disable limiter  │
│                                  │                  │ Manual override  │
└──────────────────────────────────┴──────────────────┴──────────────────┘
```

### 5.2 Cascade Failure Scenarios

**Scenario 1: Slow Database**
```
1. PostgreSQL reaches connection limit (connection pooling issue)
2. API Gateway timeouts increase
3. Feed requests are rejected (503 Service Unavailable)
4. Client apps implement retry storms
5. Traffic spike overwhelms API Gateway

Prevention:
- Connection pooling with max_overflow = 10
- Circuit breaker: If error_rate > 10% for 10s → reject with 429
- Request queue on API Gateway (max 5000 requests)
- Graceful degradation: Serve stale feed from Redis cache
```

**Scenario 2: Kafka Partition Leader Lost**
```
1. Network partition in Kafka cluster
2. Replicas elect new leader (takes 5-30 seconds)
3. Producers get "not leader" errors
4. Ingestion service retries exponentially
5. Unprocessed articles queue in memory

Prevention:
- min_insync_replicas = 2 (ensures durability)
- Retry backoff with jitter (prevents thundering herd)
- Circuit breaker: If Kafka unreachable for 60s → queue to local SQLite
- Process local SQLite once Kafka recovers
```

**Scenario 3: Elasticsearch Indexing Lag During Spike**
```
1. 10M articles ingested in 1 hour (spike from viral event)
2. ES indexing cannot keep up (bulk request queue fills)
3. Search returns old articles (6 hours old)
4. Users complain about stale search results

Prevention:
- ES write queue buffering (async indexing)
- Search fallback: PostgreSQL full-text search for last 24 hours
- Cache search results (top 1000 queries) for 5 minutes
- Document lag metrics in API response headers
```

---

## 6. SCALING STRATEGY

### 6.1 Horizontal Scaling Tiers

**Tier 1: 100K DAU (Year 1)**
```
Ingestion: 2 services × 3 replicas = 6 instances
Processing: 4 services × 4 replicas = 16 instances
Feed Ranking: 2 services × 4 replicas = 8 instances
DB: PostgreSQL (1 primary, 1 replica), Redis (3 nodes)
Cache: 10GB Redis
```

**Tier 2: 1M DAU (Year 2)**
```
Ingestion: 5 services × 5 replicas = 25 instances
Processing: 10 services × 8 replicas = 80 instances
Feed Ranking: 5 services × 8 replicas = 40 instances
Cassandra: 6 nodes (3 datacenters × 2)
DB: PostgreSQL (primary, 2 replicas), Redis Cluster (9 nodes)
Cache: 500GB Redis
```

**Tier 3: 50M DAU (Year 3)**
```
Multi-region (US-East, US-West, EU, APAC)
Per region: 10-15 Cassandra nodes
Global: 3+ Kafka clusters with cross-region replication
Read replicas: PostgreSQL in each region
```

### 6.2 Cost Optimization

| Component | Year 1 | Year 2 | Year 3 | Note |
|-----------|--------|--------|--------|------|
| Compute | $50K | $200K | $1M | Spot instances where possible |
| Storage | $10K | $100K | $500K | S3 for archived articles |
| Database | $30K | $150K | $800K | RDS → Self-managed at scale |
| Messaging | $5K | $30K | $150K | Kafka cluster grows |
| **Total** | **$95K** | **$480K** | **$2.45M** | Per-DAU cost: $19 → $0.049 |

---

## 7. MONITORING & OBSERVABILITY

### Key Metrics Dashboard

```
Real-Time Metrics:
├─ Ingestion
│  ├─ Articles/sec (target: 11.5)
│  ├─ Source availability (target: 99%)
│  ├─ Dedup rate (expect: 20-30%)
│  └─ Kafka lag (alert if > 5 min)
│
├─ Processing
│  ├─ Queue depth (alert if > 100K)
│  ├─ Processing latency p99 (target: <2s)
│  └─ Error rate (alert if > 1%)
│
├─ API
│  ├─ Feed load latency p99 (target: <200ms)
│  ├─ Cache hit ratio (target: > 80%)
│  └─ Error rate (target: < 0.1%)
│
└─ Infrastructure
   ├─ Database connections (alert if > 80%)
   ├─ Redis memory (alert if > 85%)
   └─ ES heap usage (alert if > 80%)
```

### Alerting Rules

```
CRITICAL:
- Feed load latency p99 > 500ms
- API error rate > 1%
- Kafka lag > 30 minutes
- Database replication lag > 10 seconds
- Cache eviction rate > 50%

WARNING:
- Feed load latency p99 > 250ms
- API error rate > 0.5%
- Kafka lag > 10 minutes
- Source availability < 95%
```

---

## 8. MIGRATION STRATEGY (MVP → Scale)

### Phase 1: MVP (Months 1-2)
```
Simplified Architecture:
├─ Single PostgreSQL instance (replication added later)
├─ Redis for cache (single node, upgraded to cluster in Phase 2)
├─ No Cassandra (use PostgreSQL for all data)
├─ No Elasticsearch (use PostgreSQL full-text search)
├─ Sync ingestion (no Kafka)

Capacity: ~1K DAU
Deployment: Single region (us-east-1)
```

### Phase 2: Scale (Months 3-4)
```
Add Components:
├─ PostgreSQL replication + read replicas
├─ Redis Cluster (3 nodes)
├─ Kafka for ingestion decoupling
├─ Add async processing service

Capacity: ~100K DAU
Deployment: Single region with HA
```

### Phase 3: Optimize (Months 5-6)
```
Add Components:
├─ Cassandra for articles (time-series)
├─ Elasticsearch for full-text search
├─ CDN for static content
├─ Multi-region setup (US + EU)

Capacity: 1M+ DAU
Deployment: Multi-region
```

---

## 9. API DESIGN

### Feed Endpoint

```
GET /api/v1/feed
Headers:
  Authorization: Bearer {jwt_token}
  X-Device-Id: mobile|web
  Accept-Encoding: gzip

Query Params:
  category: tech,business,sports (default: all)
  limit: 20 (max: 100)
  offset: 0
  source_id: optional filter
  since: 2026-05-01T00:00:00Z (optional)

Response (200 OK):
{
  "articles": [
    {
      "id": "uuid",
      "title": "string",
      "summary": "string",
      "url": "string",
      "source": "TechCrunch",
      "image_url": "string",
      "published_at": "2026-05-06T10:00:00Z",
      "relevance_score": 0.92,
      "is_bookmarked": false
    }
  ],
  "next_offset": 20,
  "total_count": 5000,
  "metadata": {
    "cache_hit": true,
    "response_time_ms": 145,
    "db_lag_seconds": 0
  }
}

Errors:
- 401: Invalid token
- 429: Rate limited (X-RateLimit-Remaining header)
- 503: Service unavailable (use cache)
```

### Search Endpoint

```
GET /api/v1/search
Query Params:
  q: "AI regulation"
  filter: "published_at:[now-7d TO now]"
  sort: relevance|published_at|popularity
  limit: 50
  offset: 0

Response:
{
  "results": [ {...} ],
  "total_hits": 12500,
  "search_time_ms": 450,
  "facets": {
    "source": { "TechCrunch": 2500, "Wired": 1800, ... },
    "category": { "AI": 5000, "Policy": 3000, ... }
  }
}

Note: Uses Elasticsearch async
```

---

## 10. REVISIT POINTS

As system grows, consider:

1. **Stream Processing**: Kafka Streams/Flink for real-time trending analysis
2. **ML Pipeline**: Collaborative filtering at scale (offline training, online serving)
3. **Graph Database**: Relationship between articles, topics, users
4. **Time-Series DB**: InfluxDB for metrics/analytics (instead of custom Cassandra schema)
5. **Service Mesh**: Istio/Linkerd for canary deployments and traffic management
6. **Schema Registry**: Confluent Schema Registry for Kafka message versioning
7. **GDPR Compliance**: Right to be forgotten (article deletion complexity)
8. **Monetization**: Ad placement (new ranking signals), subscription tiers

---

## 11. SUMMARY TABLE

| Component | Choice | Why | Trade-off |
|-----------|--------|-----|-----------|
| Message Queue | Kafka | Durability, replay, scaling | Operational complexity |
| Primary DB | PostgreSQL | ACID for user data | Limited write scaling |
| Time-Series DB | Cassandra | Write-heavy, time-based queries | Eventually consistent |
| Cache | Redis Cluster | Sub-ms latency, persistence | Memory cost |
| Search | Elasticsearch | Relevance ranking, faceting | Operational complexity |
| Dedup | Hybrid (hash + simhash) | Accuracy + performance | Extra latency on miss |
| Deployment | Multi-region | Availability, latency | Cost and complexity |

---

## Interview Talking Points

✅ **Strengths to highlight:**
- Thought through failure modes comprehensively
- Explicit trade-offs for every major decision
- Scaling strategy with cost analysis
- Deduplication solved with hybrid approach (hash + content-based)
- Migration path from MVP to scale

❌ **Gotchas to be ready for:**
- Interviewer asks: "What if Kafka goes down?" (Have Local SQLite fallback)
- Interviewer asks: "How do you handle Redis cache stampede?" (Use probabilistic early expiration)
- Interviewer asks: "Cost of cross-region replication?" (Acknowledge tradeoff, Kafka mirror-maker costs)
- Interviewer asks: "How detect plagiarized articles?" (Simhash, but also consider external plagiarism APIs)

---

**Document generated for interview preparation.**  
**Status**: Ready for discussion and refinement in actual interview context.
