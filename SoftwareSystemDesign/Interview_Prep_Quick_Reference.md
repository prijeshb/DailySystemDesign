# System Design Interview: Quick Reference & Case Studies
## News Aggregator + Real-World Examples

---

## PART A: COMMON INTERVIEW TRAPS & SOLUTIONS

### Trap 1: "We'll just use one database"

**Bad Answer:**
```
"Let's use PostgreSQL for everything"
```

**Why it fails:**
- Write-heavy article ingestion (1M/day) will overload OLTP
- Time-series queries (articles by date) are inefficient
- Exponential growth in table size kills query performance

**Good Answer:**
```
"I'd use PostgreSQL for transactional data (users, bookmarks) 
because we need ACID guarantees for financial consistency. 
But for articles—which are append-only and queried by time—
I'd use Cassandra. Here's why:

PostgreSQL:
- Random writes for bookmarks (high latency tolerance)
- JOIN queries needed (relationships between user & bookmarks)
- Small dataset (users/bookmarks grow slowly)

Cassandra:
- Sequential writes by timestamp (optimized)
- Simple queries (give me articles from X to Y date)
- Massive dataset (1B+ articles)
- Built-in compaction and TTL support

This separation lets each DB do what it's best at."
```

---

### Trap 2: "Let's cache everything"

**Bad Answer:**
```
"Use Redis for the entire feed, search index, and user data"
```

**Why it fails:**
- Cache invalidation is hard (Phil Karlton quote)
- 50M users × 100KB feed = 5TB of Redis
- What happens when cache misses occur?

**Good Answer:**
```
"Caching is a spectrum, not binary. I'd use it strategically:

HOT (Cache everything):
- Personalized feed for signed-in users
  - TTL: 12 hours (natural refresh)
  - Hit rate: 85% (most users reload same feed)
  - Cost: Tolerable (query is expensive)

WARM (Selective cache):
- Popular articles (top 1000 by engagement)
  - TTL: 1 hour
  - Hit rate: 60%
  - Cost: Worth it (saves ES query)

COLD (Don't cache):
- Search results (too many permutations)
  - Instead: Use ES query cache (internal)
  - TTL: 5 minutes
  - Cost: Lower memory overhead

FALLBACK: If cache fails → use DB directly
  - Redis down? → Query PostgreSQL (slow but works)
  - Circuit breaker after 3 failed cache hits"
```

---

### Trap 3: "We'll handle failures in v2"

**Bad Answer:**
```
"Once we launch, we'll add error handling"
```

**Why it fails:**
- Users will not wait for v2
- Cascading failures multiply fast
- You can't retrofit resilience

**Good Answer:**
```
"Failure first. Let me design for when things break:

Feed source fails (e.g., TechCrunch RSS down):
- Return 204 No Content (not error) + stale feed from cache
- User sees old articles vs. error page
- Gradually refresh cache once source recovers

Kafka cluster down:
- Buffer articles to local SQLite (emergency queue)
- Ingest service keeps polling sources
- Once Kafka healthy, flush SQLite → Kafka
- No articles lost, just delayed

Database connection exhausted:
- Circuit breaker opens (stop new connections)
- Return cached feed + 'Last updated 2h ago' header
- User gets partial data vs. failure
- Gradually reduce write load

Cache layer (Redis) down:
- Skip cache, query DB directly
- Add jitter to DB queries (prevent thundering herd)
- Monitor cache recovery
- Pre-warm cache during recovery"
```

---

## PART B: REAL-WORLD CASE STUDIES

### Case Study 1: Twitter (Now X) - Feed Timeline Failure

**What happened:**
In 2013, Twitter's home timeline was slow. Users reported 30-second delays.

**Root cause:**
- Every feed request was running JOIN queries across millions of rows
- Fanout (multiplying) the follow graph was expensive
- Database couldn't handle concurrent reads

**Solution (Fanout Service):**
```
OLD (Push on read):
1. User requests feed
2. Query: SELECT tweets FROM tweets WHERE author_id IN (user_follows)
3. This JOIN is expensive across billions of rows
4. Latency: 30+ seconds

NEW (Fanout on write):
1. User posts tweet
2. Fanout service pushes to all 1M followers' feeds (cache)
3. User requests feed
4. Lookup: GET /cache/user_123:feed
5. Latency: <100ms

Trade-off:
- ✅ Fast reads (users want fast feed)
- ❌ Slow writes (heavy on followers, ok)
- ❌ Cache explosion (1M users × 10K friends × 1KB = 10TB)
```

**Lesson for your interview:**
"For a news aggregator, the analog is caching feeds per user. 
We accept slow writes (personalization processing happens async) 
to achieve fast reads (user requests must be <200ms)."

---

### Case Study 2: Stripe - Duplicate Detection

**What happened:**
Stripe had to handle duplicate charge attempts from network retries.

**Root cause:**
- Clients retry on timeout (good!)
- Same request reaches API twice with same idempotency key
- Without dedup: Two charges to customer

**Solution:**
```
OLD:
1. Charge $100
2. Network timeout
3. Retry → Charge $100 again ❌

NEW (Idempotent keys):
1. Client: POST /charges 
   Headers: Idempotency-Key: "req_123_abc"
   Body: {amount: 100, currency: usd}

2. Server:
   - Hash key: sha256("req_123_abc")
   - Check if hash exists in Redis cache
   - If yes: Return cached response (same charge_id)
   - If no: Process charge, cache response

3. Retry:
   - Same key: Return cached $100 charge
   - No double charge ✅
```

**Lesson for your interview:**
"For news aggregator, I'd use URLs as idempotent keys. 
Same URL ingested twice = deduplicated via Redis set lookup. 
This is the HTTP idempotency pattern applied to bulk ingestion."

---

### Case Study 3: LinkedIn - Cascading Failures During Spike

**What happened:**
On LinkedIn's busiest traffic day, a small bug in Elasticsearch caused 
a cascade that took down the entire feed.

**Timeline:**
```
T=0:00    → ES query slow (>500ms)
T=0:05    → API timeouts increase (cascading)
T=0:10    → Users retry (thundering herd)
T=0:15    → API gateway overloaded
T=0:20    → Entire feed down (99% users affected)
T=2:00    → Recovered after manual intervention
```

**Root cause:**
```
1. Elasticsearch shard rebalance triggered
2. One query slowed down (waiting for shard)
3. API timeout window = 10 seconds
4. Clients (JS, mobile) retry exponentially: 1s, 2s, 4s, 8s
5. Retry storms hit API Gateway with 100x normal traffic
6. API Gateway CPU pegged
7. All feeds return 503 (Service Unavailable)
```

**Solution:**
```
Add circuit breaker:
IF error_rate > 10% for 10 consecutive seconds:
   CIRCUIT OPEN
   → Reject new requests with 429 (Too Many Requests)
   → Return cached feed from backup
   → No retry storms

Add bulkheads:
- Search requests: dedicated pool (10 threads)
- Feed requests: dedicated pool (100 threads)
- Bookmarks: dedicated pool (5 threads)
- One slow endpoint doesn't starve others

Add graceful degradation:
IF ES unavailable:
   → Fall back to PostgreSQL full-text search
   → Return last 24 hours of articles
   → Add 'Search is degraded' banner
   → Better than returning error
```

**Lesson for your interview:**
"Cascading failures happen when one slow component 
blocks the whole pipeline. 
Mitigation: circuit breakers, bulkheads, timeouts with fallbacks, 
and graceful degradation instead of hard failures."

---

## PART C: DECISION TREE PATTERNS

### When to Use Cache?

```
START: Do I need <100ms response?
├─ NO → Skip cache (database is fine)
└─ YES
   └─ Is the data read-heavy (>10:1 read-write ratio)?
      ├─ NO → Skip cache (invalidation cost too high)
      └─ YES
         └─ Can I tolerate 5-30min staleness?
            ├─ NO → Use database (consistency required)
            └─ YES → Use Redis cache
               └─ Set TTL based on staleness tolerance
                  └─ 5min staleness → TTL=5min
                  └─ 1hr staleness → TTL=1hr
```

### When to Shard?

```
START: Is single database hitting limits?
├─ CPU bottleneck? → Sharding won't help (scale vertically first)
├─ Memory bottleneck? → Sharding helps (split dataset)
└─ Disk I/O bottleneck?
   └─ YES → Shard by partition key
      └─ Choose key with even distribution
         └─ User ID (good: evenly distributed)
         └─ Zip code (bad: skewed toward cities)
         └─ Timestamp (bad: hot shards for recent data)
```

### When to Use Event Streaming?

```
START: Do you need durability + replay?
├─ NO → Use message queue (RabbitMQ, SQS)
└─ YES
   └─ Do you need replay >7 days?
      ├─ NO → Kafka with 7-day retention
      └─ YES → Kafka with custom retention policy
   
   └─ Do you need multiple consumers?
      ├─ NO → Point-to-point (queue)
      └─ YES → Pub-sub (Kafka topics)
```

---

## PART D: QUICK FORMULAS

### Back-of-Envelope Calculations

**Load Estimation:**
```
1M articles/day
= 1,000,000 / 86,400 seconds
= 11.5 articles/second (baseline)

But uneven: 
- Morning: 8am-10am = 5x normal = 57/sec
- Peak: 12pm-2pm = 8x normal = 92/sec
- Night: 2am-4am = 0.1x normal = 1/sec

Plan for: 100-150/sec (cover peaks + future growth)
```

**Database Size:**
```
1 article = 
  - Title: 100 bytes
  - Content: 5 KB
  - Metadata: 1 KB
  - Total: ~6 KB

1M articles = 6 GB
6 months = 180M articles = 1.08 TB

Plan for: 2 TB (includes indexes, replication)
```

**Cache Memory:**
```
5M DAU (10% of users active)
Average feed size: 20 articles
Article in feed: ~1 KB (just ID, title, image URL)
Per-user cache: 20 KB

Total cache: 5M × 20KB = 100 GB

With 80% hit ratio → saves 80M DB queries/day
Cost: $3K/month (10x Redis nodes) vs. pain of 80M DB calls
→ Cache is worth it
```

**Replication Lag:**
```
Source writes: 100 QPS
Network latency: 50ms
Disk sync: 100ms
Total lag: ~150ms

SLA: Keep lag <1 second
Monitoring: Alert if lag > 500ms
```

---

## PART E: BEHAVIORAL INTERVIEW TACTICS

### How to structure your answer (5 minutes):

```
[0-1 min] Clarify requirements
  "Are we optimizing for latency, cost, or availability first?"
  "How many users? How many articles per day?"

[1-2 min] High-level architecture
  "Here's the data flow: Sources → Ingestion → Cache → API → Users"
  (Draw box diagram)

[2-3 min] Deep dive on ONE bottleneck
  "The hardest problem is deduplication at scale because..."
  (Explain your solution: hash + simhash)

[3-4 min] Failure scenario
  "What if Elasticsearch goes down? We fall back to PostgreSQL FTS."
  (Show you think about robustness)

[4-5 min] Tradeoffs
  "Redis costs $3K/month but saves 80M DB queries. Worth it."
  (Show you think like an engineer, not a junior dev)
```

### Red flags to avoid:

❌ "We'll use a NoSQL database for everything"
❌ "Cache everything, invalidate with time"
❌ "We don't need monitoring in MVP"
❌ "Horizontal scaling fixes all problems"
❌ "Database replication is automatic, so no worries"

✅ Instead say: "Each database tool has tradeoffs. I'd use X because..."

---

## PART F: FOLLOW-UP QUESTIONS YOU SHOULD ASK

After your design, ask the interviewer:

1. **"What's our biggest constraint—latency, cost, or team bandwidth?"**
   - Shows you know different systems optimize for different goals

2. **"How do we measure success? What metrics matter?"**
   - Shows you think about observability

3. **"What's our disaster recovery strategy? RPO/RTO?"**
   - Shows you think about business continuity

4. **"How do we handle a 10x traffic spike tomorrow?"**
   - Shows you think about elasticity

5. **"What compliance requirements do we have?"**
   - Shows you think about non-functional concerns

---

## SUMMARY: Design Principles

| Principle | Application | Example |
|-----------|-------------|---------|
| **Separation of Concerns** | Different DB for different workloads | PostgreSQL (txns) + Cassandra (time-series) |
| **Fault Isolation** | One failure doesn't cascade | Redis down ≠ API down (fallback to DB) |
| **Observability First** | Can't fix what you can't measure | Dashboard for lag, cache hit rate, errors |
| **Graceful Degradation** | Partial service > No service | Return cached feed vs. error page |
| **Explicit Tradeoffs** | Every choice has cost | Cache costs money but saves latency |
| **Failure First Design** | Design for failure modes | Circuit breaker for cascades |

---

Generated for interview practice: May 6, 2026
