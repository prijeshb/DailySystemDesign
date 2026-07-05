# System Design Interview Prep: Daily Deep Dives

## Overview
This guide covers system design concepts with real-world trade-offs, failure scenarios, and component analysis. Each system includes architecture decisions, scalability considerations, and failure-first approaches.

---

## System 1: News Feed Aggregator (Similar to Twitter/Facebook Feed)

### Requirements
- Users can follow sources/creators
- Real-time feed updates
- Millions of concurrent users
- High read volume, moderate write volume
- Personalization and ranking

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Client (Web/Mobile)                                          │
└────────────┬────────────────────────────────────────────────┘
             │
        ┌────▼────────┐
        │  API Gateway│ (Load balancer, rate limiting)
        └────────┬────────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───▼──┐  ┌─────▼──┐  ┌──────▼───┐
│ Read │  │ Write  │  │  Search  │
│Cache │  │Service │  │ Service  │
│(CDN) │  │        │  │          │
└──────┘  └────┬───┘  └──────────┘
               │
        ┌──────▼──────┐
        │ Feed DB     │
        │ (PostgreSQL)│
        └─────────────┘
        
        ┌──────────────┐
        │ Cache Layer  │
        │ (Redis)      │
        └──────────────┘
        
        ┌──────────────┐
        │ Message Queue│
        │ (Kafka)      │
        └──────────────┘
        
        ┌──────────────┐
        │ Search Index │
        │(Elasticsearch)
        └──────────────┘
```

### Component Selection & Trade-offs

#### 1. **Cache Layer (Redis)**
**Choice**: In-memory cache for feed data
- **Pros**: Sub-millisecond latency, reduces DB load
- **Cons**: Limited memory, data loss on crash, eventual consistency
- **Trade-off**: Cache "hot" feeds (trending, followed users), not all feeds
- **Invalidation**: Time-based (5 min TTL) + event-based (new post invalidates)

#### 2. **Message Queue (Kafka)**
**Choice**: Async processing for writes
- **Pros**: Decouples services, handles traffic spikes, replay capability
- **Cons**: Added complexity, eventual consistency, debugging harder
- **Trade-off**: Fanout writes go through queue, but critical reads bypass for accuracy
- **Partitioning**: By user_id for ordering guarantees per user

#### 3. **Database Choice (PostgreSQL)**
**Choice**: Relational DB for structured feed data
- **Pros**: ACID guarantees, complex queries, relationships (followers)
- **Cons**: Doesn't scale horizontally easily, slower writes with high volume
- **Alternative Considered**: NoSQL (MongoDB) - would be faster writes but harder joins
- **Trade-off**: PostgreSQL for correctness, Redis for speed

#### 4. **Search (Elasticsearch)**
**Choice**: Inverted index for full-text search
- **Pros**: Fast searches (100s of docs in ms), faceted search, relevance ranking
- **Cons**: Extra system to maintain, eventual consistency with primary DB
- **Trade-off**: Eventual consistency acceptable for search, strong consistency for reads

### Failure Scenarios & Mitigation

#### Scenario 1: Redis Cache Fails
```
User Request → DB Hit (slower) → Returns stale cache if available
Impact: Increased latency (500ms → 50-100ms), but system functional
Solution: 
- Fallback to DB read
- Implement cache warming with DB queries
- Circuit breaker pattern (stop trying after N failures)
```

#### Scenario 2: Database Failure
```
Requests Queue → Message Queue persists writes
Impact: Reads fail, writes queue up
Solution:
- Replicate DB (primary-replica setup)
- Promote replica with ~30sec recovery time
- Read replicas absorb read traffic during primary failure
```

#### Scenario 3: Kafka Message Queue Bottleneck
```
Write spikes → Queue backs up → API returns 503
Impact: New posts delayed, cache becomes stale
Solution:
- Auto-scaling on queue depth
- Multiple partitions per topic
- Dead letter queue for failed messages
- Degraded mode: some users get delayed feeds
```

#### Scenario 4: Network Partition (Services Split)
```
Feed Service ↔X Kafka ↔X Cache
Impact: Feed service can't publish, cache not updated
Solution:
- Retry with exponential backoff
- Cache stale data acceptable (mark as "last updated X mins ago")
- Eventually consistent when partition heals
```

#### Scenario 5: Elasticsearch Goes Down
```
Search requests fail
Impact: Search feature unavailable
Solution:
- Fallback to simple DB query (slower but works)
- Circuit breaker returns "Search temporarily unavailable"
- Re-index from source when recovered
```

### Scaling Considerations

| Component | Bottleneck | Solution |
|-----------|------------|----------|
| API Gateway | Connection limit | Horizontal scaling + L4 load balancer |
| Cache | Memory | Eviction policy (LRU), sharding, separate cache per region |
| Database | I/O latency | Read replicas, connection pooling, batch queries |
| Message Queue | Throughput | Partitioning, async consumers, compression |
| Search Index | Index size | Sharding by date, archived indices |

---

## System 2: Parking Lot Management System

### Requirements
- Track parking spaces (occupied/available)
- Real-time availability
- Find nearest available spot
- Payment processing
- Handle thousands of concurrent users

### Architecture

```
┌──────────────────────────────────────────┐
│ Mobile App / Web                         │
└────────────┬─────────────────────────────┘
             │
        ┌────▼─────┐
        │ API GW   │
        └────┬─────┘
             │
    ┌────────┼────────┐
    │        │        │
┌───▼──┐ ┌──▼──┐ ┌──▼──┐
│Space │ │Spot │ │Pay  │
│Query │ │Book │ │Srv  │
└──────┘ └──────┘ └──────┘
    │        │        │
┌───▼────────▼────────▼──┐
│    PostgreSQL DB        │
└────────────┬────────────┘
             │
        ┌────▼─────┐
        │   Cache  │
        │  (Redis) │
        └──────────┘
        
        ┌──────────────┐
        │Location Index│
        │ (Redis + GIS)│
        └──────────────┘
        
        ┌──────────────┐
        │Event Stream  │
        │ (Kafka)      │
        └──────────────┘
```

### Component Decisions & Trade-offs

#### 1. **Location Indexing (Redis GEO + PostGIS)**
**Choice**: Dual-layer spatial indexing
- **Redis GEO**: In-memory, fast "nearby" queries within 5km
- **PostGIS**: Persistent, complex polygon queries (parkable zones)
- **Pros**: Redis for speed, PostGIS for correctness
- **Cons**: Data sync between two systems
- **Trade-off**: Cache-aside pattern - check Redis first, fallback to PostGIS

#### 2. **Space Availability Tracking (Eventual Consistency)**
**Choice**: Event-driven updates via Kafka
- **Pros**: Decoupled updates, handles sensor failures gracefully
- **Cons**: 2-10 second delay in space availability
- **Real-world**: Sensor sends "Space#123 now empty" → Kafka → Service updates cache
- **Trade-off**: 99% of users see correct availability, 1% book occupied spot (refund)

#### 3. **Booking Mechanism (Optimistic Locking)**
```
User A: GET spot (available=true)
User B: GET spot (available=true)
User A: PUT spot (if version==5) → SUCCESS
User B: PUT spot (if version==5) → CONFLICT, retry

This prevents double-booking without distributed locks
```

#### 4. **Payment Processing (External Service)**
**Choice**: Async with idempotency keys
- **Pros**: Decouples payment failures from booking
- **Cons**: Eventual consistency, refund complexity
- **Implementation**:
  ```
  User books → Generate idempotent payment ID
  → Call payment service (if timeout, check status)
  → Confirm booking only after payment confirmed
  ```

### Failure Scenarios

#### Scenario 1: Sensor Network Delays (5-30 min lag)
```
Car leaves → Sensor detects (delayed) → DB updated late
User thinks spot is full, drives past available spaces
Impact: 10-20% reduced effective capacity during rush hour

Solutions:
- Weighted availability (sensor data confidence)
- Manual override by attendant (quick refresh)
- User reports: "This spot is available!" → crowd-source
- Machine learning to predict sensor delays
```

#### Scenario 2: Double-Booking Race Condition
```
User A + B both GET space 123 (version=5)
Both reserve → User B's reservation conflicts
Impact: Booking fails, confusing UX

Solution:
- ALREADY SOLVED by optimistic locking above
- If conflict: immediate retry (99% success on retry)
- Cache warming: pre-fetch versions
```

#### Scenario 3: Payment Service Down
```
User books successfully → Payment fails → No refund issued
Impact: Disputed parking charges, angry customers

Solutions:
- Async payment with periodic retry
- Webhook callbacks from payment provider
- Manual reconciliation job (daily)
- Booking timeout (2 min) if payment not confirmed
```

#### Scenario 4: Database Crash During Rush Hour
```
Peak demand (5000 concurrent users) → Sudden no writes
Impact: Users can't book, money lost, cars circle

Solutions:
- Read replicas for queries (returns slightly stale but functional)
- Write queue to Kafka + replay when DB back
- Degraded mode: "Limited bookings available"
- Automatic failover to replica (30-60 sec recovery time)
```

#### Scenario 5: Cache Stampede
```
Popular spot's cache expires → 100 simultaneous requests hit DB
Impact: DB spike, slow responses, cache reload contention

Solution:
- Probabilistic early expiration (refresh at 80% TTL)
- Lock-based refresh: first requester refreshes, others wait
- Longer TTL for hot spots (5 min vs 1 min)
- Cache warming at off-peak times
```

### Scaling Strategy

**Sharding by Location**:
```
Lot A (500 spaces) → Shard 1
Lot B (500 spaces) → Shard 2
Lot C (500 spaces) → Shard 3

Each shard can scale independently
Geo-routing: Route queries to nearest shard
```

**Handling Peak Load**:
```
Morning rush (7-9am): 
- Pre-warm cache (15 min before)
- Increase Kafka consumer count
- Add temporary read replicas
- Queue excessive requests (degraded mode)
```

---

## System 3: Real-time Analytics Dashboard

### Requirements
- Ingest millions of events per second
- Real-time dashboards (< 1 second latency)
- Historical queries (batch jobs)
- Multi-dimensional analysis
- Cost-effective

### Architecture

```
┌──────────────────────────────┐
│ Event Sources                │
└────────────┬─────────────────┘
             │
        ┌────▼──────┐
        │Collection │
        └────┬──────┘
             │
    ┌────────▼────────┐
    │  Kafka Topics   │
    └────────┬────────┘
             │
    ┌────────┼────────┐
    │        │        │
┌───▼──┐ ┌──▼──┐ ┌───▼──┐
│Stream│ │Batch│ │Change│
│Proc. │ │Jobs │ │Data  │
│(Fast)│ │(Accu)│ │Capture
└──┬───┘ └──┬───┘ └──┬───┘
   │        │        │
┌──▼────────▼────────▼──┐
│   Data Warehouse       │
│ (Snowflake/BigQuery)   │
└────────┬───────────────┘
         │
    ┌────▼─────┐
    │OLAP Cube │
    │(Druid)   │
    └────┬─────┘
         │
    ┌────▼──────┐
    │Dashboard  │
    └───────────┘
```

### Component Trade-offs

#### 1. **Real-time vs Batch Path**
```
Real-time (Streaming):
  Events → Kafka → Spark Streaming → In-memory State
  Latency: 500ms-2s
  Accuracy: 95% (some late arrivals lost)
  Cost: Always running

Batch (Accurate):
  Events → Kafka → Stored → Batch job (hourly/daily)
  Latency: 1-24 hours
  Accuracy: 99.9% (reprocessed)
  Cost: Pay only during job

Choice: Both! Real-time for dashboards, batch for reports
```

#### 2. **Event Schema Flexibility**
**Problem**: Events evolve - new fields added, old ones removed
**Solutions**:
- Schema Registry (Confluent) - versioned schemas
- Backward compatibility: new fields optional
- Consumers handle multiple versions

**Trade-off**: More complex consumers but no coordination issues

#### 3. **Data Freshness vs Accuracy**
```
Real-time dashboards: Show best-effort numbers (98% accurate)
- "Page views: ~1.2M (updates every 30 sec)"

Historical reports: Show final numbers (99.9% accurate)
- "Page views: 1,234,567 (final count)"

Users understand the difference
```

#### 4. **Cost Optimization (Hot/Warm/Cold Data)**
```
Hot (0-7 days):   High-speed OLAP DB (expensive)
Warm (7d-1yr):    Compressed columnar format (medium)
Cold (1yr+):      Archive storage (cheap)

Queries automatically routed based on date range
```

### Failure Scenarios

#### Scenario 1: Late-Arriving Events
```
Event happens at 3:00pm
Network delay → Arrives at 3:05pm
Real-time count already published

Solutions:
- Batch job reruns at end of day (corrects count)
- Publish "adjusted" numbers after 24 hours
- Allow events up to 6 hours late in stream processor
```

#### Scenario 2: Kafka Broker Failure
```
One broker dies out of 5
Impact: 1/5 partitions unreachable

Solutions:
- Replication factor ≥ 3 (auto-failover)
- Min in-sync replicas = 2 (durability guarantee)
- 10-30 second recovery time, no data loss
```

#### Scenario 3: Streaming Job Crashes
```
Spark job processes 1M events/sec
Job crashes midway through window
Impact: Some events counted twice, some not at all

Solutions:
- Exactly-once semantics (idempotent writes)
- Store offset in sink (DB), resume from there
- Deduplication key: event_id + timestamp
```

#### Scenario 4: Data Warehouse Overload
```
10 concurrent queries, each scans 10TB
Warehouse can't handle, becomes slow/unresponsive
Impact: Dashboards timeout, users can't see data

Solutions:
- Query queuing (prioritize real-time dashboards)
- Pre-aggregated tables (5-minute summaries)
- Caching layer (query results cached 1 minute)
- Query timeout: 30 seconds max
```

#### Scenario 5: Schema Evolution Breaks Consumer
```
Event schema changes (new field, breaking change)
Old consumer code crashes
Impact: Processing stops, data piles up

Solutions:
- Schema Registry enforces compatibility
- Multiple versions coexist
- Consumer accepts both old/new format
- Gradual rollout of changes
```

---

## General Failure Patterns to Always Consider

### 1. **Cascading Failures**
```
Service A slow → Times out calling Service B
→ Connection pool exhausted → Can't serve other requests
→ Load balancer thinks all instances unhealthy
→ Traffic sent to struggling instances
→ Entire system cascades down

Prevention:
- Bulkheads (connection pools per downstream)
- Circuit breakers (stop calling failing service)
- Timeouts (don't wait forever)
- Graceful degradation (partial feature, not full failure)
```

### 2. **Thundering Herd**
```
Cache expires → 10,000 requests hit DB simultaneously
DB under load → Requests slow down → More timeouts
→ Retry storms → Even more load

Prevention:
- Probabilistic cache refresh
- Staggered TTLs
- Jittered retries
```

### 3. **Data Consistency**
```
Write succeeds → Replication fails → Reader sees stale data
Impact: User updates profile, sees old data

Prevention by tier:
- CRITICAL: Immediate consistency (payment, inventory)
- IMPORTANT: Consistent within 5 seconds (profile)
- NICE-TO-HAVE: Eventual consistency OK (analytics)
```

### 4. **Resource Exhaustion**
```
One user uploads huge file → Memory spike → OOM kill
Impact: Service dies, all users affected

Prevention:
- Request quotas per user
- Resource limits per request
- Async processing with queues
```

### 5. **Monitoring Blind Spots**
```
Service handles 99% of requests fine
Hidden failure: DB connection pool exhaustion
Only 1% of requests fail (hard to notice)
Impact: Silent data loss for those 1%

Solution:
- Monitor ALL percentiles (p50, p95, p99, p99.9)
- Alert on error rates > 0.1%
- Synthetic monitoring (fake requests)
```

---

## Interview Checklist

### Phase 1: Requirements (5 min)
- [ ] Clarify functional requirements
- [ ] Establish non-functional requirements (scale, latency, consistency)
- [ ] Identify read/write ratio
- [ ] Define single points of failure to address

### Phase 2: High-Level Design (10 min)
- [ ] Draw main components (API, storage, cache, queues)
- [ ] Explain data flow
- [ ] Discuss trade-offs per component

### Phase 3: Deep Dive (10 min)
- [ ] Choose 2-3 components to elaborate
- [ ] Discuss failure scenarios
- [ ] Explain recovery mechanisms

### Phase 4: Scaling (5 min)
- [ ] Identify bottlenecks
- [ ] Propose scaling strategies (horizontal, vertical, partitioning)
- [ ] Discuss cost implications

### Phase 5: Monitoring & Failure Handling (5 min)
- [ ] Metrics to track
- [ ] Alerting strategy
- [ ] Graceful degradation approach

---

## Key Takeaways

1. **No perfect solution**: Every choice has trade-offs. Explain them clearly.
2. **Failure-first thinking**: Design around what breaks, not what works.
3. **Real-world context matters**: Reference actual systems and known issues.
4. **Scaling is about bottlenecks**: Identify them first, then solve iteratively.
5. **Consistency is a spectrum**: Choose the right level for each data type.
