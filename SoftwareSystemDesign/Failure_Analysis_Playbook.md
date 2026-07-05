# Failure Analysis & Resilience Playbook
## News Aggregator System - Complete Failure Scenarios

---

## INTRODUCTION

This document covers the "Failure First" approach: 
For every component, answer three questions:
1. **When does THIS component fail?** (internal failure)
2. **What happens to DOWNSTREAM if upstream fails?** (cascade)
3. **How do we detect and recover?** (mitigation)

---

## SECTION 1: INGESTION SERVICE FAILURES

### 1.1 Internal Failure: Source Unavailable

**Scenario:**
```
RSS feed from TechCrunch returns 500 Internal Server Error
```

**Timeline:**
```
T=0:00     → Feed fetch returns 500
T=0:05     → Ingestion service retries (exponential backoff)
T=0:10     → Retry attempt 2 fails
T=0:20     → Retry attempt 3 fails
T=0:45     → Retry attempt 4 fails
T=0:45     → Circuit breaker opens for TechCrunch
T=1:00     → Articles from TechCrunch stop
T=2:00     → Users notice feed is stale (no TechCrunch articles)
T=4:00     → Circuit breaker half-open; test retry succeeds
T=4:05     → Circuit breaker closes; articles resume
```

**Implementation:**
```python
def fetch_feed(source_id, url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return parse_rss(response.text)
    
    except Exception as e:
        # Exponential backoff with jitter
        backoff_time = min(
            2 ** attempt_count,  # 1s, 2s, 4s, 8s...
            300  # max 5 minutes
        ) + random.uniform(0, 1)  # jitter
        
        attempt_count += 1
        
        if attempt_count >= 4:
            # Open circuit breaker
            cache.set(f"circuit:{source_id}", "open", ttl=3600)
            log.warning(f"Circuit opened for {source_id}")
            return None
        
        sleep(backoff_time)
        return fetch_feed(source_id, url)  # retry

def ingest_articles(source_id):
    # Check circuit breaker
    if cache.get(f"circuit:{source_id}") == "open":
        log.info(f"Skipping {source_id} (circuit open)")
        return 0
    
    articles = fetch_feed(source_id, url)
    if articles:
        push_to_kafka(articles)
    return len(articles)
```

**Downstream Impact:**
```
User requests feed
↓
No articles from TechCrunch in last 4 hours
↓
Feed is stale but not empty (other sources still work)
↓
User sees feed from Reuters, Bloomberg, AP (ok experience)
```

**Mitigation Strategy:**

| Detection | Action | Recovery |
|-----------|--------|----------|
| HTTP 500/503 | Log error, attempt retry | Backoff exponentially |
| Timeout (10s) | Assume dead, open circuit | Retry after 1 hour |
| Failed 4 times | Disable source | Manual health check |
| Latency > 30s | Timeout + retry | Increase timeout pool |

---

### 1.2 Downstream Failure: Kafka Unavailable

**Scenario:**
```
Kafka cluster leader election: 5 partition leaders down simultaneously
All brokers report: "NotLeaderForPartition"
```

**Timeline:**
```
T=0:00     → Kafka broker-1 crashes (disk full)
T=0:05     → Broker-2 becomes new leader; rebalancing starts
T=0:10     → Network hiccup during rebalancing
T=0:15     → Kafka cluster in bad state (no quorum)
T=0:15     → Ingestion service GET errors: "not leader"
T=0:30     → Retry queue in memory grows (max 10K articles)
T=0:45     → Memory warning (articles consuming 100MB RAM)
T=1:00     → Operator notices; manually recovers Kafka
T=1:05     → Kafka healthy again
T=1:10     → Flush articles from memory queue to Kafka
T=1:15     → Processing resumes (no articles lost)
```

**Implementation:**
```python
class IngestionService:
    def __init__(self):
        self.memory_queue = collections.deque(maxlen=10_000)  # Emergency buffer
        self.kafka_producer = KafkaProducer(...)
        self.local_db = sqlite3.connect("emergency_queue.db")
    
    def push_to_kafka(self, articles):
        try:
            for article in articles:
                # Async send with timeout
                future = self.kafka_producer.send(
                    'raw-articles',
                    value=article.to_json(),
                    timeout_ms=5000
                )
                future.get(timeout=5)  # Wait for confirmation
        
        except KafkaError as e:
            # Kafka is down
            log.error(f"Kafka push failed: {e}")
            
            # Fallback 1: In-memory queue
            for article in articles:
                if len(self.memory_queue) >= 10_000:
                    log.error("Memory queue full! Dropping articles.")
                    break
                self.memory_queue.append(article)
            
            # Fallback 2: Local SQLite
            for article in articles:
                self.local_db.execute(
                    "INSERT INTO emergency_queue (article_json) VALUES (?)",
                    (article.to_json(),)
                )
            self.local_db.commit()
            
            # Circuit breaker
            self.kafka_health_check_failed += 1
            if self.kafka_health_check_failed >= 3:
                log.critical("Kafka appears down. Alerting ops.")
                alert_ops("Kafka cluster unreachable")
    
    def flush_local_queue(self):
        """Run periodically to retry local queue"""
        for article in self.memory_queue:
            try:
                self.push_to_kafka([article])
                self.memory_queue.remove(article)
            except:
                break  # Stop if Kafka still down
        
        # Also flush SQLite
        cursor = self.local_db.execute(
            "SELECT id, article_json FROM emergency_queue LIMIT 100"
        )
        for row_id, article_json in cursor:
            try:
                self.push_to_kafka([article_json])
                self.local_db.execute("DELETE FROM emergency_queue WHERE id = ?", (row_id,))
            except:
                break
        self.local_db.commit()
```

**Downstream Impact:**
```
Kafka down for 60 minutes
↓
Articles buffered locally (not lost)
↓
Processing service starves (no articles to process)
↓
User feed becomes stale (no new articles)
↓
BUT: Once Kafka recovers, articles flush through (RTO: 60min)
```

**Mitigation:**
- ✅ Local SQLite acts as dead letter queue
- ✅ No data loss (articles retained)
- ✅ RTO: ~5 minutes (after Kafka recovery)
- ❌ RPO: 0 (no articles lost, but slow to process)

---

## SECTION 2: MESSAGE QUEUE (KAFKA) FAILURES

### 2.1 Internal Failure: Partition Full

**Scenario:**
```
Kafka disk usage on broker-1 hits 95%
Partition assignment: 100 partitions on broker-1
Rebalancing triggered to move partitions
```

**Timeline:**
```
T=0:00     → Disk space low warning (threshold: 90%)
T=0:10     → Kafka log cleaner runs (removes old segments)
T=0:20     → Still not enough space; rebalancing begins
T=0:30     → Some partitions offline (in-flight rebalance)
T=0:30     → Producer: OfflinePartition error on some topics
T=0:45     → Rebalancing completes; partitions online
T=0:45     → Producers reconnect automatically
T=1:00     → All back to normal
```

**Mitigation:**
```yaml
# Kafka broker config
log.segment.bytes: 1GB  # Smaller segments = faster cleanup
log.retention.hours: 168  # 7 days (default)
log.cleanup.policy: delete  # Not compact (for news)

# Monitoring
alert if broker_disk_usage > 80%:
   trigger: Run manual cleanup
   action: Delete old segments > 7 days

alert if pending_replicas > 0:
   severity: CRITICAL
   action: Check broker health; may need replacement
```

---

### 2.2 Downstream Failure: Consumer Lag

**Scenario:**
```
Processing service crashes; no consumer reading from Kafka
Articles queue up in Kafka for 4 hours
```

**Timeline:**
```
T=0:00     → Processing service container restarts (OOM)
T=0:00     → Consumer group disconnects
T=0:05     → Kafka lag starts increasing
T=1:00     → Lag: 100K articles (still in Kafka)
T=2:00     → Lag: 240K articles
T=3:00     → Lag: 360K articles (alerts fired)
T=4:00     → Ops notices; restarts processing service
T=4:05     → Consumer reconnects; begins reading from lag
T=5:00     → Still processing old articles
T=6:30     → Caught up (all articles processed)
```

**Mitigation:**
```
Add alerting:
- If lag > 100K articles → Page on call
- If lag > 1M articles → Escalate to team lead
- If lag growing (not stable) → Potential consumer issue

Add monitoring:
- Lag gauge: update every 10 seconds
- Graph lag over time
- Alert on sustained increase

Consumer side:
class ProcessingService:
    def __init__(self):
        self.max_lag_articles = 500_000
    
    def consume_articles(self):
        try:
            messages = consumer.poll(timeout_ms=1000)
        except Exception as e:
            log.error(f"Consumer error: {e}")
            alert_ops("Processing service consumer error")
            # Don't crash; wait for manual intervention
            sleep(30)
            return
        
        # Health check: are we keeping up?
        lag = consumer.metrics()['consumer-lag']
        if lag > self.max_lag_articles:
            # Scale out: add more consumer instances
            scale_out_signal()
        
        for article in messages:
            process(article)
```

---

## SECTION 3: DATABASE FAILURES

### 3.1 PostgreSQL: Primary Down

**Scenario:**
```
PostgreSQL primary (primary-db.prod) suffers disk corruption
Immediate failover to replica required
```

**Timeline:**
```
T=0:00     → Disk I/O error on primary
T=0:02     → Health check fails (replication lag > 30s)
T=0:03     → Automatic failover triggered
T=0:05     → Old replica promoted to new primary
T=0:08     → Old primary marked as down
T=0:10     → Connections redirect to new primary
T=0:15     → Users see increased latency (new primary warming up)
T=0:30     → New replica spawned from new primary
T=1:00     → Full HA restored (primary + replica)
T=24:00    → Old primary repaired; resynced
```

**Implementation:**
```python
# Connection pooling with failover
class DatabasePool:
    def __init__(self):
        self.primary = ConnectionPool("primary-db.prod:5432")
        self.replica = ConnectionPool("replica-db.prod:5432")
        self.failover_primary = "replica-db.prod:5432"
    
    def get_connection(self, read_only=False):
        if read_only:
            # Try replica first
            try:
                return self.replica.get()
            except ConnectionError:
                # Fall back to primary
                return self.primary.get()
        else:
            # Write only to primary
            try:
                return self.primary.get()
            except ConnectionError:
                # Check if failover happened
                if self.primary_health_check():
                    return self.primary.get()
                else:
                    # Primary is down; use failover
                    return self.failover_primary.get()
```

**Downstream Impact:**
```
API requests: User bookmarks, preferences
↓
Primary down → Failover to replica
↓
Write latency increases (primary warming up)
↓
Bookmarking requests: 500ms → 1000ms
↓
Users see lag but operation eventually succeeds
```

---

### 3.2 Cassandra: Node Down

**Scenario:**
```
Cassandra node-3 fails
Articles partition: (source_id, published_date)
Node-3 holds 1/3 of the data
```

**Timeline:**
```
T=0:00     → Node-3 network hiccup
T=0:30     → Gossip timeout; node marked down
T=0:35     → Replication factor=3; data on node-1, node-2, node-4
T=0:40     → Read requests check replica status
T=0:40     → Reads slow (wait for other replicas)
T=1:00     → Ops manually removes node-3 from cluster
T=1:05     → New node-5 bootstraps; rebalancing starts
T=2:00     → Data redistributed; reads return to normal latency
```

**Consistency Level Trade-off:**
```
Write consistency: ONE
- Write to 1 replica immediately
- Return to client immediately
- Other replicas eventually consistent
- Risk: If node fails, data on that node lost

Write consistency: QUORUM (2/3)
- Write to 2 replicas before returning
- Slower writes (wait for 2 ACKs)
- If 1 node fails, data survives on other 2
- Safe: Can lose 1 node and recover

Decision for news aggregator:
- Articles are append-only (immutable)
- Loss acceptable: Just re-fetch from source
- Use ONE for fast writes
- Use QUORUM for reads (consistency)
```

---

## SECTION 4: CACHE LAYER FAILURES

### 4.1 Redis Cluster: Master Down

**Scenario:**
```
Redis cluster: 3 master shards + 3 replicas
Master-1 (shard 1) fails
```

**Timeline:**
```
T=0:00     → Master-1 network isolated
T=0:05     → Cluster health check detects missing master
T=0:10     → Replica-1 promoted to master
T=0:15     → Cluster heals; failover complete
T=0:15     → Clients retry requests (connection timeout)
T=0:20     → Clients connect to new master
T=0:25     → All requests processing normally
```

**Handling:**
```python
class RedisCache:
    def __init__(self):
        self.cluster = RedisCluster(
            startup_nodes=[
                {"host": "redis-1", "port": 6379},
                {"host": "redis-2", "port": 6379},
                {"host": "redis-3", "port": 6379},
            ],
            skip_full_coverage_check=True,  # Allow partial cluster
        )
    
    def get_feed(self, user_id):
        cache_key = f"feed:{user_id}"
        try:
            cached_feed = self.cluster.get(cache_key)
            if cached_feed:
                return json.loads(cached_feed)
        except (ConnectionError, TimeoutError) as e:
            # Cache miss: Fall back to database
            log.warning(f"Cache error for user {user_id}: {e}")
            return self.db.fetch_feed(user_id)
        
        # Cache empty: Fetch from database
        feed = self.db.fetch_feed(user_id)
        
        # Try to update cache (best effort)
        try:
            self.cluster.set(
                cache_key,
                json.dumps(feed),
                ex=43200,  # 12 hour TTL
                nx=True  # Only if not exists
            )
        except:
            pass  # Cache update failed; that's ok
        
        return feed
```

---

### 4.2 Cache Stampede (Thundering Herd)

**Scenario:**
```
Popular article expires from cache at exact same time
1000 users request same article
All 1000 hit database simultaneously
```

**Timeline:**
```
T=0:00     → Article cache expires (TTL reached)
T=0:00     → User-1 requests article → Cache miss
T=0:00     → User-2 requests article → Cache miss
T=0:00     → ... User-1000 requests article → Cache miss
T=0:00     → 1000 concurrent database hits
T=0:05     → Database CPU at 95%; queries slow
T=0:10     → Database connection pool exhausted
T=0:15     → 503 Service Unavailable for all users
T=0:20     → Cache repopulated; requests succeed
T=0:20     → Database CPU cools down
```

**Mitigation:**
```python
def get_cached_article(article_id):
    cache_key = f"article:{article_id}"
    
    # Try cache first
    cached = cache.get(cache_key)
    if cached:
        return cached
    
    # Cache miss lock (only one process fetches DB)
    lock_key = f"lock:{cache_key}"
    
    if cache.set(lock_key, 1, nx=True, ex=5):
        # Got the lock; fetch from DB
        article = db.fetch_article(article_id)
        cache.set(cache_key, article, ex=3600)
        cache.delete(lock_key)
        return article
    else:
        # Someone else has lock; wait
        wait_count = 0
        while wait_count < 50:
            cached = cache.get(cache_key)
            if cached:
                return cached
            sleep(0.1)
            wait_count += 1
        
        # Timeout: fetch from DB ourselves
        return db.fetch_article(article_id)


# Alternative: Probabilistic expiration
def get_cached_article_v2(article_id):
    cache_key = f"article:{article_id}"
    
    # Get with TTL info
    article, ttl = cache.get_with_ttl(cache_key)
    
    if article is None:
        # Cache miss; fetch fresh
        article = db.fetch_article(article_id)
        cache.set(cache_key, article, ex=3600)
    
    elif ttl < 180:  # TTL < 3 minutes
        # Approaching expiration; refresh in background
        # but return stale data immediately
        thread_pool.submit(refresh_cache, article_id)
    
    return article
```

---

## SECTION 5: SEARCH (ELASTICSEARCH) FAILURES

### 5.1 Shard Allocation Failure

**Scenario:**
```
Elasticsearch cluster: 10 nodes, 5 shards, 2 replicas per index
Node-7 fails; shard reallocation fails due to disk space
```

**Timeline:**
```
T=0:00     → Node-7 dies
T=0:30     → Cluster detects missing node
T=1:00     → Attempts to reallocate shards from node-7
T=1:10     → Reallocation fails: Node-8 disk 85% full
T=1:10     → Shards remain unallocated
T=1:10     → Search requests to unallocated shard fail
T=2:00     → Ops adds node-11 (new capacity)
T=2:10     → Reallocation succeeds
T=3:00     → All shards healthy again
```

**Monitoring:**
```
Alerts:
- unassigned_shards > 0 → CRITICAL
- active_shards < expected → WARNING
- index.status != "green" → CRITICAL

Key metrics:
- cluster.health.active_shards
- cluster.health.unassigned_shards
- node.fs.available_percent
```

---

### 5.2 Indexing Lag During Spike

**Scenario:**
```
Viral news event: 1M articles in 1 hour (vs. normal 42K/hour)
Elasticsearch bulk indexing falls behind
```

**Timeline:**
```
T=0:00     → Viral event starts (Elon Musk breaking news)
T=0:15     → Articles arriving at 10K/sec (normal: 12/sec)
T=0:30     → ES bulk queue full; backpressure on publisher
T=0:45     → Indexing lag: 200K articles (30 min old)
T=1:00     → Search shows articles from 1 hour ago only
T=1:00     → Users complain: "Search is showing old articles"
T=2:00     → Spike subsides
T=3:00     → ES catches up; lag drops to 0
```

**Handling:**
```python
class SearchService:
    def search_articles(self, query, limit=50):
        try:
            results = es.search(
                index="articles",
                body={
                    "query": {"match": {"content": query}},
                    "size": limit,
                    "timeout": "2s"  # Hard timeout
                }
            )
            return results
        
        except TimeoutError:
            # ES too slow; fallback to PostgreSQL
            log.warning(f"ES timeout for query: {query}")
            pg_results = db.full_text_search(query, limit=limit)
            return pg_results
        
        except Exception as e:
            log.error(f"Search error: {e}")
            # Return most popular articles (cached)
            return cache.get("trending_articles") or []

class ESIndexer:
    def index_articles(self, articles):
        # Async indexing; don't block ingestion
        queue_size = len(self.async_queue)
        
        if queue_size > 100_000:
            # Queue full; apply backpressure
            log.warning(f"Async queue full: {queue_size}")
            alert_ops("Elasticsearch indexing lag > 30 min")
        
        self.async_queue.extend(articles)
    
    def bulk_index(self):
        """Run periodically (every 5 seconds)"""
        batch = self.async_queue[:1000]
        
        try:
            bulk(es, batch)
            # Remove indexed articles
            del self.async_queue[:1000]
        
        except BulkIndexError as e:
            log.error(f"Bulk index failed: {e}")
            # Retry with smaller batch
            batch = batch[:100]
            bulk(es, batch)
```

---

## SECTION 6: CASCADE SCENARIOS (Multiple Failures)

### 6.1 The Perfect Storm

**Scenario:**
```
T=0:00  → PostgreSQL primary node disk corruption
T=0:05  → Failover to replica (replica becomes new primary)
T=0:10  → During failover, replication lag = 5 minutes
T=0:10  → 5 minutes of writes lost (bookmarks from last failover)
T=0:15  → New replica not yet created (scaling up)
T=0:20  → User tries to bookmark article
T=0:20  → Primary is single node; no replica
T=0:20  → Network glitch on primary
T=0:25  → Primary unreachable
T=0:25  → NO PRIMARY = no writes possible
T=0:25  → All bookmark operations fail (users can't save articles)
T=1:00  → Ops manually restores primary from backup
T=1:15  → System operational; bookmarks working again
T=24:00 → Lost 5 minutes of bookmarks (users notified)
```

**Prevention:**
```
Design goal: RPO < 30 seconds (can lose max 30s of data)

Solution: Synchronous replication
- Writer commits only when replica ACKs
- Slow: +50ms latency
- But: Zero data loss

Trade-off:
- User action: Click "bookmark"
- With async replication: Response in 10ms
- With sync replication: Response in 60ms
- Customer perspective: acceptable (feels instant)
- Benefit: Zero data loss on failover

config:
postgresql.conf:
  wal_level = 'replica'
  synchronous_commit = 'on'  # Wait for replica ACK
  synchronous_standby_names = 'replica-db'  # Which replica to wait for
```

---

### 6.2 Cascading Timeout Scenario

**Scenario:**
```
Small delay in one service cascades into system-wide failure
```

**Timeline & Root Cause:**
```
T=0:00  → Elasticsearch query slow (5 seconds)
         Reason: Large aggregation query, GC pause

T=0:05  → API timeout waiting for ES
         Default timeout: 10 seconds → 5 second wait time left

T=0:10  → API response slow (5+ seconds)
         User browsers retry (JavaScript timeout: 30 seconds)

T=0:20  → 2x requests for same resource (retry)
         → Duplicate load on ES
         → ES CPU spikes

T=0:30  → More API timeouts
         → More retries
         → Cascading failure

T=1:00  → All APIs backed up; requests timing out
         → Frontend shows errors to all users
```

**Prevention:**
```python
# Circuit breaker pattern
class APIGateway:
    def __init__(self):
        self.es_failures = 0
        self.es_circuit_open = False
    
    def search(self, query):
        if self.es_circuit_open:
            # Return cached results instead
            return cache.get("trending_articles")
        
        try:
            result = es.search(
                query=query,
                timeout_ms=2000  # Strict timeout
            )
            self.es_failures = 0  # Reset counter
            return result
        
        except TimeoutError:
            self.es_failures += 1
            
            if self.es_failures >= 5:
                # Open circuit; stop hitting ES
                self.es_circuit_open = True
                log.critical("ES circuit open")
                return cache.get("trending_articles")
            
            raise
    
    def health_check_es(self):
        """Run every 10 seconds"""
        try:
            es.ping(timeout=1)
            if self.es_circuit_open:
                log.info("ES healthy again; closing circuit")
                self.es_circuit_open = False
                self.es_failures = 0
        except:
            pass  # ES still down

# Timeout hierarchy
Timeouts:
  Browser → API: 30 seconds (user will see spinner)
  API → Cache: 100ms (should be instant)
  API → ES: 2000ms (generous for search)
  API → Postgres: 5000ms (writes need time)
  API → Cassandra: 1000ms (reads should be fast)

Principle: Each layer has shorter timeout than upstream
If ES times out at 2s, API can timeout to browser at 5-10s
Prevents cascades
```

---

## SECTION 7: RECOVERY STRATEGIES

### 7.1 Backup & Restore

**PostgreSQL:**
```
Backup strategy:
- Full backup: Daily at 2 AM UTC (low traffic)
- WAL archiving: Continuous to S3
- Point-in-time recovery: Up to 7 days

Recovery:
- RTO: 30 minutes (restore from backup + apply WAL)
- RPO: 1 hour (last backup + WAL)

Example:
  Corruption detected at 3 PM
  Last backup at 2 AM = 13 hours of WAL
  
  Option 1: Restore to 2 AM (lose 13 hours)
  Option 2: Restore to 2:50 AM using WAL (lose ~10 hours)
  
  Choose: Option 2 (minimize data loss)
```

**Cassandra:**
```
Backup strategy:
- Snapshots: Daily (lightweight; uses hard links)
- Stored on S3 for retention
- TTL on data: Articles auto-expire after 6 months

Recovery:
- RTO: 1-2 hours (restore snapshot + rebuild indices)
- RPO: 1 day (last snapshot)

Note: With TTL, data older than 6 months deleted automatically
So "restore from 6 months ago" isn't needed
```

---

### 7.2 Testing Failures (Chaos Engineering)

```python
# Netflix Chaos Monkey approach
class ChaosMonkey:
    def inject_failure(self):
        """Run in staging; sometimes in production"""
        
        failure = random.choice([
            "kill_redis_instance",
            "slow_elasticsearch",
            "kill_postgres_replica",
            "partition_network",
            "fill_disk_90_percent",
        ])
        
        if failure == "kill_redis_instance":
            redis_cluster.remove_node(random_node)
            log.info("Killed Redis node; observing recovery")
            sleep(60)
            redis_cluster.add_node(random_node)
        
        if failure == "slow_elasticsearch":
            # Inject 5s latency to ES
            proxy.add_latency("elasticsearch", 5000)
            sleep(300)
            proxy.remove_latency("elasticsearch")
        
        # Observe: Did the system gracefully degrade?
        # Did alerts fire correctly?
        # Did users experience outage?

Test scenarios:
1. Single node failure (expect: system keeps running)
2. Cascading failure (expect: graceful degradation)
3. Network partition (expect: split-brain resolution)
4. Slow disk I/O (expect: timeout handling)
5. Memory pressure (expect: OOM killer doesn't crash)
```

---

## SECTION 8: CHECKLIST FOR INTERVIEWS

When asked about failures, work through this:

```
✅ INTERNAL FAILURE (this component fails)
   - What breaks? (this service is down)
   - How quickly detected? (health checks, timeouts)
   - How fixed? (circuit breaker, restart)

✅ UPSTREAM FAILURE (source fails, I'm downstream)
   - What's my dependency? (source data)
   - Can I degrade? (cache, old data)
   - Timeout? (give up after 5 seconds)

✅ DOWNSTREAM FAILURE (I fail, consumer is downstream)
   - What can I drop safely? (non-critical features)
   - What must I keep? (primary workflow)
   - Can I buffer? (queue, local storage)

✅ CASCADE FAILURE
   - What's the "weak link"? (slowest component)
   - How does slow propagate? (blocking calls)
   - Prevention? (circuit breakers, timeouts, backpressure)

✅ RECOVERY
   - How long to detect? (5-30 seconds)
   - How long to fix? (1-60 minutes)
   - How much data lost? (RPO: 0-1 hour)
   - How long users affected? (RTO: 1-30 minutes)
```

---

**Document generated for interview preparation: May 6, 2026**
