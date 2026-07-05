# Tinder — Failure Analysis & Recovery Playbook
**Date**: 2026-07-04  
**System**: Dating / Match-Making App

---

## Failure-First Thinking Framework

For every component, ask:
1. What if **this component fails**?
2. What if the **component ahead** (dependency) fails?
3. What if the **component behind** (downstream consumer) fails?
4. What if it **slows down** instead of failing?

---

## Component Map

```
Client → API Gateway → [Profile Service | Swipe Service | Deck Service | Notification Service]
              │               │                 │               │
           Redis GEO      Postgres          Cassandra        Kafka
                              │                 │               │
                         S3 / CDN         Match Detector   FCM / APNS
```

---

## 1. Redis Failure (GEO + Deck Cache)

### Scenario A: Redis primary fails
**Impact:**
- Deck cache miss → deck generation falls back to cold DB query
- GEO index unavailable → cannot find nearby users
- Elo + boost lookups fail

**Detection:** Redis health check fails, circuit breaker trips within 5 failures (~2s).

**Solution:**
```
1. Redis Sentinel / Cluster with 3 replicas (1 primary + 2 replicas)
   Failover: Sentinel promotes replica in ~30s
   During 30s: circuit breaker OPEN → serve fallback

2. Fallback (deck cache miss):
   → Fetch last-known deck from Postgres:
     SELECT candidate_ids FROM user_deck_snapshot WHERE user_id=$1
   → Slower (~200ms vs ~10ms), but functional
   → Last snapshot updated every 30 min → stale deck, but swipeable

3. Fallback (GEO miss):
   → Fall back to PostGIS:
     SELECT id FROM users WHERE ST_DWithin(location, $1::geography, 50000)
     ORDER BY elo_score DESC LIMIT 100
   → Higher DB load, ~100ms, acceptable for 30s outage window

4. Boost feature: fail-open (don't apply boost, refund automatically)
```

**Trade-off:** During Redis outage, deck quality degrades (stale candidates), PostGIS gets 5-10× normal load for ~30s.

---

### Scenario B: Redis SLOW (not down, high latency — 500ms instead of 5ms)
**Detection:** P99 latency alert on Redis client > 100ms.

**Impact:** Deck fetch goes from <100ms to >500ms. Swipe cards don't appear instantly.

**Solution:**
```
Timeout set to 50ms on Redis calls (not default 2s).
On timeout: treat as cache miss → PostGIS fallback.
Circuit breaker on timeout count (not just errors).

Root causes of slowness:
  - Memory pressure (eviction thrashing) → monitor used_memory / maxmemory ratio
  - Expensive GEORADIUS on large key → ensure key is sharded by city/zone
  - Noisy neighbor on same Redis cluster → dedicated Redis for GEO vs. cache
```

---

## 2. Cassandra Failure (Swipe Storage)

### Scenario A: One Cassandra node fails (3-node cluster, RF=3)
**Impact:** With RF=3, any node failure is transparent. Quorum reads/writes (W=2, R=2) still succeed with 2 nodes.

**Solution:** Auto-rebalance. Hinted handoff for writes to recovering node.  
**No user-visible impact** if cluster ≥ 3 nodes and RF=3.

### Scenario B: Two Cassandra nodes fail simultaneously
**Impact:** Quorum lost (need 2 of 3, only 1 up). Swipe writes fail.

**Detection:** Cassandra write latency spikes, timeouts.

**Solution:**
```
1. Circuit breaker OPEN on Swipe Service → rejects new swipes with 503
2. Client shows "Something went wrong, try again" — user retries
3. Idempotency: each swipe has client-generated UUID
   If user retries after Cassandra recovers: same swipe, deduplicated by UUID
   
4. Alternatively: buffer swipes in Kafka for up to 60s
   Producer writes swipe event to Kafka (durable) even if Cassandra is down
   Swipe consumer replays once Cassandra recovers
   Match detection runs on replay → matches processed in order
   
Trade-off: 60s buffer = matches delayed up to 60s during outage.
Acceptable? Yes — match notification arriving 60s late is fine.
```

### Scenario C: Cassandra slow (high read latency for match check)
**Impact:** Match detection slow → match notification delayed > 5s.

**Solution:**
```
Match check query: SELECT WHERE swiper_id=B AND swipee_id=A
  → This is a primary key lookup → should be <5ms
  → If slow: likely hot partition (celebrity with millions of likes)

Hot partition mitigation:
  Add random suffix to partition key: swiper_id + "_" + (hash(swipee_id) % 10)
  Read from all 10 sub-partitions: UNION query
  Adds complexity but prevents single-node overload
  
Trade-off: More complex queries vs. scalability for viral users.
Threshold: implement only if a user accumulates >1M swipes.
```

---

## 3. Kafka Failure (Event Bus)

### Scenario A: Kafka broker loses leader
**Impact:** Swipe events not delivered to Match Detector.  
**Recovery time:** ISR re-election ~10-30s.

**During outage:**
```
Swipe Service producer: configured with acks=all, retries=5
  → Events buffered in producer memory (up to 32MB)
  → Once leader elected: buffered events sent in order
  → No data loss if outage < 30s (producer buffer time)

If outage > 30s (producer buffer fills):
  → Swipe Service writes swipe to Cassandra AND to outbox table in Postgres
  → Outbox poller: publishes to Kafka when broker recovers
  → Match detection catches up (may be minutes late)
```

**Trade-off:** Outbox adds write latency (+2ms DB write). Only activate for broker instability.

### Scenario B: Match Detector consumer lags (processing too slow)
**Impact:** Match notifications delayed by minutes/hours.

**Detection:** Consumer group lag alert: topic=swipes, lag > 100K messages.

**Solution:**
```
1. Scale out Match Detector consumers (Kafka partition count = max consumers)
   → If 12 partitions, can run 12 consumers in parallel
   → Each partition = ordered subset of swipes (by key = swipee_id)
   → Partition by swipee_id so all likes targeting user B go to same consumer
   → Match check: SELECT WHERE swiper_id=B AND swipee_id=A → still point lookup

2. If lag persists: Match Detector CPU bottleneck
   → Profile: bottleneck is Cassandra read per event (1 read/swipe)
   → Optimization: batch 100 swipe events → single multi-get Cassandra query
   → Reduces Cassandra RPCs 100×

3. Alert threshold: lag > 50K → auto-scale consumers; lag > 500K → page on-call
```

---

## 4. Deck Service Failure

### Scenario A: Deck Service crashes
**Impact:** Users can't get new profile cards.

**Solution:**
```
1. Deck Service is stateless (reads from Redis + Postgres)
   → Kubernetes/ECS: auto-restart in <30s
   → 3 replicas always running → LB routes to healthy instances

2. If all Deck Service instances fail:
   → Client shows cached last-fetched deck (client-side cache, 20 cards)
   → User can swipe through cached cards without noticing
   → Gives ~5 min of usable swipes before deck exhausted
   
3. Stale deck trade-off:
   Client caches deck for up to 30 min. User may see someone they already swiped on.
   Bloom filter on client also prevents showing already-swiped profiles.
   False positive rate: <1% → occasionally see re-swiped profile. Acceptable.
```

### Scenario B: Deck pre-computation job fails (batch job that generates decks)

```
Background job refreshes deck cache every 30 min per user.
If job fails: deck cache TTL expires → on-demand deck generation on next fetch
On-demand: 200ms vs. <10ms from cache. Not great but not catastrophic.

Monitor: deck cache hit rate < 80% → alert → investigate job health.
```

---

## 5. Photo CDN Failure

### Scenario A: CloudFront edge node fails
**Impact:** Photos in one geographic region fail to load.

**Solution:**
```
CloudFront: global anycast — request routes to next nearest PoP automatically.
Single edge failure: transparent, ~50ms extra latency for affected region.
Multi-region failure: fall back to S3 origin (500ms latency).

Client behavior:
  - Retry photo load 3× with exponential backoff
  - Show placeholder avatar if all retries fail
  - Profile card still usable (name, age, bio visible) — user can swipe
  
Trade-off: Swipe decisions made without seeing all photos → lower match quality.
Not a critical failure path — app functional.
```

### Scenario B: S3 origin bucket unavailable
**Impact:** Cache misses fail. New photo uploads fail.

**Solution:**
```
CDN cached photos: served from edge for up to 24h (max-age=86400) → no impact for existing photos
New uploads: fail → client shows "Photo upload failed, retry"
  Retry with exponential backoff: 1s, 2s, 4s, 8s (max 3 retries)

Multi-region S3 replication (S3 CRR):
  Primary: us-east-1
  Replica: eu-west-1
  CDN origin failover: route to replica if primary fails
  
Cost: S3 CRR adds ~$0.015/GB replication + storage.
For 125TB: ~$1,875 one-time + ongoing for new uploads.
Justified for user-generated content that can't be re-created.
```

---

## 6. Postgres Failure (Users, Matches)

### Scenario A: Primary Postgres fails
**Impact:** User profile reads/writes, match creation fail.

**Solution:**
```
RDS Multi-AZ: automatic failover to standby in ~60s.
Read replicas: deck generation, profile reads → route to replica (stale by <1s)

During 60s failover:
  Profile reads: serve from Redis cache (TTL 5 min) — mostly successful
  Profile writes: fail → client shows "save failed" → user retries after failover
  Match creation: Kafka buffered → processes once Postgres recovers
  
RYOW: after user updates profile, set session flag → read from primary for 5s
```

### Scenario B: Postgres slow (connection pool exhausted)
**Detection:** PgBouncer queue depth > 50, query latency > 200ms.

```
Root cause: N+1 queries in deck generation (profile fetch per candidate)
Fix: batch SELECT WHERE id IN (...) for all deck candidates in one query

Connection pooling:
  App → PgBouncer (pool) → Postgres
  PgBouncer: 100 connections to Postgres regardless of app instance count
  Prevents connection storm on scale-out
  
Alert: PgBouncer queue > 10 → investigate slow queries
       PgBouncer queue > 50 → circuit breaker on non-critical reads
```

---

## 7. Notification Service Failure (FCM / APNS)

### Scenario A: FCM provider outage
**Impact:** Match notifications not delivered.

**Solution:**
```
Multi-provider failover:
  Primary:  FCM (Android) / APNS (iOS)
  Fallback: Expo Push, OneSignal (if integrated)
  
  If FCM returns 5xx: retry 3× → publish to dead-letter queue
  Dead-letter queue consumer: retry with 5-min, 15-min, 60-min backoff
  
In-app fallback:
  Match state stored in DB. Next time user opens app → fetch pending matches via REST
  → "You have a new match!" shown on app open even if push failed
  → Push is best-effort; DB state is source of truth
  
Trade-off: User may not know about match until next app open.
Acceptable — not a financial transaction.
```

### Scenario B: WebSocket server for real-time notification crashes

```
WebSocket server: stateless push connection (connection registry in Redis)
If server crashes: client detects disconnect → reconnect with exponential backoff (1s, 2s, 4s)
New connection routes to any healthy WebSocket server via LB

Connection registry: SET ws:{user_id} {server_id} EX 300 (updated on each heartbeat)
On server crash: registry entries expire in 5 min → no stale routing after 5 min

Match event delivery:
  1. Try WebSocket (check registry → push to server_id via Redis Pub/Sub)
  2. If server_id not found in registry: user not connected → send push notification
```

---

## 8. Elo Score Computation Failure

### Scenario: Elo update job crashes mid-run

```
Elo is updated asynchronously (not on hot path).
If job crashes: elo scores are stale but decks still generated.
Impact: deck ranking less optimal for hours until job recovers.
Not user-facing. Monitor job completion time.

Design for idempotency:
  Elo update job processes swipe events from Kafka offset checkpoint.
  On restart: resume from last committed offset → reprocesses missed events.
  Elo updates: use Cassandra conditional writes (LWT) to avoid double-counting same swipe.
  
  UPDATE elo_updates SET processed=true WHERE swipe_id=$1 IF processed=false
  → Only updates elo if swipe not yet counted
```

---

## 9. Celebrity / Viral User Problem

**Scenario:** A celebrity joins Tinder. 100K users swipe right in first hour.

```
Cassandra hot partition:
  swipee_id = celebrity → all likes land on one partition → node overwhelmed

Fix: write spreading
  Swipe key: (swiper_id, swipee_id) — celebrity's received swipes distributed by swiper
  → Fine: reads by celebrity (swiper_id = celebrity) use partition key correctly
  → Reads "who liked celebrity" require scatter-gather or separate index table
  
Deck hot spot:
  Celebrity in deck of 10M users → deck cache has celebrity_id in 10M entries
  → No issue: deck cache is keyed by viewer, not celebrity
  
Elo computation:
  Celebrity's elo updating 100K times in one hour → batch: collect in 1-min tumbling window
  → One Cassandra write per minute per user (not per swipe)
  
Notification amplification:
  Celebrity swipes right on someone → Notification Service sends 1 push
  Celebrity gets 100K right swipes → 100K match events → 100K push notifications to 100K users
  → Normal — each user gets 1 notification. Not a broadcast storm.
```

---

## Failure Priority Matrix

| Component | Failure Type | User Impact | Recovery Time | Severity |
|-----------|-------------|-------------|---------------|----------|
| Redis (deck cache) | Primary failover | Slower deck load (~200ms) | 30s auto | Medium |
| Cassandra (swipes) | 2-node loss | Swipe failures | Minutes | High |
| Kafka | Broker failover | Match delay (30s) | 30s | Medium |
| Postgres | Primary failover | Write failures (60s) | 60s auto | High |
| CDN | Edge node | Photo load failure (one region) | Transparent | Low |
| FCM | Provider outage | Push delivery failure | Next app open | Low |
| Deck Service | Crash | Client uses cached deck (5 min) | <30s auto-restart | Low |
| Notification WS | Crash | Real-time miss → push fallback | <5s reconnect | Low |
