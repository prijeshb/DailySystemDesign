# System Design: Tinder (Dating / Match-Making App)
**Date**: 2026-07-04  
**Difficulty**: Hard  
**Tags**: geo-search, recommendation, real-time, fan-out, CDN, graph

---

## First Principles: Do We Need This?

**Why does Tinder exist?**  
People want to meet others nearby with mutual interest. The core problem: how do you surface the right people at the right time without showing irrelevant or already-seen profiles?

**Core insight before building:**  
1. Discovery (who to show) and Matching (mutual like) are separate concerns — different latency/consistency needs.  
2. Photos dominate bandwidth costs — thumbnail-first loading is not optional.  
3. The "deck" (stack of profile cards) must be pre-computed — users expect instant swipe, not 200ms wait per card.

---

## Requirements

### Functional
- Users create profile with photos, bio, age, gender, preferences (age range, distance, gender)
- App shows a deck of candidate profiles (nearby, within preferences)
- Swipe right (like), left (pass), or super like
- If both users like each other → match → chat unlocked
- Real-time match notification
- Boost: pay to appear at top of others' decks for 30 min

### Non-Functional

| Metric | Target |
|--------|--------|
| DAU | 10M |
| Swipes/day | 1B (100 swipes/user avg) |
| Matches/day | 10M (1% of right swipes) |
| Deck fetch latency | <100ms p99 |
| Match notification latency | <2s |
| Photo storage | 125TB (50M users × 5 photos × 500KB) |
| Availability | 99.9% |

---

## Back of Envelope

```
Swipe rate:
  1B swipes/day ÷ 86,400s = ~11,600 swipes/sec (peak ~3×: 35,000/sec)

Deck generation:
  10M DAU, each opens app ~5 times/day, fetch ~10 cards/batch
  = 50M deck fetches/day = ~580 fetches/sec
  → cacheable, not a DB bottleneck

Photo CDN:
  Swipe card = thumbnail (50KB). Full profile load on expand = 5 photos × 100KB = 500KB
  Conservative: 1B swipes × 50KB thumbnail = 50TB/day CDN traffic
  AWS CloudFront: $0.085/GB × 50,000GB = $4,250/day = $127,500/month
  
  With pre-fetch 5 profiles × 500KB full: adds 10M DAU × 5 × 5 × 100KB = 25TB/day
  Total: ~75TB/day CDN = ~$190K/month

Budget constraint:
  Reduce thumbnail to 20KB (aggressive compression, 400×400px WebP):
    1B × 20KB = 20TB/day = $51K/month (60% savings)
  Lazy-load full photos only on expand (not pre-fetch all 5):
    Saves ~$80K/month vs. eager full-profile pre-fetch
  
  Industry practice: Tinder serves ~20-40KB WebP thumbnails, lazy loads full resolution.
  Estimated CDN budget: $50-80K/month at 10M DAU scale.

Storage:
  Swipes: 1B/day × 50 bytes = 50GB/day → Cassandra (append-only, high write)
  Matches: 10M/day × 100 bytes = 1GB/day → Postgres
  Photos: 125TB S3 (one-time, grows at 50M users × 5 × 500KB / churn rate)
```

---

## Entities & Actions

### Core Entities

```
User
  id, name, age, gender, bio
  location (lat, lng, updated_at)
  preferences (min_age, max_age, max_distance_km, gender_pref)
  elo_score (500-3000, used for deck ranking)
  photos [photo_id, url, order]
  is_boosted (bool, boost_expires_at)

Swipe
  swiper_id, swipee_id, direction (LIKE|PASS|SUPERLIKE), swiped_at

Match
  id, user_a_id, user_b_id, matched_at, is_active

Message
  id, match_id, sender_id, content, sent_at, read_at

Block / Report
  reporter_id, reported_id, reason, created_at
```

### Actions

```
profile.update     → write to Postgres + invalidate cache
photo.upload       → S3 upload → CDN propagation
location.update    → Redis GEO (every 5 min while app open)
deck.fetch         → pre-computed candidates from Deck Service
swipe.record       → write to Cassandra → async match check
match.check        → read Cassandra: did B like A? → if yes, create match
match.notify       → push notification via FCM/APNS
message.send       → write to Message DB → push to partner via WebSocket
boost.activate     → Redis key with TTL, bumps elo temporarily
```

---

## Data Flow

```
[1] Profile discovery:
User A opens app
  → app sends location ping to Location Service
  → Location Service: GEOADD users:active user_A lat lng EX 300
  → Deck Service fetches pre-computed deck for user_A (Redis cache)
  → If cache miss: run candidate query → rank → populate cache

[2] Swipe:
User A swipes right on User B
  → POST /swipes {swipee_id: B, direction: LIKE}
  → Swipe Service writes to Cassandra: (A, B, LIKE, timestamp)
  → Swipe Service publishes to Kafka: topic=swipes, key=B
  → Match Detector (Kafka consumer) reads event
  → Queries Cassandra: SELECT WHERE swiper_id=B AND swipee_id=A AND direction=LIKE
  → Found? → INSERT into matches table → publish match event

[3] Match notification:
Match event → Notification Service
  → Send FCM push to both A and B
  → If A or B has open WebSocket → push in-app banner
  → Chat thread created in Message Service
```

---

## High-Level Design

```
                        ┌─────────────────────────────────────────┐
Mobile App              │              API Gateway / LB            │
  (iOS/Android)  ──────►│  Auth (JWT) · Rate Limit · Route        │
                        └────┬────────┬──────────┬────────────────┘
                             │        │          │
                     ┌───────▼──┐ ┌───▼──────┐ ┌▼──────────────┐
                     │ Profile  │ │ Swipe    │ │ Deck          │
                     │ Service  │ │ Service  │ │ Service       │
                     └────┬─────┘ └───┬──────┘ └──────┬────────┘
                          │           │               │
                     ┌────▼───┐  ┌────▼───┐    ┌─────▼──────┐
                     │Postgres│  │Cassand.│    │  Redis     │
                     │(users, │  │(swipes)│    │  (geo,     │
                     │matches)│  │        │    │  deck cache│
                     └────────┘  └────┬───┘    │  elo, boost│
                                      │        └────────────┘
                                 ┌────▼──────┐
                                 │  Kafka    │
                                 └────┬──────┘
                                      │
                              ┌───────▼────────┐
                              │ Match Detector │
                              │ (Kafka consumer│
                              └───────┬────────┘
                                      │
                          ┌───────────▼──────────┐
                          │  Notification Service │
                          │  FCM / APNS / WS     │
                          └──────────────────────┘

Photos: S3 ──► CloudFront CDN ──► Mobile
```

---

## Low-Level Design

### 1. Deck Generation — Who to Show

**Why pre-compute?**  
Swipe UX requires instant card display. Running geo + filter + rank on demand = 200ms+ → unacceptable.

**Deck population algorithm:**

```
Step 1: Geo filter (Redis GEO)
  GEORADIUS users:active A.lat A.lng 50 km ASC COUNT 500
  → returns up to 500 active users within 50km

Step 2: Eligibility filter (in-memory after geo query)
  Filter candidates where:
    candidate.age IN [A.min_age, A.max_age]
    candidate.gender IN A.gender_pref
    A.age IN candidate.min_age..candidate.max_age  ← mutual preference
    NOT already swiped (check Bloom filter or Cassandra lookup)

Step 3: Rank by Elo score
  Sort eligible candidates by elo_score DESC
  Inject 10% random to avoid echo chamber
  Boost users → jump to top of deck (temporarily high elo)

Step 4: Cache result
  Redis key: deck:{user_id} → ordered list of candidate_ids, TTL 30 min
  Replenish when deck drops below 20 cards remaining

Step 5: Fetch profile data
  For top 10 cards: fetch name, age, thumbnail URL from Redis/Postgres
  Return to client as batch
  Client pre-fetches thumbnails from CDN
```

**Already-seen filter:**

```
Option A: Cassandra lookup
  SELECT WHERE swiper_id=A AND swipee_id IN candidates
  At scale: 500 candidates × Cassandra read = feasible but slow

Option B: Bloom Filter (preferred)
  Per-user Bloom filter stored in Redis: seen:{user_id}
  False positive rate ~0.1% → user occasionally re-sees swiped profile
  Acceptable trade-off vs. O(1) lookup
  
  GETBIT seen:{user_id} hash(candidate_id)
  → instant, no Cassandra round-trip
  
  Trade-off: Bloom filter only grows, never shrinks.
    Fix: reconstruct monthly from Cassandra (batch job)
    Or: use Counting Bloom Filter (supports deletes, 4× memory)
```

### 2. Elo Rating System

Tinder publicly confirmed Elo-style scoring (before their "Intelligent Matching" rebrand).

```
New user: elo = 1400 (average)

When A swipes right on B:
  A is "choosing" B → B's elo goes up by:
    Δ = K × (1 - expected_outcome)
    expected_outcome = 1 / (1 + 10^((A.elo - B.elo) / 400))
    K = 32 (adjustment factor)

When A swipes left on B:
  B's elo goes down:
    Δ = K × (0 - expected_outcome)

Effect: Being liked by high-elo users raises your elo more than being liked by average users.

Super like: counts as 3× normal like in elo calculation
Boost: temporary elo multiplier (×3 for 30 min) → shown to more users

Trade-off:
  Pro: self-calibrating, quality signal from peer behavior
  Con: new users start low and get shown less → cold-start problem
  Fix: new user boost (first 7 days, ×1.5 elo for deck insertion)
       And: show new users to established users to collect initial signals quickly
```

### 3. Swipe Storage — Cassandra Schema

```sql
CREATE TABLE swipes (
  swiper_id  UUID,
  swipee_id  UUID,
  direction  TEXT,   -- LIKE | PASS | SUPERLIKE
  swiped_at  TIMESTAMP,
  PRIMARY KEY ((swiper_id), swipee_id)
) WITH CLUSTERING ORDER BY (swipee_id ASC);

-- Lookup: did B like A?
-- Query pattern: SELECT WHERE swiper_id = B AND swipee_id = A
-- Direct row lookup: O(1)

-- Partition key = swiper_id → all swipes by one user in same partition
-- 10M users × 3,650 swipes/year (avg) = 36.5B rows → partition sizes fine
-- Compaction handles old data, TTL on PASS rows after 90 days (reduce storage)
```

### 4. Match Detection

```
Kafka consumer group: match-detector

On LIKE event (A liked B):
  1. Query Cassandra: SELECT direction FROM swipes
                      WHERE swiper_id = B AND swipee_id = A
  2. direction = LIKE or SUPERLIKE?
     → Yes: create match
     → No / not found: no match yet

Match creation (idempotent):
  INSERT INTO matches (id, user_a_id, user_b_id, matched_at)
  ON CONFLICT (user_a_id, user_b_id) DO NOTHING
  -- Covers duplicate Kafka delivery (at-least-once)

Publish to Kafka: topic=matches
  → Notification Service consumes → FCM push to both
  → Chat Service consumes → create empty chat thread
```

### 5. Photo Pipeline

```
Upload flow:
  Client → Multipart upload to Photo Service
  Photo Service → S3 (original, e.g. 4MB)
  S3 trigger → Lambda → resize to:
    thumbnail: 400×400px WebP, ~20KB
    medium:    800×800px WebP, ~80KB
    full:      1200×1200px WebP, ~200KB
  → Store all 3 in S3 under /photos/{user_id}/{photo_id}/{size}.webp
  → CDN (CloudFront) serves all reads

Serving:
  Deck card: thumbnail URL (pre-signed or public CDN URL)
  Profile expand: medium URL
  Profile zoom: full URL only on tap
  
  CDN Cache-Control: max-age=86400 (photos rarely change)
  Photo update: new photo_id → new CDN URL → no cache busting needed
```

### 6. Location Update

```
Client pings location every 5 min while app is open, or on significant movement (100m+).

Location Service:
  GEOADD users:active {lat} {lng} {user_id}
  EXPIRE users:active:{user_id} 300s  ← user considered "active" if pinged in last 5 min

For deck generation, use location snapshot (not real-time):
  Store lat/lng in Postgres: last_location_updated_at
  Deck query uses stored location (good enough — user doesn't teleport)
  Redis GEO index updated on each ping for "active users" query
  
Trade-off:
  Using Redis GEO: fast proximity query, stale by up to 5 min
  Using PostGIS on primary DB: fresh, slower, higher load
  → Redis GEO for deck discovery, PostGIS for exact "within X km" verification
```

### 7. Boost Feature

```
User purchases Boost (30 min):
  → SET boost:{user_id} 1 EX 1800   (Redis, 30 min TTL)
  → Record in Postgres: boosts (user_id, purchased_at, expires_at) for analytics

Deck generation checks:
  If GETBIT boost:{candidate_id}: inject at top of ranked list
  Elo score for boosted user: elo × 3 (during boost window only)

Revenue: $3-10 per boost. At 10M DAU, 1% purchase = 100K boosts/day = $300K-$1M/day
```

---

## Trade-offs Summary

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| Deck pre-computation | Pre-compute + Redis cache | Stale deck (30 min) vs. instant swipe UX |
| Already-seen filter | Bloom filter | 0.1% false positive (re-shows swiped profile) vs. O(1) check |
| Swipe storage | Cassandra | High write throughput; no joins (match check is point lookup) |
| Match detection | Async Kafka | 1-2s match notification delay; decoupled from swipe hot path |
| Photo serving | Thumbnail-first CDN | 60% CDN cost savings; full photo only loaded on expand |
| Location | Redis GEO (5-min staleness) | Saves PostGIS load; users don't move far in 5 min |
| Elo scoring | Async batch update | Not real-time; elo updates lagged by minutes; acceptable |
| Caching | Cache-aside for profile data | Stale profile if user updates bio mid-session; TTL 5 min |

---

## AWS Cost Estimate (10M DAU)

| Component | Est. Monthly Cost |
|-----------|------------------|
| CDN (CloudFront, 20TB/day thumbnails) | ~$51,000 |
| S3 storage (125TB photos) | ~$2,875 |
| Cassandra (r5.2xlarge × 6, 3 AZ) | ~$5,200 |
| Postgres (db.r5.4xlarge Multi-AZ) | ~$4,500 |
| Redis (cache.r6g.2xlarge × 3) | ~$1,800 |
| Kafka (MSK, 3-broker) | ~$1,500 |
| App servers (ECS, auto-scale) | ~$8,000 |
| Lambda (photo resize) | ~$500 |
| FCM/APNS | Free |
| **Total** | **~$75,000/month** |

Revenue context: Tinder Gold ~$30/month × 5% paying users × 10M DAU = $15M/month.  
Infrastructure is ~0.5% of revenue at this scale.

**Budget constraint example:**  
If CDN budget is cut to $20K/month, options:  
- Reduce thumbnail quality further (15KB vs 20KB — visible degradation)  
- Limit pre-fetch to 3 cards instead of 5  
- Introduce progressive loading (solid color placeholder → low-res → high-res)  
- Show fewer photos per profile card  
**Limitation:** User engagement drops ~8-15% when photo quality degrades (from A/B test data at image-heavy apps).
