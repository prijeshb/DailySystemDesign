# Tinder — Interview Q&A
**Date**: 2026-07-04  
**Format**: Interviewer questions + strong answers + follow-up chains

---

## Opening Questions

**Q: Design Tinder.**

> Start with a clarifying question: "Should I focus on core swipe/match, or include messaging and recommendations?"

**Strong opening:**
"Tinder has three distinct subsystems: Discovery (finding candidates), Matching (swipe + mutual like), and Chat. I'll focus on Discovery and Matching since that's where the interesting design challenges are — CDN cost for photos, geo-based candidate filtering, and low-latency deck generation."

---

## Requirements Phase

**Q: What's the scale you're designing for?**

> "I'll assume 50M registered users, 10M DAU, ~100 swipes per user per day = 1B swipes/day. That's ~11,600 swipes/sec average, ~35K/sec at peak. Matches: 1% of right swipes = 10M/day."

**Q: What latency targets matter most?**

> "Swipe UX must feel instant — deck fetch should be <100ms p99. Match notification can tolerate 1-2s. Photo load: first frame under 200ms on 4G."

---

## Design Phase — Deck Generation

**Q: How do you decide which profiles to show a user?**

> "Three steps: geo filter → eligibility filter → rank.
>
> First, I query Redis GEO (GEORADIUS) for up to 500 active users within 50km — this is O(log N + M) and runs in ~5ms.
>
> Then filter by mutual preference: age in range, gender match. Also exclude already-swiped profiles using a per-user Bloom filter in Redis — O(1) check with ~0.1% false positive rate.
>
> Finally rank by Elo score descending, inject 10% random to avoid echo chamber, boost users at top. Cache result in Redis (deck:{user_id}) for 30 min."

**Q: Why pre-compute the deck instead of computing on demand?**

> "Swipe UX is tactile. If the next card takes 200ms to appear, the rhythm breaks. Pre-computing means the next card is always waiting in cache — instant display. The staleness (30 min) is acceptable: if someone updates their profile mid-session, the viewer sees the old version for at most 30 min."

**Q: Why a Bloom filter for the already-seen check? Why not just query Cassandra?**

> "Cassandra query for already-swiped profiles: I have 500 candidates and need to check each — that's 500 point lookups or one multi-key query. Bloom filter is O(1), stored in Redis, returns in <1ms. False positive rate of 0.1% means 1 in 1000 candidates is wrongly excluded. A user re-sees a swiped profile once in a while — not catastrophic. But I'd reconstruct the Bloom filter monthly from Cassandra to handle bloom filter growth."

**Follow-up Q: What's the Bloom filter false positive trade-off in this context?**

> "False positive = bloom filter says 'seen this user' when we haven't. Effect: user misses a potential match. At 0.1% and 500 candidates, that's 0.5 candidates excluded per deck — essentially invisible. If we increased false positive rate to 1% to save memory, we'd miss 5 candidates per deck — still likely acceptable. The correct threshold is business-driven: how much match opportunity are we willing to sacrifice for the memory savings?"

---

## Matching System

**Q: How does the match detection work?**

> "Matching is a read-your-own-writes problem on a graph. When A swipes right on B:
> 1. Write (A, B, LIKE) to Cassandra — partition key = A, clustering key = B, O(1) write.
> 2. Publish swipe event to Kafka (async).
> 3. Match Detector consumes the event, does: SELECT WHERE swiper_id=B AND swipee_id=A.
> 4. If found with direction=LIKE: INSERT into matches table, publish match event.
> 5. Notification Service consumes match event → FCM push to both users.
>
> Total latency from swipe to notification: ~1-2s."

**Q: Why Kafka between Swipe Service and Match Detector instead of direct call?**

> "Decoupling. If Match Detector is slow or down, swipes still complete instantly — Kafka buffers them. Match detection can catch up asynchronously. Also, multiple consumers: Notification Service and Chat Service both consume the match event independently without either blocking the other.
>
> The cost: eventual consistency — matches are detected 1-2s after the swipe, not instantly. For a dating app, this is fine."

**Q: What if Kafka consumer lags fall way behind?**

> "Monitor consumer lag. If lag grows past 100K messages (~10 min of swipes), auto-scale consumers. Kafka partitions by swipee_id — all likes targeted at user B go to the same partition, so match check is a partition-local operation. At 35K swipes/sec and 12 partitions, each consumer handles ~3K events/sec. Each event: one Cassandra point read (~2ms). That's 6K ms of work/sec on one thread — should be fine. If not: batch Cassandra reads (100 events → one multi-key read)."

---

## Elo / Ranking

**Q: How would you rank candidates for the deck?**

> "Elo score — a per-user score updated based on who swipes right on them. Being liked by high-Elo users raises your Elo more than being liked by average users.
>
> Formula: Δ = K × (actual - expected), where expected = 1 / (1 + 10^((rater_elo - rated_elo)/400)).
>
> K=32 standard. New users start at 1400. High-Elo users appearing at top of more decks get more exposure → more right swipes → their Elo reinforces itself.
>
> Trade-off: self-reinforcing loop. Fix: inject 10% random candidates from outside top Elo — ensures new users get some exposure, prevents complete stratification."

**Q: Tinder said they moved away from Elo. What might they use instead?**

> "A multi-signal model: engagement rate (right swipe %, messages sent after match), photo quality score (from a vision model), profile completeness, recency, mutual attribute compatibility (beyond just preferences — same-interest matching). Probably a learned ranking model (two-tower neural network) trained on downstream outcome (message sent, conversation lasted > 10 messages) rather than just mutual like. Elo is simple and interpretable but doesn't capture nuance."

---

## Photos & Cost

**Q: Photos are expensive. How do you control CDN costs?**

> "Three strategies:
> 1. Thumbnail-first: serve 20KB WebP at 400×400px for swipe cards. Only load full 200KB photo when user taps to expand. 10× size reduction.
> 2. Lazy CDN: CDN pull model — only cache photos people actually request. Long-tail user photos may never be served, no need to push to all edges.
> 3. Resize on upload: Lambda triggered on S3 upload generates thumbnail, medium, full sizes. We serve the right size for the right context.
>
> At 1B swipe impressions/day × 20KB = 20TB/day CDN = ~$51K/month. Without thumbnails (loading full 500KB per swipe): $250K/month."

**Q: What if we need to further cut costs — say, our CDN budget is $20K/month?**

> "Options in decreasing order of user experience impact:
> 1. Reduce pre-fetch: only pre-fetch 2 cards instead of 5 (saves 60% pre-fetch traffic).
> 2. Compress more aggressively: 12KB thumbnail at quality=70 WebP — slight visual degradation.
> 3. P2P image sharing within local network (aggressive, complex — probably not worth it).
> 4. Geo-restrict full-resolution to WiFi connections only (mobile data = compressed).
>
> Trade-off: A/B testing at image-heavy apps shows engagement drops ~8-15% per noticeable quality reduction step. Calculate: is $30K/month CDN savings worth an 8% reduction in swipes → 8% reduction in matches → 8% reduction in subscription upsell?"

---

## Failures & Edge Cases

**Q: What happens if a user swipes while offline?**

> "Client queues the swipe locally (SQLite or local cache). On reconnect, sends queued swipes in order. Each swipe has a client-generated UUID. Server applies them idempotently (ON CONFLICT DO NOTHING on Cassandra). Matches detected on server side as normal.
>
> Edge case: user swipes while offline, partner's profile changed (deleted account, changed photos). Server checks if swipee still active before processing."

**Q: What if Redis goes down for 30 seconds?**

> "Deck generation falls back to PostGIS query on Postgres (configured with 5,000ms timeout instead of Redis's 50ms — we allow more time for the fallback path). Deck fetch goes from <10ms to ~150ms. Noticeable but not catastrophic.
>
> Boost feature: fail-open. We skip boost injection, auto-refund boost minutes lost.
>
> GEO index unavailable: fall back to Postgres ST_DWithin query. Higher DB load — add read replicas for GEO queries if this is a concern."

**Q: Celebrity problem — what happens when a famous person joins?**

> "Two concerns: (1) Celebrity's deck: they need to see real profiles, not just matches from their own fanbase. We cap their view to users who opted into 'discover celebrities' preference. (2) Celebrity's incoming swipes: 100K right swipes per hour. Cassandra handles this fine since swipes are partitioned by swiper_id — celebrity's incoming swipes are distributed across all swiper partitions. The Elo update job batches their score updates into 1-min windows to avoid thundering herd on that single Elo write."

**Q: How do you handle fake profiles / bots that swipe right on everyone?**

> "Behavioral signals: bot patterns = 100% right swipe rate, swipes at non-human speed (>10/sec), no profile completeness, newly created account. Rate limit: max 50 swipes/min per user (sliding window in Redis). Anomaly detection: ML model on swipe pattern features. On detection: shadow-ban (bot sees normal UX but their likes don't register) rather than hard ban (prevents them from knowing and changing behavior)."

---

## Deep Follow-up Questions

**Q: Your Bloom filter reconstructs monthly. What happens to match quality in week 4 if the filter has grown large and false positive rate has increased?**

> "By week 4, a per-user Bloom filter with 30 days × 100 swipes/day = 3,000 elements might have false positive rate drift from 0.1% to 0.3% (depends on initial size allocation). Effect: 0.3% of 500 candidates = 1.5 wrongly excluded per deck — still negligible. But if user is heavy (3,000 swipes/month), the filter would be sized for that at creation time. Key: size Bloom filter based on expected 3-month lifetime (9,000 elements) to stay under 0.5% false positive throughout."

**Q: Why partition Kafka by swipee_id and not swiper_id for Match Detector?**

> "Match check queries: 'Did B like A?' — swiper_id=B. If we partition by swipee_id (which is A, the person swiped on), all swipe events for A land on same partition → Match Detector for that partition has all of A's incoming likes in memory or local Cassandra scope. This doesn't actually help the Cassandra query (it's still a point lookup). The real reason: if we ever wanted to do in-memory matching within the consumer (cache B's outgoing likes), partitioning by swipee_id puts all events where B was liked on one consumer, letting that consumer maintain a local cache of 'who has B liked' → avoids Cassandra read entirely. This is an optimization path worth mentioning."

**Q: What's your database schema for matches, and how does chat unlock work?**

> "Matches table (Postgres):
> ```
> matches (id UUID PK, user_a UUID, user_b UUID, matched_at TIMESTAMP, is_active BOOL)
> UNIQUE constraint on (LEAST(user_a, user_b), GREATEST(user_a, user_b))
> ```
> The LEAST/GREATEST ensures canonical ordering — (A,B) and (B,A) are the same match.
>
> Chat Service: when it consumes a match event, it creates a chat thread:
> ```
> threads (id UUID PK, match_id UUID, created_at TIMESTAMP)
> messages (id UUID PK, thread_id UUID, sender_id UUID, content TEXT, sent_at TIMESTAMP)
> ```
>
> Chat is locked until match exists: every message send checks JOIN threads t ON t.match_id = m.id WHERE m.is_active = true. If match deleted (unmatched), message API returns 403. No soft delete complexity — is_active=false is enough."

---

## Behavioral / Design Process Questions

**Q: You said inject 10% random candidates. How would you decide if it should be 5% or 20%?**

> "A/B test. Metric: not immediate engagement (right swipe rate) but downstream outcome — message rate, conversation length, subscription conversion. 10% random might slightly reduce short-term right swipe rate but increase match quality (better conversations → more Gold subscriptions). Run 30-day cohort experiment. Also: ethical dimension — 100% Elo ranking creates self-reinforcing inequality. Injecting randomness is both a product choice (discover unexpected people) and a fairness mechanism."

**Q: Should we store PASS swipes in Cassandra?**

> "Yes, with a TTL (90 days). Reasons: (1) Prevent re-showing passed profiles in the deck — Bloom filter can have false negatives on collision; Cassandra is authoritative. (2) Potential feature: 'undo swipe' (Tinder Gold) — need PASS records to undo. (3) Analytics: pass rate by age/distance helps tune recommendation model.
>
> Storage cost: PASS swipes are ~70% of all swipes (most people pass more than they like). 0.7B × 50B = 35GB/day. Cassandra with LZ4 compression: ~7GB/day. 90-day retention: 630GB — very manageable at Cassandra scale."

---

## Quick Reference — Key Numbers

| Metric | Value |
|--------|-------|
| Swipes/sec (peak) | ~35,000 |
| Deck fetch/sec | ~580 |
| Cassandra write throughput needed | 35K/sec (well within Cassandra capacity) |
| Redis GEO GEORADIUS latency | ~5ms |
| Deck cache hit rate target | >85% |
| Thumbnail size | 20KB WebP |
| CDN cost (10M DAU) | ~$51K/month |
| Match detection latency | 1-2s (async Kafka) |
| Elo range | 500-3000 |
| Bloom filter false positive target | <0.1% |
| S3 photo storage | ~125TB |
