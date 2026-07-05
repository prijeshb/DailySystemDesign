# URL Shortener — Interview Q&A
**Date:** 2026-05-15  
**Format:** Interviewer question → ideal answer with reasoning

---

## Round 1: Problem Scoping (First 5-10 minutes)

**Q: "Design a URL shortener like bit.ly."**

> Don't jump in. Start with questions.

**Your response:** "Before I start designing, I'd like to clarify a few things to make sure I'm building the right system. A few questions..."

- Is this read-heavy or write-heavy?
- Do we need custom short codes, or just random ones?
- Do we need click analytics?
- Do links expire?
- What's the scale — how many URLs per day and how many redirects?

**Why this matters to the interviewer:** They're testing whether you scope before building. Engineers who jump straight to architecture without requirements are red flags.

---

**Q: "Let's say 100M URLs written per day and 10B redirects per day. Go."**

**Your response (back-of-envelope first):**
> "Let me do some quick math before designing.
> - 100M/day writes = ~1,200 writes/sec (manageable with a single well-tuned DB)
> - 10B/day reads = ~115,000 reads/sec (this is where we need caching heavily)
> - Read:write ratio is 100:1 — this is an extremely read-heavy system
> - Storage: 100M × 365 × ~500 bytes ≈ 18TB/year — feasible with sharding after year 2
>
> So the core design challenge is serving 115K reads/sec with < 10ms latency. I'll focus the design there."

---

## Round 2: Core Design Questions

**Q: "How do you generate the short code?"**

**A:** Walk through three options:
1. **Hash (MD5/SHA256, take first 7 chars)** — Easy, but collision-prone at scale. On collision, need retry logic with salt. Also, two users shortening the same URL get the same code — sometimes desirable, sometimes not.
2. **Auto-increment ID → Base62 encode** — My preferred approach. Guaranteed unique. 62^6 = 56 billion codes with just 6 chars. Downside: sequential IDs are guessable (security concern). Fix: XOR with a random secret before encoding.
3. **Pre-generated Key Pool (KGS)** — Best for very high scale. Service pre-generates millions of unique keys, Shortener service picks from pool. O(1) key acquisition, no DB round-trip for key generation.

> **Follow-up: "What if two Shortener Service instances grab the same key from the pool?"**  
> In KGS approach, the key is "claimed" in a transaction: `UPDATE unused_keys SET used=true WHERE key=? AND used=false`. Only one transaction succeeds (optimistic locking). Alternatively, each instance is given a pre-allocated exclusive batch.

---

**Q: "Should you return a 301 or 302 redirect?"**

**A:**  
- **301 Permanent:** Browser caches it forever. Future visits from same browser skip your server → saves load, but you lose analytics on repeat visitors.  
- **302 Temporary:** Every visit hits your server → full analytics, but higher origin load.  
- **Decision:** If analytics are required (which they almost always are for a URL shortener business), use **302**. If analytics don't matter and cost reduction is priority, use **301**.

> **Follow-up: "A user complains their updated link still redirects to the old URL in their browser."**  
> Classic 301 problem — browser cached the old redirect permanently. No way to clear it server-side. This is why you should default to 302 for links that can be updated. Or use `Cache-Control: no-store` on the redirect response and handle caching explicitly at CDN level.

---

**Q: "How do you scale the redirect service to handle 115K req/sec?"**

**A:**  
"The redirect service is stateless — it just looks up short_code → original_url and returns a redirect. Scaling it is straightforward:

1. **Horizontal scaling:** Add more instances behind a load balancer. At ~10K req/s per instance (modest), we need ~12 instances minimum, with buffer.
2. **Multi-layer caching:**
   - L1: Local in-process LRU cache (top 100K URLs per instance, ~50MB RAM)
   - L2: Redis Cluster (entire hot dataset, ~60GB RAM)
   - L3: CDN (absorbs 60%+ of traffic before it reaches your servers)
3. **Stateless design** makes auto-scaling trivial — spin up new instances on load spike with no warm-up needed (except L1 cache, which fills within minutes).

The key insight is that URL popularity follows a power law (Zipf's law) — 1% of URLs get 99% of clicks. Those top URLs will be in L1 cache of every instance, making the hot path essentially a memory lookup."

---

**Q: "How does your system handle a URL going viral — say, 1 million requests/second to a single short code?"**

**A:**  
"This is the hotspot problem. A few mitigations:

1. **L1 in-process cache** is the most important — every redirect service pod has this URL in RAM. 1M req/s across 100 pods = 10K/pod, easily handled from memory.
2. **CDN caching** — if the redirect is cacheable (which it is for a static URL), CDN handles most of it without hitting your origin at all.
3. **Request coalescing** — if the URL isn't cached yet and 100 threads all get a cache miss simultaneously, only one thread fetches from DB, others wait for that result (singleflight pattern in Go, for example).
4. **No locking needed** — the redirect is read-only, so no write contention."

> **Follow-up: "What if the viral URL is being updated frequently by the owner?"**  
> Now you have a read-heavy AND write-heavy hotspot on the same key. Cache invalidation becomes painful. Mitigation: queue updates and apply them in batches to reduce cache churn. Or accept brief inconsistency (show old URL for up to 60s after update).

---

**Q: "How would you implement click analytics without slowing down the redirect?"**

**A:**  
"Decouple analytics from the critical redirect path entirely:

1. Redirect service handles the 302 and **immediately** returns to the client.
2. Async: After sending the response, redirect service publishes a lightweight `ClickEvent` to Kafka. This takes ~1ms and doesn't block the response.
3. Analytics consumer reads from Kafka, aggregates, writes to ClickHouse (columnar store optimized for analytics queries).
4. For near-real-time click counts, I maintain a Redis counter (`INCR clickcount:{short_code}`). This is atomic, O(1), and doesn't touch the relational DB.

The user experience for analytics has eventual consistency (~5-10 second lag), which is totally acceptable for a dashboard."

> **Follow-up: "What if Kafka goes down?"**  
> The redirect still works — Kafka is fire-and-forget from the redirect service. If Kafka is unreachable, we drop the analytics event and log a warning. A few missed click events during a Kafka hiccup is acceptable. Alternatively, the service can buffer events in local memory for short bursts and retry.

---

## Round 3: Deep Dives

**Q: "How does your DB sharding work for this system?"**

**A:**  
"The primary lookup key is `short_code`, so I shard on that. 

Using consistent hashing: `shard_id = hash(short_code) % N`. With 16 shards, each shard handles 1/16th of writes (~75 writes/s) and reads.

Why not shard by user_id? The read path only knows `short_code` — it doesn't know who created the link. Sharding by user_id would require a two-step lookup (find user's shard, then find the link), which is slower and more complex.

The downside of sharding by short_code: queries like 'show all links for user X' must fan out across all shards. We can mitigate this with a secondary index table (user → list of short_codes) stored on a separate cluster, or by maintaining a denormalized 'user links' table per user."

---

**Q: "How would you handle link expiration?"**

**A:**  
"Two parts:

1. **Soft expiry:** Store `expires_at` timestamp. On redirect, check if `expires_at < now()` → return 410 Gone instead of redirecting. Fast, no background jobs needed.

2. **Hard cleanup (storage reclamation):** A scheduled background job runs daily:
```sql
DELETE FROM short_links 
WHERE expires_at < NOW() - INTERVAL '30 days'  
AND is_active = FALSE;
```
Soft-delete first (mark is_active=false) → hard delete after 30 days grace period.

3. **Return expired codes to the pool:** After hard delete, the short_code can be reused. Add it back to KGS's `unused_keys` table. This prevents code exhaustion over decades.

Cache consideration: Set Redis TTL = `expires_at - now()`. The cached entry auto-expires exactly when the link does — no manual invalidation needed."

---

**Q: "How do you prevent someone from enumerating all shortened URLs?"**

**A:**  
"Enumeration attack — if codes are sequential (`aaaaaa`, `aaaaab`...), an attacker can scrape all URLs.

Mitigations:
1. **Shuffle the base62 alphabet** with a secret seed before encoding. Sequential DB IDs map to seemingly random codes.
2. **XOR the ID with a secret** before base62 encoding: `encode(id XOR secret)`. Reversible for lookup, but non-sequential to observers.
3. **Rate limiting** on the redirect endpoint: > 1000 unique short_code requests from one IP in 1 minute → block.
4. **Require authentication** for sensitive links (private links feature).
5. **Don't embed sequential IDs in the hash** — use KGS with random pre-generated codes instead.

For most business use cases, shuffled base62 is sufficient. For highly sensitive links (internal documents), add private link mode with token-based access."

---

**Q: "A short link is used for a phishing site. How do you detect and handle this?"**

**A:**  
"Multi-layer approach:

1. **At write time:** Check against Google Safe Browsing API. Block if the original URL is flagged. This is the most effective prevention.

2. **Proactive scanning:** A background scanner periodically re-checks all active links against Safe Browsing (URLs can become malicious after creation).

3. **User reports:** `POST /report/{short_code}` endpoint. After N reports, auto-disable link and queue for review.

4. **At redirect time:** Check a local blocklist (bloom filter of known-bad short_codes, updated every 5 minutes from a central list). Near-zero latency, high recall.

5. **Transparency redirect:** For suspicious (not confirmed malicious) links, show an interstitial page: 'You are about to visit [url]. Continue?' This gives the user agency.

Response to confirmed malicious: `is_active = false`, CDN purge, log for abuse team."

---

## Round 4: System Properties

**Q: "Is your system eventually consistent or strongly consistent? Where does that matter?"**

**A:**  
"Intentionally mixed:

- **Strong consistency** for link creation — if I create `sho.rt/abc123`, anyone who immediately visits it must get the right redirect. I achieve this by writing to primary DB and synchronously invalidating Redis before returning success.
- **Eventual consistency** for analytics — click counts may be a few seconds behind. Totally acceptable for a dashboard.
- **Eventual consistency** for cache** — after a link update, old URL might be served for up to CDN TTL (1 hour worst case). Acceptable for most updates. For critical changes, I provide an explicit cache purge.

The key insight: strong consistency is expensive (synchronous operations, coordination). I apply it only where the user experience degrades meaningfully without it."

---

**Q: "How would you add a feature where users can see which country their link is most popular in — in near real time?"**

**A:**  
"Near real-time (< 30 second lag) country analytics:

1. Redirect service already publishes `ClickEvent {short_code, country_code, timestamp}` to Kafka.
2. Add a **Flink streaming job** that:
   - Windows over last 5 minutes of events per short_code
   - Counts events by country_code
   - Emits aggregates to Redis: `HSET analytics:{short_code}:country US 1542 IN 834 ...`
3. Analytics API reads from Redis directly: `HGETALL analytics:{short_code}:country` → O(1) lookup.

This gives < 30s latency with no DB involvement in the read path. For historical analytics (last 30 days), query ClickHouse."

---

## Common Mistakes to Avoid

| Mistake | Why It's Wrong | Correct Approach |
|---|---|---|
| Using synchronous analytics write | Doubles redirect latency | Async via Kafka, fire-and-forget |
| Returning 301 redirect | Breaks analytics, hard to update | Return 302 for updatable links |
| Single DB with no replication | Single point of failure | Primary + Multi-AZ replica + read replicas |
| Sharding by user_id | Read path doesn't know user_id | Shard by short_code |
| In-memory-only key generation | Race conditions at scale | DB sequence or KGS with transactions |
| Ignoring cache invalidation | Stale redirects after updates | Kafka-based invalidation events |
| Missing rate limiting | Trivially abusable | Token bucket per IP + per user |

---

## Signal Words & Phrases Interviewers Love

Use these to show depth:

- **"Power law distribution / Zipf's law"** — explains why 20% of links get 80% of traffic → justifies LFU cache
- **"Thundering herd"** — shows you know cache stampede risk
- **"Idempotent write"** — KGS key claiming must be idempotent (retry-safe)
- **"Fail open vs. fail closed"** — analytics fails open (drop event); safety checks fail closed (block redirect)
- **"Backpressure"** — if Kafka consumer is slow, Kafka provides natural backpressure rather than overwhelming downstream
- **"Singleflight pattern"** — for deduplicating simultaneous cache misses on the same key
- **"Read-your-writes consistency"** — after creating a link, the creator must immediately be able to use it

---

## Quick Reference: Decision Cheat Sheet

| "Should I use X?" | Answer |
|---|---|
| SQL vs NoSQL? | SQL (Postgres) — ACID for uniqueness, joins for user queries |
| Redis vs Memcached? | Redis — supports TTL, INCR, persistence, cluster mode |
| 301 vs 302? | 302 if analytics needed (almost always) |
| Hash vs auto-increment for key? | Auto-increment + base62 (simpler, guaranteed unique) |
| Sync vs async analytics? | Always async — never block the hot path |
| CDN caching? | Yes — absorbs 60%+ of traffic for popular links |
