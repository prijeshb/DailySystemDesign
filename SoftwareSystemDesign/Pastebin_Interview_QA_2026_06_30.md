# Pastebin — Interview Q&A
*Date: 2026-06-30*

---

## Opening Questions

**Q: Walk me through the system at a high level.**

A: Pastebin is fundamentally a key-value store with a web front-end. Users submit text content; the system assigns a short random ID; future requests for that ID return the content. The interesting design problems are: (1) generating collision-free short IDs at scale, (2) making reads extremely fast for viral pastes, (3) handling expiry correctly, and (4) storing large-vs-small content efficiently.

---

**Q: What are the read/write patterns? How does that shape the design?**

A: Read-heavy, roughly 10:1. Most pastes are written once and read many times — especially if shared publicly. This pushes toward aggressive caching: Redis for hot pastes, CDN for very popular ones. The write path is simple (generate ID, store) but needs to be reliable. The read path is the hot path that needs to be fast (<100ms p99).

---

## Deep-Dive Questions

**Q: How do you generate short unique IDs?**

A: Two approaches:
1. **Pre-generated pool**: a background job generates millions of random 6-char base62 strings, stores them in a `keys_available` table. On paste create, atomically claim a key via `SELECT ... FOR UPDATE SKIP LOCKED`. No collision possible since each key is checked for uniqueness at generation time.
2. **Snowflake → base62**: take a Snowflake 64-bit ID, encode in base62. Produces 10-11 chars (longer) but no extra service needed.

I'd use the pre-generated pool for interview purposes — it shows I understand operational concerns and gives deterministic 6-char URLs.

---

**Q: Why base62 instead of base64?**

A: Base64 uses `+` and `/` which are problematic in URLs (need percent-encoding). Base58 (used by Bitcoin) additionally removes `0`, `O`, `I`, `l` to avoid visual confusion. For Pastebin, base62 is the right balance: URL-safe, human-readable alphabet, no excluded chars needed since pastes aren't typed by hand.

---

**Q: How do you handle expiry?**

A: Two layers:
1. **Redis TTL**: when we cache the paste, set TTL = min(expires_at - now, 24h). Cache auto-evicts. Expired cache miss → DB lookup → past expiry → return 404.
2. **DB cleanup job**: runs every 5 minutes, `DELETE FROM pastes WHERE expires_at < now() LIMIT 5000`. S3 objects get lifecycle rules (delete after 180 days).

Why not rely only on TTL? Redis is a cache, not the source of truth. If cache is cold and someone requests an expired paste, the DB check is the authoritative gate. The cleanup job is for storage reclamation, not for enforcing expiry.

---

**Q: Should content be stored in Postgres or S3?**

A: It depends on size. For pastes ≤10KB (90th+ percentile), storing inline in a Postgres `TEXT` column is better: single read, no S3 round-trip, simpler code. For larger pastes, S3 makes sense: Postgres rows stay lean for index scans and metadata queries, and S3 is 10× cheaper per GB than Postgres EBS storage. The decision point of 10KB comes from Postgres's TOAST threshold (~2KB) and the observation that most code snippets are small. I'd measure and adjust.

---

**Q: How do you make a popular paste fast?**

A: Layered caching:
1. **CDN** (CloudFront): `Cache-Control: max-age=300` on GET /p/{id}. A viral paste on Reddit gets 50K reads/min → CDN absorbs >95% at edge nodes.
2. **Redis**: hot pastes served from Redis in <1ms, never hitting Postgres.
3. **Request coalescing** (singleflight): if CDN is cold and 1000 requests arrive simultaneously for the same paste, only 1 hits the DB; others wait on the Redis result.

The key insight: the DB never needs to handle viral load directly if CDN + Redis are properly configured.

---

**Q: What happens when a user deletes a paste that's cached at CDN?**

A: This is the stale-cache-after-delete problem. Two approaches:
1. **CDN invalidation**: call CloudFront's invalidation API immediately after delete. Propagates globally in ~30s. Costs $0.005 per path (cheap).
2. **Visibility rule**: only `public` pastes get CDN caching. `private` and `unlisted` pastes use `Cache-Control: no-store`. This is simpler and avoids needing to invalidate for the sensitive case.

For GDPR "right to erasure," approach 2 is more reliable — private data never enters CDN. Combine with short TTLs for public pastes (5min staleness acceptable for non-sensitive content).

---

**Q: How do you prevent abuse? (spam, malware, CSAM)**

A: Multi-layer:
1. **Rate limiting**: anonymous = 10 creates/hour per IP (Redis counter). Authenticated = 50/hour. Enforced at API Gateway.
2. **Content scanning**: integrate with a content moderation API (AWS Rekognition for images in pastes, custom text classifier for malware signatures/abuse patterns). Async on create — flag and quarantine.
3. **CSAM**: integrate PhotoDNA hash matching (Microsoft) at upload time. Immediate delete + report.
4. **Abuse reporting**: per-paste "Report" button → human review queue.
5. **CAPTCHA** for anonymous creates at threshold.

The key principle: make bad actors slow (rate limit), detect retroactively (content scan), and respond to user reports (moderation queue).

---

**Q: The interviewer asks: "What if two users create a paste at the exact same millisecond and get the same key from the pool?"**

A: With the pre-generated pool approach, this can't happen. The `SELECT ... FOR UPDATE SKIP LOCKED` is atomic — the DB locks the row, and the second transaction will skip to the next available key. They cannot both claim the same key.

If we were generating keys on-the-fly (random + check-existence): yes, two could generate the same random string. We'd check `INSERT INTO pastes ... ON CONFLICT (id) DO NOTHING` and retry on conflict. With 62^6 = 56B space and 2.5M creates/day, collision probability is astronomically low but not zero — the retry handles it.

---

**Q: How would you scale writes to 100× (from 29 writes/sec to 2900 writes/sec)?**

A: Current bottleneck at 29 writes/sec is comfortably within a single Postgres instance (~5000 simple writes/sec). At 2900 writes/sec:

1. **Vertical scale Postgres first**: db.r6g.4xlarge handles ~20K writes/sec. Covers 7× growth.
2. **Shard if needed**: shard by `paste_id % N` (N shards). Writes/reads go to specific shard. Key pool must be shard-aware.
3. **Content to S3**: ensure all content (not just large) goes to S3, removing the write payload from Postgres (just store metadata row — ~100 bytes vs 5KB).
4. **Key Generator**: horizontally scale, each instance owns a key range.

For Pastebin at 2900 writes/sec, vertical scale + S3 offload is sufficient — no sharding needed.

---

**Q: Interviewer follow-up: "What if we need full-text search across all pastes?"**

A: This is an optional feature. Core approach:
1. **Elasticsearch** (or OpenSearch): index paste content + metadata (title, language, user_id).
2. **Sync via Kafka + CDC**: Postgres writes → Kafka topic `paste.created` → Elasticsearch consumer indexes.
3. **Public pastes only**: search only indexes `visibility=public` pastes.
4. **Index anonymized versions**: strip sensitive-looking content (API keys, passwords via regex) before indexing.

Latency: Elasticsearch full-text search < 100ms for typical queries.
Cost: AWS OpenSearch, 2-node cluster = ~$200/month. Acceptable given feature value.

The trade-off: near-real-time indexing (1-2s lag from Kafka). Acceptable for search. The privacy concern (searching others' public pastes) is the harder product question.

---

## Behavioral / Trade-off Questions

**Q: "You have a $200/month budget. What do you cut and what does that mean for reliability?"**

A: At $200/month:
- Drop CloudFront CDN → all reads hit the API server directly. Max capacity ~500 reads/sec without CDN absorbing viral spikes. Viral pastes will take down the service.
- Single RDS (no Multi-AZ) → DB failure = 5-10 min downtime until manual recovery.
- Single Redis → cache restart causes DB load spike.
- No horizontal scaling → a single traffic spike can overwhelm the service.

The honest answer: $200/month gives you a service that works fine at low traffic but has no resilience. First thing to add with more budget: Multi-AZ RDS ($57/month extra) — that protects your data. Second: Redis Sentinel. Third: CDN. Prioritize data durability over availability optimizations.

---

**Q: "Why not just use a database as both the primary store and the cache? Redis seems like an extra component."**

A: Fair question. At low scale (<100 reads/sec), Postgres alone is fine. Redis adds value when:
1. Read latency matters: Postgres even on SSD = 1-5ms. Redis = 0.1ms. For p99 SLA of 100ms, this headroom matters.
2. Read amplification: 290 reads/sec could spike 50× for viral content. Redis absorbs those; Postgres would need expensive vertical scaling.
3. Temporary state: rate limit counters, distributed locks, key pool — these belong in Redis anyway.

The rule: Redis as cache on top of Postgres, not instead of. Postgres is source of truth; Redis is performance optimization.

---

## Common Interviewer Follow-ups

| Question | Core of Answer |
|----------|---------------|
| "How do you prevent ID enumeration (someone scanning all pastes)?" | IDs are random 6-char base62 — not sequential. Scanning 56B IDs would take years. For private pastes, add auth check independent of ID secrecy. |
| "How do you handle duplicate content?" | Content-addressed storage (SHA256 of content as dedup key) optionally. Saves S3 storage. But complicates delete (don't delete S3 object if other pastes share same hash). Trade-off: complexity vs cost. Worth only if >5% duplicates observed. |
| "How do you make it work offline?" | PWA with service worker caching. Out of scope for backend interview. |
| "What's your DB schema for analytics?" | ClickHouse table: `paste_views (paste_id, viewer_ip_hash, country, referrer, viewed_at)`. Aggregated hourly. Postgres never gets hit for analytics. |
| "How do you handle GDPR deletion?" | Soft delete first (deleted_at timestamp), purge after 30 days. Log deletion in audit table (paste_id, deleted_by, reason, timestamp) — retained for compliance. S3 delete + CDN invalidation on demand. |
