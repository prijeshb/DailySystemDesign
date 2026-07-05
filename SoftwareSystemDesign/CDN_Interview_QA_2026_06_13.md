# CDN — Interview Q&A
**Date:** 2026-06-13

---

## Opening Questions

**Q: Design a CDN. Where do you start?**

A: I start with first principles — why does a CDN exist? Speed of light. A user 150ms away from origin can't get a response faster than 300ms (2 RTTs). You can't software-engineer around physics. A CDN places content within 10ms of the user. That's the fundamental value proposition.

Then I frame scope: are we designing the edge serving layer (PoP → user), the origin integration, cache invalidation, or the control plane? In 45 minutes, I'll focus on: routing users to nearest PoP, the caching layer at edge, origin shield, and cache invalidation.

---

**Q: What's the difference between a CDN and a load balancer?**

A: Different layers, different goals.
- Load balancer distributes requests across servers in one region (horizontal scaling of one origin).
- CDN distributes content across the globe, physically near users. Reduces latency via proximity, not just load distribution.
- A CDN effectively reduces origin load by serving most content from cache — the origin may only see 1-5% of user requests.
- Load balancers are typically in-path stateful L4/L7 proxies. CDN edge servers are stateful caches that usually have their own BGP presence and PoPs globally.

---

## Routing Questions

**Q: How does Anycast work? Can multiple locations have the same IP?**

A: Yes. BGP (Border Gateway Protocol) allows multiple autonomous systems to announce the same IP prefix. Internet routers forward packets to whichever AS has the shortest path — which is usually the geographically closest PoP. The user's ISP peers with the CDN's PoP, so traffic stays local.

Cloudflare uses 104.16.0.0/12 from 300+ PoPs. Every PoP announces the same block. A user in Mumbai hits the Mumbai PoP, a user in London hits Amsterdam — no DNS lookup needed.

Failover is automatic: if a PoP goes down, it stops announcing the prefix. BGP convergence (~30s) reroutes users to next-nearest PoP.

---

**Q: Anycast vs DNS-based geo-routing — when do you use which?**

A: 
- **Anycast**: sub-30s automatic failover, more precise routing (BGP topology vs IP geolocation DB), no DNS dependency. Requires BGP peering infrastructure globally. Chosen by Cloudflare, Google. Better for high-traffic, DDoS mitigation (BGP black-holing during attacks).
- **DNS geo**: simpler, works with standard infrastructure, easy multi-CDN setup (CNAME to CDN). Failover via TTL (30-60s). Chosen by Akamai, CloudFront. Good when you need CNAME flexibility or mix multiple CDN providers.

For a new CDN at scale: Anycast. For a product using an existing CDN: DNS geo with short TTLs is fine.

---

## Caching Questions

**Q: A customer deploys a bug in their JS file cached at your CDN with a 1-year TTL. How do they fix it?**

A: Two paths:
1. **URL versioning (correct approach, zero latency):** Deploy the fix as a new filename with a new content hash: `app.a3b4c5.js` → `app.f9e2d1.js`. Update HTML to reference the new filename. CDN sees a different URL → cache miss → fetches fresh. Old file expires naturally. No purge needed, effective globally in milliseconds.
2. **Hard purge (if URL versioning wasn't used):** Call CDN Purge API with the URL. Control plane distributes purge event to all PoPs via Kafka/message queue. PoPs delete the cached key. ~2s to propagate globally. Next request causes cache miss → fresh fetch. Stale window ≤ 2s.

The lesson: always use content-fingerprinted URLs for assets. TTL can be 1 year with zero staleness risk because content hash in filename changes when content changes.

---

**Q: What is `stale-while-revalidate` and when would you use it?**

A: `stale-while-revalidate=N` means: serve the cached (stale) response immediately AND asynchronously revalidate in the background. The user gets a fast response. The updated version is ready for the next user.

Example: `Cache-Control: max-age=60, stale-while-revalidate=30`
- For 60s: serve fresh, no revalidation.
- At 60s-90s: serve stale immediately, but trigger background fetch to origin.
- At 90s: if background fetch failed, content is truly expired.

Use when: content changes moderately (news headlines, trending items, product prices) and you'd rather users see 30s stale content than wait for origin round-trip. Do NOT use for: financial data, inventory counts, authentication state.

---

**Q: How do you cache personalized content like a user's profile page?**

A: True personalization (different per user) cannot be shared via CDN — it would serve one user's data to another. Options:

1. **Don't cache at CDN**: `Cache-Control: private, no-store`. CDN skips. This is correct for authentication-required pages.
2. **Cache the shell, personalize at client**: CDN caches the page skeleton (HTML without user data). JS fetches personalized data via API calls (which are uncached or short-TTL cached per-user at browser level).
3. **Edge Side Includes (ESI)**: CDN caches sections of a page independently. Non-personalized sections (nav, footer, product catalog) cached long. Personalized sections (user name, cart count) fetched fresh from origin per request and stitched in at edge. Used by Akamai, Varnish.
4. **Segment-level caching with Vary**: `Vary: Accept-Language` → cache separate copies per language. Coarse segmentation, not per-user. Works for locale-specific content.

---

**Q: What is cache key normalization and why does it matter?**

A: The cache key determines whether two requests hit the same cached object. If key normalization is wrong, you either:
- **Fragment unnecessarily**: `?a=1&b=2` and `?b=2&a=1` are different keys but should hit same cache → cache fills up with duplicates, hit ratio suffers.
- **Fail to differentiate**: Two different requests get the same key → serve wrong content (security bug).

Normalization includes:
- Sort query parameters alphabetically
- Lowercase scheme, host, header names
- Strip irrelevant headers (tracking UTM params `?utm_source=google` shouldn't bust cache)
- Strip `Cookie` and `Authorization` from cache key (or you'd have per-user cache = no sharing)
- Include `Accept-Encoding` if you store compressed and uncompressed variants separately

---

**Q: What's cache poisoning? Give an example and prevention.**

A: Cache poisoning: attacker causes CDN to store malicious content for a URL, then CDN serves it to all users.

Classic example — HTTP Host header injection:
```
GET /header-injection HTTP/1.1
Host: attacker.com
X-Forwarded-Host: victim.com

If the origin uses X-Forwarded-Host to build URLs in the response, and the CDN doesn't include X-Forwarded-Host in the cache key, the response (containing attacker.com links) gets cached as the response for victim.com/header-injection. Every user requesting that URL now gets the poisoned response.
```

Prevention:
1. Strict cache key: only include URL path + whitelisted headers. Never X-Forwarded-* unless explicitly keyed.
2. Don't cache if Set-Cookie in response (personalized).
3. Validate Content-Type matches expectation for URL (`.css` URL returning `text/html`? Don't cache).
4. Only cache defined success codes (200, 301, 404, 410). Never cache 5xx long-term.
5. CSP headers at edge: restrict what the page can execute even if poisoned.

---

## Failure Questions

**Q: Your CDN's origin shield goes down. What happens and how do you protect origin?**

A: Without mitigation: all 100+ PoPs send their cache misses directly to origin. At 5% miss rate and 1M req/s aggregate: 50K req/s suddenly hits origin. Origin likely overwhelmed.

Defense-in-depth:
1. **stale-if-error at edge**: `Cache-Control: stale-if-error=3600` — PoPs serve stale content for up to 1 hour when they can't reach shield or origin. Most users see no impact.
2. **Passive shield HA**: warm standby shield in another AZ. Failover via health check, ~30s.
3. **Adaptive TTL escalation**: control plane detects shield failure → broadcasts config to all PoPs: "multiply all TTLs by 5." Miss rate drops by 5x → origin protected even with no shield.
4. **Origin auto-scaling**: cloud-based origin can scale horizontally under sudden load (EC2 ASG, Cloud Run, etc.) — buys time for shield recovery.

---

**Q: 10 million users suddenly hit a page at exactly the same second (concert pre-sale). How does your CDN handle it?**

A: This is the thundering herd problem. I handle it at multiple levels:

First, I ensure the page is cached with `stale-while-revalidate`:
- Content is served from cache at each PoP. No origin contact for cached content. 10M requests → all PoP cache HITs → origin sees nothing.

For the cache miss scenario (first request or after expiry):
- **Request coalescing**: within each PoP edge server, only 1 request goes to origin per unique cache key. The other 999 concurrent misses park on a wait queue, served from cache when the first request completes.
- **Origin shield**: even if 200 PoPs all have a cache miss simultaneously, they all hit the shield. Shield has 95%+ hit rate → at most 5% of PoPs' misses reach origin.
- **Background revalidation**: at 80% of TTL elapsed, one server proactively refreshes the cache. By the time the concert goes live, cache is warm everywhere.

If the content is NOT cacheable (dynamic checkout pages):
- Rate limiting at CDN edge (WAF rules)
- Virtual waiting queue (Ticketmaster pattern) — users get a position and a token, admitted in batches
- Origin horizontal scaling + circuit breaker

---

**Q: How would you design cache purge at scale for a CDN with 300 PoPs?**

A: 

1. **API endpoint**: `POST /v1/cache/purge` with `{url, customer_id, pattern_type: exact|wildcard}`.
2. **Write to DB**: Create purge_job record (id, customer_id, url, status=PENDING, created_at).
3. **Publish to Kafka**: topic `cache.purge`, partition by hash(url). Durability: Kafka retains 7 days.
4. **PoP consumers**: each PoP has a dedicated Kafka consumer group. Receives purge event → deletes matching cache keys → sends ACK to control plane.
5. **Tracking**: control plane marks PoP as ACK'd. Job = COMPLETE when all PoPs ACK (or 30s timeout).
6. **Customer API**: returns `purge_id`. Customer polls `GET /v1/cache/purge/{id}` for status and which PoPs are still pending.

Wildcard purge (`/user/123/*`): more expensive — PoP must scan key index. Rate-limited separately (100/min vs 10K/min for exact purge).

SLA: 95% of PoPs purged in <2s, 99.9% in <10s. Control plane failure: Kafka queues purges, delivered on recovery.

---

## Trade-off Questions

**Q: Long TTL vs short TTL — what are the trade-offs?**

A: 
| | Long TTL (hours/days) | Short TTL (seconds/minutes) |
|--|--|--|
| Cache hit ratio | High (95%+) | Lower (80-90%) |
| Origin load | Very low | Higher |
| Content freshness | Stale risk | Near-fresh |
| Stale content on deploy | Problem (must purge) | Self-heals quickly |
| DDoS protection | Strong (can serve from cache even if origin down) | Weaker (cache expires, needs origin) |

**My recommendation:** Use both appropriately.
- Immutable assets (fingerprinted filenames): 1 year TTL, zero staleness risk.
- HTML pages: 60-300s + stale-while-revalidate.
- API responses: 5-30s depending on data change frequency.
- Do NOT use long TTL for mutable content unless you have a robust purge pipeline.

---

**Q: CDN adds a caching layer but also adds complexity. When would you NOT use a CDN?**

A: Three cases:
1. **Single-region users**: all users within 10ms of origin. CDN adds latency (DNS lookup, extra hop) with no proximity benefit.
2. **Fully dynamic, personalized, non-cacheable content**: cache hit ratio ~0%. CDN becomes an expensive pass-through proxy.
3. **Very small scale**: CDN cost and operational overhead not justified. A small Nginx cache on origin covers the need.

In practice, for any product with global users serving significant static assets, CDN is almost always worth it. The latency benefit and origin protection pay for themselves quickly.

---

## Follow-up Questions Interviewers Ask

**Q: How would you add real-time analytics at the edge? (e.g., requests per second by country)**

A: Each edge server writes access logs to a local Kafka producer (async, non-blocking to request path). Kafka streams to ClickHouse. ClickHouse handles 1M inserts/s, enables real-time query: `SELECT country, count(*) FROM cdn_requests WHERE ts > now()-60s GROUP BY country`. Control plane serves customer dashboard from ClickHouse. Latency: ~5-10s from request to appearing in dashboard.

**Q: How would you implement signed URLs to prevent hotlinking?**

A: Signed URL = URL contains HMAC signature: `/protected/file.pdf?expires=1234567890&token=HMAC_SHA256(secret, path+expires)`. Edge validates signature before serving. If invalid or expired → 403. Key rotation: customer has up to 2 active signing keys (smooth rotation). Used for: video platforms (DRM-lite), file download links, API access tokens baked into URLs.

**Q: Your CDN is serving a customer's content but the customer's origin has a security incident. How do you stop serving their content immediately?**

A: 
1. Customer calls CDN: `POST /v1/emergency-disable/{customer_id}` (auth required).
2. Control plane: immediately push config to all PoPs "reject all requests for customer X" (config push via UDP multicast to PoPs — faster than Kafka for urgent updates, ~500ms global).
3. PoPs start returning 503 for this customer's traffic.
4. Purge all cached content for that customer: wildcard purge across all PoPs.
5. Return traffic to origin only after customer confirms security issue resolved.

This is why CDN config must be pushable in <1s globally for emergency cases — separate fast-path from the normal 30s config polling.
