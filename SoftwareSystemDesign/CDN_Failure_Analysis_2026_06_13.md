# CDN — Failure Analysis
**Date:** 2026-06-13

---

## Failure Map

```
End User
    │
[DNS/Anycast Routing]  ← can fail
    │
[PoP Edge Server]      ← can fail (individual server OR full PoP)
    │
[Origin Shield]        ← can fail
    │
[Origin Server]        ← can fail
    │
[Cache Invalidation]   ← can fail (stale content risk)
```

---

## Failure 1: Single Edge Server Crash

**What fails:** One server within a PoP.

**Impact:** Traffic on that server's consistent-hash ring segment needs rerouting.

**Detection:** PoP load balancer health check fails (TCP port 443 check every 1s).

**Recovery:**
```
LB removes dead server from pool (within 3s of failure).
Consistent hash ring: re-map failed server's keys to neighbor.
Cold cache on neighbor → brief miss storm on those keys.

Mitigation:
  Virtual nodes: 150 vnodes per server → re-mapped keys spread across many neighbors
  → no single neighbor overwhelmed.
  Origin shield absorbs miss storm at 95% hit rate.
```

**What does NOT break:** Other servers in PoP unaffected. No data loss (cache is a cache, not source of truth).

---

## Failure 2: Full PoP Down (Data Center Failure)

**What fails:** Entire regional PoP unreachable.

**Impact:** All users in that region lose their nearest edge. Latency spikes.

**Detection:**
- External monitoring pings each PoP every 30s
- BGP health check fails → withdraw route announcement

**Recovery:**

With Anycast:
```
PoP stops announcing IP prefix to BGP.
BGP propagation: 15-45s for global convergence.
Users automatically route to next-nearest PoP.
Impact: latency increases for that region (e.g., Mumbai PoP down → Singapore PoP serves, +30ms)
No manual intervention. Fully automatic.
```

With DNS geo:
```
Health monitor detects PoP unreachable.
Updates DNS record: remove Mumbai PoP IP, add Singapore PoP IP.
TTL = 30s → users get updated IP within 30s.
DNS caching: users with cached old record see failure for up to 30s.
```

**Partial PoP failure (some servers up, some down):**
- Harder to detect. Symptom: elevated error rate on health check, not full 100%
- Threshold: >5% error rate on synthetic probe → declare PoP degraded → remove from routing
- Drain traffic gradually (not sudden removal which could spike neighbors)

---

## Failure 3: Origin Shield Down

**What fails:** Intermediate cache between PoPs and origin.

**Impact:** PoP misses go directly to origin. Origin gets 20x normal request volume.

**Without mitigation:**
```
100 PoPs × 50,000 req/s × 5% miss rate = 250K req/s normally absorbed by shield
250K req/s → origin suddenly → origin overwhelmed → cascade failure
```

**Mitigation — Defense in Depth:**

**Layer 1: Stale-while-revalidate**
```
While shield is down, PoPs stop revalidating (no one to revalidate with).
Cache-Control: stale-while-revalidate=300
→ PoPs continue serving stale content (up to 5 min) without any origin/shield contact.
Origin and shield load = 0 during the stale window.
```

**Layer 2: stale-if-error**
```
Cache-Control: stale-if-error=3600
→ Even if PoP detects shield/origin error, serve stale content for up to 1 hour.
Users see stale content, not errors.
```

**Layer 3: Shield HA (Active-Passive)**
```
Primary Shield: US-East-1
Passive Shield: US-East-2 (synced via async replication)

Failover: health check fails → PoPs switch shield endpoint
DNS failover: CDN internally maintains two shield endpoints, automatic failover
Recovery time: ~30s
```

**Layer 4: Adaptive TTL escalation**
```
Control plane detects shield failure:
  Broadcasts config update to all PoPs: "extend TTLs by 5x"
  PoPs hold cached content 5x longer → 5x fewer misses → origin protected
  When shield recovers → config update: restore original TTLs
```

---

## Failure 4: Origin Server Down

**What fails:** The source of truth is unreachable.

**What happens:**
- PoP and shield cache HIT: unaffected. Cached content served normally (this is the majority).
- Cache MISS: can't fetch new content. Response depends on `stale-if-error` header.

**Scenarios:**

**Cached content:**
```
Cache-Control: stale-if-error=3600
→ Serve stale content for up to 1 hour after origin fails.
→ Users see old content, but site stays up.
→ Buys time for origin recovery.
```

**Uncached content (origin required):**
```
CDN cannot serve something it has never cached.
Options:
  1. Return 503 with Retry-After: 60 (honest, for APIs)
  2. Serve fallback HTML/page (pre-configured in CDN rules): "Site temporarily unavailable"
  3. Redirect to status page (CDN routing rule: if origin 5xx → redirect to /status.html — cached at CDN)
```

**Preventing this (origin protection):**
- Origin shield + high TTLs = very few origin requests → origin can be smaller/cheaper
- Serve everything cacheable from CDN; origin only handles truly dynamic requests (login, checkout)
- Multi-region origins: CDN fails over between origin regions if one fails

---

## Failure 5: Cache Poisoning Attack

**What it is:** Attacker crafts a request that causes CDN to cache a malicious/wrong response, then serve it to all users.

**Attack vectors:**

**1. Cache key confusion:**
```
Attacker: GET /page HTTP/1.1
           X-Forwarded-Host: evil.com

CDN uses X-Forwarded-Host in response without including it in cache key.
→ CDN caches response for /page but response contains evil.com data.
→ All users requesting /page get poisoned response.
```

**2. HTTP request splitting:**
```
GET /%0d%0aX-Cache-Bypass: true HTTP/1.1
→ CRLF injection in URL bypasses cache or injects headers into CDN response
```

**3. Web Cache Deception:**
```
Attacker: GET /profile/page.css HTTP/1.1
CDN sees .css extension → caches it (it's a stylesheet right?)
But origin actually returns /profile authenticated user data for this URL.
→ CDN caches user's private profile page. Attacker fetches /profile/page.css → gets it.
```

**Prevention:**

```
1. Strict cache key normalization:
   Only include: path, query params, a whitelist of headers (Accept-Encoding, Accept-Language)
   NEVER: X-Forwarded-* headers, unrecognized custom headers

2. Never cache based on file extension alone:
   Check Content-Type of response, not URL extension.
   If Content-Type: text/html but URL is *.css → don't cache.

3. Don't cache responses with Set-Cookie or Authorization:
   Indicates personalized/authenticated content → must not be shared

4. Sanitize Vary header:
   If response Vary: Cookie → treat as uncacheable (too granular)

5. Validate response status:
   Only cache: 200, 203, 301, 302, 404, 410
   Never cache: 500, 502, 503 (for long; max 10s stale-if-error)

6. Signed/token URLs for sensitive content:
   URL contains HMAC: /private/file.pdf?token=abc&expires=1234567890
   CDN validates signature before serving → attacker can't forge URL
```

---

## Failure 6: Thundering Herd at Cache Expiry

**What it is:** Popular content's cache TTL expires simultaneously at a PoP. Thousands of concurrent requests all miss cache and hit origin at once.

**Scenario:**
```
Taylor Swift concert tickets page:
  Cached at all PoPs, max-age=60s
  At T=60s: 500,000 concurrent users → all 500K requests see MISS simultaneously
  → 500K origin requests in 1 second
  → Origin crashes
```

**Fixes:**

**1. Request coalescing (first line of defense):**
```
Within one PoP edge server:
  Thread 1: MISS → acquire coalesce_lock for key K → fetch origin
  Threads 2-999: MISS → lock held → park on condition variable for key K
  Thread 1 completes fetch → populate cache → signal all parked threads
  Threads 2-999 all served from newly populated cache

Origin sees: 1 request per PoP server, not 999.
```

**2. Jitter TTL:**
```
Don't expire all copies at identical time.
Instead of max-age=3600: set max-age = 3600 + random(0, 300)
→ expiry spread over 5-minute window → misses spread out → origin manageable
```

**3. Background revalidation (stale-while-revalidate):**
```
Cache-Control: max-age=60, stale-while-revalidate=30
→ At T=60: content "stale" but served while ONE request revalidates in background
→ Users never see a miss; origin gets 1 revalidation per PoP server, not N
```

**4. Early revalidation (proactive refresh):**
```
PoP monitors TTL countdown.
At 80% of TTL elapsed → background fetch to refresh.
By the time TTL=0, cache already has fresh content.
No miss window. No thundering herd.
Cost: slightly more origin requests (prefetching before expiry).
```

---

## Failure 7: BGP Route Hijack (Anycast CDN)

**What it is:** Malicious AS announces same IP prefix with shorter AS path → attracts CDN traffic.

**Real examples:**
- 2008: Pakistan Telecom accidentally null-routed YouTube globally for 2 hours
- 2020: Russian ISP hijacked Google, Amazon, and CDN prefixes for ~1 hour
- 2010: China Telecom attracted 15% of internet traffic for 18 minutes

**Impact on CDN:** User traffic routed to attacker's servers. Man-in-the-middle possible on non-HTTPS. For HTTPS: traffic directed to wrong location → TLS cert mismatch → browser error (no plaintext leak).

**Mitigations:**

**1. RPKI (Resource Public Key Infrastructure):**
```
Route Origin Authorization (ROA): cryptographically proves "AS 13335 owns 104.16.0.0/12"
Signed by Regional Internet Registry (ARIN, RIPE, etc.)
ISPs with RPKI validation enabled: reject routes that violate ROAs

Status: ~40% of prefixes have RPKI, adoption growing.
Cloudflare publishes its ROAs and validates all peers.
```

**2. HTTPS everywhere:**
```
Even if traffic is hijacked, TLS certificate validation fails.
Browser: "Certificate error" → user sees warning, doesn't send data.
Not silent — user is alerted. Data not compromised.
HSTS Preload: browsers require HTTPS for known domains → no HTTP downgrade.
```

**3. BGP monitoring:**
```
Cloudflare, Akamai run BGP monitoring services (RPKI monitors, BGPstream).
Alert within 2 minutes of unexpected route announcement.
Response: contact hijacking ISP, publish correct ROA, reach out to upstream peers.
```

---

## Failure 8: Cache Stampede via Hotspot Key

**What it is:** Single ultra-popular content item concentrates load on a few cache servers within a PoP (consistent hashing makes one server "own" each key).

**Scenario:**
```
Viral video thumbnail: 10M requests/minute
Consistent hash assigns this key → Server A in PoP
Server A: overwhelmed. Servers B-Z: idle.
→ Server A crashes from hotspot. Thumbnail shows 503 for all 10M users.
```

**Fix: Key sharding**
```
For known-hot keys: replicate across S "shards"
  hot_key → hot_key_shard_0 ... hot_key_shard_9

Request comes in:
  if is_hot(key): shard = random(0, S-1); fetch key_{shard}
  else: normal consistent hash

All S shards serve content (same content, just multiple copies).
Each shard gets 1/S of the load.

Detecting hot keys: Count-Min Sketch on request rate per key.
If estimated_rate(key) > HOT_THRESHOLD: mark as hot, enable sharding.
```

**Fix: Jigsaw pattern (multiple cache tiers within PoP)**
```
Tier A: consistent hash → server owns key → 1 copy
Tier B (hotspot relief): random server from pool → any server can serve it

On lookup:
  1. Check Tier A (owned server)
  2. If miss AND key is marked hot → check Tier B (random servers)
  3. If still miss → fetch origin → populate both tiers
```

---

## Failure 9: CDN Cache Unavailable (Control Plane Failure)

**What it is:** CDN control plane (config/purge) is unreachable. Edge servers still work but can't receive new config or purge commands.

**Impact:**
- Serving: unaffected (edge servers operate independently from cached config)
- Purges: queued, not executed → stale content stays cached longer than desired
- Config changes: not applied → rule updates delayed

**Design principle: Edge must operate standalone:**
```
Edge servers load config at startup into local memory.
Heartbeat: poll control plane every 30s for config updates.
On control plane failure: edge continues with last-known-good config.
Purge queue: backed up on Kafka with long retention (7 days).
When control plane recovers: Kafka replay delivers all pending purges.
```

This is why CDN edge servers are designed to be "dumb forwarders with local state" — not dependent on control plane for serving decisions.

---

## Failure Summary

| Failure | Detection | Recovery | User Impact |
|---------|-----------|----------|-------------|
| Single edge server | LB health check (3s) | Consistent hash ring reblanace | Brief miss spike (<30s) |
| Full PoP down | BGP monitor / synthetic probe | Anycast re-routing (30-45s) | Higher latency for region |
| Origin shield down | Health check (5s) | stale-while-revalidate, passive shield failover | Stale content up to stale-if-error TTL |
| Origin down | Response error from shield | stale-if-error serves cached content | Uncached content → 503 |
| Cache poisoning | Security scan, anomaly detection | Purge affected keys, deploy fix | Malicious content served until purged |
| Thundering herd | Origin error rate spike | Request coalescing, stale-while-revalidate | Brief origin spike, mitigated at edge |
| BGP hijack | BGP monitoring alert (2min) | RPKI validation, peer notifications | TLS breaks → user alert (no data leak) |
| Hotspot key | Count-Min Sketch alerts | Key sharding across servers | Brief overload on owning server |
| Control plane down | Synthetic health check | Edge serves from cached config; Kafka replays purges | Stale config/purge delays only |
