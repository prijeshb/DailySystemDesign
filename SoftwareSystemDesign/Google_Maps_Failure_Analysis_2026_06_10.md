# Google Maps — Failure Analysis
**Date:** 2026-06-10  
**Approach:** Failure-first. For each component: what if it fails, what if the component before it fails, what if the component after it fails.

---

## Failure Map

```
Client
  ↓
API Gateway
  ↓
┌─────────────────────────────────────────────────────┐
│ Routing Service ──reads──► Traffic DB (Cassandra)   │
│      │                          ▲                   │
│      └──────────► Road Graph    │                   │
│                  (In-memory CH) │                   │
└─────────────────────────────────│───────────────────┘
                                  │
Location Ingest → Kafka → Flink ──┘

Geocoding Service → Elasticsearch
Tile Service → S3 → CDN
Place Search → PostGIS + Redis GEO
```

---

## 1. Routing Service Failure

### The Component Fails
**Symptom:** Users can't get routes; 503 errors.

**Immediate impact:** Navigation breaks for new sessions. Active navigation sessions using cached route on device are unaffected temporarily.

**Mitigation:**
1. **Auto-restart:** K8s/ECS restarts container. CH graph loads from in-memory snapshot → ~30-60s to rebuild. For 30-60s gap → clients retry with exponential backoff.
2. **Multiple routing servers:** Load balancer detects unhealthy instance, routes to healthy peers.
3. **Degraded mode:** Fall back to simpler Dijkstra on a smaller graph (coarser, faster to load). Accept longer computation time.
4. **Client-side cache:** Last computed route valid for ~5 min; client continues navigation on cached route and queues re-route request.

**Prevention:** Pre-warm routing servers; keep graph snapshot on local disk (avoids full reload from source). Rolling deploys — never take all servers down simultaneously.

### Upstream Failure: API Gateway Fails
- Routing service never receives request
- Mitigation: Multi-region API gateways behind global load balancer (Anycast DNS)
- Health check every 10s; automatic failover to next region < 30s

### Downstream Failure: Traffic DB (Cassandra) Fails
**Scenario:** Routing service can't fetch real-time traffic weights.

**Impact:** Routes computed without current traffic → potentially suboptimal routes; ETA inaccurate.

**Mitigation:**
1. **Stale cache:** Routing service has in-memory traffic cache (30s TTL). On Cassandra failure, serve from stale cache. Traffic is rarely seconds-critical — stale-by-minutes is acceptable.
2. **Historical fallback:** If cache also expired, use historical speed by hour-of-day (pre-loaded at startup from file). Route is less accurate but still valid.
3. **No hard failure:** Routing must never refuse to serve because traffic is unavailable. Traffic is advisory, not required.

**Detection:** Cassandra health check; routing server logs "using historical traffic" alert. PagerDuty after 5 minutes.

---

## 2. Traffic Ingestion Pipeline Failure

### Kafka Fails
**Scenario:** Location updates from users can't be persisted.

**Immediate impact:** Traffic stops updating. After 30s, routing servers serve stale traffic; after minutes, fall back to historical.

**Mitigation:**
- Kafka has 3-replica topics. Single broker failure = no data loss (other replicas serve).
- Full cluster failure (rare): Location Ingest Service buffers in-memory (bounded queue, ~30s buffer). Kafka recovery < 30s for most cases.
- If Kafka down > buffer size: drop location updates (GPS probes, not orders — loss acceptable). Traffic becomes stale, not incorrect.
- Alert: Kafka consumer lag monitor; alert if lag > 10M messages.

**Upstream: Location Ingest Service Fails**
- Location updates buffered on client (every 3-5s update). Client retries on reconnect.
- Active navigation: client keeps navigating with last known route. Re-routes trigger when Ingest Service recovers.

**Downstream: Flink Aggregator Fails**
- Kafka retains messages (7-day retention). Flink consumer group resumes from last committed offset.
- Traffic update gap during downtime: stale speeds served. Acceptable.
- Recovery: Flink restarts, replays buffered updates, catches up within minutes.

### Cassandra Write Failure
- Flink writes edge speeds to Cassandra. Write fails.
- **Retry:** Flink retries with exponential backoff (up to 3 attempts).
- **Dead letter topic:** Failed writes go to Kafka dead letter topic; replayed once Cassandra recovers.
- **Impact if prolonged:** Traffic data not updated. Routing falls to historical speeds.

---

## 3. Tile Service Failure

### CDN Edge Node Fails
- **Impact:** Users in that region see slow tile loads (fallback to S3 origin).
- **Mitigation:** CDN has 100s of edge nodes; any single failure rerouted to nearest healthy edge.
- **User impact:** Map loads ~100ms slower (origin vs edge). No functional failure.

### S3 (Origin) Fails
- **Impact:** New tile requests (CDN miss) fail to fetch. CDN returns stale cached tiles.
- **Trade-off:** Tiles change rarely (road updates are infrequent). Stale-by-hours is usually fine for most tile content.
- **Mitigation:** S3 99.999999999% (11 nines) durability. Multi-region S3 replication for critical zoom levels.
- **Degraded mode:** Client retries tile load with exponential backoff; displays map with some tiles blank while retrying.

### Tile Renderer Fails (On-Demand Tiles)
- Affects only cache-miss paths for new/rare tiles.
- **Mitigation:** Queue tile render requests; retry. Serve blank/placeholder tile until render completes.
- Very low traffic path (most tiles are pre-rendered and CDN-cached).

---

## 4. Geocoding Service Failure

### Elasticsearch Fails
**Scenario:** Address lookup fails; users can't search destinations.

**Mitigation:**
1. **ES cluster:** 3-node minimum; single node failure = cluster still healthy.
2. **Read replicas:** Query can go to any replica.
3. **Degraded mode fallback:** Maintain a simpler key-value store of top-10M addresses (city + pin code → lat/lng). Handles 80% of queries. Slow rollover if ES is completely down.
4. **Cache:** Geocoding results cached in Redis (TTL 24h). Popular addresses (airport, railway station) rarely need live ES query.

**Upstream: DNS / Network Failure**
- Client can't resolve address. Degrades navigation from "search + navigate" to "enter coordinates manually" — rare but exists.
- Mitigation: Client app caches recently searched places.

---

## 5. Road Graph Data Corruption / Staleness

### Wrong Road Data (Map Error)
**Scenario:** A road is closed, but graph still shows it open. Users routed onto closed road.

**Mitigation:**
1. **Incident reports:** Users report road closed → Incident Service marks edge as blocked → traffic overlay sets speed = 0 (effectively removes from routing).
2. **Patrol vehicles / street-level imagery:** Periodic ground truth validation.
3. **Feedback loop:** When many users deviate at same point → ML model flags potential closure → human review queue.

**Detection:** Anomaly: 1000+ users rerouting away from same edge in 30 min → auto-trigger review.

### CH Graph Precomputation Bug
**Scenario:** New code produces incorrect shortcuts → wrong routes.

**Prevention:** Shadow deployment — compute routes in parallel on old + new CH; diff results; alert if > 0.1% divergence.
**Recovery:** Blue/green routing server deployment. Old version still running. Rollback in < 1 min.

---

## 6. Location Data Privacy Failure

### User Location Leaked
**Scenario:** Location updates (5M/s) exposed to unauthorized service.

**Mitigation:**
1. **Anonymization at ingest:** Strip user_id → replace with session_id (rotated every 15 min). Traffic aggregation doesn't need identity.
2. **Differential privacy:** Add noise to location before aggregation. Individual user location irreversible from aggregate speed data.
3. **Data retention:** Raw location probes deleted after 48h. Aggregated traffic data kept longer.
4. **Access control:** Traffic Aggregator can only write to Cassandra, not read raw location stream.

---

## 7. Cascading Failure: New Year's Eve

**Scenario:** 10x normal traffic at midnight → all users searching routes simultaneously.

**Chain:**
```
10x requests → Routing Service overloaded → response time > 5s 
→ clients retry → 20x load (retry amplification)
→ Routing Service OOM → restarts → CH graph reload (60s gap)
→ All servers restart simultaneously → 60s complete outage
```

**Prevention:**
1. **Rate limiting at API Gateway:** 1000 route requests/second per region. Queue excess with 202 Accepted + poll URL.
2. **Retry with jitter:** Client retries after random(1-10)s, not immediately.
3. **Rolling restart prevention:** K8s PodDisruptionBudget: min 50% routing pods always up.
4. **Pre-scale:** Known high-demand events (New Year, Diwali) → pre-provision 3x capacity 1h before.
5. **Result caching:** Popular route queries (A to B at 11:55pm Dec 31) → identical results → cache at API Gateway level (TTL 60s). Collapse 1000 identical requests into 1 CH computation.

---

## 8. Failure Prevention Summary

| Failure | Detection | Prevention | Recovery |
|---------|-----------|------------|----------|
| Routing service crash | K8s health check, latency alert | Replicated pods, rolling deploy | Auto-restart, graph pre-loaded from snapshot |
| Traffic DB unavailable | Cassandra health check | In-memory traffic cache (30s) | Fall back to historical speeds; zero hard failures |
| Kafka down | Consumer lag monitor | 3-replica topics | In-memory buffer on ingest; replay on recovery |
| CDN edge failure | Synthetic monitoring (probe every 30s per edge) | 100s of redundant edge nodes | Automatic reroute to neighbor edge |
| Map data error | User deviation anomaly detection | Incident overlay, shadow routing validation | Edge blocked via incident; human review queue |
| Traffic spike (New Year) | Pre-known event calendar | Pre-scale, API Gateway rate limit, result cache | Jittered retry, partial degradation |
| Location data leak | Audit logs, access review | Anonymization at ingest, short retention | Rotate session IDs, purge raw data |
