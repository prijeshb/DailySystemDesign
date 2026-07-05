# Live Streaming Platform — Failure Analysis
**Date:** 2026-06-23

---

## Failure Map

```
Streamer → [Ingest LB] → [Ingest Node] → [Kafka] → [Transcoder] → [S3] → [CDN] → Viewer
                                                                              ↕
                                              [Chat API] → [Kafka] → [Fanout] → [Redis] → [WS Server]
```

For each component: what happens if IT fails, if the component BEFORE it fails, if the component AFTER it fails.

---

## 1. Ingest Load Balancer

### Component fails
- Streamers cannot connect → stream goes offline
- **Detection:** Health check from external monitor (Route 53 health check)
- **Prevention:** Multi-AZ NLB (AWS Network Load Balancer); stateless → easy failover
- **Recovery:** DNS failover to backup NLB in <30s

### Upstream (Streamer connection) fails
- Streamer's internet drops mid-stream → RTMP TCP connection drops
- Ingest node sees connection close → marks stream as `reconnecting`
- Grace period: 60s before marking stream offline (OBS reconnects in ~5s)
- **Key detail:** Streamer must reconnect to the *same* ingest node (stream state is in-memory)
  - Consistent hash on stream_key ensures same node is tried first
  - If that node is down: session metadata written to Redis → any node can resume
- **Player experience:** HLS player continues playing buffered segments; manifest stops updating after ~10s → player shows "stream ended" if grace period expires

### Downstream (Kafka) fails
- Ingest node can't publish raw segments → buffers in memory
- Memory limit: ~30s of raw video (~30MB)
- If Kafka recovers within 30s: flush buffer → no visible gap
- If Kafka down >30s: ingest node must drop frames → segment gap
- **Prevention:** Kafka 3-AZ replication (ISR=2); producer ACK=all

---

## 2. Ingest Node

### Component fails (mid-stream)
- Raw video stops flowing to transcoder
- **Detection:** Segment watchdog — if no new segment_ready event for >5s on an active stream → alert
- **Recovery:**
  - Streamer must reconnect (RTMP is stateful)
  - On reconnect: new ingest node picks up (session in Redis)
  - ~5–15s gap in segments for viewers → player buffers → sees freeze then resumes
- **Prevention:** Ingest nodes are in ASG; unhealthy instances replaced; stream_key → node mapping stored in Redis with 60s TTL

### Upstream (CDN/internet) brings bad input
- Corrupt H.264 bitstream → FFmpeg crash
- **Prevention:** Ingest node validates NAL unit headers; drops corrupt GOPs
- Rate limiting per stream_key prevents abuse

### Downstream (Kafka) slow
- Backpressure → ingest node queue fills → drop oldest frames to maintain live edge
- Frame dropping is better than latency accumulation

---

## 3. Kafka (Message Bus)

### Component fails (full cluster outage)
- Transcoder starvation: no new segments → stream freezes for viewers
- **Prevention:** 3 broker cluster + Zookeeper ensemble; RF=3, min ISR=2
- **Recovery:** If all 3 brokers lost → data loss (segment gap); ingest nodes buffer 30s
- **Alternative:** Use AWS Kinesis (managed) to eliminate operational overhead

### Partition leader fails
- Leader election: ~5–10s
- Producers pause; consumers pause
- **Impact:** 5–10s segment gap → player freezes then catches up
- **Prevention:** Enable `unclean.leader.election=false` to prevent data loss

### Consumer lag grows (transcoder slow)
- Segments queue up → viewers see increasing latency
- **Prevention:** Monitor consumer lag (Prometheus + Grafana); auto-scale transcode workers on lag
- **Alert threshold:** lag > 10s (5 segments behind) → trigger scale-out

---

## 4. Transcoding Workers

### Component fails (worker crashes mid-segment)
- Segment partially written → incomplete .ts file in S3
- **Detection:** Segment integrity check (FFprobe validates output before publishing segment_ready)
- **Recovery:** Re-consume Kafka message (at-least-once delivery) → re-transcode segment
- **Idempotency:** segment S3 key = `{stream_id}/{quality}/{seq}.ts`; S3 PUT is idempotent → safe retry

### Worker is slow (overloaded)
- Transcoding falls behind real-time → latency accumulates
- 1 FFmpeg 1080p transcode requires ~2–4 vCPU at real-time speed
- **Prevention:** Worker autoscale on queue depth; if can't keep up → drop to lower quality ladder
- **Graceful degradation:** Disable 1080p for streams with >1s transcode lag; re-enable when capacity frees

### Upstream (Kafka) delivers duplicate segment
- Kafka at-least-once → worker may process same raw segment twice
- **Idempotency:** Check if S3 key exists before writing; skip if present (S3 HEAD request)
- OR: use conditional PUT (`If-None-Match: *`)

### Downstream (S3) is slow/unavailable
- Worker can't write segment → retries with exponential backoff
- **Circuit breaker:** After 5 failures in 10s → worker publishes to dead-letter topic + alerts
- **Failover:** Write to secondary bucket in different region; manifest service reads from both

---

## 5. S3 Segment Storage

### Component fails (S3 outage — rare but happened: 2017 us-east-1 outage)
- All segment reads fail → CDN cannot fetch new segments → viewers see freeze
- **Prevention:** 
  - Primary: S3 us-east-1
  - Failover: S3 us-west-2 (CRR - Cross-Region Replication, ~5s lag)
  - CDN origin failover: if primary returns 5xx → try secondary
- **Cost:** CRR doubles storage cost; only do it for live segments (not VoD archive)

### Segment read is slow (high latency)
- CDN cache miss path becomes slow → first viewer per PoP segment request takes >500ms
- **Prevention:** S3 Transfer Acceleration; CDN with aggressive segment caching (TTL=300s for .ts)
- **Monitor:** CDN origin-fetch latency p99; alert if >200ms

### S3 throttling (high request rate)
- S3: 3,500 PUT/s and 5,500 GET/s per prefix
- 100K streams × 4 qualities × 0.5 writes/s = 200K PUTs/s total
- **Prevention:** Prefix sharding: `/{stream_id[0:2]}/{stream_id}/{quality}/{seq}.ts`
  - 256 prefixes (00–ff) → each handles ~781 PUTs/s → well under limit

---

## 6. CDN

### CDN PoP fails
- Viewers in that region get routed to next PoP (GeoDNS failover)
- Cache miss storm at next PoP → origin S3 spike
- **Prevention:** CDN shield (origin shield) — all PoPs fetch from shield, not directly from S3
  - Shield = 1 intermediate cache → collapses origin requests

### CDN cache miss storm (stream starts)
- First N viewers in each PoP trigger simultaneous origin fetches for same segment
- **Prevention:** Request coalescing — CDN queues simultaneous identical requests, makes 1 origin fetch, fans out response
- AWS CloudFront does this by default ("request collapsing")

### Manifest (.m3u8) cache TTL too long
- Viewer gets stale manifest → misses new segments → stream appears frozen
- **Fix:** .m3u8 TTL = 1–2s (same as segment duration); set `Cache-Control: max-age=2`
- CDN must respect this; some CDNs have minimum TTL (Fastly: 0s supported, CloudFront: min 0s)

---

## 7. Chat WebSocket Servers

### WS server crashes
- Viewers lose WS connection → client auto-reconnects in ~2s
- **State:** WS server is stateless for chat (session in client JWT)
- **Redis Pub/Sub:** New WS server subscribes to channel → receives all subsequent messages
- **Impact:** 2s chat gap during reconnect → acceptable

### Redis Pub/Sub node fails
- All fanout for channels on that shard stops
- **Prevention:** Redis Sentinel (auto-failover in ~30s) or Redis Cluster
- **Chat gap:** 30s on failover → noticeable; mitigate with Redis Cluster (sub-second failover)

### Chat fanout lag (popular channel)
- 1M viewers × 1 msg/s = 1M WS pushes/sec
- Redis single-threaded PUBLISH becomes bottleneck
- **Prevention:** Shard Redis by channel_id; top channels get dedicated Redis instance
- **Monitor:** Redis command latency; alert if PUBLISH >10ms

### Chat spam / abuse
- Flood attack: bot sends 1K msgs/sec per connection
- **Prevention:**
  - Rate limiter (token bucket, Redis): 1 msg/3s default, 1 msg/1s for subscribers
  - Auto-moderator: ML model inline; blocks if p(spam) > 0.9
  - IP-level rate limit at API gateway

---

## 8. Viewer Count Service

### HLL clock skew / expired heartbeats
- Viewer closes browser without disconnect event → counted as active for TTL duration
- **Solution:** Heartbeat every 30s; Sorted Set `ZADD viewers:{channel_id} {now} {user_id}`
- On display: `ZCOUNT viewers:{channel_id} {now-60s} +inf`
- Overcounts by at most 1 heartbeat interval (30–60s stale entries)

### Viewer count race condition on stream start
- Stream goes live → 10K viewers join simultaneously → 10K simultaneous ZADD + ZCOUNT
- **Prevention:** Redis pipelining; viewer count computed async every 10s (not on every join)
- Eventual consistency acceptable for display (nobody needs exact count instantly)

---

## 9. End-to-End Latency Budget

```
Source capture → encoder → RTMP → ingest → transcode → S3 → CDN → player buffer → display

                │ 33ms (1 frame)     │      │ 200ms     │ 2000ms │ 200ms │ 2000ms │
                ▼                   ▼      ▼           ▼        ▼       ▼        ▼
Total LL-HLS target: ~3–5s end-to-end latency
Standard HLS:        ~15–30s

Chat latency: source → Kafka → fanout → Redis → WS → viewer ≈ 50–200ms
```

**Latency vs. cost:**
- 2s segments: 3× CDN requests vs 6s segments
- WebRTC ingest: <500ms latency but complex, not supported by OBS (major tool)
- Twitch uses LL-HLS for co-stream and interactive features where latency matters

---

## Cascading Failure Scenario: Transcoder Avalanche

**Trigger:** 5 popular esports tournaments start simultaneously → 500K new viewers in 60s

**Chain:**
1. 500K new viewers → CDN cache miss storm → 100K origin fetches/s
2. S3 throttles → CDN returns 503 → players retry → 200K origin fetches/s
3. Transcoding queue backs up (GPU shortage) → segment latency increases
4. Viewers refresh → more connections → WS server overload → chat drops
5. Streamers see "stream error" → reconnect storms → ingest LB saturated

**Prevention:**
1. Pre-warm CDN for scheduled events (manifest service pushes early)
2. S3 prefix sharding (prevents throttling)
3. Transcode capacity pre-provisioned 30 min before known events
4. Rate limit viewer connection storm per channel (leaky bucket on connection rate)
5. Ingest LB: SYN cookies + connection rate limiting per IP
