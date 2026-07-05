# Live Streaming System — Interview Q&A
**Date:** 2026-06-07

> How interviewers ask, follow-up patterns, and strong answers.

---

## Opening Question

**"Design a live streaming platform like Twitch."**

### Strong opening move
Don't jump to architecture. Clarify first:
> "Before I design, a few questions. Are we targeting gaming (longer streams, high quality) or short social clips (Instagram Live)? What latency is acceptable — sub-second or 5-15s? And should we support VOD after stream ends?"

Expected answers to push for:
- Scale: 10M concurrent viewers, 100K streamers
- Latency: ≤15s acceptable (typical Twitch)
- VOD: yes
- Chat: yes

Then frame your approach:
> "I'll design using HLS over CDN — it scales to millions but adds 5-15s delay. I'll explicitly call out where WebRTC would be better and when the trade-off shifts."

---

## Q1: Why HLS and not WebRTC for delivery?

**Interviewer ask:** "Can't we just use WebRTC for everything? It's lower latency."

**Answer:**
WebRTC is peer-to-peer. At 1M viewers, the broadcaster can't maintain 1M peer connections — their upload bandwidth dies, and CDNs can't cache peer-to-peer streams.

HLS works by cutting video into small files (.ts segments) that CDNs can cache and serve to millions simultaneously. The broadcaster only uploads once to an ingest server; CDN does the fan-out.

Trade-off table I'd draw:

| | HLS/DASH | WebRTC |
|--|---------|--------|
| Latency | 5–30s | <1s |
| Scale | Millions via CDN | ~10K (MCU/SFU required) |
| CDN cacheable | Yes | No |
| Use case | Broadcasting | Video calls, ultra-low-latency |

For Twitch-style: HLS wins. For gaming tournaments where 1s matters: HLS with low-latency HLS (LHLS, 1–3s) or WebRTC with an SFU.

**Follow-up:** "What if the interviewer says latency must be <3s?"
→ Mention Low-Latency HLS (Apple LL-HLS): reduces segment to 200ms partial segments, playlist hints. CDN-compatible, 2–4× more complex.

---

## Q2: How does adaptive bitrate work?

**Interviewer ask:** "A viewer switches from WiFi to 4G. How does their quality adjust automatically?"

**Answer:**
We serve a master playlist (master.m3u8) that lists all quality variants:
```
1080p60 → 6 Mbps
720p60  → 4 Mbps
480p    → 1.5 Mbps
360p    → 800 Kbps
```

The HLS player (HLS.js) measures bandwidth on each segment download. If segment took longer than expected → estimated bitrate is below next variant threshold → player switches to lower quality variant for next request. Switch is seamless: player just requests a different quality's m3u8.

No server involvement in quality selection — it's entirely client-side. This is key: the server doesn't need to know which quality each viewer is watching.

---

## Q3: How do you handle the HLS playlist without it going stale?

**Interviewer ask:** "How often does the playlist update? What if CDN caches it too long?"

**Answer:**
Playlist (live.m3u8) is re-uploaded to S3 every 2s with a new segment appended and oldest removed (sliding window of 10 segments = 20s).

CDN TTL: we set `Cache-Control: max-age=2, s-maxage=1` on the playlist. CDN edge holds it for 1–2s max, then re-fetches from S3. This gives ~2–4s of extra latency beyond transcoding, which is acceptable.

Segment files (.ts) are immutable — once written they never change. Set `Cache-Control: max-age=86400`. CDN caches them indefinitely → zero S3 load for popular streams once warmed.

**Follow-up:** "Won't 1M viewers all polling the playlist every 2s overwhelm S3?"
→ That's why CDN caches even short-TTL responses. 1M viewers → nearest CDN edge has the playlist; S3 only gets 1 request per edge PoP per 2s. With 200 PoPs globally, that's 100 S3 requests/s for 1M viewers. Totally fine.

---

## Q4: How do you count viewers?

**Interviewer ask:** "How do you show '1.2M watching' in real-time?"

**Answer:**
Exact counting for 10M concurrent users is expensive. We use **HyperLogLog** — a probabilistic data structure with ~0.81% error and fixed 12KB memory regardless of cardinality.

Every 30s, viewer sends: `POST /heartbeat {stream_id, user_id}`
→ `PFADD stream:{stream_id}:viewers {user_id}` + `EXPIRE 60s`

After 2 missed heartbeats (60s), user's heartbeat expires and HyperLogLog no longer counts them.

`PFCOUNT stream:{stream_id}:viewers` → O(1) → "1.2M watching"

**Follow-up:** "Why not a Redis Set (exact)?"
→ Redis Set stores each user_id. At 10M viewers on a popular stream: 10M × 36 bytes UUID = ~360MB per stream. With 1000 popular streams = 360GB in Redis. HyperLogLog is 12KB per stream regardless. 

---

## Q5: How do you prevent two streamers from using the same stream key?

**Interviewer ask:** "What if someone else knows my stream key and goes live?"

**Answer:**
Three layers:
1. `stream_key` is a random 20-char token, shown once, stored hashed. Attacker can't derive it from the username.
2. Ingest server: on RTMP connect, check DB for `WHERE stream_key_hash = hash(provided_key) AND status != 'LIVE'`. If already LIVE → reject new connection (only one active stream per key).
3. Rotation: streamer can invalidate old key and get a new one instantly from dashboard.

**Follow-up:** "What if the legitimate streamer drops and attacker reconnects before them?"
→ Status check: when stream ends (RTMP disconnect), status remains LIVE for 30s (grace period for OBS reconnect). Attacker connecting in that window → rejected (stream_key already LIVE). After 30s with no reconnect → status = ENDED → rotation allowed.

---

## Q6: How does VOD work?

**Interviewer ask:** "I end my stream. When can viewers watch the replay?"

**Answer:**
VOD is nearly instant — no re-encoding needed. All segments are already on S3.

When stream ends:
1. Kafka event: `stream.ended`
2. VOD Worker: list all `{stream_id}/{quality}/*.ts` from S3 (ordered by seq_num)
3. Generate new `vod_master.m3u8` pointing to full segment list + `#EXT-X-ENDLIST` tag
4. Upload to S3 → VOD is immediately seekable

`#EXT-X-ENDLIST` signals to player: this is a complete VOD, not a live stream. Player enables seek bar, knows total duration.

**Ready within:** 1–2 minutes of stream end (just S3 list + playlist file write).

**Follow-up:** "What about editing highlights or clipping?"
→ Client sends `{start_time, end_time}` → Clip Service filters segment list to that range → generates new playlist → serves. No re-encoding: just playlist slicing. (Note: can't cut mid-segment cleanly without re-encoding, so clips snap to 2s segment boundaries.)

---

## Q7: How do you handle a streamer getting hundreds of thousands of concurrent viewers suddenly (viral)?

**Interviewer ask:** "A streamer's stream goes viral. Millions join in 10 minutes. How does the system cope?"

**Answer:**
This tests three things:

**Transcoder:** Same load regardless of viewers — one transcoder per stream. Viewers don't affect transcoding. ✓

**CDN:** CDN auto-scales by design. First viewer for a segment → cache miss → S3 fetch. Second viewer onward → cache hit. With millions of viewers all requesting the same segment at nearly the same time, the CDN has it cached after the first miss. The concern is the "cache stampede" at stream start: thousands of viewers joining simultaneously, all requesting the same uncached segments.
Fix: CDN request coalescing (CloudFront, Fastly both do this) — multiple requests for the same uncached object are collapsed into one origin fetch.

**Viewer count Redis:** HyperLogLog handles 10M PFADDs. Redis cluster can do 1M ops/s. Need to shard by stream_id across cluster nodes if a single stream gets 10M heartbeats/30s = ~333K ops/s. That's on one Redis node — doable, but monitor.

**Chat:** Most concerning. Chat write rate scales with viewers. At 1M viewers and 1% chat rate = 10K messages/sec for one stream. Chat service should shard by stream_id. Cassandra handles writes well. But Redis pub/sub fan-out: 1 message → 1M WebSocket servers need it. Use Kafka instead of pure pub/sub: `chat.{stream_id}` topic → chat WS servers consume, fan-out to their connected viewers only.

---

## Q8: What if the streamer's internet is bad? (Packet loss, unstable upload)

**Interviewer ask:** "The streamer has 20% packet loss. What happens to viewers?"

**Answer:**
RTMP runs over TCP — TCP handles retransmission transparently. The cost: increased latency as TCP waits for retransmits. With 20% packet loss, latency grows.

On the viewer side:
- HLS player has a buffer (typically 3 segments = 6s)
- If transcoder falls behind due to retransmit delays → playlist stops updating → players stall

Prevention:
- Ingest server: if encoder falls behind by >2 segments → drop frames (prefer latency over frozen)
- Viewer player: adaptive buffer — increase buffer from 3 to 6 segments when playlist update frequency drops (reduces buffering at cost of more latency)

Better solution: Twitch uses their own ingest protocol (RTMPS with bandwidth estimation). Streamer client reports available bandwidth → ingest server signals max bitrate → OBS adjusts encoder in real-time.

---

## Q9: Deep Dive — Segment Size Trade-off

**Interviewer ask:** "You chose 2-second segments. Why not 6 seconds or 500ms?"

| Segment duration | Latency | S3 PUT requests | CDN efficiency | ABR switch speed |
|-----------------|---------|-----------------|----------------|-----------------|
| 500ms | ~2-5s | 2× per sec per quality | Poor (tiny TTL ratio) | Fast |
| 2s | ~5-10s | 0.5× per sec | Good | Medium |
| 6s | ~15-20s | ~0.17× per sec | Best | Slow |

**2s reasoning:**
- Twitch standard: viewer latency budget is 10-15s acceptable
- 2s segments × 10-segment playlist = 20s buffer → covers most network jitter
- Fast enough ABR switching (quality changes take ≤4s to feel different)
- S3 PUT rate: 100K streams × 4 qualities × 0.5/s = 200K PUTs/sec — manageable

**For LL-HLS (Low Latency):** Use 200ms partial segments + playlist hints → 1–3s latency, but 30× more S3 writes. Only worth it for live sports/esports where latency vs spoilers matters.

---

## Q10: How does chat scale to millions of viewers?

**Interviewer ask:** "If 2M people are watching and chatting, how does chat work?"

**Answer:**

Two-layer fan-out:
```
Viewer sends chat → Chat API → Kafka topic: chat.{stream_id}
                                    ↓
                         Chat WS servers (partitioned by stream_id)
                         Each server: N viewers connected via WebSocket
                         Consume from Kafka → push to each connected WS
```

**Why Kafka instead of Redis pub/sub:**
- Redis pub/sub: if one WS server crashes mid-message, message lost
- Kafka: durable, WS server reconnects + resumes from last offset → no messages dropped
- Backpressure: if WS server is slow, Kafka holds messages; pub/sub drops them

**Chat moderation at scale:**
- Async: message → Kafka → moderation consumer (ML model) → if bad, emit delete event → all WS servers delete from local state + Cassandra
- Chat appears immediately (optimistic), deleted asynchronously if flagged

**Cassandra schema:**
```
CREATE TABLE chat_messages (
  stream_id UUID,
  time_bucket int,       -- unix_time / 3600 (hourly buckets)
  message_id  timeuuid,  -- CQL timeuuid = timestamp + unique
  user_id UUID,
  text TEXT,
  PRIMARY KEY ((stream_id, time_bucket), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC);
```
Partition by (stream_id, time_bucket) → prevents hot partition; one stream one hour = one partition.

---

## Concepts This Design Introduces

- **HLS / Adaptive Bitrate Streaming** — see Concepts_Index #18
- **HyperLogLog** — see Concepts_Index #19
- **CDN Pull vs Push** — see Concepts_Index #20
- **RTMP Ingest** — covered inline
- **Kafka for Chat Fan-out** — extends Concept #9 (Event-Driven)
