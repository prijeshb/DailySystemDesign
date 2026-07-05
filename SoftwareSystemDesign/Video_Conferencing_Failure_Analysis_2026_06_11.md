# Video Conferencing — Failure Analysis
> Date: 2026-06-11 | Companion to: Video_Conferencing_System_Design_2026_06_11.md

---

## Failure Map

```
Client ──────────► Signaling LB ──────────► Signaling Server ──────────► Redis / Postgres
   │                    │                          │
   │  UDP/SRTP           │                          │
   └────────────► SFU Node ──────────► Recorder Service ──────────► S3
                     │
                     └──────────────► Cross-region Cascade Link
```

---

## Component Failures

### 1. SFU Node Fails (All Media Drops for Rooms on That Node)

**What happens:** All participants in meetings on that SFU suddenly lose audio/video. Signaling channel may still be alive (different servers). Clients see frozen video.

**Detection:**
- SFU health check fails (heartbeat every 1s to health monitor)
- Signaling server loses contact with SFU node
- Participants send RTCP timeout reports → signaling detects no media flow

**Recovery:**
```
1. Health monitor marks SFU node DEAD
2. Signaling service migrates affected rooms to healthy SFU node
3. Broadcasts to all clients: { type: "media_reconnect", new_sfu: "sfu-us-east-3" }
4. Each client re-negotiates WebRTC (new SDP offer/answer to new SFU)
5. ~2-5 second interruption, then media resumes

Key: ICE restart — WebRTC supports "ICE restart" which renegotiates transport
     without dropping the peer connection object. Clients just need new ICE credentials.
```

**Prevention:**
- SFU nodes have redundant NICs and power
- No more than N rooms per SFU node (cap at 80% capacity)
- Rooms larger than X participants automatically cascaded across 2 SFU nodes (if one fails, fallback SFU has all streams)

---

### 2. Signaling Server Fails (Room State Lost)

**What happens:** WebSocket connections drop. Clients can no longer exchange SDP, receive speaker events, or send chat.

**Detection:** WebSocket heartbeat (`PING/PONG`). Client detects disconnect in <5s.

**Recovery:**
```
1. Client reconnects to signaling (with exponential backoff + jitter)
2. New signaling server rebuilds room state from Redis
   (Redis is source of truth for active room state, not the signaling server's memory)
3. Client sends: { type: "rejoin", meeting_id, participant_id, last_chat_seq }
4. Server responds: full room state (participants, streams, missed chat messages)

Key: signaling servers are STATELESS — all state in Redis
     Any server can handle any reconnecting client
```

**During signaling downtime:** Media continues flowing through SFU (control plane down ≠ data plane down). Participants just can't join/leave or see new speaker events. Meeting continues degraded.

---

### 3. TURN Server Fails

**What happens:** ~15% of participants who depend on TURN relay lose media. Direct-connected participants unaffected.

**Detection:** ICE connectivity checks fail. Client WebRTC reports `failed` state.

**Recovery:**
```
1. Client's ICE agent automatically tries backup TURN server
   (App provides 3-5 TURN server candidates in ICE configuration)
2. ICE restart on backup TURN — ~3-5s disruption
3. If all TURN fail: participant gets degraded to audio-only (UDP port 443 as last resort)
```

**Prevention:**
- Multiple TURN servers per region
- TURN servers behind load balancer (but must use UDP LB or sticky sessions per ICE candidate)
- Capacity: TURN servers sized at 120% of expected peak relay traffic

---

### 4. Network Packet Loss (Partial — Affects One Participant's Stream)

**Symptom:** Video freezes, audio choppy, pixelated frames.

**Solutions (layered):**

**A. NACK (Negative Acknowledgement):**
```
Receiver detects missing RTP sequence number
Sends RTCP NACK to sender: "please retransmit seq=1042"
Sender retransmits packet
Latency: 1 RTT for retransmit
Works for: brief loss spikes, <5% loss rate
Does NOT work for: high loss (retransmit also gets lost) or high latency (>100ms RTT)
```

**B. FEC (Forward Error Correction):**
```
Sender transmits redundant packets alongside originals
Example: every 5 RTP packets → 1 FEC packet (XOR of the 5)
If any 1 of those 5 is lost: receiver reconstructs from FEC
Cost: +20% bandwidth overhead
Works for: random/burst loss up to 20%
Does NOT help: if 2+ packets in FEC group are lost
```

**C. Adaptive Bitrate via REMB/TWCC:**
```
Receiver sends bandwidth estimate to SFU
SFU switches to lower simulcast layer
Lower bitrate = more resilient to congestion-induced loss
```

**D. Packet Loss Concealment (PLC) — Audio:**
```
Audio codec (OPUS) has built-in PLC
If packet missing: generates "concealed" audio (interpolated from surrounding frames)
Transparent for up to 10% loss
For >10% loss: audible degradation → rely on layer switching
```

---

### 5. Recording Service Fails Mid-Meeting

**What happens:** Recording is interrupted. Partial recording exists.

**Recovery:**
```
Recording service crashes:
  1. SFU stops forwarding RTP to dead recorder
  2. Recording state in DB: status = RECORDING
  3. Watchdog detects dead recorder process
  4. New recorder starts, re-subscribes to SFU RTP streams
  5. Gets a new RTP stream (new ssrc, new sequence numbers)
  6. Records second portion

Post-processing:
  Segment 1: 00:00 → 23:45 (partial, before crash)
  Segment 2: 23:52 → end   (7-second gap)
  FFmpeg concat: stitch both segments, fill gap with black frame + silence
  
DB update: recording.gap_detected = true (flag for user "recording has 7s gap")
```

**Prevention:**
- Recorder runs on separate hardware from SFU (SFU failure ≠ recorder failure)
- Recorder checkpoints to S3 every 30s (flush raw RTP segments)
- On crash: only lose last 30s, not entire recording

---

### 6. Redis Fails (Room State Lost)

**What happens:** Signaling servers can't read room state. New joins fail. Reconnecting participants get error.

**Recovery:**
```
Redis Sentinel / Cluster setup:
  Primary Redis fails → Sentinel elects new primary within ~10s
  
During 10s failover:
  In-flight joins: fail with 503, client retries (exponential backoff)
  Active participants: already connected via WebRTC → media unaffected
  Existing WebSocket connections: still open on signaling servers
    (Signaling servers cache participant state in process memory as secondary cache)
  
After failover:
  New Redis primary may be slightly stale (replication lag ~1-2s)
  Inconsistency: Redis might not know about a participant who joined in last 2s
  → Participant re-sends "rejoin" signal → server re-inserts into Redis
```

**Prevention:**
- Redis Cluster with 3 shards, 1 replica each (6 nodes total)
- Room state has short TTL (meeting duration + 2hr) — auto-cleanup
- Signaling server in-memory cache as read-through (reduces Redis dependency for reads)

---

### 7. Large Meeting Spike (Participant Count Jump: 2 → 200)

**What happens:** SFU node suddenly must forward 200× more streams. Bandwidth and CPU spike.

**Detection:** SFU reports CPU > 80% / bandwidth > 90% capacity.

**Recovery (horizontal scale):**
```
1. Meeting coordinator detects large meeting
2. Spawns additional SFU nodes
3. Splits meeting across 2-3 SFUs using cascade topology:
   SFU-1: participants 1-100 (publisher fans out to SFU-1)
   SFU-2: participants 101-200 (gets streams from SFU-1 via cascade)
   
4. Cascade link: SFU-1 → SFU-2 sends only active speaker streams (not all 200)
   SFU-2 handles local subscriptions independently
```

**Prevention:**
- Meetings with >50 participants pre-assigned to multi-SFU topology at join time
- Webinars (1 → many): presenter sends, audience only receives → asymmetric topology
- Large meeting mode: disable video for non-speakers, audio-only for audience

---

### 8. Cross-Region Cascade Link Fails

**What happens:** Participants in different regions can no longer see each other. Same-region participants still communicate.

**Detection:**
- Cascade SFU monitors RTP flow on cross-region link
- 3s of no packets → declare link down

**Recovery:**
```
Option A: Reroute via backup cascade path (if available)
Option B: Migrate all participants to single region
  - Signal all clients in Region B to switch SFU to Region A
  - Higher latency for Region B participants, but meeting continues
  - Acceptable for short-term failover

Option C: Meeting splits — each region continues independently
  - For large meetings: this is unacceptable
  - For 2-person call between regions: both try direct SFU-to-SFU paths
```

---

## Failure Tolerance Matrix

| Failure | Scope | Auto-Recovery | User Impact | RTO |
|---------|-------|--------------|-------------|-----|
| Single SFU node | Rooms on that node | Yes (ICE restart) | ~3-5s video freeze | <10s |
| Signaling server | All clients on that server | Yes (reconnect) | Chat/join events pause | <5s |
| TURN server | 15% of users | Yes (backup TURN) | 3-5s audio/video drop | <10s |
| Redis (sentinel failover) | New joins only | Yes (10s) | New joins fail briefly | ~10s |
| Recording service | Recording only | Yes (restart) | Gap in recording | <60s |
| Cascade link | Cross-region visibility | Partial (region migration) | Latency increase | <30s |
| Packet loss <5% | One stream | Auto (NACK/FEC) | Brief quality drop | <1s |
| Packet loss >20% | One stream | Layer switch | Quality degradation | <2s |

---

## Real-World Failures

**Zoom Outage 2020:** Rapid growth during COVID overloaded specific data center regions. SFU cluster saturation caused meeting drops for users in those regions. Fix: geographic load balancing and over-provisioning SFU capacity.

**Google Meet Audio Echo:** Common failure mode — when speaker output leaks into microphone. Not infrastructure failure, but acoustic echo cancellation (AEC) in WebRTC client. Fix: AEC algorithm in OPUS/WebRTC already handles this, but fails on external speakers without headphones.

**Discord Bot Attacks:** Participants joining and spamming media streams to overload SFU. Fix: rate limit participants per meeting, max stream count per participant, abuse detection before SFU admission.
