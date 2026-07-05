# Video Conferencing System Design (Zoom / Google Meet)
> Date: 2026-06-11 | Difficulty: Hard | Category: Real-Time, Media, Distributed

---

## First Principles Check

**Do we really need a custom conferencing system?**
Yes — third-party SDKs (Twilio, Agora) cost ~$0.004/minute/participant. At 1M concurrent minutes/day that's $4K/day = $1.4M/year. For scale beyond ~50K concurrent participants, building own SFU + signaling pays off.

**What is the actual hard problem here?**
Two separate planes:
1. **Control plane** (signaling): who's in the room, what streams exist, call setup — relatively small data, high reliability needed
2. **Data plane** (media): audio/video packets, real-time, massive volume, latency-critical — completely different architecture

Never conflate them. The databases, protocols, and failure modes differ entirely.

---

## Entities

```
Meeting          id, host_id, title, status, created_at, ended_at, recording_enabled
Participant      id, meeting_id, user_id, join_time, leave_time, role (host/attendee), sfu_node_id
MediaStream      id, participant_id, kind (audio/video/screen), ssrc, codec, state
ChatMessage      id, meeting_id, sender_id, text, timestamp
Recording        id, meeting_id, s3_key, duration, status
```

---

## Actions

| Action | Protocol | Notes |
|--------|----------|-------|
| Create meeting | HTTPS REST | Returns meeting_id + join link |
| Join meeting | WebSocket (signaling) | SDP offer/answer exchange |
| Send media | UDP (SRTP via WebRTC) | Direct to SFU node |
| Receive media | UDP (SRTP via WebRTC) | From SFU node |
| Screen share | WebRTC (separate stream) | Higher resolution, lower fps |
| Chat | WebSocket (same signaling channel) | Piggyback on control plane |
| Leave | WebSocket + HTTP | Cleanup participant state |
| Record | Internal (SFU → recorder service) | Async, offloaded |

---

## Data Flow

### Control Plane (Signaling)
```
Client → Load Balancer → Signaling Server (WS)
              │
              ├─ Room State: Redis (per meeting, TTL = meeting duration + 1hr)
              └─ Persistent: Postgres (meeting metadata, participants log)

Room state in Redis:
  meeting:{id}:participants → Hash { participant_id → { user_id, sfu_node, streams } }
  meeting:{id}:chat_history → List (last 1000 msgs)
```

### Media Plane (SFU)
```
Publisher (Camera/Mic)
    │
    │  UDP/SRTP (WebRTC) — one upload stream per track
    ▼
SFU Node (Selective Forwarding Unit)
    │
    │  UDP/SRTP — one download stream per subscriber
    ├──► Subscriber A
    ├──► Subscriber B
    └──► Subscriber C

Publisher uploads:  1 video stream + 1 audio stream  (always constant)
SFU forwards:       N-1 receive streams per subscriber (decreasing via simulcast)
```

### Why SFU over P2P and MCU

```
P2P (mesh):
  N participants → each publishes to N-1 peers
  N=10: each client uploads 9 streams simultaneously
  Upload bandwidth = O(N) per client → kills mobile/weak connections
  No central control → hard to record, admit/kick

MCU (Multipoint Control Unit):
  Server mixes all streams into one composite video
  Client uploads 1, downloads 1 — minimal bandwidth
  But: server transcodes every frame = O(N) CPU per room
  Doesn't scale cost-effectively; latency from mixing

SFU (Selective Forwarding Unit): ✓ Industry standard
  Client uploads 1 stream to SFU
  SFU forwards selectively (no decoding) to each subscriber
  Server CPU: packet routing only, not transcoding
  Client bandwidth: 1 upload + N-1 downloads (but adaptive via simulcast)
```

---

## High-Level Architecture

```
                              ┌──────────────────────────────────┐
                              │         Control Plane             │
                              │                                   │
Client ──HTTPS──► API Gateway ──► Meeting Service (REST)          │
                              │        │                          │
Client ──WSS────► LB ────────►│   Signaling Service (WS)         │
                              │        │              │           │
                              │     Redis          Postgres       │
                              └──────────────────────────────────┘

                              ┌──────────────────────────────────┐
                              │          Media Plane              │
                              │                                   │
Client ──UDP/SRTP────────────►│   SFU Node (region-local)        │
                              │        │                          │
                              │   SFU Cluster (per region)        │
                              │        │                          │
                              │   Cascade Relay ──────────────►  │
                              │   (cross-region SFU bridge)       │
                              └──────────────────────────────────┘

                              ┌──────────────────────────────────┐
                              │         Async Services            │
                              │                                   │
                              │   Recording Service               │
                              │   (SFU → S3 via RTP dump)        │
                              │                                   │
                              │   Analytics (Kafka → ClickHouse)  │
                              └──────────────────────────────────┘
```

### Meeting Join Flow (Step by Step)

```
1. Client → POST /meetings/{id}/join (HTTP)
   Server: create Participant row, select best SFU node (geographically closest)
   Response: { sfu_host, turn_servers, ice_candidates }

2. Client → WebSocket to Signaling Server
   Client sends: SDP Offer (which codecs, resolution, bitrate it supports)
   Server responds: SDP Answer (negotiated parameters)

3. ICE negotiation (STUN/TURN):
   Client tries STUN first → gets public IP:port (works if no symmetric NAT)
   Falls back to TURN if NAT traversal fails → media relayed through TURN server
   
4. WebRTC connection established:
   Client publishes tracks → SFU node
   Client subscribes to other participants' tracks ← SFU forwards

5. Signaling broadcasts to all participants in room:
   { type: "participant_joined", participant_id, display_name, tracks: [audio, video] }
   Each existing participant subscribes to new streams
```

---

## Low-Level Design

### SFU Node Internals

```
Per meeting room on SFU node:
  PublisherTracks: Map<participant_id, RTPTrack[]>
  Subscribers:     Map<participant_id, RTCPeerConnection>

On receiving RTP packet from publisher:
  For each subscriber in room:
    if subscriber.wants_this_track(ssrc):
      forward(packet, subscriber.connection)   // no decode, just forward

"Wants this track" = active speaker + pinned + selected streams
  SFU selects which streams to forward per subscriber based on:
    1. Simulcast layer (bandwidth-adaptive)
    2. Active speaker detection (VAD — voice activity detection)
    3. Explicit pin (user pinned someone)
```

### Simulcast (Bandwidth Adaptation)

```
Publisher sends 3 parallel qualities simultaneously:
  Layer H (high):   1080p @ 2.5Mbps
  Layer M (medium): 480p  @ 600Kbps
  Layer L (low):    180p  @ 100Kbps

SFU selects layer per subscriber based on subscriber's available bandwidth:
  Full broadband → send H layer
  Mobile/weak  → send L layer
  SFU switches layers WITHOUT re-negotiating WebRTC connection

How SFU knows subscriber bandwidth:
  RTCP REMB (Receiver Estimated Maximum Bitrate) sent by subscriber back to SFU
  TWCC (Transport-Wide Congestion Control) — more accurate, modern alternative
  SFU reads estimate, picks layer accordingly
```

### STUN / TURN / ICE

```
STUN (Session Traversal Utilities for NAT):
  Client → STUN Server: "what's my public IP?"
  STUN Server → Client: "your public IP:port is X.X.X.X:12345"
  Works for most home NATs (full cone, restricted cone)

TURN (Traversal Using Relays around NAT):
  When STUN fails (symmetric NAT, enterprise firewalls)
  All media relayed through TURN server
  Client ← TURN → SFU
  Cost: TURN relay = high bandwidth cost
  Rule: ~10-15% of users need TURN

ICE (Interactive Connectivity Establishment):
  Collects all candidate paths (local IP, STUN-derived IP, TURN relay)
  Tests all paths, picks best (lowest RTT)
  Handles failover: if direct path breaks, falls back to TURN
```

### Active Speaker Detection

```
Problem: with 50 participants, don't want to show 50 video tiles
Solution: show top 5 active speakers, switch when someone starts talking

VAD (Voice Activity Detection):
  Client: detect local audio level
  Send AudioLevel RTP header extension on every audio packet
  SFU aggregates per participant
  
Active speaker list:
  SFU maintains sorted list by recent audio level (sliding 300ms window)
  On change → broadcast event to all via signaling:
    { type: "speaker_change", active: ["user_A", "user_B", ...top5] }
  Each subscriber switches to display the current active speakers
```

### Recording Architecture

```
Do NOT decode+re-encode (expensive). Instead:

SFU forks RTP streams for recording participants:
  SFU → Recorder Service (separate process/node)
  Recorder writes raw RTP/OPUS/VP8 to disk

Post-processing (async, after meeting ends):
  Raw RTP → FFmpeg → .webm / .mp4
  Upload to S3
  Update Recording row: status=READY, s3_key

Separate from SFU: recording failure must NOT affect live meeting
```

### Database Schema (Key Tables)

```sql
-- Postgres: persistent, billing, analytics
meetings (
  id UUID PK,
  host_id UUID,
  title VARCHAR,
  status ENUM('scheduled','active','ended'),
  started_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  recording_enabled BOOLEAN
);

participants (
  id UUID PK,
  meeting_id UUID FK,
  user_id UUID,
  join_time TIMESTAMPTZ,
  leave_time TIMESTAMPTZ,
  sfu_node_id VARCHAR,
  role ENUM('host','attendee'),
  INDEX (meeting_id, join_time)
);

-- Redis: ephemeral room state
meeting:{id}:state          → HASH { status, host_id, participant_count }
meeting:{id}:participants   → HASH { participant_id: JSON blob }
meeting:{id}:chat           → LIST of serialized chat messages
participant:{id}:tracks     → SET of active track ssrcs
```

---

## Trade-offs

### SFU Topology: Single SFU vs Cascaded SFU

| Approach | Latency | Cost | Fault Domain |
|----------|---------|------|--------------|
| Single SFU (all in one region) | Low for that region. High for remote participants. | Simple | One node = one room |
| Cascaded SFU (SFU per region, interconnected) | Low for all participants | Higher (multiple SFUs) | Region failure only kills regional participants |

**Rule:** Cascade when participants span multiple regions. Same-city meeting → single SFU.

```
Cascade:
  India participants → Mumbai SFU
  US participants   → Virginia SFU
  Mumbai SFU ──────────────────► Virginia SFU  (one cross-region stream)
  Not: each India user streaming to US directly
```

### Codec: VP8 vs H.264 vs AV1

| Codec | Compression | CPU (encode) | Hardware Support | Use |
|-------|-------------|--------------|-----------------|-----|
| VP8 | Good | Low | Wide | Safe default (all WebRTC clients) |
| H.264 | Good | Very low (HW accel) | Universal | Mobile (hardware encoder) |
| AV1 | Excellent (-50% vs H.264) | High | Modern only | Future default |

**Rule:** Negotiate per-client. Send H.264 to mobile (battery), VP8 to desktop (compatibility), AV1 if both sides support.

### TURN Cost

```
Only 10-15% of users need TURN relay. But those users relay full video.
At 1M concurrent users: 150K TURN users × 1Mbps avg = 150Gbps TURN bandwidth
TURN = expensive. Optimization:
  1. UDP preferred (lower relay overhead than TCP TURN)
  2. TURN over TCP port 443 only as last resort (firewall bypass)
  3. Geo-locate TURN servers (user hits nearest)
```

---

## Capacity Estimation

```
Scale: 10M daily meetings, avg 8 participants, avg 45 min

Concurrent meetings peak: 1M meetings
Concurrent participants: 8M
Media bandwidth per participant: 
  Upload: 1 video stream ~500Kbps + 1 audio ~50Kbps = ~550Kbps
  Download: ~3 streams (active speakers) × 400Kbps avg = ~1.2Mbps

Total ingress to SFU fleet: 8M × 550Kbps = 4.4Tbps
Total egress from SFU fleet: 8M × 1.2Mbps = 9.6Tbps
→ SFU fleet must handle ~14Tbps

SFU node: 40Gbps NIC, ~70% utilization = 28Gbps effective
Nodes needed: 14Tbps / 28Gbps = ~500 SFU nodes globally (distributed)

Signaling:
  WebSocket messages: join/leave/chat/speaker events
  1M meetings × 2 events/min = 2M WS messages/min = 33K/sec → trivial
```

---

## Real-World References

- **Zoom**: Uses SFU architecture. Zoom's media servers selectively forward based on active speaker. See: Zoom Engineering Blog (2020) on their split control/media plane.
- **Google Meet**: WebRTC + custom SFU. Uses TWCC for congestion control. Cascaded SFUs for global rooms.
- **Discord**: Moved from WebRTC SFU to custom protocol (DAVE) for E2E encrypted audio. SFU still for video.
- **Mediasoup** (open-source SFU): reference implementation showing track-level forwarding logic.
- **Facebook Messenger Rooms**: MCU hybrid — mixing only when >4 participants.
