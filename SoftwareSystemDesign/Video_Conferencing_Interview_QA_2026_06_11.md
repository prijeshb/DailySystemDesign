# Video Conferencing — Interview Q&A
> Date: 2026-06-11 | Companion to: Video_Conferencing_System_Design_2026_06_11.md

---

## Opening Questions

**Q: Design a video conferencing system like Zoom.**

**Strong opening move:**
> "Before jumping in — can I clarify scope? Are we focusing on group calls (many participants) or 1:1, is recording needed, do we care about mobile clients? Also I want to separate control plane vs media plane early — they have completely different architectures."

Then establish:
- Scale: how many concurrent meetings, participants per meeting
- Features: screen share, recording, chat, waiting room
- Quality vs latency trade-off (real-time = low latency > perfect quality)

---

## Core Architecture Questions

**Q: Why not just use P2P (peer-to-peer) WebRTC for a group call?**

A: P2P works for 1:1 or small groups. For N participants:
- Each client must upload N-1 streams simultaneously
- N=10: each client uploads 9 video streams
- Mobile at 500Kbps video × 9 = 4.5Mbps upload — kills battery and cellular plans
- No central control — can't record, can't admit/kick, can't do active speaker detection

SFU solves this: each client uploads once, SFU routes selectively.

---

**Q: Explain the difference between SFU and MCU.**

A:
- **SFU (Selective Forwarding Unit):** Receives streams, forwards packets without decoding. Server doesn't understand video content. CPU cost = O(bandwidth), not O(participants). Clients decode.
- **MCU (Multipoint Control Unit):** Decodes all streams, composites into one frame, re-encodes, sends one stream to each participant. Client sees one video. Server CPU = O(N) decoding + encoding per frame. Very expensive at scale.

SFU is preferred for most use cases. MCU used only in: legacy SIP integration, or very large broadcast where client bandwidth must be absolute minimum.

---

**Q: How does ICE/STUN/TURN work? Why do we need TURN?**

A:
- **STUN:** Discovers your public IP. Works when your router does simple NAT.
- **TURN:** Relay server. Used when NAT is symmetric (enterprise/carrier-grade NAT) — STUN fails because different outbound port for each destination. ~10-15% of users need TURN.
- **ICE:** Collects all candidate paths (local, STUN, TURN), tests them in parallel, picks the best. Falls back if best path fails.

Trade-off: TURN relay costs a lot of bandwidth (all media goes through it). Design must geo-distribute TURN servers and not over-provision for the 85% who don't need it.

---

**Q: How does simulcast work and why is it important?**

A: Publisher sends 3 encoding layers simultaneously (high/medium/low resolution). SFU picks which layer to forward to each subscriber based on their available bandwidth (measured via TWCC/REMB). Key benefits:
1. No re-encoding on server (forward which layer, not transcode)
2. Adaptive per-subscriber (mobile gets low, desktop gets high, from same source)
3. Layer switch is seamless (<200ms) without ICE renegotiation

Without simulcast: SFU would need to transcode (MCU territory). Or all subscribers get same quality regardless of bandwidth.

---

## Scaling Questions

**Q: How do you scale SFU servers?**

A: SFU nodes are stateless in terms of room logic (state in Redis). Scale horizontally:
1. Assign rooms to SFU nodes on join (consistent hash by meeting_id → same node for all in room)
2. Node capacity ~80% → spawn new nodes
3. Large rooms (>50 participants) pre-split across 2+ SFU nodes via cascade

Cascade = SFU-1 forwards publisher streams to SFU-2. SFU-2 handles subscriptions for its participant group. Cross-region: one cascade stream per active track, not N streams per participant.

---

**Q: How would you handle a 1000-person webinar vs. a 10-person meeting differently?**

A: They are fundamentally asymmetric:

Webinar:
- 1-5 presenters publish video/audio
- 995 attendees only receive (no upload)
- SFU receives 1-5 streams, fans out to 995
- Attendee bandwidth: 1 stream download (vs meeting = N-1 streams)
- Attendees can be "viewer only" — no WebRTC upload at all (HLS fallback possible)

Meeting:
- All participants publish + subscribe
- Active speaker mode limits downloads to top-5 videos

Architecture difference: webinar SFU topology is a broadcast tree, not full mesh forwarding.

---

**Q: How do you handle active speaker switching without jarring jumps?**

A:
1. VAD (Voice Activity Detection) runs on client, embeds audio level in RTP header extension
2. SFU aggregates levels per participant with smoothing window (~300ms)
3. Speaker change event only fired when new speaker is dominant for >500ms (debounce)
4. Client pre-buffers incoming video frames (~200ms) to avoid freeze during layer switch
5. Some clients show "dominant speaker" transitions with short animation

---

## Data / Storage Questions

**Q: How do you store recordings efficiently?**

A: Two phases:
1. **Live:** SFU forks RTP to Recorder Service. Recorder writes raw RTP to disk in real-time. Very cheap — no decode/encode, just byte copying. ~20-30% of original media bitrate as storage.
2. **Post-processing:** After meeting ends, FFmpeg transcodes raw RTP → MP4/WebM (seekable, compressed). Upload to S3. Store: `{ meeting_id, s3_key, duration, status }`.

Key: keep live recording (phase 1) completely separate from post-processing (phase 2). A recording post-processing failure doesn't affect the live meeting.

---

**Q: How do you handle chat in a 1000-person meeting?**

A: Chat is control plane, not media plane. Options:
- Piggyback on signaling WebSocket: fine for small meetings
- For large meetings (>100 participants): fan-out via Kafka
  - Signaling server publishes chat event to `meeting:{id}:chat` Kafka topic
  - All signaling servers subscribed → push to their WebSocket clients
  - Redis list stores last 1000 messages for joiners catching up

Rate limiting: 1 message per 2 seconds per participant to prevent spam in large meetings.

---

## Failure / Edge Case Questions

**Q: What happens when an SFU node crashes mid-meeting?**

A:
1. Health monitor detects node down (<3s heartbeat timeout)
2. Meeting coordinator migrates rooms to healthy SFU
3. Signals all clients: ICE restart with new SFU credentials
4. Clients re-do WebRTC handshake (new ICE candidates) — ~3-5s video freeze
5. Media resumes. Meeting continues.

Signaling channel (WebSocket) is unaffected — different servers.

---

**Q: What if a participant's internet drops for 30 seconds, then reconnects?**

A:
1. ICE connectivity fails → WebRTC marks connection "failed"
2. Client shows "reconnecting..." to user
3. Client attempts ICE restart (re-negotiates transport, keeps same peer connection)
4. If ICE restart succeeds: streams resume, SFU sends keyframe → video recovers
5. If ICE restart fails: full WebRTC reconnect (new SDP, new connection object)
6. Signaling "rejoin" signal → server puts participant back in room state

Other participants: see that participant's video frozen. After ~5s, tile shows "disconnected". When rejoined, tile resumes.

---

**Q: How do you prevent a single large meeting from taking down the entire SFU cluster?**

A:
1. **Isolation:** Large meetings run on dedicated SFU pool, isolated from small meetings
2. **Cascade topology:** Spread large meetings across 3+ SFU nodes; no single node is critical
3. **Rate limiting:** Max streams per meeting (e.g., 25 simultaneous video streams). Above 25 → audio-only for non-active-speakers
4. **Shard by meeting_id:** Noisy neighbor is contained to their SFU node
5. **Resource quotas:** Per-meeting CPU and bandwidth limits at SFU level

---

**Q: How do you detect and handle one participant causing audio echo for everyone?**

A: This is an Acoustic Echo Cancellation (AEC) problem, not an infrastructure problem.

The fix is in the client (WebRTC's built-in AEC):
- WebRTC audio pipeline: AEC → AGC (Automatic Gain Control) → ANS (Noise Suppression) → OPUS encoder
- AEC models the speaker output and subtracts it from microphone input

When AEC fails (e.g., Bluetooth speaker with high latency):
- Server-side: can detect echo as correlation between participant's audio and another's output
- But server can't fix it — must signal the client to mute or adjust
- Practical: Zoom/Meet UI shows "you may be causing echo" and suggests using headphones

Infrastructure can't solve acoustic problems — escalate to client.

---

## Trade-off Questions

**Q: When would you use UDP vs TCP for media?**

A:
- **UDP (default):** Lower latency. Lost packets = just lost (stale video frame). Real-time media should drop rather than delay.
- **TCP:** Packets retransmitted in-order. Causes buffering, latency spikes. Bad for real-time.

**Exception:** TURN over TCP port 443 as last resort (firewalls that block UDP). Acceptable only because TURN is last resort — latency will be bad but better than nothing.

Rule: WebRTC defaults to UDP. Falls back to TURN/TCP only when UDP is blocked.

---

**Q: How does E2E encryption work for video calls? What's the trade-off?**

A:
- Default WebRTC: SRTP — encrypted in transit, SFU decrypts to forward (SFU-terminated encryption)
- True E2E: Insertable Streams API (WebRTC feature) — client encrypts content, SFU forwards encrypted packets without decrypting

E2E encryption trade-offs:
| | SRTP (default) | E2E (Insertable Streams) |
|--|---|---|
| SFU can see media | Yes | No |
| Recording possible | Yes | No (without key sharing) |
| Active speaker detection | Yes | Partial (audio level in header, not content) |
| Compatibility | Universal | Modern browsers only |
| Implementation complexity | Low | High |

Discord adopted E2E via DAVE protocol. Zoom offers optional E2E for meetings. Default = SRTP because recording + active speaker need server access.

---

**Q: You said SFU selects which simulcast layer to forward. What if the SFU makes wrong choice?**

A: SFU relies on TWCC (Transport-Wide Congestion Control) for bandwidth estimation. TWCC is accurate but has lag — bandwidth estimate is based on last ~1s of packets.

Problems:
1. **Sudden congestion:** SFU sends high layer → congestion builds → estimate drops → SFU switches layer → already caused freezes
2. **Overestimation:** TWCC may overestimate bandwidth on good bursts → SFU sends high layer → congestion

Solutions:
- SFU maintains hysteresis: must see sustained bandwidth for N seconds before upgrading layer (avoids thrash)
- Conservative initial estimate: start with low layer, upgrade gradually
- Google's GoogCC algorithm (used in Chromium/OPUS): combines loss-based + delay-based congestion signals for better accuracy

---

## Follow-up / Deep-dive Questions

**Q: How would you add a waiting room feature?**

A:
- Participant state: `WAITING` before host admits
- SFU connection established (ICE, WebRTC handshake) but no streams subscribed yet
- Signaling server holds participant in `waiting_room:{meeting_id}` set
- Host sees waiting list via signaling push event
- Host sends "admit" → server moves participant to active → SFU subscribes them to streams
- Key: don't connect media until admitted. But DO do WebRTC setup — saves ~2s delay on admit.

**Q: How would you implement a "raise hand" feature?**

A: Pure signaling, no media involvement.
- Client sends `{ type: "raise_hand", participant_id }` on WebSocket
- Signaling server updates participant state in Redis, broadcasts to all in room
- Host's UI shows sorted list by raise-hand timestamp
- No SFU involvement at all.

**Q: How would you handle live transcription?**

A:
- Transcription service subscribes to audio streams via SFU (same fork as recording)
- Sends audio to speech-to-text (Google STT / AWS Transcribe / Whisper)
- STT returns text with timestamps
- Signaling server broadcasts captions: `{ type: "caption", speaker_id, text, timestamp }`
- Latency: ~800ms-2s (acceptable for captions, not for translation)
- Cost: charge premium feature (STT APIs are expensive at scale)
