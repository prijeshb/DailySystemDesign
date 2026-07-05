# IoT Data Pipeline — Interview Q&A
> Date: 2026-06-29

---

## Round 1: Problem Scoping

**Q: Design an IoT data platform for 1 million smart home devices. Where do you start?**

A: I start by questioning whether we need a dedicated IoT pipeline at all. Plain HTTP fails here because IoT devices have constrained memory (can't run HTTP client stacks), unreliable connectivity, and publish at 50K messages/sec — far too expensive over HTTP due to header overhead. So yes, we need it.

Then I clarify scale: 1M registered devices, realistically 500K active at once, each publishing every 10 seconds = 50K messages/sec, ~10 MB/sec ingest. I'd call that medium-scale but with IoT-specific constraints.

The core entities are: Device, DeviceShadow (desired vs reported state), TelemetryRecord, Rule, Alert, Command.

---

**Q: Why MQTT and not HTTP or WebSocket?**

A: Three reasons:

1. **Protocol fit:** MQTT is designed for constrained devices — 2-byte fixed header (vs ~800 bytes for HTTP), persistent session survives network blips, built-in QoS levels.
2. **QoS semantics:** MQTT has at-most-once, at-least-once, and exactly-once delivery at the protocol level. Critical for commands (execute exactly once) vs telemetry (loss acceptable).
3. **Pub/sub model:** Devices publish to topics; multiple services subscribe without coupling. With HTTP, devices would need to know the server's address and each consumer service would need a separate endpoint.

WebSocket is bidirectional but requires HTTP upgrade and doesn't have native pub/sub, QoS, or retained messages.

---

## Round 2: Architecture Deep-Dive

**Q: How does a device know what state to apply when it reconnects after being offline for an hour?**

A: That's the Device Shadow pattern. Every device has a shadow document with two sections:
- `desired`: what the user/system wants the device to be
- `reported`: what the device last told us it is
- `delta`: `desired - reported`, computed by the Shadow Service

When a device reconnects, it subscribes to `$shadow/delta/{device_id}` and immediately receives the latest delta as a retained MQTT message. It applies the delta (e.g., "temperature setpoint changed to 22°C") and then publishes the updated `reported` state. Once `desired == reported`, the delta is empty.

The shadow is stored in Redis for <1ms reads and in Postgres for durability.

---

**Q: How do you handle 50,000 messages per second flowing into Kafka?**

A: At 50K messages/sec × 200B = 10 MB/sec, this is well within Kafka's limits (single broker handles ~300 MB/sec). The key design choices:

- 50 partitions on `telemetry.raw`, keyed by `device_id` → preserves per-device order, enables 50 Flink parallel consumers
- 3 brokers across 3 AZs, `replication.factor=3`, `min.insync.replicas=2`
- `acks=all` from EMQX → no data loss on broker failure
- 7-day retention → Flink can fall behind by up to 7 days without losing data

The EMQX Rule Engine (built-in) acts as the Kafka producer — EMQX's native Kafka bridge transforms MQTT messages to Kafka records with zero custom code.

---

**Q: Why InfluxDB over Postgres or Cassandra for time-series data?**

A: Three things InfluxDB does natively that require painful workarounds elsewhere:

1. **Retention policies** — `raw_7d` auto-deletes after 7 days, `hourly_90d` after 90 days. In Postgres you'd need a cron job + partition pruning.
2. **Continuous queries** — downsampling from raw → hourly aggregates runs automatically. In Cassandra you'd need a separate Spark job.
3. **Compression** — InfluxDB's TSM storage engine achieves ~10:1 compression on float time-series. 864 GB/day raw → ~86 GB stored.

The trade-off: InfluxDB doesn't support joins or foreign keys, so device metadata (owner, type) lives in Postgres. Queries join at the application layer.

I'd consider TimescaleDB if we need SQL compatibility or Cassandra if we need multi-region writes at >10B messages/day.

---

## Round 3: Failure Scenarios (Interviewer Digs In)

**Q: What happens if EMQX goes down?**

A: A single EMQX node failing is handled by the 3-node cluster — HAProxy detects the failure within 5 seconds, removes the node, devices reconnect to remaining 2 nodes. Session state is replicated across the cluster, so reconnecting devices resume their QoS sessions without data loss.

If all 3 EMQX nodes fail: devices start reconnecting as soon as EMQX restarts. The risk is a thundering herd — 500K devices all reconnecting simultaneously. Fix: EMQX returns 503 → devices interpret connection failure as a signal to use jittered exponential backoff (`delay = min(base × 2^n, 60) + random(0, 10)s`). Reconnections spread over ~60 seconds instead of all hitting at once.

No telemetry is permanently lost — devices buffer QoS 1 messages locally (if configured) and retransmit on reconnect.

---

**Q: A temperature rule says "if temp > 30°C, turn on AC." A sensor glitches and sends 1000 readings of 99°C in 2 seconds. What happens?**

A: Without protection: 1000 alerts fire, 1000 commands sent to the AC. That's an alert storm.

**Solution — three layers of defense in Flink:**

**Layer 1 — Session Window (prevents spike triggers):**
Don't trigger on a single point. Require the condition to hold continuously for N minutes.

```
Flink session window = 5 minutes

Glitch: 1000 readings of 99°C over 2 seconds
  → window opens at t=0, closes at t=2s (gap > 5min threshold not met)
  → window duration = 2s < required 5min → NO alert fired ✓

Genuine overheating: temp > 30°C sustained for 6 minutes
  → window stays open, duration exceeds threshold → alert fires ✓
```

A 2-second spike never opens a 5-minute window — glitch silently dropped.

**Layer 2 — Cooldown / Debounce (prevents repeat firing):**
Even if a condition genuinely holds, fire at most once per cooldown period.

```python
class RuleState:
    last_alert_time: Timestamp  # per (rule_id, device_id)

def evaluate(event, state, rule):
    if condition_met(event, rule):
        elapsed = now() - state.last_alert_time
        if elapsed >= rule.cooldown_sec:   # e.g. 900s = 15 min
            emit_alert()
            state.last_alert_time = now()
        # else: suppress — cooldown active
```

**Layer 3 — Outlier filter (catches sensor glitches before rules even see them):**
In the Flink validation step, flag readings statistically impossible for the sensor type.

```python
# Temperature sensor valid range: -40°C to 85°C (hardware spec)
if value > SENSOR_MAX or value < SENSOR_MIN:
    tag as invalid → route to dead-letter topic → skip rule evaluation
```

**Combined result:**
```
1000 readings of 99°C in 2s:
  Layer 3 catches it first  → flagged as out-of-range → rules never see it
  Layer 1 backup            → 2s window < 5min threshold → no alert
  Layer 2 backup            → cooldown prevents spam even if L1/L3 miss

Genuine 30°C sustained for 6 min:
  Layer 3: valid range → passes
  Layer 1: 6min window > 5min threshold → alert fires once
  Layer 2: cooldown resets → next alert earliest in 15 min
```

---

**Q: How do you handle a device sending a telemetry timestamp from the year 2000 (dead RTC battery)?**

A: In the Flink validation step, I check the event timestamp against server receive time. If the delta exceeds 24 hours (in either direction), I replace the timestamp with server receive time and tag the record with `clock_skew=true`. 

```python
if abs(event.timestamp - server_time) > 86400:
    event.timestamp = server_time
    event.flags.clock_skew = True
```

This ensures the InfluxDB record lands in the correct time bucket and isn't immediately deleted by the 7-day retention policy. The `clock_skew` tag can be used for device health monitoring — devices frequently flagged for skew likely have dying RTC batteries.

---

## Round 4: Scale & Cost Follow-ups

**Q: The team wants to use AWS IoT Core to simplify ops. What's the cost impact?**

A: At our scale (50K messages/sec, 500K devices), AWS IoT Core pricing:
- Messaging: $1 per million messages × 129.6B messages/month = **$129,600/month**
- Connectivity: $0.042 per million connection-minutes × 500K devices × 43,200 min/month = **$907/month**
- Total: **~$130,500/month** — 13× over our $10K budget

AWS IoT Core makes sense at smaller scale: under ~1K devices sending infrequently, it's under $100/month with zero ops burden. At our scale, self-managed EMQX at ~$550/month is the only viable option. The trade-off is operational burden: we manage EMQX upgrades, TLS certificate rotation, and custom ACL logic.

---

**Q: How would you scale to 10 million devices?**

A: Three changes:

1. **EMQX:** Scale horizontally to 10 nodes (~1M connections each). EMQX supports cluster-link for cross-datacenter federation. At 500K messages/sec, add Kafka partitions to 200 (matching Flink parallelism).

2. **InfluxDB:** Switch to InfluxDB Clustered (enterprise) or consider Apache Parquet + DuckDB for historical queries. At 10× scale, raw writes are 100 MB/sec → InfluxDB Clustered or a distributed alternative like QuestDB.

3. **Shadow Service:** At 5M active devices, Redis shadow cache grows to 5 GB — still fits in a large Redis node, but switch to Redis Cluster for horizontal scaling.

4. **Multi-region:** Deploy an EMQX cluster per region (US, EU, APAC). Each region writes to its own Kafka cluster. A cross-region Kafka MirrorMaker replicates to a global analytics cluster for aggregated reporting.

---

## Round 5: Common Interviewer Follow-ups

**Q: How do you ensure a command executes exactly once?**

A: Two layers:
1. **MQTT QoS 2** between server and device: 4-packet handshake guarantees the MQTT layer delivers the message exactly once.
2. **Device-side deduplication**: Device stores last N `command_id` values. On receiving a command, checks if `command_id` already executed → if yes, resends ACK and skips execution.

For non-idempotent commands (lock, meter reset), both layers are required. For idempotent commands (set temperature), QoS 1 is fine — duplicate "set to 22°C" has no extra effect.

---

**Q: How do you handle a device that's offline when a command is sent?**

A: Three-part answer:

1. **Persist in CommandLog:** `status='pending'`, `expires_at = now() + 5min`. Device shadow's `desired` state already reflects what we want.

2. **Deliver on reconnect:** When device reconnects, EMQX delivers any queued QoS 1/2 messages from the persistent session. If expired, EMQX drops; Shadow delta mechanism handles state sync instead.

3. **Expiry handling:** Cron every 5 minutes marks pending commands past `expires_at` as `expired`. API returns 410 Gone. User decides whether to re-issue. For safety-critical commands (unlock), notify user immediately rather than silently expiring.

---

**Q: Can two users simultaneously control the same device? What happens?**

A: Yes, and without protection they'd silently overwrite each other. The shadow document uses optimistic locking:

- Each shadow has a `version` integer
- API reads current shadow → returns `version: 7`
- User A writes `{ desired: { mode: 'cool' }, version: 7 }` → SQL `WHERE version = 7 AND version++` → succeeds, version becomes 8
- User B writes `{ desired: { mode: 'heat' }, version: 7 }` → SQL `WHERE version = 7` → 0 rows affected → 409 Conflict returned
- User B must re-read shadow (now version 8), see User A's change, and decide whether to override

This prevents silent last-write-wins. For consumer products, we might relax this to "most recent write wins" with a UI notification ("Another user changed this device's settings").

---

## Key Concepts to Mention Proactively

| Concept | When to Mention |
|---------|----------------|
| MQTT QoS levels (0/1/2) | Whenever discussing message delivery guarantees |
| Device Shadow pattern | Bidirectional state sync, offline recovery |
| TSDB downsampling | Storage cost optimization for time-series |
| LWT (Last Will and Testament) | Device offline detection |
| Jittered backoff | Any thundering herd risk on reconnect |
| Rule cooldown + session window | Alert storm prevention |
| Optimistic locking on shadow | Concurrent user access |
| Kafka partition key = device_id | Per-device ordering in stream |

---

## One-Liner Answers (for rapid follow-ups)

- **"Why not store telemetry in Postgres?"** → Random-write B-tree doesn't compress well for time-series; no native downsampling; full table scan for range queries.
- **"Why Kafka between EMQX and Flink?"** → Decouples ingest from processing; Flink can fall behind without backpressuring devices; enables multiple consumers without EMQX knowing about them.
- **"How do you test your rule engine?"** → Unit: inject mock Flink event, assert alert emitted. Integration: replay production Kafka snapshot. Chaos: kill InfluxDB mid-job, verify Flink retries from checkpoint.
- **"What's your RPO/RTO?"** → RPO: near-zero (Kafka 7d retention covers all outage scenarios). RTO: 90s (Flink checkpoint restore) to 5 min (InfluxDB failover).
