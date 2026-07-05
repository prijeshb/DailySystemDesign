# IoT Data Pipeline — Failure Analysis
> Date: 2026-06-29 | System: IoT Data Pipeline

**Failure-first principle:** Every component will fail. Design for it.

---

## Component Map (Who Can Fail)

```
Device → EMQX → Kafka → Flink → InfluxDB
                       → Shadow Svc → Redis
                                   → Postgres
                       → Alert Svc → WebSocket
                                   → Postgres
```

---

## 1. EMQX Broker Node Failure

### Scenario A: Single Node Dies (of 3)
**Impact:** ~167K devices lose their TCP connection. Devices reconnect.

**What happens:**
- Devices detect TCP broken → back-off retry (exponential, 1s → 2s → 4s → max 60s)
- HAProxy health check fails for dead node → removes from pool within 5s
- Devices reconnect to remaining 2 nodes (~7ms TCP handshake + TLS 1.3 = ~30ms)
- EMQX clustered: session state replicated → device resumes QoS 1 session, no message loss

**Risk:** Remaining 2 nodes absorb 167K extra connections.
- Each node now handles: 167K + 250K = ~333K/500K max → stays under 50% headroom
- Kafka producers (EMQX → Kafka bridge) redistribute automatically

**Prevention:**
- Health checks: L4 TCP check every 5s, unhealthy threshold = 2 failures
- Session persistence: `session.expiry_interval = 300s` → device can reconnect within 5 min and resume
- Retained messages: critical shadow deltas published with `retain=true` → new connection gets last known state

### Scenario B: All EMQX Nodes Fail (Cluster Split / Config Bug)
**Impact:** All 500K devices disconnected. No telemetry ingest.

**What fails:**
- All telemetry stops flowing to Kafka
- Dashboard shows all devices "offline"
- In-flight QoS 1 messages in device retry buffer for up to `keep_alive × 1.5` = 22 seconds

**Recovery:**
1. Auto-restart via systemd: `Restart=always, RestartSec=10s`
2. Devices begin reconnecting (exponential backoff → thundering herd risk)
3. **Thundering herd fix:** HAProxy returns 503 → devices see connection refused → use jittered backoff (not just exponential)
   ```
   reconnect_delay = min(base * 2^attempt, 60) + random(0, 10) seconds
   ```
4. Once EMQX up, reconnections spread over ~60 seconds not all at once
5. Kafka: 7-day retention → no data lost; just a gap in the timeline

**Detection:** CloudWatch alarm on `emqx_connections_count` drops below 400K.

---

## 2. Kafka Broker Failure

### Scenario A: One Broker of Three Fails
**Impact:** Partitions on the failed broker are unavailable until leader election.

**Leader election time (MSK):** ~30 seconds (ZooKeeper mode) or ~5 seconds (KRaft mode).

**What happens during 30s outage:**
- EMQX → Kafka bridge: producer receives `LeaderNotAvailableException` → retries with backoff
- 30s × 50K msg/sec = 1.5M messages buffered in EMQX or dropped if buffer full
- InfluxDB write lag: Flink pauses consuming from unavailable partitions

**Data Loss Risk:**
- `acks=all` configured → message NOT confirmed until all replicas acknowledge → no data loss on producer side
- Messages in EMQX bridge buffer (default 10MB) → backpressure → devices accumulate QoS 1 retry buffer on device

**Recovery path:**
- New leader elected → consumers (Flink) resume from last committed offset
- All buffered messages processed in order
- Gap in InfluxDB timeline corresponds exactly to outage duration

**Prevention:**
- `replication.factor=3, min.insync.replicas=2`
- MSK Multi-AZ: each broker in separate AZ
- Monitor: `UnderReplicatedPartitions > 0` → alert

### Scenario B: Kafka Consumer (Flink) Falls Behind
**What it is:** Flink processes slower than ingest rate → consumer lag grows.

**Cause:** InfluxDB write slow, rule evaluation CPU-heavy, or Flink checkpoint taking too long.

**Impact:**
- Increasing delay between device sending data and dashboard showing it
- Rules trigger late → automation actions execute late (cold room stays cold)
- If lag exceeds 7 days: offset falls behind retention → data permanently lost

**Detection:** Monitor `consumer_lag > 100K` on Kafka consumer group `flink-telemetry`.

**Prevention:**
- Flink parallelism = partition count (50) → one thread per partition
- InfluxDB writes: batched async (500ms windows, 1000 points) → not on critical path
- Kafka retention: 7 days (safely exceeds any realistic lag scenario)
- Auto-scale Flink task managers on lag metric

---

## 3. Flink (Stream Processor) Failure

### Scenario: Flink Job Crashes
**State at risk:** Per-device rule evaluation state (rolling window, last alert time).

**Without checkpointing:**
- Flink restarts from latest Kafka offset → loses all in-memory state
- Rules with 5-minute window conditions reset → might miss alerts that were building up
- Kafka reprocesses last X messages (depends on restart offset) → possible duplicate InfluxDB points

**With checkpointing (every 30s):**
- Flink restores from last checkpoint → at most 30s of state lost
- Resumes from Kafka offset saved in checkpoint → exactly-once semantics restored
- InfluxDB: idempotent writes (same timestamp + device = overwrite) → duplicates safe

**Configuration:**
```
checkpointing.interval: 30s
checkpointing.mode: EXACTLY_ONCE
state.backend: RocksDB  ← spills to disk for large state
state.checkpoints.dir: s3://iot-checkpoints/flink/
```

**Recovery time:** ~90s (checkpoint restore + Kafka offset seek + warm-up)

---

## 4. InfluxDB Failure

### Scenario A: Primary InfluxDB Node Down
**Impact:** No new telemetry written. Dashboard shows stale data.

**What happens:**
- Flink sink: write fails → Flink backs off → retries (exponential, max 3min)
- Kafka lag grows during outage (but 7-day retention = safe)
- Redis still has device shadows → real-time device state still visible
- Dashboard: telemetry chart freezes at last-known point

**Recovery:**
- Manual: promote InfluxDB replica (if OSS without Enterprise clustering: manual switchover ~5 min)
- Flink resumes → processes backlog → data written in order (InfluxDB accepts out-of-order up to `ingestion-max-drift` window)

**Prevention:**
- InfluxDB Enterprise: 2 data nodes + anti-entropy → automatic failover
- Alternative: InfluxDB OSS + Prometheus scrape InfluxDB metrics + PagerDuty alert
- Backup: Flink retries buffer up to 7 days (Kafka retention) → zero data loss even with multi-hour InfluxDB outage

### Scenario B: InfluxDB Slow (High Write Latency)
**Cause:** Too many series (high cardinality tags), disk I/O saturation.

**Anti-pattern to avoid:**
```
// WRONG: request_id as a tag → unbounded series count
measurement: telemetry, tags: { device_id, request_id }
// 500K devices × 1M unique request_ids = 500B series → OOM in InfluxDB
```

**Rule:** Only low-cardinality fields as tags. `device_id` = 1M unique values (OK for InfluxDB). `request_id` = never a tag.

---

## 5. Device-Side Failures

### Scenario A: Device Goes Offline
**Detection:**
- MQTT keep-alive: device sends PINGREQ every 15 seconds
- EMQX: if no PINGREQ within `keep_alive × 1.5 = 22.5s` → marks connection dead
- EMQX publishes LWT (Last Will and Testament) message to `status/{device_id}` = "offline"
- Status Service receives LWT → UPDATE devices SET status='offline', last_seen=now()

**LWT configuration on device:**
```json
{
  "topic": "status/{device_id}",
  "payload": "offline",
  "qos": 1,
  "retain": true
}
```

**User impact:** Dashboard shows device offline badge. Commands queued with `expires_at = now + 5min`.

### Scenario B: Device Sends Wrong Timestamp (Clock Skew)
**Problem:** Device with dead RTC battery sends timestamp from epoch 0 (1970-01-01) or year 2000.

**Impact on InfluxDB:**
- Point written at wrong timestamp → appears in year 2000 partition → never shows in dashboards
- Retention policy deletes it immediately (7-day window)

**Fix:**
Flink validation step:
```python
MAX_PAST_DRIFT = 60 * 60 * 24  # 24 hours
MAX_FUTURE_DRIFT = 300          # 5 minutes

if abs(event.timestamp - server_receive_time) > MAX_PAST_DRIFT:
    event.timestamp = server_receive_time  # replace with server time
    event.flag_clock_skew = True
```

### Scenario C: Device Sends Duplicate Data (Reconnect Retry)
**Why:** QoS 1 = at-least-once. On reconnect, device retransmits last unacked messages.

**Impact:** InfluxDB receives same `(device_id, sensor_type, timestamp)` twice.

**Fix:** InfluxDB last-write-wins for same timestamp + same series → duplicate is idempotent. No action needed.

---

## 6. Command Never Acknowledged

**Scenario:** Server sends command to device, device either misses it (offline) or crashes mid-execution.

**State:** `command_log.status = 'sent'` forever.

**Detection:**
```sql
-- Run every 5 minutes
SELECT command_id, device_id, cmd_type, sent_at
FROM command_log
WHERE status = 'sent' AND expires_at < now();
```

**Recovery:**
1. Cron marks these `status='expired'`
2. Alert sent to user: "Command not acknowledged — device may be offline"
3. User can retry command → new `command_id` generated → same flow

**QoS 2 for critical commands:**
MQTT QoS 2 ensures command delivered to device exactly once even through broker restart. But it requires 4-packet exchange (PUBLISH → PUBREC → PUBREL → PUBCOMP). Only use for:
- Lock/unlock commands (dangerous to execute twice)
- Firmware update trigger (expensive to run twice)
- Financial/billing actions (smart meter reset)

---

## 7. Rule Engine Alert Storm

**Scenario:** Temperature sensor glitches → sends 10,000 readings of 99°C → rule fires 10,000 times → 10,000 commands sent to AC unit.

**Without debounce:** AC unit receives 10,000 "turn on max cool" commands → potential damage.

**Fix — Rule Cooldown in Flink State:**
```python
class RuleState:
    last_alert_time: Timestamp  # per (rule_id, device_id)

def evaluate(event, state, rule):
    if condition_met(event, rule):
        time_since_last = now() - state.last_alert_time
        if time_since_last >= rule.cooldown_sec:
            emit_alert(event, rule)
            state.last_alert_time = now()
        # else: silently suppress (debounce active)
```

**Additional defense — Flink session window:**
```python
# Only trigger if condition holds for 5 consecutive minutes (session window)
# Prevents single-spike false positives
trigger_condition = value > 30 AND window_duration >= 300s
```

---

## 8. Shadow Service Failure

### Scenario A: Redis Fails
**What breaks:**
- Shadow reads now go to Postgres (100ms vs <1ms) — 100× slower
- Rule engine: reads shadow to check device state before sending command → slowdown
- Dashboard: device state panel latency degrades

**Recovery:**
- Circuit breaker on Redis reads: if Redis latency > 50ms or errors > 5%, fail-open to Postgres
- Shadow Service: writes always go to Postgres first → Redis is always rebuildable from Postgres
- On Redis restart: warm-up job reads top 100K active devices → pre-populates Redis

### Scenario B: Shadow Version Conflict
**Scenario:** Two users simultaneously update same device's desired state.

**Without versioning:** Last write wins → first user's change silently overwritten.

**Fix — Optimistic Locking:**
```sql
UPDATE device_shadows
SET desired = desired || $new_desired::jsonb,
    version = version + 1
WHERE device_id = $1 AND version = $expected_version;
-- 0 rows → version conflict → return 409 Conflict to API caller
```

API returns `{ "error": "conflict", "current_version": 8 }` → client re-reads shadow, merges, retries.

---

## Failure Matrix

| Component | Failure Type | Data Loss? | Recovery Time | Prevention |
|-----------|-------------|------------|---------------|------------|
| EMQX node | Single node crash | No (QoS 1 retry) | 30s (reconnect) | 3-node cluster, session persistence |
| EMQX cluster | Full outage | No (device retry) | 2-5 min | systemd restart, jittered client backoff |
| Kafka broker | Leader election | No (acks=all) | 5-30s | 3 replicas, min.insync.replicas=2 |
| Flink crash | Job failure | No (checkpoint) | 90s | 30s checkpoints to S3 |
| InfluxDB | Node down | No (Kafka buffer) | 5-30 min | Replica, Kafka 7d retention |
| Redis (shadow) | Crash | No (Postgres primary) | 2-5 min | Write-through, warm-up from PG |
| Postgres | Node failover | No (Multi-AZ RDS) | 60-120s | RDS Multi-AZ automatic failover |
| Device offline | Network / power | Partial (device buffer) | Device-dependent | QoS 1 retry, LWT detection |
| Clock skew | Device RTC dead | No (server timestamp) | Immediate | Flink drift validation |
| Alert storm | Sensor glitch | N/A | Immediate | Rule cooldown + session window |
