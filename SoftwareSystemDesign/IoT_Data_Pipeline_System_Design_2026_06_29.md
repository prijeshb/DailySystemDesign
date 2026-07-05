# IoT Data Pipeline — System Design
> Date: 2026-06-29 | Difficulty: Hard | Real-world: AWS IoT Core, Google Home, Azure IoT Hub

---

## First Principles Check

**Do we really need a dedicated IoT pipeline?**

Plain HTTP REST API fails here for three reasons:
1. **Device constraints** — MCUs with 64KB RAM can't maintain TLS sessions for HTTP long-polling
2. **Connection pattern** — devices reconnect constantly (power cycles, cellular dropouts); HTTP is stateless but MQTT persists sessions
3. **Message volume** — 50K messages/sec from 500K devices is 50× more expensive on HTTP than MQTT (HTTP = ~2KB overhead per message vs MQTT = ~2 bytes fixed header)

**Why not Kafka directly from devices?**
Kafka clients require JVM or heavy client libraries — not viable on embedded devices. MQTT is the universal IoT protocol; Kafka is the internal message bus.

---

## Scale Assumptions

| Metric | Value |
|--------|-------|
| Total registered devices | 1,000,000 |
| Concurrently active | 500,000 |
| Publish rate per device | 1 message / 10 seconds |
| Messages/second (peak) | 50,000 msg/sec |
| Message size (avg) | 200 bytes |
| Ingest throughput | 10 MB/sec |
| Daily writes | 4.32 billion |
| Raw storage/day | ~864 GB (before compression) |

---

## Budget Analysis — $10,000/month

### Why AWS IoT Core is Out

```
AWS IoT Core pricing: $1 per million messages
50K msg/sec × 86,400s × 30 days = 129.6 billion messages/month
Cost: 129,600 × $1/million = $129,600/month  → 13× over budget
```

**Budget constraint:** Managed IoT Core is off the table. Self-managed MQTT broker (EMQX) is required.

### Self-Managed Architecture Cost

| Component | Config | Monthly Cost |
|-----------|--------|-------------|
| EMQX on EC2 (3× m5.2xlarge) | 17K connections/node | $550 |
| Amazon MSK Kafka (3× kafka.m5.2xlarge) | 3 AZs | $1,400 |
| Flink on EKS (4× m5.xlarge) | Stream processing | $450 |
| InfluxDB on EC2 (2× r5.2xlarge) | Primary + replica | $1,200 |
| RDS PostgreSQL (db.r5.large Multi-AZ) | Shadow, rules, alerts | $500 |
| Redis ElastiCache (cache.r6g.large) | Device shadow hot cache | $200 |
| S3 (30 TB cold archive) | Downsampled history | $690 |
| NAT Gateway + data transfer | — | $400 |
| CloudWatch + misc | — $200 |
| **Total** | | **~$5,590/month** |

**Headroom:** ~$4,400/month remains for traffic spikes, second region, or growth.

### Limitation from Budget Constraint
Choosing self-managed EMQX over AWS IoT Core means:
- **No managed per-device policy enforcement** — must implement own ACL in EMQX
- **Manual TLS certificate rotation** — need custom PKI or ACM Private CA ($400/month extra)
- **No built-in device registry** — must maintain in Postgres
- **Ops overhead:** EMQX upgrades, EMQX failover must be handled manually

---

## Entities

```
Device
  device_id    UUID (primary key)
  owner_id     UUID
  type         ENUM (thermostat, camera, sensor, lock, plug)
  firmware_ver String
  last_seen    Timestamp
  status       ENUM (online, offline, unknown)

DeviceShadow
  device_id    UUID (FK)
  desired      JSONB   -- what user wants
  reported     JSONB   -- what device last reported
  delta        JSONB   -- desired - reported (computed)
  version      INT     -- optimistic lock
  updated_at   Timestamp

TelemetryRecord [InfluxDB]
  device_id    tag
  sensor_type  tag    (temperature, humidity, motion, power_watts)
  owner_id     tag
  value        float
  unit         string
  timestamp    nanosecond

Rule
  rule_id      UUID
  owner_id     UUID
  name         String
  condition    JSONB  -- { field: 'temperature', op: '>', value: 30, window_sec: 300 }
  action       JSONB  -- { type: 'command', cmd: 'SET_MODE', payload: {...} }
  cooldown_sec INT    -- min seconds between consecutive triggers (debounce)
  enabled      BOOL

Alert
  alert_id     UUID
  rule_id      UUID (FK)
  device_id    UUID (FK)
  triggered_at Timestamp
  resolved_at  Timestamp (nullable)
  severity     ENUM (info, warning, critical)

CommandLog
  command_id   UUID
  device_id    UUID (FK)
  cmd_type     String
  payload      JSONB
  status       ENUM (pending, sent, acked, failed, expired)
  sent_at      Timestamp
  acked_at     Timestamp (nullable)
  expires_at   Timestamp
```

---

## Data Flow

```
[Device]
   │  MQTT TLS (QoS 1) — topic: telemetry/{device_id}/{sensor_type}
   ▼
[EMQX Cluster — 3 nodes]
   │  EMQX Rule Engine bridges to Kafka
   ├─► Kafka: telemetry.raw        (50 partitions, key=device_id)
   ├─► Kafka: shadows.updates      (20 partitions)
   └─► Kafka: commands.inbound.ack (10 partitions)

[Kafka]
   │
   ├─► [Flink Stream Processor]
   │       │  validates, enriches (joins device metadata)
   │       ├─► InfluxDB (time-series writes, batched 500ms)
   │       └─► Flink Rule Evaluator
   │               │  stateful per-device (keyed stream)
   │               └─► Kafka: alerts.triggered
   │
   ├─► [Shadow Service]
   │       │  reads shadows.updates
   │       ├─► Redis HSET shadow:{device_id}  (hot cache, <1ms reads)
   │       └─► Postgres shadow_history (versioned, durable)
   │
   └─► [Alert Service]
           │  reads alerts.triggered
           ├─► Postgres alerts table
           ├─► Kafka: commands.outbound (for action-type rules)
           └─► WebSocket Server → Dashboard

[Command Path]
  User/API → Kafka: commands.outbound
           → EMQX → topic: cmd/{device_id}
           → Device executes → publishes ack to cmd/ack/{device_id}
           → Kafka: commands.inbound.ack
           → Command Service updates CommandLog.status = 'acked'
```

---

## High Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│  IoT Devices (thermostats, cameras, sensors, plugs)             │
│  ↕ MQTT/TLS  ↕ MQTT/TLS  ↕ MQTT/TLS                           │
└──────────────┬──────────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────┐
│  EMQX Cluster (3 nodes, HAProxy L4 LB, 500K concurrent conns)  │
│  Built-in Rule Engine → Kafka bridge                            │
└──────────────┬──────────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────┐
│  Apache Kafka (3 brokers, MSK)                                  │
│  telemetry.raw | shadows.updates | commands.* | alerts.*        │
└──┬───────────┬───────────────┬────────────────────────────────  ┘
   │           │               │
   ▼           ▼               ▼
[Flink]   [Shadow Svc]   [Alert Svc]
   │       Redis+PG        PG + WS
   ▼
[InfluxDB]
   │
   ▼
[S3 Archive]  ← continuous downsampled export (hourly/daily)

                    [API Gateway]
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          [REST API]  [WS Server]  [Query Svc]
          Device Mgmt  Real-time   InfluxDB + S3
          Rules CRUD   Alerts/Telemetry   Historical
```

---

## Low Level Design

### MQTT Topic Schema

```
Telemetry (device → broker):
  telemetry/{device_id}/temperature
  telemetry/{device_id}/humidity
  telemetry/{device_id}/motion
  telemetry/{device_id}/power_watts

Shadow (device ↔ broker):
  $shadow/get/{device_id}         ← device requests shadow
  $shadow/update/{device_id}      ← device reports state
  $shadow/delta/{device_id}       ← broker pushes desired diff

Commands (broker → device):
  cmd/{device_id}                 ← server sends command
  cmd/ack/{device_id}             ← device acks command
```

### EMQX Authentication
```
Device connects with:
  ClientID: device_{device_id}
  Username: device_id
  Password: HMAC-SHA256(device_secret, timestamp) — rotating credential

EMQX ACL (per device):
  ALLOW PUBLISH  telemetry/{client_id}/#
  ALLOW PUBLISH  $shadow/update/{client_id}
  ALLOW SUBSCRIBE cmd/{client_id}
  ALLOW SUBSCRIBE $shadow/delta/{client_id}
  DENY  *  (catch-all)
```

### MQTT QoS Levels Used

| Message Type | QoS | Why |
|-------------|-----|-----|
| Telemetry | 1 (at least once) | Duplicate reads are OK; loss is not |
| Shadow update | 1 | Re-apply is idempotent (versioned) |
| Command (cmd/{id}) | 2 (exactly once) | Command must execute exactly once |
| Command ACK | 1 | Idempotent — ack is safe to redeliver |

### Flink Topology

```
[Kafka Source: telemetry.raw]
     │
     ▼
[Validate + Parse]         ← drop malformed, parse JSON payload
     │
     ▼
[Enrich: join device metadata]  ← broadcast stream from Postgres snapshot
     │
     ├──► [InfluxDB Sink]  (async, batched 500ms, max 1000 points/batch)
     │
     └──► [Rule Evaluator — keyed by device_id]
               │  Per-device state: rolling 5-min window, last alert time
               ├── Condition met + cooldown elapsed? → emit alert event
               └──► [Kafka Sink: alerts.triggered]
```

### Device Shadow — Update Flow

```
1. User sets desired state via API:
   PUT /devices/{id}/shadow
   Body: { "desired": { "mode": "cool", "setpoint": 22 } }

2. Shadow Service:
   BEGIN;
   UPDATE device_shadows
   SET desired = desired || '{"mode":"cool","setpoint":22}'::jsonb,
       version = version + 1
   WHERE device_id = $1;
   COMMIT;

   Redis HSET shadow:{device_id} desired '{"mode":"cool","setpoint":22}' version 8

3. Compute delta = desired - reported:
   If device reported { "mode": "idle" } → delta = { "mode": "cool", "setpoint": 22 }

4. Publish to MQTT: $shadow/delta/{device_id}
   Payload: { "delta": { "mode": "cool", "setpoint": 22 }, "version": 8 }

5. Device receives delta, applies it, publishes updated reported state:
   $shadow/update/{device_id}
   Payload: { "reported": { "mode": "cool", "setpoint": 22 }, "version": 8 }

6. Shadow Service: delta = {} → shadow reconciled ✓
```

### InfluxDB Schema + Downsampling

```
Measurement: telemetry
Tags:  device_id, sensor_type, owner_id
Fields: value (float64), unit (string)
Time:  nanosecond precision

Retention Policies:
  raw_7d:    raw points, 7-day TTL
  hourly_90d: 1h aggregates (MEAN, MAX, MIN), 90-day TTL
  daily_2y:  1d aggregates, 2-year TTL → then archive to S3 Parquet

Continuous Query (auto-runs every 1h):
  SELECT MEAN(value) AS mean, MAX(value) AS max, MIN(value) AS min
  INTO hourly_90d.telemetry
  FROM raw_7d.telemetry
  GROUP BY time(1h), device_id, sensor_type
```

### Command Reliability — At-Least-Once with Dedup

```
Server:
1. INSERT INTO command_log (command_id, device_id, ..., status='pending', expires_at=now+5min)
2. Publish to Kafka: commands.outbound (key=device_id)
3. EMQX delivers to device via QoS 2
4. Device executes → publishes ACK to cmd/ack/{device_id}
5. Command Service reads ACK → UPDATE command_log SET status='acked'

Retry (if no ACK within 60s):
  Requeue to Kafka with same command_id
  Device checks command_id → already executed → send ACK again (idempotent)

Expiry:
  Cron every 5min: UPDATE command_log SET status='expired'
                   WHERE status='pending' AND expires_at < now()
  → Alert user: device may be offline
```

---

## Component Trade-offs

| Decision | Chosen | Why | Trade-off |
|----------|--------|-----|-----------|
| EMQX vs AWS IoT Core | EMQX | Budget ($5.5K vs $129K/month) | Ops burden: EMQX upgrades, failover, cert rotation |
| InfluxDB vs TimescaleDB | InfluxDB | Native TSDB, downsampling built-in, Flux query lang | TimescaleDB = SQL compatibility; InfluxDB = better compression at IoT scale |
| InfluxDB vs Cassandra | InfluxDB | Native TTL per retention policy, no manual tombstoning | Cassandra: better for multi-region, more battle-tested at 10B+ writes/day |
| Redis for shadow | Redis | Sub-millisecond reads, 500K device states fit in RAM (500K × 1KB = 500 MB) | Shadow lost on Redis restart → must re-read from Postgres (100ms cold) |
| Flink for rules | Flink | Stateful windows (5-min condition tracking) | Spark Streaming: simpler but higher latency; Lambda: no window state |
| QoS 1 telemetry | QoS 1 | Tolerates occasional duplicate (idempotent); QoS 2 = 2× MQTT round trips | Risk: rare duplicate point in InfluxDB (Flink dedup window handles it) |

### Caching Trade-off: Device Shadow in Redis
```
PRO: Shadow reads < 1ms (real-time dashboard, rule evaluation)
CON: Redis eviction under memory pressure → miss → Postgres fallback (100ms)
     Redis restart → cold cache → thundering herd from rule engine
FIX: Shadow Service always writes to Postgres first, then Redis.
     On miss, load from Postgres + warm cache. Never serve stale data as authoritative.
```

### InfluxDB Downsampling Trade-off
```
PRO: 10:1 compression; hourly aggregates serve 90% of dashboard queries
CON: Can't reconstruct original samples from hourly aggregates.
     If you need "exactly what happened at 10:03:47" after 7 days → gone.
     Must decide: raw retention = 30 days? (3× storage cost) or accept loss of raw > 7 days
```
**Decision:** Keep raw 7 days. If forensic data is needed beyond 7 days, export raw to S3 before TTL hits (nightly job archives previous day's raw to S3 Parquet).

---

## Back of Envelope Summary

```
Messages/sec:          50,000
Ingest throughput:     10 MB/sec
Kafka write (3×):      30 MB/sec across 3 brokers
InfluxDB writes/sec:   50,000 points/sec (well within r5.2xlarge limits ~200K/sec)
Redis shadow memory:   500K devices × 1 KB = 500 MB (fits in cache.r6g.large = 13 GB)
Flink state:           500K devices × 2 KB state = 1 GB (fine)

Storage:
  InfluxDB raw 7d:     864 GB/day × 7 = 6 TB, compressed ~600 GB
  InfluxDB hourly 90d: 2.4 GB/day × 90 = 216 GB
  S3 archive 2y:       ~100 MB/day → 72 GB/year
  Postgres (PG):       Command logs: 50K cmds/sec (if all have cmds) — not realistic;
                       assume 1K cmds/sec → 86M cmds/day × 500B = 43 GB/day
                       In practice: cmds << telemetry, ~1 GB/day PG
```

---

## Real-World References
- **AWS IoT Core + Timestream**: AWS architecture blog (2022) — same pattern, managed
- **Influx blog: "IoT at Scale"** — 50K writes/sec benchmarks on InfluxDB OSS
- **EMQX whitepaper**: 1M concurrent MQTT connections on 8-core server
- **Tesla Fleet Telemetry** (open-source): MQTT → Kafka → custom store, same topology
- **Google Home** (per GCP Next 2023): Pub/Sub → Dataflow → Bigtable (equivalent to Kafka → Flink → wide-column store)
