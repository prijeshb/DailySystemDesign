# Distributed Message Queue — Failure Analysis
**Date:** 2026-05-23 | Companion to: Message_Queue_System_Design_2026_05_23.md

---

## Failure Taxonomy

```
Layer 1: Producer fails
Layer 2: Broker (Leader) fails
Layer 3: Broker (Follower) fails
Layer 4: Consumer fails
Layer 5: Coordinator fails
Layer 6: Network partition
Layer 7: Storage fails
Layer 8: Metadata store fails
```

---

## Layer 1 — Producer Fails

### Case A: Producer crashes mid-batch (before send)
```
Symptom: Messages in local buffer → lost
Detection: Producer never sent; no broker record
Prevention:
  - Use synchronous send for critical messages (flush before crash)
  - For idempotent producers: retry is safe (dedup by PID+seq)
Recovery: Application retries on restart; idempotent = no duplicate
```

### Case B: Producer crashes after send, before ACK
```
Symptom: Broker may or may not have written; producer doesn't know
Detection: Producer timeout → ambiguous state
Prevention: enable.idempotence=true
  → Broker stores (PID, seq) → retried message deduped
Recovery: Retry → broker deduplicates → at-least-once with no duplicate
```

### Case C: Producer sends duplicate messages (retry storm)
```
Symptom: retries.max=INT_MAX → network blip → thousands of retries
Detection: broker-side duplicate metrics, consumer sees duplicates
Prevention:
  1. enable.idempotence=true (within session)
  2. Transactional producer (cross-session)
  3. Consumer-side idempotency: upsert by message ID
```

### Case D: Producer overwhelms broker (no backpressure)
```
Symptom: Broker OOM or disk full; producer gets NotLeaderException
Detection: broker.disk.used > 90%, produce latency spikes
Prevention:
  - max.block.ms: producer blocks if buffer full (instead of dropping)
  - Quota enforcement: broker throttles producers over limit
  - Alert on disk usage; auto-expand with tiered storage
```

---

## Layer 2 — Leader Broker Fails

### Case A: Clean shutdown
```
Flow:
  1. Broker sends controlled shutdown signal
  2. Transfers leadership for all partitions (preferred leader election)
  3. Consumers reconnect to new leader via metadata refresh
Downtime: ~seconds (metadata refresh interval)
Impact: Brief pause; no data loss if acks=all
```

### Case B: Hard crash (OOM, kernel panic)
```
Flow:
  1. ZooKeeper/KRaft detects session timeout (~30s default)
  2. Controller triggers leader election for affected partitions
  3. ISR follower with highest LEO (Log End Offset) elected
  4. Producer/consumer get NotLeaderException → retry with new metadata
Data loss risk:
  - acks=1: last message may not be on followers → LOST
  - acks=all + min.ISR: no loss, message not ACKed until replicated
Prevention:
  - acks=all + min.insync.replicas=2 (non-negotiable for critical data)
  - Tune session.timeout.ms vs. detection latency tradeoff
```

### Case C: Leader has stale data (unclean leader election)
```
Symptom: Non-ISR broker elected; missing last N messages
When: All ISR members down, unclean.leader.election.enable=true
Risk: Consumers read messages that were overwritten → inconsistency
Prevention:
  - unclean.leader.election.enable=false (default Kafka 1.0+)
  - Accept unavailability over inconsistency (CP over AP)
Recovery:
  - If happened: consumer offsets may point to overwritten range
  - Detect via consumer offset > leader LEO → reset to earliest safe offset
```

### Case D: Slow leader (partial failure)
```
Symptom: Followers fall out of ISR; min.ISR violated; producers block
Detection: ISR shrink alerts, produce latency > SLO
Prevention:
  - JVM GC tuning (G1GC, avoid stop-the-world pauses)
  - Separate network interface for replication vs. client traffic
  - Disk I/O isolation (dedicate disks to Kafka)
Recovery: Auto ISR expansion once follower catches up
```

---

## Layer 3 — Follower Broker Fails

### Case A: Follower crashes
```
Impact: Reduced replication factor; leader continues serving
Risk: If leader also fails → fewer replicas to elect from
Detection: ISR shrink metric
Recovery:
  1. Follower restarts → truncates to leader's checkpoint
  2. Re-fetches missing segments
  3. Re-joins ISR once caught up (replica.lag.time.max.ms)
Prevention: Alert on ISR < replication_factor; replace node quickly
```

### Case B: Follower too slow (replication lag)
```
Symptom: Follower removed from ISR; effective RF drops to 1
Cause: GC pause, disk I/O contention, network congestion
Detection: replica.lag.max.messages or replica.lag.time.max.ms exceeded
Prevention:
  - Monitor replication lag continuously
  - Separate replication and client network paths
  - Right-size broker hardware (NVMe SSD for log storage)
```

---

## Layer 4 — Consumer Fails

### Case A: Consumer crashes before committing offset
```
Symptom: On restart, re-reads already-processed messages
Impact: Duplicate processing
Prevention:
  - Make consumer logic idempotent (upsert, dedup by message key)
  - Use DB transaction + offset commit atomically (outbox pattern)
Recovery: Restart → reprocess from last committed offset → idempotent handler = safe
```

### Case B: Consumer crashes after committing offset
```
Symptom: Messages skipped (at-most-once)
When: enable.auto.commit=true and crash between commit and processing
Prevention: Disable auto-commit; commit only after successful processing
```

### Case C: Consumer lag grows unbounded (slow consumer)
```
Symptom: Consumer can't keep up with produce rate
Causes: Slow downstream DB, CPU-heavy processing, network bottleneck
Detection: consumer_lag metric > threshold
Prevention:
  1. Scale consumers (add instances up to num_partitions)
  2. Parallel processing within consumer (thread pool per partition)
  3. Increase partitions (requires topic recreation or partition reassignment)
  4. Dead Letter Queue (DLQ): skip poison messages, reprocess later
```

### Case D: Poison message (bad message causes consumer crash loop)
```
Flow: Consumer reads msg → crashes → restarts → reads same msg → crash loop
Prevention:
  - Catch exceptions; after N retries → send to DLQ topic
  - DLQ = separate topic for manual inspection
  - Alert on DLQ size
```

### Case E: Consumer group rebalance storm
```
Symptom: Consumers keep joining/leaving → perpetual rebalance → no progress
Cause: Long processing time > session.timeout.ms → coordinator thinks consumer dead
Prevention:
  - Increase session.timeout.ms and heartbeat.interval.ms
  - Use max.poll.interval.ms correctly (time budget per poll)
  - Process asynchronously; commit offsets separately
  - Use Incremental Cooperative Rebalance (avoid stop-the-world)
```

---

## Layer 5 — Coordinator (Group Coordinator Broker) Fails

```
Symptom: Consumer group can't commit offsets or rebalance
Detection: Consumer logs "coordinator not available"
Recovery:
  1. Coordinator is just a broker → new broker elected as coordinator
  2. Consumers retry FindCoordinator request
  3. __consumer_offsets topic remains available (replicated)
Downtime: ~seconds to minutes depending on election speed
Prevention: Ensure __consumer_offsets replication factor ≥ 3
```

---

## Layer 6 — Network Partition

### Case A: Producer isolated from leader
```
Symptom: Producer gets NetworkException → retries with backoff
Recovery: Auto-reconnect; idempotent producer handles safe retry
```

### Case B: Follower isolated from leader (split-brain risk)
```
Symptom: Follower stops replicating → removed from ISR
Recovery: Network heals → follower truncates to leader epoch → re-syncs
Split-brain prevention: Kafka uses leader epoch (monotonic counter)
  → Follower with stale epoch rejected as leader
  → Ensures only one authoritative leader per partition
```

### Case C: Network partition between brokers (full split)
```
With acks=all + min.ISR=2:
  → Minority partition: can't ACK producers → producers block → safe
  → Majority partition: continues serving
  → No split-brain writes
Trade-off: Availability reduced (minority brokers stall)
Kafka choice: Consistency over availability (CP in CAP theorem)
```

---

## Layer 7 — Storage Failure

### Case A: Disk corruption
```
Detection: CRC mismatch on log segment read
Recovery:
  1. Mark segment as corrupt
  2. Truncate to last valid offset
  3. Re-replicate from leader
Prevention:
  - RAID-10 for local durability (not RAID-5, too slow for sequential writes)
  - CRC validation on every message
  - Tiered storage (S3 as cold backup)
```

### Case B: Disk full
```
Symptom: Broker can't append → LogDirNotFoundException for producers
Recovery:
  1. Trigger retention deletion (reduce log.retention.bytes)
  2. Move old segments to tiered storage
  3. Add disk / replace broker
Prevention:
  - Alert at 75% disk usage
  - Tiered storage offloads cold segments automatically
  - auto.leader.rebalance to spread load
```

---

## Layer 8 — Metadata Store Fails (ZooKeeper / KRaft)

### ZooKeeper Failure
```
Impact:
  - No new leader elections
  - No topic creation/deletion
  - Existing producers/consumers continue (metadata cached)
Recovery: ZooKeeper quorum auto-heals; ZK is itself a Raft cluster (3 or 5 nodes)
Prevention: 5-node ZK ensemble (tolerates 2 failures)
Migration path: KRaft mode removes ZooKeeper dependency entirely
```

### KRaft Failure (Kafka 3.x+)
```
KRaft: Built-in Raft among broker-controllers
  - 3 controller nodes (odd number for quorum)
  - Active controller = Raft leader
  - Metadata log replicated like a topic
Failure of 1 controller: auto re-election in ~seconds
Failure of 2 controllers (3-node): read-only (no new elections)
Prevention: 5-node controller quorum for higher tolerance
```

---

## Failure Response Matrix

| Component | Detection | Auto-Recovery | Manual Action | Data Loss Risk |
|---|---|---|---|---|
| Producer crash | App logs / alerts | Retry + dedup | None if idempotent | Low (acks=all) |
| Leader crash | ZK/KRaft heartbeat | ISR election | Replace broker | Zero (acks=all) |
| Follower crash | ISR shrink metric | Re-sync on restart | Replace if persistent | None |
| Consumer crash | Consumer lag alert | Restart + re-read | Idempotent handler | Duplicates only |
| Poison message | Crash loop | DLQ after N retries | Manual DLQ review | None |
| Disk full | Disk usage alert | Tiered offload | Add disk | None if caught early |
| ZK/KRaft down | Controller alerts | Quorum auto-heals | Add node | None |
| Network split | Partition metrics | Majority continues | Fix network | None (CP design) |

---

## Operational Runbook Highlights

```
1. ISR shrink alert:
   → Check broker GC logs, disk I/O, network saturation
   → If persistent: replace broker node

2. Consumer lag spike:
   → Check downstream service latency
   → Scale consumer group (if lag > 1M messages)
   → Check for poison messages (crash loops)

3. Broker disk > 80%:
   → Trigger compaction (kafka-log-dirs.sh)
   → Enable tiered storage or reduce retention

4. Produce latency > SLO:
   → Check ISR count (min.insync.replicas violation?)
   → Check network between producer and broker
   → Check leader GC pauses

5. Rebalance storm:
   → Increase max.poll.interval.ms
   → Switch to Cooperative Sticky assignor
   → Check consumer processing time
```
