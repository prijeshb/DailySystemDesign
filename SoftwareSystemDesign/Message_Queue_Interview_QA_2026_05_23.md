# Distributed Message Queue — Interview Q&A
**Date:** 2026-05-23 | Companion to: Message_Queue_System_Design_2026_05_23.md

---

## Opening Questions (Scoping)

**Q: Design a distributed message queue.**
> Start here — don't jump to Kafka internals. Scope first:
> "Before I design this, let me clarify a few things. Are we building a pub-sub system or a point-to-point queue? Do consumers need ordering guarantees? What's the expected message size and throughput? Do we need exactly-once delivery? What retention window?"
> 
> Then: "Based on typical requirements, I'll assume: pub-sub model, at-least-once delivery, messages up to 1MB, ~1M msgs/sec write throughput, 7-day retention."

**Q: Why not just use a database as a message queue?**
> "A DB polling approach works at small scale but falls apart because:
> 1. Polling adds latency and unnecessary load
> 2. DB isn't optimized for sequential log appends — B-tree indexes slow down
> 3. No fanout — multiple consumers means multiple reads of same row
> 4. Retention management is painful
> A message queue's sequential log structure gives us 10-100x better throughput."

---

## Core Concept Questions

**Q: How does Kafka achieve such high throughput?**
> Three mechanisms working together:
> 1. **Sequential writes** — always appends to end of log; HDD sequential write ≈ 600MB/s vs. random ≈ 100 MB/s
> 2. **Zero-copy** — `sendfile()` syscall sends data from page cache directly to NIC, bypassing userspace
> 3. **Batching + compression** — producer batches messages; snappy/lz4 cuts network payload 4x
> Together: single broker can handle 600MB/s+ sustained.

**Q: Explain partitioning. Why not one big log?**
> "One partition = one leader = one thread appending. At 1M msg/s, that's a single bottleneck.
> Partitioning = horizontal scaling unit. Each partition has its own leader on a different broker. 20 partitions across 20 brokers = 20x parallelism.
> Trade-off: ordering is only guaranteed within a partition. If you need order (e.g., all events for user X in order), use key-based partitioning so all user X events go to the same partition."

**Q: What's the difference between at-most-once, at-least-once, exactly-once?**
> - **At-most-once:** commit before processing. Crash after commit = message skipped. Good for metrics where losing one datapoint is fine.
> - **At-least-once:** commit after processing. Crash before commit = re-read = duplicate. Requires idempotent consumers.
> - **Exactly-once:** Kafka achieves this via transactions (transactional.id + read_committed isolation). Writes to output topic and commits offset atomically. Performance cost ~10-20%.
> "In practice, exactly-once end-to-end requires both Kafka transactions AND idempotent downstream (e.g., database upserts)."

**Q: What is the ISR (In-Sync Replica) set?**
> "ISR is the set of replicas that are fully caught up with the leader (within replica.lag.time.max.ms). The producer only waits for ISR members to ACK when acks=all — not all replicas. This gives durability without waiting for slow/dead followers. If a follower falls behind, it's removed from ISR until it catches up."

**Q: What happens when a leader fails?**
> 1. ZooKeeper/KRaft detects leader gone (session timeout)
> 2. Controller picks the ISR member with highest LEO (Log End Offset) as new leader
> 3. New leader advertises itself via metadata
> 4. Producers/consumers get NotLeaderException → metadata refresh → retry to new leader
> "If acks=all was used, no data loss — the elected ISR member had all committed messages."

---

## Deep-Dive / Follow-up Questions

**Q: How do you prevent duplicate messages from reaching consumers?**
> Two layers:
> 1. **Producer side:** enable.idempotence=true — broker deduplicates by PID + sequence number
> 2. **Consumer side:** idempotent processing — use message key as dedup ID; upsert instead of insert; store processed IDs in Redis with TTL
> "True exactly-once requires both. In most systems, at-least-once + idempotent consumers is the pragmatic choice."

**Q: How would you handle a poison message (message that always crashes the consumer)?**
> "Consumer crash-loops on the same offset. Fix:
> 1. Catch all exceptions in consumer poll loop
> 2. Track retry count per message (in-memory or external counter)
> 3. After N retries, publish to DLQ (Dead Letter Queue) topic
> 4. Continue processing next message
> 5. Alert ops team; human reviews DLQ
> The DLQ is just another Kafka topic. You can replay it after fixing the bug."

**Q: How does consumer group rebalancing work, and what are its problems?**
> "Standard rebalance is stop-the-world: all consumers in a group stop, coordinator re-assigns all partitions, everyone resumes. During rebalance, no messages processed — causes latency spikes.
> Trigger: consumer joins/leaves, heartbeat missed (crash), new partitions added.
> Solution: Incremental Cooperative Rebalance (Kafka 2.4+) — only partitions that need to move are revoked; others continue. Dramatically reduces pause time."

**Q: How would you scale consumers if they can't keep up?**
> "Consumer lag = consumers slower than producers. Options in order:
> 1. **Scale out:** add consumer instances to the group (up to num_partitions)
> 2. **Add partitions:** lets you add more consumers (requires reassignment)
> 3. **Optimize consumer:** batch DB writes, async processing, faster downstream
> 4. **DLQ for slow messages:** skip expensive messages, process separately
> If lag is structural, increase partitions. Key insight: you can't have more consumer instances than partitions — extras sit idle."

**Q: How do you implement message ordering in a distributed queue?**
> "Global ordering is incompatible with scale — it requires one partition. Instead:
> - Use **key-based partitioning**: all messages with same key go to same partition → ordered per key
> - Example: all events for order_id=123 go to partition (hash(123) % N)
> - This gives per-entity ordering at scale
> - If you truly need global ordering: single partition, accept throughput limit, or use a different system (e.g., a log-structured DB)"

**Q: Explain log compaction. When would you use it?**
> "Instead of deleting old messages by time/size, compaction keeps only the latest message per key. Old values are garbage-collected.
> Use case: changelog topic for a distributed state store. Key = entity ID, value = latest state. Consumers can reconstruct current state by replaying from beginning.
> Example: user profile updates. After compaction, only the most recent profile per user_id remains.
> Trade-off: compaction is async and background — there's a window where duplicate keys exist in uncompacted segments."

**Q: How does tiered storage work and why is it important?**
> "Brokers have expensive NVMe SSDs — you want hot data on disk, cold data on S3.
> Tiered storage: broker uploads old segments to S3 automatically. Consumers fetching old data → broker proxies from S3. Broker local disk only holds recent data.
> This decouples compute (brokers) from storage (S3) — you don't need huge disks to keep 90 days of data. Cost reduction: NVMe at $X/GB vs. S3 at $X/50GB."

---

## Architecture / Design Questions

**Q: How would you design a Kafka cluster for 1M messages/sec?**
> Capacity math:
> - 1M msgs × 1KB = 1 GB/s write; × 3 replicas = 3 GB/s disk
> - Single broker NVMe: ~1-2 GB/s; need ~3+ brokers for writes alone
> - Add headroom: 10-15 brokers for throughput, more for retention
> - Partitions: 1M/50K per partition = 20 partitions minimum; 100 for headroom
> - Replication: RF=3, min.ISR=2
> - ZooKeeper/KRaft: 3-5 controller nodes

**Q: How would you monitor a Kafka cluster in production?**
> Critical metrics:
> 1. `consumer_lag` per group per partition — primary SLO metric
> 2. `under_replicated_partitions` — ISR shrink signal
> 3. `offline_partitions_count` — should always be 0
> 4. Produce/fetch latency (p99)
> 5. Broker disk usage %
> 6. Request queue size (broker overload)
> Alerting: consumer_lag > X → page on-call; under_replicated > 0 for 5 min → page.

**Q: How would you handle a Kafka broker running out of disk?**
> Immediate:
> 1. Trigger log deletion (kafka-log-dirs.sh --describe → identify large topics)
> 2. Reduce retention for non-critical topics temporarily
> 3. Move partition leadership off this broker (kafka-preferred-replica-election)
> Medium-term:
> 4. Enable tiered storage (S3 offload)
> 5. Add broker nodes and rebalance partitions
> Prevention: Alert at 75%; auto-tiered storage; capacity planning per topic.

---

## Behavioral / Tradeoff Questions

**Q: Kafka vs. RabbitMQ — when would you choose each?**
> | | Kafka | RabbitMQ |
> |---|---|---|
> | Model | Log (pull) | Queue (push) |
> | Retention | Days/weeks | Until consumed |
> | Throughput | Millions/sec | Thousands/sec |
> | Replay | Yes (seek to offset) | No |
> | Routing | Topic + partition | Flexible (exchanges, bindings) |
> | Ordering | Per partition | Per queue |
> 
> "Choose Kafka for event streaming, high throughput, replay needed. Choose RabbitMQ for task queues, complex routing, low message count, immediate deletion after consume."

**Q: If you had to pick between losing a message or delivering it twice, which would you choose?**
> "Almost always deliver twice — make consumers idempotent.
> Losing a message means a user's order wasn't processed, a payment failed silently, a notification never sent. Duplicates are recoverable if you design for it. Data loss usually isn't.
> Exception: metrics/telemetry — losing one datapoint is acceptable; simplicity wins."

---

## Scenario / Curveball Questions

**Q: Your consumer lag is growing but you've already maxed out consumer parallelism (consumers = partitions). What do you do?**
> "Adding consumers won't help — at saturation. Options:
> 1. **Optimize consumer:** profile what's slow — DB writes? Network calls? Add batching/caching.
> 2. **Increase partitions:** allows adding more consumers. Note: partition count can only increase, never decrease. Plan ahead.
> 3. **Fan out to worker pool:** one consumer → pushes to in-process thread pool → parallel processing per partition.
> 4. **Schema evolution:** are messages bigger than expected? Compression issue?"

**Q: You need to replay all messages from the last 7 days for a new service. How?**
> "This is Kafka's killer feature. New consumer group with earliest offset:
> ```
> --group new-service --reset-offsets --to-earliest --execute
> ```
> New group has no committed offsets → starts from beginning of retention window.
> The existing consumers are unaffected — each group tracks its own offsets independently."

**Q: A developer accidentally published sensitive PII to a Kafka topic. How do you remove it?**
> "Kafka is an append-only log — you can't delete individual messages directly. Options:
> 1. **Log compaction + tombstone:** if topic is compacted, publish null value for that key → compaction removes it eventually (not immediate)
> 2. **kafka-delete-records:** truncate up to a specific offset (nukes everything before it — nuclear option)
> 3. **Encryption at rest + key rotation:** encrypt messages with per-user key; delete key = messages unreadable
> Long-term: encrypt PII fields at write time; never publish raw PII to shared topics."
