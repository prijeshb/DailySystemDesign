# Stock Exchange — Failure Analysis
**Date:** 2026-06-09

---

## Failure-First Thinking

For each component: What if it fails? What if the component before it fails? What if the component after it fails?

---

## 1. Order Gateway Failure

### What fails
- Client can't submit orders
- In-flight orders (received but not sequenced) are lost

### Impact
- Orders in Gateway memory but not yet in WAL → lost
- Orders already in WAL → safe, will be matched on recovery

### Solution
```
1. Multiple Order Gateways (active-active, load balanced)
   - Stateless — each gateway independently validates and forwards to sequencer
   - Client retries to a different gateway on failure

2. Client-side retry with idempotency key
   - Client generates UUID before first attempt
   - Retries carry same UUID
   - Sequencer deduplicates: if order_id already in WAL → return "already accepted"

3. Client timeout + status check
   - If no ACK in 500ms → query Order Status API
   - "Not found" → retry; "OPEN" → don't retry
```

### Gateway Before/After Failures
- **Before (broker client fails):** No order submitted. Market unaffected. Broker client reconnects and resubmits.
- **After (Sequencer unreachable):** Gateway holds order in local buffer, retries with backoff. If Sequencer doesn't recover within SLA → reject order, return error to broker.

---

## 2. Sequencer Failure

### What fails
- No new orders enter the matching engine
- Market effectively halted for affected instruments

### Impact
- Orders in gateway queues pile up
- Market data feed still runs (reflects current order book state)
- No new trades until sequencer recovers

### Solution
```
Primary Sequencer + Hot Standby:

Primary:
  - Assigns seq_no, writes WAL, forwards to Matching Engine
  - Heartbeats to ZooKeeper every 100ms

Standby:
  - Replicates WAL in real-time (synced within 10ms)
  - Watches ZooKeeper for heartbeat
  - If no heartbeat for 500ms → promote self to primary
  - Continues seq_no from last synced position

Recovery gap:
  - Any orders in Primary's in-flight buffer (assigned seq_no, not yet replicated)
  - Solution: Primary does not ACK to gateway until WAL is replicated to standby
  - Cost: ~2ms additional latency (sync replication), eliminates data loss
```

### Sequence Gap Detection
```
Matching Engine tracks last received seq_no.
If it receives seq_no=1005 after 1003 (gap at 1004):
  - Pause processing at 1005
  - Request resend from Sequencer: "send me seq_no=1004"
  - Resume once 1004 received and inserted in order
```

---

## 3. Matching Engine Failure (Most Critical)

### What fails
- In-memory order book lost
- Active orders in book may never be matched
- Open orders appear "in limbo" — submitted but not cancelled or filled

### Impact
- All open orders for affected instruments need recovery
- Without recovery, order state in client systems doesn't match exchange
- **Regulatory issue:** Exchange must reconcile every open order

### Solution: WAL-Based Recovery
```
Recovery procedure:
  1. New Matching Engine instance starts
  2. Reads entire WAL from Sequencer log (or replicated copy)
  3. Replays all orders in seq_no order:
     - PLACE → add to order book
     - CANCEL → remove from order book
     - TRADE already recorded → skip (don't re-execute)
  4. Resulting in-memory order book = exact state before crash

Recovery time:
  WAL for 1 trading day (9:15am - 3:30pm) = ~500K orders × 200 bytes = 100MB
  Replay at 2M orders/sec = ~0.25 seconds
  Sub-second recovery is achievable
```

### Hot Standby Matching Engine
```
Primary ME → processes orders, publishes trades
           → also broadcasts to standby ME (shadow mode)

Standby ME:
  - Receives same order stream
  - Maintains identical order book
  - Does NOT publish trades
  - On primary failure → promote → start publishing

Verification: periodically compare order book checksum between primary and standby
If mismatch → alert, investigate (should never happen if sequence is identical)
```

### Component After Fails (Trade Store unreachable)
```
Matching Engine generates trade but can't write to Trade Store.
Options:
  1. Write trade to local buffer → retry async → at-least-once to Trade Store
  2. Trade Store is downstream — matching can continue; trades buffered in Kafka
     Kafka topic: trades (durable, replicated)
     Trade Store consumes from Kafka → writes to DB → no data loss

Rule: Matching Engine never waits on Trade Store write. Decouple via Kafka.
```

---

## 4. Risk Service Failure

### What fails
- Pre-trade risk checks can't be performed

### Options

| Approach | Behavior | Risk |
|----------|----------|------|
| **Fail-open** | Allow orders through without check | Rogue algo can place unlimited orders, bad fund check |
| **Fail-closed** | Reject all orders | Market halts — severe operational impact |
| **Cached mode** | Use last known risk parameters (stale up to 30s) | Small window of allowing slightly stale limits |

**Decision:**
```
Fail-closed for individual account-level orders.
Rationale: Financial exposure of a rogue order >> cost of rejecting some legitimate orders.
Cache fund checks for 5s (acceptable staleness) — covers momentary blips.
If Risk Service down > 30s → switch to fail-closed, notify brokers.
```

---

## 5. Market Data Feed Failure

### What fails
- Clients see stale prices
- Clients can't see current bid/ask
- HFT algos start trading on stale data

### Solution: Sequence Numbers + Gap Detection
```
Every market data message has a sequence number.

Client-side logic:
  expected_seq = last_received + 1
  if received_seq > expected_seq:
    GAP detected (missed messages)
    → request resend for missing seq range from Feed Handler
    → OR re-subscribe from last known state

Feed Handler maintains a small rolling buffer (last 1000 messages)
for resend requests.
```

### Snapshot + Incremental Architecture
```
On initial connect or after long gap:
  Client requests SNAPSHOT: full current order book depth (top 20 levels)
  Then switches to INCREMENTAL: only changes broadcast

This is the standard approach in all real exchanges (e.g., NASDAQ ITCH/OUCH).
```

### UDP Multicast Loss (HFT tier)
```
HFT co-location clients:
  - Use sequence numbers to detect gaps
  - Maintain their own recovery channel (TCP retransmit server)
  - Gap in < 5ms → retransmit; gap > 5ms → they pause trading

Regular clients (WebSocket tier):
  - Feed Handler buffers and guarantees delivery over TCP
  - No gap problem for them
```

---

## 6. Duplicate Trade Prevention (Critical)

### How duplicates can happen
```
Scenario 1: Network retry
  Order Gateway sends order to Sequencer
  Sequencer processes it but ACK is lost
  Gateway retries with same order_id
  Sequencer receives it again

Scenario 2: Matching Engine replay during recovery
  WAL has: [seq=100: PlaceOrder A], [seq=101: Trade A+B]
  During replay, seq=101 (trade) should NOT re-execute
  Only rebuild order book state, don't re-emit trade events
```

### Deduplication layers
```
Layer 1: Sequencer dedup
  Before writing to WAL: check if order_id already in dedup cache (Redis, 24h TTL)
  Found → return "already accepted" with cached response
  Not found → write to WAL, add to dedup cache

Layer 2: Trade ID generation
  trade_id = hash(buy_order_id + sell_order_id + execution_timestamp_rounded_to_tick)
  Trade Store: INSERT ON CONFLICT (trade_id) DO NOTHING

Layer 3: WAL replay awareness
  Recovery mode flag → Matching Engine does NOT emit trade events
  Only rebuilds order book
  Trade events re-emitted only for trades AFTER recovery checkpoint
```

---

## 7. Flash Crash / Cascade Failure

### What happens
```
Trigger: Large institutional SELL order hits market (e.g., 10M shares of HDFC)
  → Price drops 5% in 2 seconds
  → Stop-loss orders trigger for thousands of accounts
  → Each stop-loss generates a MARKET SELL
  → More price drop → more stop-losses
  → Cascade: price drops 20% in 60 seconds
```

### Real example
- **May 6, 2010:** Dow Jones dropped 1000 points (9%) in minutes. A single mutual fund's algo selling $4.1B of futures triggered HFT cascade.

### Prevention
```
Circuit Breaker (instrument-level):
  ±5%  from open price → 2-minute pause (auto-resume)
  ±10% from prev close → 15-minute halt
  ±20% → halt for rest of day

Market-wide circuit breaker (NSE/BSE):
  Sensex/Nifty drops 10% → halt all trading 45 minutes
  Sensex/Nifty drops 15% → halt all trading for the day

Order velocity check:
  Single account placing > 100 orders/sec → auto-suspend, alert risk team

Price band checks per order:
  Limit order price > 5% away from current market → reject (prevent fat finger)
```

---

## 8. Settlement Service Failure

### What fails
- Trades are recorded but positions/balances not updated
- User sees "trade confirmed" but portfolio doesn't reflect it

### Solution
```
Settlement is decoupled from matching:
  Matching Engine → Kafka topic "trades"
  Settlement Service consumes from Kafka → idempotent processing

Settlement Service down:
  Trades queue in Kafka (durable, replicated)
  When Settlement recovers → processes queue in order
  At-least-once delivery + idempotent settlement (trade_id check) = exactly once semantics

T+2 means:
  Actual transfer of securities and money happens 2 business days later
  Intraday position updates are "soft" (book entries)
  Real transfer failure is extremely rare (covered by clearing house guarantee)
```

---

## Summary: Failure → Impact → Fix

| Component | Failure Impact | Fix |
|-----------|---------------|-----|
| Order Gateway | Orders rejected | Active-active, client retry + idempotency |
| Sequencer | Market halts | Hot standby, sync WAL replication |
| Matching Engine | Order book lost | WAL replay, hot standby ME |
| Risk Service | Unchecked orders | Fail-closed after 30s, cache for blips |
| Market Data Feed | Stale prices | Seq numbers, snapshot+incremental, gap fill |
| Trade Store | Trades not persisted | Kafka buffer, async write, idempotent insert |
| Settlement Service | Positions not updated | Kafka queue, idempotent processing on recovery |
| Full exchange | Flash crash | Circuit breakers, velocity limits, price bands |
