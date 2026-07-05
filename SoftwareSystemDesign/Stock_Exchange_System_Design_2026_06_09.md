# Stock Exchange — Trading System
**Date:** 2026-06-09  
**Difficulty:** Hard  
**Real-world refs:** NSE, BSE, NYSE, Binance, Zerodha (Kite)

---

## 0. First Principles — Do We Even Need This?

**The problem:** Millions of buyers and sellers want to trade the same stock. How do you match them fairly, accurately, and fast — without missing a trade or executing one twice?

**Why not just use a regular database?**
- A DB serializes writes through disk I/O. Order matching at peak = 500K orders/sec on a single instrument (AAPL on earnings day). A Postgres `UPDATE` takes ~1ms → max 1000 ops/sec. Not even close.
- The order book is a *pure in-memory* data structure. Matching is CPU-bound, not I/O-bound.

**Why is ordering so critical?**
- Two orders arrive 1µs apart. Which one matches first changes who gets the trade, at what price. With money, this is legally binding. Total ordering is non-negotiable.

**Why a "hold" (reserve) isn't enough here?**
- Unlike ticketing, trades are reversible (cancel), time-based (GTC, IOC), and partially fillable. The state machine is richer.

**Core constraints:**
1. **Zero duplicate trades** — idempotency at every layer
2. **Total ordering** — global sequence number assigned before matching
3. **Durability before execution** — WAL written before order enters matching engine
4. **Financial accuracy** — no approximation, no eventual consistency for positions

---

## 1. Scope & Requirements

### Functional
- Place order (limit, market, stop)
- Cancel / modify order
- Order matching (price-time priority)
- Real-time market data: last traded price, bid/ask, depth
- Trade confirmation to broker
- Portfolio positions and account balance

### Non-Functional
- **Throughput:** 500K orders/sec per instrument at peak
- **Latency:** Order-to-confirmation < 1ms (matching engine internal), < 10ms end-to-end
- **Consistency:** Strong — no double fills, no missed trades
- **Availability:** 99.99% during market hours; planned downtime overnight for reconciliation
- **Auditability:** Every order, every state change, every trade → immutable log

### Out of Scope
- Margin lending / derivatives clearing
- Regulatory reporting (FIX protocol specifics)
- HFT co-location specifics
- Retail brokerage UI

---

## 2. Entities

```
Instrument
  symbol          string       e.g. "RELIANCE"
  isin            string
  lot_size        int          minimum tradeable quantity
  tick_size       decimal      minimum price increment (e.g. ₹0.05)
  circuit_limits  {upper, lower}  ± % beyond which trading halts

Order
  order_id        UUID         client-generated (idempotency key)
  seq_no          uint64       assigned by sequencer (global, monotonic)
  broker_id       UUID
  account_id      UUID
  instrument      string
  side            enum(BUY, SELL)
  type            enum(LIMIT, MARKET, STOP_LIMIT)
  price           decimal      null for MARKET
  quantity        int
  filled_qty      int          starts 0, incremented on partial fills
  status          enum(OPEN, PARTIAL, FILLED, CANCELLED, REJECTED)
  time_in_force   enum(DAY, GTC, IOC, FOK)
  created_at      timestamp
  updated_at      timestamp

Trade
  trade_id        UUID
  buy_order_id    UUID
  sell_order_id   UUID
  instrument      string
  price           decimal      execution price
  quantity        int
  executed_at     timestamp

Account
  account_id      UUID
  broker_id       UUID
  cash_balance    decimal      available for buying
  margin_blocked  decimal      reserved for open orders

Position
  account_id      UUID
  instrument      string
  quantity        int          net holding
  avg_price       decimal      cost basis
```

---

## 3. Actions & Data Flow

```
1. PLACE ORDER
   Client → Broker System → Order Gateway → [Risk Check] → Sequencer → Matching Engine
                                                                              ↓
                                                                     Trade Event
                                                                    ↙          ↘
                                                          Settlement Svc    Market Data Svc
                                                               ↓                   ↓
                                                     Update Position/Balance   Broadcast to all clients

2. CANCEL ORDER
   Client → Broker → Order Gateway → Sequencer → Matching Engine (remove from book)

3. MARKET DATA SUBSCRIPTION
   Client → Market Data Gateway → Subscribe to instrument feed
   Matching Engine → publishes on every trade/quote change
```

---

## 4. High-Level Design

```
                    ┌──────────────┐
  Broker Client ──► │ Order Gateway│ (FIX / REST / WebSocket)
                    │  - Auth      │
                    │  - Validate  │
                    │  - Dedupe    │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ Risk Service │ ← Pre-trade: funds check, position limits
                    └──────┬───────┘
                           │ (approved order)
                    ┌──────▼───────┐
                    │  Sequencer   │ ← Assigns global seq_no, writes WAL
                    └──────┬───────┘
                           │ (sequenced order)
                    ┌──────▼────────────┐
                    │  Matching Engine  │ ← In-memory order book per instrument
                    │  (single-threaded │   Price-time priority matching
                    │   per instrument) │
                    └──┬──────────┬─────┘
                       │          │
          ┌────────────▼──┐  ┌────▼───────────────┐
          │ Trade Store   │  │  Market Data Feed   │
          │ (Postgres/    │  │  (UDP multicast to  │
          │  ClickHouse)  │  │   feed handlers)    │
          └───────────────┘  └────────────────────┘
                ↓
        Settlement Service
        (T+2 netting, position updates)
```

---

## 5. Low-Level Design

### 5.1 Order Book Structure

```python
# Per instrument, in memory
class OrderBook:
    bids: SortedDict[price → deque[Order]]  # descending price (best bid = highest)
    asks: SortedDict[price → deque[Order]]  # ascending price (best ask = lowest)

# SortedDict from sortedcontainers (Python) or TreeMap (Java)
# deque per price level → FIFO within same price (time priority)
```

**Why not a heap?**
- Heap gives O(log N) for top but O(N) for cancel (need to find by order_id)
- TreeMap + HashMap(order_id → Order) gives O(log P) top + O(1) cancel by id
  - P = number of distinct price levels (typically << total orders)

**Memory estimate:**
```
100K open orders per instrument × 200 bytes/order = 20MB per instrument
500 instruments × 20MB = 10GB — fits in one server's RAM
```

### 5.2 Matching Algorithm (Price-Time Priority)

```
Incoming BUY order (price=150, qty=100):

Ask side:
  145 → [Order_A:50, Order_B:30]   ← best ask
  147 → [Order_C:100]
  150 → [Order_D:200]

Step 1: best_ask = 145 ≤ buy_price 150? YES → match
  Fill with Order_A:50  → Trade(qty=50, price=145)   [buy remains: 50]
  Fill with Order_B:30  → Trade(qty=30, price=145)   [buy remains: 20]
  145 level exhausted, remove

Step 2: next best_ask = 147 ≤ 150? YES
  Fill with Order_C: 20 → Trade(qty=20, price=147)   [buy fully filled]

Result: 3 partial trades, all at ask prices (price improvement for buyer)
```

**Key rules:**
- BUY fills at ask price (seller's limit price) — buyer gets price improvement
- SELL fills at bid price — seller gets price improvement
- MARKET order = fill at whatever is available, any price
- IOC (Immediate or Cancel) = fill what matches now, cancel the rest
- FOK (Fill or Kill) = fill entire quantity or cancel entirely

### 5.3 Sequencer — Total Ordering

```
Problem: Two order gateways receive orders simultaneously.
         Without global ordering, matching engine sees non-deterministic input.

Solution: Single-threaded sequencer

Order Gateway 1 ──┐
Order Gateway 2 ──┼──► Sequencer (single process)
Order Gateway 3 ──┘        │
                           │ Assigns monotonically increasing seq_no
                           │ Writes to WAL: (seq_no, order)
                           ↓
                    Matching Engine reads WAL in-order

If Sequencer restarts: replay WAL from last seq_no → perfect recovery
```

**Why single-threaded sequencer isn't a bottleneck:**
```
Pure in-memory operation: read from network buffer, assign int, write to log
Throughput: ~2M seq assignments/sec on modern hardware
Peak exchange load: ~500K orders/sec → sequencer has 4× headroom
```

**WAL format (append-only, binary):**
```
[seq_no: uint64][timestamp: uint64][order_length: uint32][order: bytes]
```

### 5.4 Write-Ahead Log (WAL)

```
Rule: Order is durably written to WAL BEFORE it enters the matching engine.

Timeline:
  t=0ms: Order received at sequencer
  t=0.1ms: Written to WAL (fsync)
  t=0.2ms: Order handed to matching engine
  t=0.5ms: Match executes, trade generated
  t=0.6ms: Trade written to Trade Store
  t=1ms: Acknowledgment sent to broker

If crash at t=0.4ms (after WAL, before trade):
  Recovery: replay WAL → re-run matching from last checkpoint → no lost orders
If crash at t=0.05ms (before WAL write):
  Order treated as never received → broker retries with same order_id → deduped
```

### 5.5 Pre-Trade Risk Check

Before sequencing, verify:
```
BUY order:
  cash_available >= price × quantity         — funds check
  position + quantity <= max_position_limit  — concentration limit

SELL order:
  holdings[instrument] >= quantity           — short sell check (unless margin enabled)
  
Global:
  order_rate[account] < 100 orders/sec       — prevent spam / runaway algo
```

**Trade-off:** Risk check adds ~0.1ms latency but prevents bad trades that would need costly reversal.

### 5.6 Market Data Feed

```
Two tiers:

Tier 1 — Ultra-low latency (co-located HFTs):
  Matching Engine → UDP multicast (no TCP handshake, no ACK)
  ~50 microseconds from trade execution to HFT receipt
  No guarantee of delivery — HFTs handle gaps themselves

Tier 2 — Regular clients (Zerodha Kite, broker apps):
  Feed Handler (subscribes to UDP, buffers, adds seq_no)
       ↓
  WebSocket / SSE push to client
  ~5-50ms latency
  Guaranteed delivery (TCP), gaps filled by re-request
```

**Market data payload (per trade):**
```json
{
  "type": "trade",
  "seq": 1000453,
  "symbol": "RELIANCE",
  "price": 2847.50,
  "qty": 200,
  "side": "BUY",
  "timestamp": 1749432000123456
}
```

### 5.7 Circuit Breaker (Market Halt)

```
Rule: If instrument price moves > X% from reference price in Y minutes → halt trading

Example (NSE rules):
  ±10% from previous close → 15-minute halt
  ±15% → 45-minute halt
  ±20% → trading suspended for the day

Implementation:
  Matching Engine, after each trade:
    current_price = last_trade_price
    reference = previous_close OR open_price
    pct_move = (current_price - reference) / reference × 100
    IF abs(pct_move) >= CIRCUIT_LIMIT:
      instrument.status = HALTED
      reject all new orders for this instrument
      broadcast halt notification via market data

  After halt duration:
    manual admin unlock OR automatic resume
```

**Why circuit breakers exist:**
- Flash crash 2010: Dow dropped 1000 points in minutes due to HFT cascade
- Fat finger error: Trader enters 1B shares instead of 1M → price collapses
- Market halt gives time to detect and correct algo errors before catastrophic loss

---

## 6. Sharding Strategy

**Per-instrument sharding** (natural partition key):

```
instrument → shard (consistent hashing by symbol)

RELIANCE → Shard 1 (Matching Engine instance 1)
INFY     → Shard 2 (Matching Engine instance 2)
TCS      → Shard 3 (Matching Engine instance 3)

Single instrument = single shard = single matching engine = no cross-shard coordination
```

**Cross-instrument orders (index arbitrage)?**
- Out of scope for most interviews
- Real answer: atomic cross-instrument locking via 2-phase commit, or disallow them

---

## 7. Trade-offs Summary

| Decision | Choice | Trade-off |
|----------|--------|-----------|
| Order book storage | In-memory (RAM) | Fast matching but state lost on crash → WAL for recovery |
| Sequencer | Single-threaded | Total ordering but single point of failure → hot standby |
| Market data delivery | UDP multicast for HFT | Zero latency but no delivery guarantee → HFT handles gaps |
| Matching | Price-time priority | Fair, predictable but doesn't maximize fill rate like pro-rata |
| Risk check | Synchronous pre-trade | Prevents bad trades but adds ~0.1ms latency per order |
| Settlement | T+2 batch netting | Reduces capital requirement but delays final transfer |
| Persistence | WAL → async DB sync | Durable + fast; briefly inconsistent if crash before DB sync |

---

## 8. Real-World References

- **NSE (India):** Uses NEAT system. Co-location facility allows HFTs to be within 300m of matching engine.
- **NYSE / NASDAQ:** FIX protocol for order submission. ITCH protocol for market data (UDP, 5M msgs/sec).
- **Binance:** Centralized crypto exchange. Similar architecture. Processes 1.4M orders/sec peak.
- **Zerodha Kite:** Retail broker. Connects to NSE via broker API. Their latency ~5-10ms (NSE's own < 0.5ms).
- **LMAX Exchange:** Published their "Disruptor" ring buffer pattern — single-threaded matching engine, lock-free, 6M orders/sec.
