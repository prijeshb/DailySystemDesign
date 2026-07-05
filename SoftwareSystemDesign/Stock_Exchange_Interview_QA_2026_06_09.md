# Stock Exchange — Interview Q&A
**Date:** 2026-06-09

---

## Opening Questions

**Q: Design a stock exchange / trading system.**

**How to open:**
> "Before diving in — are we designing the full exchange (matching, market data, settlement) or just the order management system from a broker's perspective? And is the scale NSE-level (500K orders/sec) or a smaller crypto exchange?"

This scopes the conversation. Most interviews mean: full exchange, focus on matching engine and market data.

**Recommended opening frame:**
> "The core challenge is: how do you match buy/sell orders fairly, at microsecond latency, with zero duplicate trades? That drives everything — in-memory order book, single-threaded sequencer for total ordering, WAL for durability."

---

## Deep-Dive Questions

**Q: Why can't you use a database for the order book?**

> Postgres INSERT + SELECT takes ~1ms. Peak exchange = 500K orders/sec. That's 500K × 1ms = 500 seconds of DB work per second — impossible.
>
> Order book is pure in-memory. A TreeMap insertion is O(log P) where P = price levels (typically ~100-1000). At 500K/sec that's ~50µs of CPU work — fast enough.
>
> Durability handled separately via WAL. Write-ahead log is append-only, sequential, very fast.

---

**Q: How do you ensure no duplicate trades?**

Three layers:
1. **Client idempotency key** (UUID) → Sequencer deduplicates before writing WAL
2. **WAL recovery is non-emitting** → Matching Engine replays WAL on restart to rebuild state, but doesn't re-emit trade events for already-recorded trades
3. **Trade ID is deterministic** → `hash(buy_order_id + sell_order_id)` → DB INSERT ON CONFLICT does nothing

---

**Q: What is price-time priority? Why not pro-rata?**

> Price-time priority: best price first; ties broken by time (FIFO).
> Everyone knows the rule. Fair and predictable — first order at best price gets filled first.
>
> Pro-rata: ties at same price → quantity filled proportionally to order size.
> Used in some options markets. Favors large orders, discourages small ones.
>
> Price-time is the default in equities (NSE, NYSE, NASDAQ). Pro-rata used on some CME futures contracts where market makers post large passive orders.

---

**Q: How does the sequencer ensure total ordering without being a bottleneck?**

> The sequencer is single-threaded and does only one thing: read from a network buffer, assign an integer, write to an append-only log. No branching, no computation.
>
> At 2M assignments/sec capacity (modern hardware), and NSE peak at ~500K orders/sec, the sequencer has 4× headroom. Single-threaded is not a bottleneck here.
>
> If scale grows: shard by instrument group. Each shard has its own sequencer. Orders for different instruments don't need global ordering relative to each other.

---

**Q: How do you handle partial fills?**

> Incoming BUY for 100 shares, best ask only has 60 shares available:
> - Trade generated for 60 shares
> - Remaining 40 shares stay in order book as an open order with `filled_qty=60, status=PARTIAL`
> - Next matching opportunity fills the remaining 40

> IOC (Immediate or Cancel): if 40 can't fill immediately → cancel the remainder
> FOK (Fill or Kill): if full 100 can't fill at once → cancel the entire order (no partial)

---

**Q: What happens if the matching engine crashes mid-match (between two trades from a multi-fill)?**

> This is where WAL saves us.
>
> Order enters matching engine → might generate 3 partial trades.
> Crash after trade 1, before trade 2.
>
> Recovery:
> - Replay WAL → rebuild order book to exact pre-crash state
> - Re-run matching → generates same trade 1 (idempotent, DB ignores duplicate), then trade 2, then trade 3
>
> Key: recovery is deterministic. Same input (WAL) → same output (trades). Matching is pure function with no external side effects during execution.

---

**Q: How would you scale for 10,000 instruments?**

> Natural partition: one matching engine per instrument (or per instrument group).
>
> ```
> RELIANCE → ME shard 1
> INFY     → ME shard 2
> ...
> ```
>
> No cross-shard coordination needed for single-instrument orders.
> Add more shards as more instruments are listed.
> Each shard has its own sequencer and order book.
>
> Hot instruments (NIFTY50 stocks) may get dedicated hardware. Less active stocks share instances.

---

**Q: How do circuit breakers work? How do you implement them?**

> After every trade execution, the matching engine checks:
> ```
> pct_change = |last_price - reference_price| / reference_price × 100
> if pct_change >= 10%: halt instrument for 15 minutes
> ```
>
> Implementation: a flag `instrument.halted = true` in the order book.
> Any new order for a halted instrument → rejected immediately at Order Gateway (cached halt status).
>
> The halt is also broadcast via market data feed so all clients know.
>
> Resume: either timer-based (automatic after 15 min) or manual admin override.
>
> **Flash crash protection bonus:** Velocity limit — if a single account places > 100 orders/sec, auto-suspend that account's trading. Protects against runaway algos.

---

**Q: Why UDP multicast for market data? Isn't TCP more reliable?**

> For HFT co-location clients: latency is the product. A TCP ACK round trip adds ~50µs. UDP multicast removes that — data is broadcast to all subscribers simultaneously with no per-receiver overhead.
>
> The trade-off: UDP has no delivery guarantee. HFT clients handle this themselves via sequence number gap detection + a separate TCP retransmit channel for filling gaps.
>
> For regular clients (retail, broker apps): Feed Handlers sit between the UDP multicast and clients. Feed Handler subscribes to UDP, buffers, adds reliability, and serves clients over WebSocket/TCP. Retail clients get ~5-50ms latency — acceptable.
>
> **Rule:** Two tiers of market data. Same source, different delivery mechanisms based on latency requirement.

---

**Q: How do you prevent wash trading (person buying from themselves to manipulate price)?**

> This is mostly a compliance / surveillance layer, not core exchange architecture.
>
> At matching time: check if `buy_order.account_id == sell_order.account_id`. If yes → don't match, or flag as suspicious.
>
> For sophisticated wash trading across accounts (same beneficial owner, different accounts): post-trade surveillance. SEBI requires brokers to report suspicious patterns. Exchange runs analytics on trade data to detect correlated accounts.
>
> Interview answer: mention it as a compliance overlay, not the matching engine's job. Matching engine is a market mechanism — surveillance is a separate system reading from the trade stream.

---

## Follow-Up / Curveball Questions

**Q: What is T+2 settlement? Why not T+0?**

> T+2: trade executed today, actual transfer of shares and money happens 2 business days later.
> Why: legacy banking clearing infrastructure, gives time to verify net obligations before transfer.
>
> Modern crypto exchanges are T+0 (atomic swap). NSE moving to T+1 (implemented 2023). T+0 for equities requires real-time fund and share verification — harder with legacy systems.

---

**Q: What's the difference between a limit order and a market order? Which is harder to implement?**

> Limit order: "Buy 100 RELIANCE at max ₹2800." Won't execute above ₹2800.
> Market order: "Buy 100 RELIANCE at whatever price is available now."
>
> Market orders are deceptively tricky:
> - What if the order book is thin? Market order eats through multiple price levels (slippage).
> - What if the book is empty? Market order has no price to fill against — must queue or reject.
> - Exchange risk: market order during a fast market can fill at extreme prices (fat finger protection: reject market orders beyond ±5% of last price).

---

**Q: How does your design differ for a crypto exchange vs a stock exchange?**

| | Stock Exchange | Crypto Exchange |
|--|---|---|
| Settlement | T+2, clearing house | T+0, atomic (blockchain or internal ledger) |
| Hours | 9:15am - 3:30pm | 24/7 |
| Instruments | ~5000 stocks | 1000s of trading pairs |
| Regulation | SEBI/SEC strict | Varies by jurisdiction |
| Circuit breakers | Mandatory | Optional (Binance has them, some don't) |
| Short selling | Regulated, margin | Perpetual futures widely used |

> Core matching engine architecture is identical. Differences are in settlement, risk, and regulatory layers.

---

**Q: What monitoring would you add?**

Critical metrics to alert on:
```
- Order-to-ACK latency P99 > 10ms → alert
- Sequencer WAL replication lag > 50ms → alert  
- Order book depth < 5 levels → market thinning warning
- Circuit breaker triggered → immediate alert + human review
- Account order velocity > 90 orders/sec → near-limit warning
- Trade store Kafka lag > 1000 messages → settlement delay risk
- Matching Engine CPU > 80% → capacity warning
```

---

## Quick-Fire Answers

**Q: What data structure for the order book?**
TreeMap<Price, Deque<Order>> for each side + HashMap<OrderId, Order> for O(1) cancel.

**Q: Why Kafka between Matching Engine and Trade Store?**
Decouples them. ME never waits on DB write. Trades durable in Kafka even if Trade Store is down.

**Q: How do you handle daylight saving time for market open/close?**
Exchange operates in local time (IST/ET). Schedule jobs use local timezone — cron accounts for DST. Rare but a real production issue.

**Q: What's the LMAX Disruptor?**
Ring buffer pattern for inter-thread messaging without locks. Used in LMAX Exchange's matching engine. 6M orders/sec, single-threaded, cache-friendly. Real-world proof that single-threaded + WAL beats concurrent DB writes for this use case.

**Q: How to handle an instrument that starts trading (IPO)?**
Pre-open session: collect all orders for 15 minutes, then single price discovery auction to find equilibrium opening price. All orders at or better than that price fill at the single clearing price. Then regular continuous matching begins.
