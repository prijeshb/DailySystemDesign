# Payment System — Interview Q&A — 2026-05-20

> How interviewers probe this topic. Answer the "why" not just the "what."

---

## Opening Questions

**Q: Design a payment processing system.**

Start here:
> "Before jumping in — are we building a payment gateway like Stripe, or a checkout flow for an e-commerce platform? And what scale — consumer app or enterprise B2B?"

Expected clarifications: scale (TPS), supported methods (card/UPI/wallet), synchronous vs async settlement, regions/compliance.

---

## Core Design Questions

**Q: How do you prevent double charges on retry?**

> Idempotency keys. Client generates a UUID per payment attempt and sends it as a header. Server checks Redis before processing — if key exists, return cached response without re-charging. Key stored with 24h TTL. Lock during processing prevents race conditions.

Follow-up: *What if Redis is down?*
> Fall back to DB query on `idempotency_key` column (indexed). Slower but safe.

---

**Q: How do you handle the case where the gateway charges the user but your response is lost?**

> This is the hardest problem in payments. Three-part answer:
> 1. Use the order's own ID as the gateway reference ID — so we can query the gateway anytime.
> 2. Never trust only the synchronous response. All confirmations come via webhook.
> 3. Reconciliation job every 5 min queries gateway for PROCESSING orders > 3 min old.

Follow-up: *What if the gateway itself doesn't know?*
> That's a gateway bug — escalate to Stripe/Razorpay ops. In the meantime, hold the order in a DISPUTE state with ops alert.

---

**Q: How do you store money amounts?**

> Always as integers (smallest currency unit — paise, cents). Never float or decimal. `10.50 USD = 1050`. Float arithmetic causes rounding errors that compound at scale. Financial regulations require exact arithmetic.

---

**Q: How would you shard the payments database?**

> Shard by `user_id % N`. Benefits: all user history on one shard, no cross-shard joins. Problem: high-volume merchants (like Flipkart) create hot shards. Solution: separate merchant DB sharded by `merchant_id`. Cross-reference via `order_id`.

Follow-up: *How do you rebalance shards when you add capacity?*
> Consistent hashing minimizes data movement. Or: virtual shards (1000 virtual → 10 physical), reassign virtual shards to new physical nodes.

---

**Q: Explain the Saga pattern in the context of payments.**

> Two-phase commit is too slow and has a coordinator SPOF. Saga breaks the distributed transaction into a sequence of local transactions with compensating actions.
> 
> Example: DEBIT user → CREDIT merchant → UPDATE order.
> If step 2 fails: run compensating transaction CREDIT user back (refund).
> 
> Implemented via Kafka events: each step emits an event; next step consumes it.

Follow-up: *What if the compensating transaction also fails?*
> Store failed compensations in a dead-letter queue. Alert ops. Manual intervention for edge cases. System is eventually consistent, not instantly.

---

**Q: How do you design the ledger?**

> Append-only double-entry bookkeeping. Every payment creates two rows: debit from payer, credit to payee. Never update or delete ledger rows — immutable audit trail. Balance is computed as `SUM(credits) - SUM(debits)`. Partition ledger table by month for query performance.

Follow-up: *How do you handle balance queries at scale?*
> Cache running balance in a separate `wallets` table. Update via optimistic locking. Recompute from ledger if cache is stale (reconciliation).

---

**Q: What happens if both your DB write and Kafka publish need to succeed atomically?**

> Outbox pattern. Write both the business record and the outbox event in a single DB transaction. A separate poller reads unpublished outbox events and publishes to Kafka, then marks them published. Guarantees at-least-once delivery. Consumers must be idempotent.

---

## Scaling Questions

**Q: How do you handle 100,000 payment requests per second?**

> - Stateless payment service: horizontal scale behind load balancer
> - Connection pooling: PgBouncer in front of Postgres (prevents connection exhaustion)
> - Read replicas for status checks and history
> - Redis cluster for idempotency checks (sub-ms)
> - Kafka for async decoupling of downstream processing

---

**Q: How do you implement rate limiting per user?**

> Token bucket in Redis. Each user gets N tokens per minute. Each payment consumes one token. Atomic check-and-decrement with Lua script.
> 
> ```
> redis.eval("
>   local current = redis.call('GET', KEYS[1])
>   if current and tonumber(current) >= tonumber(ARGV[1]) then return 0 end
>   redis.call('INCR', KEYS[1])
>   redis.call('EXPIRE', KEYS[1], 60)
>   return 1
> ", 1, 'rate:user:123', 10)
> ```

---

## Trade-Off Questions

**Q: Why Kafka instead of direct DB writes from webhooks?**

| Approach | Pro | Con |
|---|---|---|
| Direct DB write | Simple | Tight coupling, webhook handler must be up |
| Kafka | Decoupled, replayable | Extra infra, eventual consistency |

> At scale, Kafka wins: webhook handler can process bursts, events can be replayed, multiple consumers (ledger, notifications, analytics) subscribe independently.

---

**Q: Synchronous vs asynchronous payment confirmation?**

> Show "payment initiated" immediately (synchronous order creation). Final confirmation via webhook (async). This is how Stripe works — charge is synchronous but webhook confirms. For UPI: synchronous collection request, async debit confirmation.

---

**Q: Why not use distributed transactions (2PC)?**

> 2PC holds locks across all participants until coordinator confirms. If coordinator crashes, locks are held indefinitely. At payment scale (millions of TPS), this kills throughput. Saga with compensation is preferred — eventual consistency with clear rollback path.

---

## Deep-Dive Follow-Ups

**Q: How do you handle partial refunds?**

> New Transaction row with type=REFUND and amount=partial_amount. Two new ledger entries (reverse direction). Original order status → PARTIALLY_REFUNDED. Track `refunded_amount` on the order to prevent over-refunding.

**Q: How do you detect fraudulent payments?**

> Multi-layer: (1) Rule engine: velocity checks (5 payments in 1 min), amount threshold, unusual geo. (2) ML model: features = merchant_category, time_of_day, device_fingerprint, IP risk score. (3) 3DS challenge for high-risk. All async — don't block payment for low-risk, block for high-risk.

**Q: How do you handle currency conversion?**

> Fetch exchange rates from provider (ECB, Wise) every 5 min, cache in Redis. Store both original amount (user's currency) and settlement amount (merchant's currency) on the order. Rates are locked at order creation time.

**Q: How do you ensure PCI DSS compliance?**

> Never store raw card numbers. Tokenize at source (Stripe Elements/RazorpayJS runs in their iframe). We receive only a token. Tokens stored encrypted at rest. Network: card data never touches our servers — only the token.

---

## Common Mistakes to Avoid

- ❌ Using floats for money → use integers (paise/cents)
- ❌ Retrying gateway blindly → query status first
- ❌ Synchronous webhook processing → acknowledge immediately, process async
- ❌ Assuming DB write + Kafka publish are atomic → use outbox pattern
- ❌ Not having reconciliation → single point of truth gaps
- ❌ Caching account balances → always read from DB or versioned wallet table
