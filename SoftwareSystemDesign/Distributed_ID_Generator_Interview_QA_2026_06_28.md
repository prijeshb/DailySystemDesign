# Distributed ID Generator — Interview Q&A
*Date: 2026-06-28*

---

## How Interviewers Open This Problem

> "Design a unique ID generator for a distributed system."
> "How would you generate unique IDs at scale without a centralized database?"
> "Twitter uses something called Snowflake — how would you design that?"

This starts as a scoping problem. Interviewer wants to see if you ask the right questions first.

---

## Clarifying Questions YOU Should Ask

1. **"What type of IDs — numeric or alphanumeric?"**
   → Numeric 64-bit integers are most common (DB-friendly)

2. **"Does order matter? Strictly sequential or just roughly time-sortable?"**
   → Strictly sequential requires a single writer (kills scale). Roughly sortable (ms precision) is achievable.

3. **"What's the required throughput?"**
   → Determines how many nodes needed.

4. **"Should the ID be decodable — should it embed a timestamp?"**
   → Yes → Snowflake. No → UUID works.

5. **"Any security concern about exposing ID structure?"**
   → Sequential IDs leak volume (competitor can count your IDs). Mitigation: XOR shuffle or per-entity scramble.

---

## Core Questions & Answers

### Q1: Walk me through the bit layout of a Snowflake ID.

**Answer:**
64-bit integer structured as:
```
| 0 | 41 bits: ms timestamp | 10 bits: machine ID | 12 bits: sequence |
```
- Sign bit always 0 (keeps ID positive in Java long / most languages)
- 41-bit timestamp relative to custom epoch → 69 years of IDs
- 10-bit machine ID → 1,024 unique generators
- 12-bit sequence → 4,096 IDs per millisecond per node

**Follow-up:** "Why 41 bits for timestamp and not 32?"
32-bit seconds overflows in 2038 (Unix timestamp problem). 41-bit milliseconds gives 69 years — a practical engineering choice, not a fundamental limit.

---

### Q2: What happens if two nodes get the same machine ID?

**Answer:**
ID collision is possible. Same node ID + same ms + same sequence = identical ID.

Root causes: ops error, ZooKeeper session expiry, container clone.

Mitigation:
- ZooKeeper ephemeral znodes for machine ID lease (auto-released on crash)
- Heartbeat-based lease renewal — if heartbeat fails, node pauses generation
- On startup, assert machine ID uniqueness against a shared registry

Detection (late): unique constraint violation in downstream DB.

**Follow-up:** "Why not just use UUIDs then to avoid this?"
UUID v4 solves collision but at cost of index fragmentation (random write pattern), no embedded timestamp, 128-bit storage. Snowflake is the right tradeoff for 95% of systems.

---

### Q3: What if the system clock goes backward?

**Answer:**
NTP correction can step the clock backward. If we naively use `current_time`, we'd generate a lower timestamp than the last ID → sequence overlap possible.

Three options:
1. **Wait** — spin until `current_ms > last_ms`. Safe, max latency = clock correction (~128ms).
2. **Refuse** — raise exception, circuit-break. Safe, causes brief unavailability.
3. **Use last_ms** — treat correction as same-ms. Safe only if correction < 1ms and sequence not exhausted.

Production choice (Twitter, Discord): wait + alert on-call if skew > 10ms.

**Follow-up:** "What's the maximum clock correction you'd tolerate before alerting?"
Anything > 5ms is unusual and warrants a page. EC2 chrony typically slews (gradual), never steps — so >1ms is a red flag (VM migration or paused instance).

---

### Q4: How do you handle sequence exhaustion?

**Answer:**
Sequence is 12 bits (4,096/ms/node). Exhaustion means >4M req/sec to a single node — practically impossible without saturating all CPUs first.

If it does happen: generator busy-waits for the next millisecond. Max wait: 1ms.

**Follow-up:** "What if you genuinely needed 10M IDs/sec from a single process?"
Switch to µs-resolution timestamps (40 bits for µs = ~34 years), freeing bits for a larger sequence. Or distribute across multiple goroutines with pre-allocated sequence ranges.

---

### Q5: Why not use Redis INCR for distributed IDs?

**Answer:**
Redis INCR is atomic and works — but:
1. **Single point of failure** — Redis down = no IDs
2. **Network hop on every ID** — 1ms latency vs ~10ns for in-memory Snowflake
3. **Not sortable by time** — just a counter, no timestamp embedding
4. **Throughput limit** — Redis single-threaded: ~1M INCR/sec max; Snowflake: 4M/ms/node

Redis INCR is fine for low-volume counters (like Flickr's ticket server for photo IDs). Not for high-throughput distributed systems.

---

### Q6: What's the difference between UUID v4 and UUID v7?

**Answer:**

| Property | UUID v4 | UUID v7 |
|----------|---------|---------|
| Size | 128 bits | 128 bits |
| Structure | Random | 48ms timestamp + random |
| Sort order | Random | Time-sortable |
| Coordination | None | None |
| B-tree perf | Poor (random writes) | Good (monotonic inserts) |
| Standard | RFC 4122 | RFC 9562 (2024) |

UUID v7 is the modern alternative to Snowflake when you don't want to run an ID service. Trade-off: 128-bit (vs 64-bit Snowflake), no machine ID embedded.

**When to use UUID v7 over Snowflake:**
- You want zero infrastructure (no ID service to run)
- You can tolerate 128-bit IDs
- You don't need to decode machine ID from the ID

---

### Q7: How does Instagram generate IDs inside Postgres?

**Answer:**
Instagram uses a `pl/pgsql` function inside each Postgres shard — no separate service.

```sql
-- Simplified Instagram approach
CREATE SEQUENCE insta_id_seq;

CREATE OR REPLACE FUNCTION next_id(OUT result bigint) AS $$
DECLARE
  epoch bigint := 1314220021721;   -- Instagram epoch
  seq_id bigint;
  now_ms bigint;
  shard_id int := 5;               -- hardcoded per shard
BEGIN
  SELECT nextval('insta_id_seq') % 1024 INTO seq_id;
  now_ms := FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000);
  result := (now_ms - epoch) << 23;
  result := result | (shard_id << 10);
  result := result | seq_id;
END;
$$ LANGUAGE plpgsql;
```

**Benefit:** No separate service — ID generation and row insert are in the same transaction.
**Trade-off:** ID generator is coupled to DB shard. DB failure = no IDs for that shard.

---

### Q8: If a node crashes and restarts, could it generate duplicate IDs?

**Answer:**
No, because:
1. Clock has advanced — new `current_ms` > any previously used `last_ms`
2. Sequence resets to 0 — but since the ms is different, no overlap
3. Even if clock is same ms (restart in <1ms): sequence starts at 0, previous IDs had sequences 0–N → only collision if sequence exactly matches a previously generated ID in that ms (extremely unlikely)

**The only real risk:** Machine reuses same machine ID AND clock is exactly at same ms. Probability is ~1 in 4,096 per ms — and only if the clock didn't advance at all during restart. In practice: restart takes >>1ms.

---

### Q9: How would you shard IDs across multiple data centers?

**Answer:**
Split the 10-bit machine ID field into: `5 bits datacenter ID + 5 bits node ID` → 32 DCs × 32 nodes per DC = 1,024 total. This is exactly what Twitter's original Snowflake did.

```
| 0 | 41-bit ts | 5-bit DC | 5-bit node | 12-bit seq |
```

**Follow-up:** "What if a DC goes down — can another DC pick up its IDs?"
No and you don't want it to. Machine IDs are unique per node — a different DC generates IDs with its own machine ID. Clients route to the nearest DC; on failure, failover to secondary DC. All IDs remain globally unique.

---

### Q10: Could an attacker predict your next ID?

**Answer:**
Yes. Snowflake IDs are predictable — timestamp is the current time, machine ID is fixed, sequence increments by 1. An attacker can enumerate IDs to scrape data or enumerate accounts.

Mitigations:
1. **Authorization checks** — never trust "I have the ID" as proof of ownership. Verify ownership server-side.
2. **ID scrambling** — XOR or bijective encryption of the output: `public_id = encrypt(snowflake_id, secret_key)`. Internal storage uses real Snowflake ID; external API returns scrambled ID.
3. **HashIDs / Sqids** — library that encodes integers as short strings with a secret salt.

**Follow-up:** "Does this matter for something like a tweet ID?"
For public content: not really. For private resources (order IDs, invoice IDs): yes, scramble them.

---

## Common Follow-Up Paths

**If you design Snowflake →** Interviewer asks about clock skew, then machine ID conflict, then what if all ID nodes fail.

**If you say UUID →** Interviewer asks about B-tree fragmentation, then you explain UUID v7 or Snowflake.

**If you say auto-increment →** Interviewer asks about sharding, then you explain the step-increment problem.

**If you over-engineer (distributed consensus per ID) →** Interviewer pushes back on latency. Key insight: IDs don't need consensus, they need uniqueness.

---

## Answer Template (60-second verbal answer)

> "For a distributed ID generator, I'd use a Snowflake-style 64-bit integer: 41 bits for millisecond timestamp, 10 bits for machine ID, 12 bits for sequence — giving 4,096 IDs per millisecond per node with no coordination on the hot path. Machine IDs are assigned via ZooKeeper ephemeral znodes at startup. The main failure cases are clock skew — handled by spin-waiting — and machine ID conflicts — handled by ZooKeeper leases with heartbeats. For a startup budget or zero-infra scenario, UUID v7 is a modern alternative: time-sorted, no service needed, but 128-bit instead of 64-bit."
