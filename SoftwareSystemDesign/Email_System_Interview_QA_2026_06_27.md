# Email System — Interview Q&A
*Date: 2026-06-27*

---

## How Interviewers Open This Problem

> "Design Gmail." (broad open-ended)

> "Design an email service for 200 million users."

> "How would you build the backend for an email system that supports web and mobile clients?"

> "Our email platform is hitting storage limits and search is slow — how would you redesign it?"

---

## Round 1: Clarifying Questions (Ask These First)

**Q (You): What clients need to be supported — just web, or also native IMAP clients like Outlook?**
Why it matters: IMAP support requires a stateful protocol proxy layer. If web-only, you can use REST API only — much simpler.

**Q (You): Do we need to interoperate with external email (the internet), or is this an internal company email system?**
Why it matters: External email requires SMTP gateway, SPF/DKIM/DMARC authentication, IP reputation management. Internal only: skip all of that, much simpler.

**Q (You): What's the retention requirement?**
Why it matters: Drives storage tiering architecture and cost. 1 year vs 7 years = dramatically different storage spend.

**Q (You): What's the expected scale — DAU, emails per user per day?**
Why it matters: 10M DAU is SQS-able. 200M DAU requires Kafka + Cassandra.

**Q (You): Is full-text search a requirement?**
Why it matters: Full-text search requires Elasticsearch, a whole separate indexing pipeline. If only metadata search (sender, date, subject), you can get away with Cassandra queries.

---

## Round 2: Core Design Questions

**Q: Walk me through what happens when I click Send.**

A:
1. Client makes POST /emails/send with body, recipients, optional attachment IDs (already uploaded)
2. API validates sender rate limits (Redis sliding window: 500/hr)
3. Email body stored in S3 at `emails/bodies/{SHA256(body)}` — content-addressed dedup
4. Metadata written to Cassandra: email_id, thread_id, sender, recipients, body_ref
5. Event published to Kafka topic `email.outbound`
6. SMTP relay cluster consumes from Kafka → DNS MX lookup for recipient domain → SMTP delivery
7. If same-domain recipient: delivery worker writes directly to recipient's Cassandra partition (skip external SMTP)
8. Delivery receipt (success or bounce) written back to Cassandra, user notified

Key to say: "The user gets a 200 OK after step 4 (durable write). SMTP delivery is async — it's guaranteed to be attempted but not guaranteed to land instantly."

---

**Q: How do you group emails into threads?**

A: Three-pass algorithm:
1. Check `In-Reply-To` header → look up that message_id → return its thread_id
2. Check `References` header (list of ancestor message IDs) → same lookup chain
3. Subject normalization as last resort: "Re: Meeting" → "meeting", search for thread with same normalized subject from same participant group within 7 days

Most interviewers are happy with step 1 and 2. Mention step 3 shows depth. Important caveat: subject-only matching is unreliable — two unrelated people can have same subject. Scope it to same participants + same week.

---

**Q: How does search work at scale?**

A: Two-tier:
- Elasticsearch: full-text search on subject, body snippet, sender, labels. Only last 6 months in hot index (cost constraint).
- Cassandra: metadata search beyond 6 months (sender, date range, folder) — no full-text

Elasticsearch index is per-user-shard (user_id used as routing key). Every search query MUST include `user_id` as mandatory filter — non-negotiable for privacy and performance.

Indexing pipeline: Delivery Worker writes email → publishes `email.indexed` event → Indexer consumer → Elasticsearch bulk API (batch 1000 docs/request, 500ms flush interval).

Lag acceptable: search index is eventually consistent, 1–5s after delivery.

---

**Q: How do you handle spam?**

A: Staged pipeline to control cost:
1. **Reputation check** (sync, in MX gateway): SPF/DKIM/DMARC + IP blocklist. Reject score > 90. Cost: ~1ms.
2. **Bayesian classifier** (async, delivery worker): pre-computed word probability lookup. Score 0–100. Cost: ~10ms.
3. **Neural classifier** (async, only for borderline score 30–70): BERT-based, fine-tuned on spam corpus. Cost: ~50ms.

Why staged? At 350K emails/sec, running neural on everything = ~$500K/month in GPU compute. Staged approach: neural touches only ~5% of traffic.

Feedback loop: user marks email as spam → training signal added to per-user Bayesian model + global model.

---

**Q: What database would you use for email storage, and why not PostgreSQL?**

A: Cassandra, not Postgres.

**Why Cassandra wins here:**
- Write pattern: every inbound email creates N rows (one per recipient, one per folder label) — purely append-only, no joins, no transactions across rows
- Read pattern: always (user_id, folder, timestamp DESC) — this is a perfect Cassandra partition key
- Scale: 10B rows/day, each node handles 100K writes/sec — need 100+ Postgres instances to match
- TTL built-in: set TTL per row → auto-expires old emails → no explicit delete storms

**Why Postgres fails here:**
- A single `user_emails` table at this scale requires hundreds of shards — you're reimplementing Cassandra
- B-tree indexes on high-write tables (10B rows/day) → constant index maintenance, write amplification
- JOINs aren't needed here (every email is self-contained per recipient)

**Where Postgres wins:** User accounts table, billing, quota enforcement — structured data with constraints. Keep both in the system.

---

**Q: How do you prevent a single large email blast from taking down the system?**

A: Async fan-out via Kafka.

When an email arrives to a group alias with 1M members:
- Delivery worker doesn't write 1M rows synchronously
- Writes ONE email body to S3, ONE metadata record to a "pending delivery" queue in Kafka
- Fan-out workers (horizontal, 50+ instances) each handle a range of recipients
- Each worker writes its batch of inbox records to Cassandra
- Total time: 1M writes / (50 workers × 50K writes/sec each) = ~0.4 seconds

The MX gateway returns 250 OK to sender within milliseconds. Fan-out is entirely async and horizontally scalable.

**Follow-up Q: What about a user opening their inbox during fan-out?**
Their email may not appear yet. This is acceptable — it will appear within seconds. Show a banner: "Your inbox is syncing..." if no emails returned for a known thread_id.

---

**Q: How does IMAP work with your system?**

A: IMAP is a stateful protocol — clients expect persistent folder state, UID sequences, and IDLE notifications. We bridge it via an IMAP proxy:

```
Outlook / Apple Mail → IMAP Proxy (stateful per connection) → Email Read API → Cassandra
```

The proxy:
- Maintains per-connection UID map: IMAP UIDs (sequence numbers) → internal email_ids
- Handles IMAP IDLE: proxy subscribes to Redis pub/sub channel for that user → pushes EXISTS command to client when new email arrives
- Handles FETCH: proxy calls internal API → fetches from Cassandra (metadata) + S3 (body)

Why not IMAP directly on Cassandra? IMAP has complex state (flags, UIDNEXT, UIDVALIDITY) that doesn't map cleanly to our data model. The proxy isolates protocol complexity from storage layer.

---

**Q: Walk me through how you'd handle email quotas.**

A: Three-layer approach:

1. **Quota tracking**: `quota_used_bytes` counter in Redis per user. Updated atomically on every email write.
2. **Soft limit (90%)**: Email notification to user, banner in UI — delivery continues.
3. **Hard limit (100%)**: Return SMTP 552 "Mailbox full" to sending MTA. Sender receives bounce. Except: system emails (OTP, password reset) bypass quota check.

**Race condition fix:** Quota check + delivery must be atomic. Use Redis `SET quota:lock:{user_id} NX EX 1` — only one delivery per user at a time for quota enforcement. On lock miss: brief wait + retry (max 3 retries over 200ms).

---

## Round 3: Deep Dive / Follow-Up Questions

**Q: How do you ensure emails aren't lost if your SMTP relay crashes mid-delivery?**

A: The relay is a Kafka consumer. It commits Kafka offset ONLY after recording a successful delivery receipt in Cassandra. If it crashes mid-delivery, offset not committed → message redelivered on recovery → retries SMTP send.

To prevent double-delivery: check delivery_records table before sending. If record exists for (email_id, recipient) → skip. Even if delivery reaches recipient twice, RFC 5322 Message-ID header is the same → recipient's MTA deduplicates silently.

---

**Q: How do you handle email forwarding / aliases?**

A: Alias resolution happens in the delivery pipeline before fan-out:
- `team@company.com` → looks up alias_members table → expands to [alice@, bob@, charlie@]
- Expanded list → individual delivery records per resolved recipient
- Prevents N-level alias loops: track expansion depth, reject if > 5 levels

For external forwarding (`alice@company.com` → `alice@gmail.com`):
- Re-inject through SMTP relay with original sender preserved
- Must update DMARC/SPF headers (SRS — Sender Rewriting Scheme) to avoid spam classification at Gmail

---

**Q: Your Elasticsearch is down. How does user experience degrade?**

A: Circuit breaker (Concept #32) wraps all Elasticsearch calls:
- OPEN state: search falls back to Cassandra metadata-only query
- User sees subject/sender/date results, no full-text body matches
- Banner: "Full search temporarily unavailable"
- Critical flows (inbox load, email open) use Cassandra only — unaffected

---

**Q: A user's account is compromised and they send spam to 1 million users. How do you detect and stop this?**

A: Three detection layers:
1. **Rate anomaly**: Redis rate limiter fires at 500 emails/hr → account suspended automatically
2. **Recipient pattern**: 95%+ new recipients (never emailed before) in a short window → flag for review
3. **Spam report feedback**: if >0.1% of recipients mark as spam within 1hr → auto-suspend + alert security team

Recovery: account requires human review + re-auth (MFA) to re-enable. All emails sent during suspension period → attempt recall (internal recipients: mark deleted; external: cannot recall).

---

## Common Mistakes to Avoid

1. **Using Postgres as primary email store** — explain why Cassandra fits better (see Round 2 above)
2. **Synchronous fan-out for group emails** — must be async via Kafka
3. **Using SES for outbound at scale** — cost is prohibitive; mention self-hosted SMTP
4. **Forgetting IMAP compatibility** — many interviewers will ask how Outlook connects
5. **Search without user isolation** — every Elasticsearch query MUST include user_id filter
6. **Storing email body in Cassandra** — bodies are variable size, often large; store in S3, reference by key
7. **No spam filter feedback loop** — mention user reports → training data → model improvement
