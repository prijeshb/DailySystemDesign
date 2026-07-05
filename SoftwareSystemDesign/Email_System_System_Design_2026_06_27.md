# Email System (Gmail-like) — System Design
*Date: 2026-06-27*

---

## First Principles — Do We Even Need This?

**Problem:** Users need asynchronous communication that works across platforms, organizations, and time zones — with no requirement that both parties are online simultaneously.

**Why not use existing providers (SES, Mailchimp)?**
At small scale: use them. At 200M+ users with 5B emails/day, provider cost is $500K+/day for outbound alone — you build your own SMTP relay.

**Why not just a chat system?**
Email is a *standard protocol* (SMTP/IMAP/POP3). It must interoperate with the entire internet's email ecosystem. Anyone with any email client (Outlook, Apple Mail, Thunderbird) must be able to send to and receive from our users. That's not optional.

**Core insight:**
Email = storage + protocol bridge. You're building: (1) a massive append-only document store per user, (2) an SMTP protocol gateway that speaks to the global mail ecosystem, (3) a search index over private per-user data.

---

## Scale Estimates

**Assumptions:**
- 200M registered users, 50% DAU = 100M DAU
- 50 emails received/user/day → 10B inbound/day = **116K/sec avg**, peak 3× = **350K/sec**
- 5 emails sent/user/day → 1B outbound/day = **11.5K sends/sec**
- Email body avg: 15KB (no attachments). Attachments: 20% of emails, avg 2MB
- Attachment contribution: 1B × 20% × 2MB = 400TB/day in attachments

**Storage (daily ingestion):**
```
Email bodies:    10B × 15KB = 150TB/day
Attachments:     1B × 20% × 2MB = 400TB/day
Metadata/index:  10B × 1KB = 10TB/day
Total:           ~560TB/day
```

**Storage at rest (tiered):**
```
Hot (0–90 days):   560TB × 90 = 50PB → Cassandra + S3 Standard
Warm (90d–1yr):    50PB equivalent → S3 Infrequent Access
Cold (1–7yr):      ~1EB → S3 Glacier
```

**Read patterns:**
- User opens inbox → last 50 emails for a folder (paginated)
- User searches → Elasticsearch query
- IMAP FETCH → individual email by UID

---

## AWS Cost (Back of Envelope)

| Component | Monthly Volume | Unit Cost | Monthly Cost |
|-----------|---------------|-----------|--------------|
| SES outbound (if used) | 30B emails | $0.10/1K | **$3,000,000** |
| Self-hosted SMTP relay (m5.2xl × 200) | 24×30hr | $0.384/hr | $55,296 |
| S3 Standard (hot email bodies) | 50PB | $0.023/GB | $1,177,600 |
| S3 Glacier (cold archive) | 200PB | $0.004/GB | $819,200 |
| Cassandra (i3en.6xl × 60 nodes) | — | $2.752/hr | $118,886 |
| Elasticsearch (r5.4xl × 30 nodes) | — | $1.008/hr | $21,773 |
| Redis (ElastiCache r6g.2xl × 20) | — | $0.484/hr | $6,969 |
| EC2 application tier (m5.xl × 100) | — | $0.192/hr | $13,824 |
| **Total** | | | **~$2.2M/month** |

**Budget constraint — SES for outbound is 55× more expensive than self-hosted:**
- SES: $3M/month vs self-hosted SMTP: $55K/month
- Decision: self-hosted Postfix/Haraka cluster for all outbound SMTP
- S3 + lifecycle rules are non-negotiable for cost control:
  - Day 1–90: Standard ($0.023/GB)
  - Day 91–365: S3-IA ($0.0125/GB) → saves 46%
  - Day 365+: Glacier ($0.004/GB) → saves 83%
- Without lifecycle policies: storage cost 3× higher within 2 years

**Limitation at $2M/month budget:**
- Full-text search retained for 6 months only (Elasticsearch hot index)
- Search older than 6 months → degraded: metadata search only (sender, subject, date)
- Attachments streamed directly from S3; never hot-cached (too large)

---

## Entities & Actions

### Entities
```
User
  user_id, email_address, display_name, quota_bytes, quota_used_bytes,
  timezone, created_at

Email
  email_id (UUID), message_id (SMTP RFC 5322 header), thread_id,
  sender_address, recipients[], subject, body_ref (S3 key),
  size_bytes, has_attachment, labels[], received_at, read_at

Thread
  thread_id, root_message_id, subject_normalized,
  participants[], last_email_at, email_count

Attachment
  attachment_id, email_id, filename, content_type, size_bytes, s3_key

Label
  label_id, user_id, name (INBOX/SENT/TRASH/SPAM or custom), color

DeviceToken
  token_id, user_id, platform, fcm_token, last_seen_at
```

### Actions
```
send_email(from, to[], cc[], bcc[], subject, body, attachments[])
receive_email(smtp_envelope)   → internal delivery
get_inbox(user_id, folder, page_token, limit)
search_email(user_id, query, filters)
get_thread(thread_id, user_id)
move_to_label(email_id, user_id, label)
delete_email(email_id, user_id)
mark_read(email_id, user_id)
```

---

## Data Flow

```
OUTBOUND (user sends):
  Client → API Gateway → Email Compose API
    → Rate Limiter check (max 500 emails/hr/user)
    → Attachment upload → S3 (pre-signed URL, direct from client)
    → Body stored: S3 key = emails/{user_id}/{email_id}/body
    → Email record → Cassandra (metadata)
    → Kafka topic: email.outbound
         ↓
    SMTP Relay Cluster (Postfix/Haraka)
    → DNS MX lookup for recipient domain
    → SMTP delivery to external MX server (or internal if same domain)
    → Delivery receipt → update email.status in Cassandra

INBOUND (email received from internet):
  External SMTP → MX Gateway (port 25, TLS)
    → SPF check: is sender IP authorized for sender domain?
    → DKIM check: verify cryptographic signature in headers
    → DMARC check: policy enforcement (reject/quarantine/none)
    → Rate limit: max 100 emails/min per sending IP
    → Spam filter (Bayesian + rule engine)
    → If spam score > threshold → route to SPAM label
    → Store body → S3
    → Expand recipients (e.g., group aliases)
    → Fan-out: for each recipient → Kafka topic: email.inbound.{shard}
         ↓
    Delivery Workers (per shard)
    → Write email metadata to Cassandra (user_id partition)
    → Write to Search Index (Elasticsearch, async)
    → Push notification to user device (if online)
    → IMAP IDLE clients notified via server push

SAME-DOMAIN FAST PATH:
  sender@company.com → recipient@company.com
  Skip external SMTP → internal Kafka → direct delivery
  No DNS MX lookup, no TLS handshake, ~10ms vs ~200ms external
```

---

## High-Level Design

```
                    Internet
                       │
              ┌────────▼────────┐
              │  MX Gateway     │  ← inbound port 25
              │  (SMTP ingress) │
              └────────┬────────┘
                       │ SPF/DKIM/DMARC + Spam Filter
                       │
              ┌────────▼────────┐
              │     Kafka       │  email.inbound.{0..N}
              └────────┬────────┘
                       │
         ┌─────────────┼──────────────┐
         │             │              │
    ┌────▼────┐  ┌─────▼─────┐  ┌────▼────┐
    │Delivery │  │  Search   │  │  Push   │
    │Workers  │  │  Indexer  │  │ Notify  │
    └────┬────┘  └─────┬─────┘  └─────────┘
         │             │
    ┌────▼────┐  ┌─────▼──────┐
    │Cassandra│  │Elasticsearch│
    │(metadata│  │ (full-text) │
    └─────────┘  └────────────┘
         │
    ┌────▼────┐
    │  S3     │  ← email bodies + attachments
    └─────────┘

Client access:
  Client → API Gateway → Email Read API → Cassandra + S3
                       → Search API → Elasticsearch
                       → IMAP Proxy → Cassandra (RFC-compliant)

Outbound path:
  Client → Compose API → S3 (body) + Cassandra (meta) → Kafka
         → SMTP Relay → Internet
```

---

## Low-Level Design

### Database Schema (Cassandra)

**user_emails table** — primary access pattern: all emails for a user in a folder
```
PRIMARY KEY ((user_id, folder), received_at DESC, email_id)

Query: SELECT * FROM user_emails
       WHERE user_id = ? AND folder = 'INBOX'
       ORDER BY received_at DESC
       LIMIT 50;
```

Why Cassandra?
- Write pattern: every email creates N rows (one per recipient folder) — append-only, no joins
- Read pattern: always by (user_id, folder) — maps directly to partition key
- Scale: 10B rows/day, Cassandra handles 100K writes/sec per node
- TTL: set TTL on cold-archive rows, auto-deletes → avoids explicit delete storms

**email_by_message_id table** — for thread resolution
```
PRIMARY KEY (message_id)
→ email_id, thread_id

Used by: thread grouper when processing In-Reply-To header
```

**threads table**
```
PRIMARY KEY ((user_id), thread_id)
clustering key: last_email_at DESC

Stores: thread_id, subject_normalized, participants[], last_email_at, unread_count
```

### Thread Grouping Algorithm

```python
def resolve_thread(email: Email) -> str:
    # Step 1: Check In-Reply-To header (most reliable)
    if email.headers.get("In-Reply-To"):
        parent_msg_id = email.headers["In-Reply-To"]
        parent = db.get_email_by_message_id(parent_msg_id)
        if parent:
            return parent.thread_id

    # Step 2: Check References header (chain of ancestors)
    for ref_msg_id in reversed(email.headers.get("References", [])):
        parent = db.get_email_by_message_id(ref_msg_id)
        if parent:
            return parent.thread_id

    # Step 3: Subject normalization (loose matching, last resort)
    # "Re: Meeting tomorrow" → "meeting tomorrow"
    normalized = normalize_subject(email.subject)
    existing_thread = db.find_thread_by_subject(email.sender_domain, normalized, 
                                                 within_days=7)
    if existing_thread:
        return existing_thread.thread_id

    # Step 4: New thread
    return generate_thread_id()
```

**Trade-off:** Subject matching (Step 3) is fuzzy — "Meeting tomorrow" from two unrelated people with same subject gets incorrectly threaded. Gmail has this bug too. Fix: scope subject matching to same participants only.

### Spam Filter Pipeline

```
Stage 1 — Reputation Check (sync, <1ms):
  SPF fail → score += 30
  DKIM fail → score += 25
  DMARC fail → score += 20
  Sender IP in blocklist (Spamhaus/SORBS) → score += 50 (auto-reject if > 90)

Stage 2 — Rate check (sync, Redis INCR, <1ms):
  Sending IP: max 1000 emails/hr → excess rejected with 421 temp fail
  Sender domain: max 10K emails/hr → excess rejected

Stage 3 — Content analysis (async, ~10ms):
  Bayesian classifier: P(spam | features)
  Features: word frequencies in subject+body, URL reputation, HTML-to-text ratio
  score += bayesian_score × 40

Stage 4 — ML model (async, ~50ms, only for borderline):
  If 30 < score < 70: run neural classifier (BERT-based, fine-tuned on spam corpus)
  Updates score

Final routing:
  score < 30:  INBOX
  30–70:       SPAM (user can mark as not-spam → trains model)
  score >= 70: Reject (server returns 550 to sender)
```

**Why Bayesian first, neural second?**
Bayesian: O(1) lookup of word probabilities, ~10ms for 1KB email, handles 95% of obvious spam.
Neural: more accurate but 50ms per email — run only on borderline cases to save compute.
At 350K inbound/sec, running BERT on all = 70K GPU-ms/sec = ~$500K/month in inference cost alone. Staged approach: neural only touches ~5% of traffic.

### Body Storage (S3 + Deduplication)

```
Problem: corporate "all-hands" email to 200K employees
→ 200K copies of same body = massive waste

Solution: Content-addressed storage (Concept #67)
  body_hash = SHA256(body_bytes)
  S3 key = emails/bodies/{body_hash}

  Before storing:
    if S3.head_object(key) exists → skip upload, just store reference
    else → upload body → store reference

  Per-user storage: Cassandra row stores body_hash (not full body)
  On fetch: get hash from Cassandra → GET from S3

Attachment dedup: same SHA256 approach
  all-hands email with 500KB PDF × 200K employees
  Without dedup: 200K × 500KB = 100GB
  With dedup: 500KB (1 copy) + 200K × 64-byte hash references
```

**Trade-off:** Dedup requires SHA256 computation on ingest (cheap, ~1ms for 100KB). Privacy concern: if two users have exact same private email body, they share storage. Fix: scope dedup only to mass-delivery emails (recipient count > 100).

### IMAP Proxy Layer

IMAP is stateful (folder state, sequence numbers, IDLE push). Rather than native Cassandra → IMAP translation in every client, a proxy layer handles the impedance mismatch:

```
Thunderbird/Apple Mail
   ↓ IMAP (RFC 3501)
IMAP Proxy (stateful per connection)
   ↓ translates to internal API calls
Email Read API
   ↓
Cassandra + S3
```

The proxy maintains per-session UID mapping (IMAP UIDs → internal email_ids) and handles IDLE (long-poll for new mail notification).

### Rate Limiting (Outbound)

```
Per-user limits (Redis sliding window):
  emails_per_hour: 500
  emails_per_day: 2000
  recipients_per_email: 500

Anti-abuse:
  New account (< 7 days): email_per_hour: 50
  Verified account (domain or phone): normal limits
  Suspicious pattern (100% external, all new recipients): → review queue
```

---

## Trade-Off Summary

| Decision | Chose | Trade-off |
|----------|-------|-----------|
| Cassandra for metadata | Write throughput, no joins | No ad-hoc aggregations; query patterns must be known upfront |
| S3 for bodies | Unlimited scale, cheap | Cold read latency ~100ms vs in-DB ~5ms; must pre-warm for IMAP FETCH |
| Elasticsearch for search | Fast full-text | Storage cost limits index to 6 months; older search is degraded |
| Content-addressed dedup | Massive storage savings for bulk mail | Adds SHA256 compute on ingest; privacy risk for small recipient lists |
| Self-hosted SMTP relay | 55× cheaper than SES | Operational complexity; IP reputation management required |
| Bayesian+neural staged | Low inference cost | Neural only for borderline — ~0.5% misclassification on clear spam |
| Async fan-out via Kafka | Delivery workers can scale independently | Delivery is eventual, not instant (usually <1s but not guaranteed) |
