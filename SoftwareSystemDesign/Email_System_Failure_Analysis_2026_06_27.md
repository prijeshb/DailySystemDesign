# Email System — Failure Analysis
*Date: 2026-06-27*

---

## Failure-First Thinking

> For every component: what if IT fails? What if the component BEFORE it fails? What if the component AFTER it fails?

---

## 1. MX Gateway (SMTP Ingress) Fails

### Scenario
All MX gateway pods crash (OOM, bad deploy) during high inbound volume.

### Impact
- External senders receive: `421 Service Temporarily Unavailable` (temp fail)
- SMTP spec requires senders to retry for 24–48 hours → **no mail lost**
- If down > 48hr → senders give up → permanent bounce (`550`) to senders → mail lost

### Component Before Fails (External Sender MTA)
External sender's MTA goes offline mid-connection:
- Our gateway receives partial DATA → detects TCP close before `<CRLF>.<CRLF>`
- SMTP connection closed without QUIT → gateway discards partial email (never writes to Kafka)
- Sender gets no ACK → retries with full email → deduplicated by message_id

### Component After Fails (Kafka)
Gateway receives email but cannot publish to Kafka:
```
Option A (current): Gateway buffers in local memory queue → retries Kafka publish
  Risk: if gateway crashes while buffering → email lost
  Mitigation: gateway writes to local WAL (disk) before returning 250 OK to sender
              On recovery → replay WAL → re-publish to Kafka

Option B: Gateway writes to SQS as fallback
  SQS → re-publishes to Kafka when Kafka recovers
  Cost: SQS complexity, additional latency
```
**Best practice:** Return 250 OK to sender only after durable write (WAL or Kafka ACK=all).

### Prevention
- 3 MX gateway replicas across AZs (DNS Round Robin)
- If 1 AZ fails → DNS removes it from MX records within 60s (low TTL = 60s)
- Kubernetes HPA: scale out on connection count > 10K per pod
- Graceful drain: SIGTERM → stop accepting new connections → finish in-flight → exit

---

## 2. Spam Filter Service Fails

### Scenario
Spam filter pods crash or become slow (>2s latency).

### Impact
Without spam filter → either block ALL inbound (bad) or deliver ALL unfiltered (worse).

### Fail-Open vs Fail-Closed Decision
```
Option A — Fail-closed (block all while filter is down):
  Pro: no spam delivered
  Con: legitimate mail delayed/lost, SLA breach, angry users

Option B — Fail-open (deliver without filtering):
  Pro: mail keeps flowing
  Con: spam flood reaches users

Option C — Graceful degradation (recommended):
  Stage 1 (sync, always runs): SPF/DKIM/DMARC checks + IP blocklist
    These run in MX Gateway itself, not in separate service
    If spam filter down → these still catch 80% of obvious spam
  Stage 2 (async, in delivery worker): ML scoring
    If down → skip ML stage, deliver based on Stage 1 result only
    Flag emails as "unscored" → re-score when spam filter recovers via message replay
```

### Component Before Fails (Kafka)
Spam filter is a Kafka consumer. If Kafka partition is unavailable:
- Consumer pauses on that partition
- No message loss (Kafka retains)
- On Kafka recovery → consumer resumes from last committed offset

### Component After Fails (Delivery Worker)
Spam filter scores email but delivery worker is down:
- Spam filter writes score to Kafka (not directly to delivery worker)
- Delivery worker reads from Kafka when recovered → processes with correct spam score
- No data coupling between spam filter and delivery worker

---

## 3. Cassandra (Metadata Store) Fails

### Scenario
Cassandra node failure or quorum loss.

### Impact
- Write failure: inbound email processed but cannot write metadata → email "orphaned" (body in S3, no inbox record)
- Read failure: users cannot load inbox

### Quorum Configuration
```
Cassandra: RF=3 (replication factor), QUORUM writes (W=2, R=2)
W + R > RF (2+2 > 3) → consistent reads

One node dies:
  W=2 still achievable with 2 remaining nodes → writes continue
  R=2 still achievable → reads continue
  Zero service impact

Two nodes die:
  W=2 fails (only 1 node) → writes fail → cannot accept inbound emails
```

### What Happens When Cassandra Write Fails (inbound email)
```
Delivery Worker flow:
  1. Read email from Kafka
  2. Write metadata to Cassandra  ← fails here
  3. Commit Kafka offset          ← never happens

On Cassandra recovery:
  Delivery worker restarts → reads same Kafka offset (not committed)
  → retries Cassandra write → idempotent (email_id already unique in Cassandra)
  → commits offset → email delivered

No email loss because Kafka offset is the safety net.
```

**Critical rule:** Always commit Kafka offset AFTER successful DB write, never before.

### Hinted Handoff
```
If target replica is down during write:
  Coordinator stores the write as "hint" for the down node
  When down node recovers → coordinator replays hints → node catches up
  Window: 3 hours (configurable). If down > 3hr → use Merkle tree repair instead
```

### User Cannot Load Inbox (Read Failure)
- Cache Layer (Redis): last 50 emails per user cached, TTL=60s
- If Cassandra down: serve cached inbox, degrade gracefully with banner: "Some emails may be missing"
- Search still works (Elasticsearch unaffected)

---

## 4. S3 (Body Storage) Fails

### Scenario
S3 regional outage (rare, AWS SLA 99.99%, ~53 min/year).

### Impact
- Inbound: email metadata can still be written to Cassandra (body_ref stored, body_hash stored) → email entry appears in inbox
- On open: S3 fetch fails → user sees "Unable to load email body" → degraded UX, not data loss
- Outbound: cannot store body → block send (cannot proceed without durable body)

### Prevention
- S3 Cross-Region Replication: replicate all email bodies to secondary region asynchronously
- Replication lag: ~1-5 minutes
- On primary region S3 failure: Email Read API falls back to secondary region S3

### Attachment Upload Failure (Client → S3 pre-signed URL)
```
Client uploads attachment → S3 pre-signed URL → PUT fails mid-upload
  Client retries: S3 multipart upload allows resume from last successful part
  If pre-signed URL expired (default 15 min): client requests new URL from API
  API checks: partial upload exists in S3? → issue new URL for same multipart upload ID
  → resume from last part

If client crash mid-upload:
  Orphaned multipart upload → S3 lifecycle rule: abort incomplete multipart after 24hr
  → no orphaned storage accumulation
```

---

## 5. SMTP Relay (Outbound) Fails

### Scenario
SMTP relay cluster crashes during outbound delivery.

### Impact
- User's "send" already succeeded (email in Cassandra + S3 + Kafka)
- SMTP relay is a Kafka consumer → if it crashes, offset not committed
- On recovery → re-reads from Kafka → retries delivery → recipient may get duplicate

### Deduplication for Outbound
```
SMTP relay writes delivery record before attempting send:
  delivery_attempts (email_id, recipient, attempt_at, message_id_header, status)

On retry → check: is there a successful delivery record for (email_id, recipient)?
  Yes → skip (already delivered)
  No  → attempt delivery

If recipient SMTP server receives duplicate (relay sends twice before recording success):
  Recipient's SMTP server deduplicates by RFC 5322 Message-ID header (standard behavior)
  Two SMTPs with same Message-ID → second is silently dropped
```

### Bounces and Delivery Failures
```
Hard bounce (550 Mailbox not found):
  → mark delivery as permanently failed
  → notify sender (email: "Delivery failed to X@domain.com")
  → if 3 consecutive hard bounces to same domain → flag domain as invalid

Soft bounce (421/450 Temporarily unavailable):
  → retry with exponential backoff: 5min, 30min, 2hr, 6hr, 24hr, 48hr
  → after 48hr → hard fail → notify sender

Deferral queue (Redis Sorted Set):
  ZADD deferred_emails {next_retry_timestamp} {email_id:recipient}
  Worker: ZRANGEBYSCORE deferred_emails 0 {now} → pull due emails → retry
```

---

## 6. Elasticsearch (Search Index) Fails

### Scenario
Elasticsearch cluster goes down or becomes slow.

### Impact
- User search returns no results → degraded, not broken
- Email delivery, inbox load, IMAP access all unaffected (Cassandra-based)
- Indexing queue (Kafka topic: email.index) backs up

### Degraded Search Mode
```
If Elasticsearch unreachable (circuit breaker open):
  Fall back to Cassandra-based search:
    SELECT * FROM user_emails 
    WHERE user_id = ? AND folder = ?
    AND received_at >= ?    ← date filter only
    AND sender = ?           ← if sender filter provided

  Limitations: no full-text search, no body search
  User sees banner: "Search is limited. Full search will resume shortly."
```

### Index Backlog Recovery
```
During outage: Kafka topic email.index retains unprocessed emails
On Elasticsearch recovery:
  Indexer resumes from last committed offset
  Backlog of 2hr × 350K emails/sec = 2.5B emails to index

Catch-up strategy:
  Normal indexing: async, 1 worker per partition
  Recovery mode: 10 workers per partition, bulk index (1000 docs/request)
  Catch-up rate: 10× normal → 20hr outage → 2hr catch-up
```

---

## 7. Delivery Worker Fails Mid Fan-Out

### Scenario
User receives email to a group alias (100 members). Delivery worker crashes after writing 60 out of 100 inbox records.

### Impact Without Protection
- 60 members see email, 40 do not
- Worker retries from Kafka → re-delivers to all 100 → 60 get duplicate

### Protection: Idempotent Delivery with Dedup Key
```
Before writing each inbox record:
  key = SHA256(email_id + recipient_user_id)
  
  INSERT INTO user_emails (user_id, folder, email_id, ...)
  IF NOT EXISTS   ← Cassandra lightweight transaction

  LWT is atomic: if row already exists → no-op, no duplicate
```

Cost: Cassandra LWTs are 2× slower than regular writes (requires PAXOS round-trip).
Mitigation: Use LWT only for group emails (recipient count > 10). Single-recipient emails use regular INSERT with idempotent re-delivery being harmless (last-write-wins, same data).

---

## 8. User Quota Exceeded

### Scenario
User at quota limit (e.g., 15GB free tier). New email arrives.

### Options
```
Option A (Reject): Return SMTP 552 "Mailbox full" to sender
  Sender gets bounce → no silent data loss
  User notified by separate quota-exceeded email (exempt from quota check)

Option B (Deliver anyway, mark over-quota):
  Always deliver critical emails (password reset, bank OTP)
  Mark user as over-quota → block new sends until cleaned up

Best practice:
  Soft limit at 90%: warn user, allow delivery
  Hard limit at 100%: reject inbound with SMTP 552
  Exception: system emails (password reset, OTP) always bypass quota check
```

### Quota Tracking
```
quota_used is updated on every email write → Cassandra counter
Problem: Cassandra counters are eventually consistent

Race: User receives 10 emails simultaneously → 10 concurrent quota reads → all
      pass quota check → all write → quota exceeds limit

Fix: quota check runs in a serialized quota service (one lock per user_id):
  Redis: SET quota:lock:{user_id} NX EX 1 → only one concurrent quota check
  After successful delivery → increment quota atomically
  On lock contention → queue briefly → recheck
```
