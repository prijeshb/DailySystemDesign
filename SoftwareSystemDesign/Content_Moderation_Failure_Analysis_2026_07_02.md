# Content Moderation System — Failure Analysis
**Date:** 2026-07-02 | **System:** Content Moderation

---

## Failure Matrix

| Layer | Component Fails | Upstream Effect | Downstream Effect | Detection | Prevention | Recovery |
|-------|-----------------|-----------------|-------------------|-----------|------------|----------|
| Ingest | Kafka broker down | Content upload stalls if sync write | No moderation events emitted | Kafka broker health metrics, consumer lag | Multi-broker Kafka (min ISR=2), producer retries | Kafka leader re-election (< 30s); producer retries with backoff |
| Hash check | Redis cluster down | None (async) | Hash check skipped | Redis ping probe, error rate spike | Redis Sentinel/Cluster, local fallback cache | **Fail-open to ML pipeline** — do NOT block upload; hash check is best-effort |
| ML Inference | GPU node down | None (async) | Jobs pile in queue | SageMaker endpoint health check, queue depth | Auto-scaling GPU fleet, min 3 nodes | Scale-out from ASG; pending jobs re-consumed from Kafka (idempotent) |
| ML service | Model corrupt/OOM | — | All decisions are QUEUE (fallback) | Inference error rate > 1% | Canary deploy, shadow mode | Rollback to previous model version via SageMaker model registry |
| Decision aggregator | Bug causes all to AUTO-APPROVE | — | Policy bypassed | Auto-approve rate anomaly alert (> baseline + 3σ) | Shadow mode testing, rate-of-change alerts | Circuit breaker: if approve rate > threshold, route 100% to human queue |
| Review queue DB | Postgres primary down | Tasks not written | Moderators see empty queue | RDS Multi-AZ failover | Multi-AZ RDS, automatic failover | Failover < 60s (RDS Multi-AZ) |
| Action service | Down when decision issued | — | Content not actioned | Dead letter queue size alert | DLQ on SQS, Action service retries | Consume DLQ, re-apply actions idempotently (action_id dedup key) |
| Moderator tool | UI unavailable | — | Human review backlog grows | Uptime monitoring, queue depth alert | Multi-region tool deployment | Redirect moderators to backup UI; pause new task assignment |
| NCMEC hash feed | Feed not updated | Hash DB stale | New CSAM variants miss hash match | Feed freshness timestamp alert | Scheduled ingestion with freshness check | Alert trust & safety team; increase ML sensitivity temporarily |

---

## Scenario Deep-Dives

### 1. ML Classifier Goes Rogue (All Content Auto-Removed)
**Trigger:** Bad model deployment — new version has a bug causing confidence to always be 1.0.

**Blast radius:** Every post on the platform gets auto-removed. Users cannot post. Engagement drops to zero.

**Detection:** 
- Auto-remove rate spikes from ~2% baseline to 100% within 5 minutes
- User appeal rate spikes 50×
- Business metric alert: DAU drop, post success rate drops

**Prevention:**
- Shadow mode: New model runs in parallel for 1hr; decisions compared to baseline
- Canary: Route 1% of traffic to new model; watch decision distribution
- Hard rate limit: Auto-remove rate cannot exceed 5% of traffic; excess → queue

**Recovery:**
1. Automated: Circuit breaker triggers, routes all to human queue (overload risk)
2. Operator: Roll back model in SageMaker (< 2 min with blue/green endpoints)
3. Re-evaluate all auto-removed content from past 1hr using restored model
4. Restore eligible content; proactive user notification

---

### 2. Hash Database Corrupted (False Positives)
**Trigger:** Bad hash batch injected into NCMEC feed (e.g., legitimate image hashes added by error).

**Blast radius:** Legitimate content (news photos, profile pictures) auto-removed.

**Detection:** 
- Appeal rate spike on hash-matched removals
- Moderator override rate on hash-matched reviews > 10% (baseline ~0.1%)

**Prevention:**
- Hash DB accepts updates only from signed NCMEC feed (HMAC verified)
- Dual-hash validation: pHash AND PDQ must both match for auto-remove
- Canary: New hash batch applied to sample of recent content; if > X matches, hold for review

**Recovery:**
1. Identify poisoned hash batch by timestamp correlation
2. Soft-delete hashes from affected batch (mark as quarantined, not removed)
3. Re-process all content removed in that window: re-run ML, send to human review
4. Coordinate with NCMEC to trace source of bad hashes

---

### 3. Human Review Queue Overload
**Trigger:** Viral news event (war, disaster) spikes borderline content 10×. ML queues 10M items instead of 1M.

**Blast radius:** Review backlog grows to days. Borderline harmful content stays up too long.

**Detection:**
- Queue depth crosses SLA breach threshold (depth / reviewer throughput > target hours)
- Time-in-queue P95 alert

**Prevention:**
- Elastic reviewer pool: On-call queue of trained contractors activated via trigger
- Priority escalation: When backlog > 2hr SLA, re-classify P2 tasks as auto-approve (accept false negative) to clear queue
- ML confidence threshold auto-tuning: When queue > X depth, raise auto-remove threshold by 0.01 (automated policy response — requires approval gate)

**Recovery:**
1. Activate surge reviewer pool (pre-contracted, 4hr SLA to start)
2. Emergency policy: Categories with < 0.1% overturn rate → auto-decide based on ML alone temporarily
3. Post-incident: Retrain ML with event-specific data (news event hate speech patterns)

---

### 4. Appeal System Unavailable
**Trigger:** Appeal service DB down. Users cannot submit or view appeals.

**Blast radius:** Users wrongly banned with no recourse. DSA compliance risk (EU requires appeal mechanism).

**Detection:** Appeal submission error rate, appeal service health check

**Prevention:** Multi-AZ appeal DB, CDN-cached appeal status page

**Recovery:**
- Static "Appeal under maintenance" page with email fallback
- Email-based appeal intake during downtime (manual triage team on standby)
- SLA: Restore < 1hr (DSA requires appeal mechanism always available)

---

### 5. Component Before ML (Kafka) Fails
**Effect:** No new moderation jobs created. Content uploads succeed but are never moderated.

**Detection:** Consumer group lag = 0 AND new upload rate > 0 AND moderation job creation rate = 0 → anomaly

**Prevention:** Kafka multi-broker (3 brokers min), producer confirms on ISR=2

**Recovery:** Content uploaded during outage is at-risk (unmoderated). On Kafka recovery, all lag replays automatically. Content uploaded in gap must be retroactively scanned (run a catch-up job querying upload DB for content with no associated moderation job).

---

### 6. Component After ML (Action Service) Fails
**Effect:** Decisions made but not applied. ML says "remove" but content stays up.

**Detection:** Decision written timestamp vs. action applied timestamp divergence > SLA

**Prevention:** SQS DLQ; Action service consumes from SQS (at-least-once delivery)

**Recovery:** Action service restarts and re-consumes from SQS. Idempotency key = `decision_id` prevents double-action. DLQ retries with exponential backoff (1s → 2s → 4s → 30s → DLQ after 5 attempts).

---

## Idempotency Strategy

All moderation actions are idempotent via `decision_id`:

```sql
INSERT INTO content_actions (content_id, decision_id, action, applied_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (decision_id) DO NOTHING;  -- idempotent re-delivery safe
```

Hash check writes: `SETNX hash:{phash} {category}` — atomic, no double-write.

---

## Monitoring Alerts

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Auto-remove rate anomaly | > baseline × 3σ or < 0.1% | P0 |
| ML inference error rate | > 1% over 5min | P0 |
| Hash DB freshness | > 6hr since last update | P1 |
| Review queue SLA breach | P0 queue > 30min depth | P0 |
| Appeal service unavailable | > 5min downtime | P0 (DSA) |
| Kafka consumer lag | > 100K messages (> ~90 sec delay) | P1 |
| Action DLQ depth | > 1000 messages | P1 |
| Model overturn rate | > 5% of human reviews overturning auto-decisions | P2 (model quality) |
