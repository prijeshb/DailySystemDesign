# Content Moderation System Design
**Date:** 2026-07-02 | **Difficulty:** Hard | **Companies:** Meta, TikTok, YouTube, LinkedIn, Twitter/X

---

## First Principles: Do We Even Need This?

**Problem:** At scale, humans cannot review every piece of content.
- Facebook: ~100M posts/day. 200K human moderators at 200 posts/hr = 40M reviews/day. Gap = 60M posts unreviewed daily.
- Legal mandates: EU Digital Services Act (DSA), FOSTA-SESTA, NetzDG (Germany), require platform accountability.
- Advertiser safety: brands pull ads from harmful content adjacency.

**Answer:** Yes — the system is necessary. The design question is: *what proportion to automate vs. escalate to humans, and at what latency?*

---

## Scale Estimates

| Metric | Value |
|--------|-------|
| New posts/day | 100M |
| Images/day | 500M (posts + stories) |
| Videos/day | 10M |
| Comments/day | 1B |
| Peek QPS (content ingestion) | ~15,000 RPS |
| Target latency (CSAM, violence) | < 60 seconds |
| Target latency (hate speech, spam) | < 10 minutes |
| Target latency (borderline) | < 2 hours |

---

## AWS Budget Estimate

**Constraint:** $80K/day infra budget (realistic for a mid-tier platform)

| Component | Spec | Cost/day |
|-----------|------|----------|
| ML Inference (GPU fleet) | 50× g4dn.xlarge @ $0.526/hr | ~$630 |
| Hash lookup (PhotoDNA equiv) | Redis cluster 10× r6g.2xlarge | ~$200 |
| Kafka (content stream) | 10× r5.2xlarge | ~$120 |
| Review queue (SQS) | 1.6B msgs × $0.0000004 | ~$640 |
| PostgreSQL (decisions DB) | 2× db.r6g.4xlarge Multi-AZ | ~$90 |
| S3 (content metadata + logs) | 50TB × $0.023/GB | ~$1,150 |
| Lambda (orchestration) | 1B invocations × $0.0000002 | ~$200 |
| Data transfer + misc | — | ~$500 |
| **Total infra** | | **~$3,530/day** |
| Human reviewer cost | 5,000 reviewers × $15/hr × 8hr | ~$600,000/day |

**Key insight:** Infrastructure is <1% of total cost. Human reviewers dominate. Every 1% improvement in ML precision/recall eliminates ~$6,000/day in human review costs. This is why ML investment has huge ROI.

**Budget-driven design constraint:** With 90% ML auto-decision accuracy and $80K cap, we can afford to send only ~2% of content to human review (2M posts/day ÷ 5,000 reviewers × 400 tasks/day = feasible).

---

## Entities

```
Content          { content_id, user_id, type, url, text, created_at, hash }
ModerationJob    { job_id, content_id, status, created_at, resolved_at }
PolicyCategory   { category_id, name, severity, auto_action_threshold }
Decision         { decision_id, job_id, type[auto|human], action, confidence, policy_id }
ReviewTask       { task_id, job_id, queue_priority, assigned_moderator, deadline }
Appeal           { appeal_id, content_id, user_id, reason, status, resolution }
Moderator        { moderator_id, tier, skill_categories[], daily_review_count }
HashRecord       { hash, content_type, known_violation_category }  ← PhotoDNA-style
```

---

## Actions

1. Content submitted → ingest pipeline
2. Hash check against known violation DB (synchronous, < 5ms)
3. ML classifier inference (async, ~500ms)
4. Route decision: auto-approve / auto-remove / queue-for-human
5. Human moderator reviews task, makes decision
6. Action applied: remove / warn / restrict / approve
7. User appeals
8. Senior moderator reviews appeal
9. Policy feedback loop: model retrained on new labeled data

---

## Data Flow

```
[User submits content]
       │
       ▼
[API Gateway] ──→ [Content Store (S3 + metadata in Postgres)]
       │
       ▼
[Kafka: content.created topic]
       │
       ├──→ [Hash Checker Service]  ← synchronous Redis lookup, < 5ms
       │          │ KNOWN VIOLATION
       │          └──→ [Auto-Remove + Alert]
       │
       ├──→ [ML Orchestrator]
       │          │
       │          ├──→ [Text Classifier (BERT-based)]
       │          ├──→ [Image Classifier (CLIP/ResNet)]
       │          ├──→ [Video Sampler → frame classifiers]
       │          │
       │          └──→ [Decision Aggregator]
       │                    │
       │         CONFIDENCE > 99%  →  AUTO-REMOVE
       │         CONFIDENCE 70-99% →  QUEUE (priority by severity)
       │         CONFIDENCE < 70%  →  AUTO-APPROVE (low-risk categories)
       │
       └──→ [Review Queue Service]
                  │
                  ├──→ [P0 Queue: CSAM, terrorism] → specialist reviewers
                  ├──→ [P1 Queue: violence, self-harm] → trained reviewers
                  └──→ [P2 Queue: spam, nudity] → general reviewers
                            │
                       [Moderator Tool]
                            │
                       [Decision Written] ──→ [Action Service]
                                                    │
                                               [Notification Service → User]
                                               [Content Store: status updated]
                                               [Kafka: decision.made topic → Analytics]
```

---

## High-Level Design

### 3 Pipelines

**1. Sync Hash-Match (< 60 sec SLA)**
- Compute perceptual hash (pHash/PhotoDNA) at upload time
- Redis SET lookup against known-violation hash DB (populated from NCMEC, industry hash-sharing)
- If match: instant remove, no ML needed
- *Handles CSAM, known terrorist content — must-catch-all categories*

**2. Async ML Pipeline (60 sec – 10 min SLA)**
- Kafka consumer pulls content events
- Ensemble of classifiers: text, image, video frames, metadata (account age, geolocation, posting velocity)
- Output: `{category: "violence", confidence: 0.97, action: "auto_remove"}`
- Decision thresholds per policy category (configurable, not hard-coded)

**3. Human Review Queue (2 hr – 24 hr SLA)**
- Priority queues per severity
- Skills-based routing: CSAM → specialist, regional policy → local language team
- Moderator tool shows: content + context + similar past decisions + policy text
- Time-boxed tasks (if not picked in SLA, escalates automatically)

---

## Low-Level Design

### Hash Checker
```python
# At upload
phash = compute_phash(image_bytes)   # 64-bit perceptual hash
pdq  = compute_pdq(image_bytes)      # Meta's PDQ hash (more robust)

# Redis bloom filter first (avoid lookup on clean content)
if bloom.contains(phash):
    exact = redis.get(f"hash:{phash}")
    if exact: auto_remove(content_id, category=exact.category)
```

### ML Decision Aggregator
```python
def aggregate_decision(signals: List[Signal]) -> Decision:
    # Max severity wins (conservative)
    top = max(signals, key=lambda s: s.severity * s.confidence)
    
    policy = PolicyCategory.get(top.category)
    if top.confidence >= policy.auto_remove_threshold:  # e.g., 0.99
        return Decision(action=REMOVE, type=AUTO)
    elif top.confidence >= policy.queue_threshold:       # e.g., 0.70
        priority = compute_priority(top.severity, account_risk_score)
        return Decision(action=QUEUE, priority=priority)
    else:
        return Decision(action=APPROVE, type=AUTO)
```

### Review Queue Priority Formula
```
priority_score = severity_weight × (1 - confidence) × account_risk_multiplier × recency_boost
```
- High severity (CSAM=10) + low confidence = moderate priority (needs human eyes)
- Medium severity + near-threshold confidence = high priority (likely violation, borderline)

### Appeal State Machine
```
PENDING → UNDER_REVIEW → UPHELD (content stays removed)
                       → OVERTURNED (content restored, classifier retrained with this example)
                       → ESCALATED → LEGAL_REVIEW
```

---

## Trade-offs

### Cache (Redis Hash DB)
- **Pro:** 5ms lookup vs 500ms ML inference; catches 100% of known violations
- **Con:** Only catches *known* content. Novel CSAM/terrorism evades hash check. Hash DB must be continuously updated.
- **Decision:** Always run hash check AND ML in parallel for novel content detection.

### Async vs Sync ML Inference
- **Async (Kafka):** Decoupled, survives traffic spikes, can retry. Adds latency.
- **Sync (inline):** Instant decision at upload. Cascades failures to content upload.
- **Decision:** Async for all content except live video (where sync pre-screening required).

### Auto-Remove vs Queue at Threshold 0.95
- Raising threshold (0.99): Fewer false positives, more human review needed, $$$
- Lowering threshold (0.90): More false positives (creators wrongly banned), user trust damage
- **Real-world:** Meta uses 0.99 for auto-remove on CSAM (zero tolerance), 0.85 for spam.

### Monolith Classifier vs Ensemble
- **Single model:** Simpler, faster to update. Bad at multi-signal cases.
- **Ensemble:** Each classifier specialized (text/image/video/account behavior). More robust, slower, complex to coordinate.
- **Decision:** Ensemble with an aggregator layer. Modular — can retrain individual classifiers.

### Batch vs Real-time Retraining
- **Real-time:** Model stays current (new hate speech trends). Expensive, risky (unstable).
- **Batch daily:** Stable, auditable. Lags emerging threats.
- **Decision:** Batch nightly retraining. Hot-reload rule engine for rapid policy response.

---

## System Design Diagram (Text)

```
                    ┌─────────────────────────────────────────┐
                    │           CONTENT INGESTION             │
                    │  API GW → Upload Service → S3 + Postgres│
                    └───────────────┬─────────────────────────┘
                                    │ Kafka: content.created
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                       ▼
     ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────┐
     │  Hash Checker   │  │  ML Orchestrator  │  │ Metadata Risk   │
     │ (Redis pHash)   │  │ (Text+Image+Video)│  │ Scorer          │
     └────────┬────────┘  └────────┬─────────┘  └────────┬────────┘
              │                    │                       │
              └────────────────────┴───────────────────────┘
                                   │
                          ┌────────▼────────┐
                          │Decision Aggreg. │
                          └────────┬────────┘
              ┌────────────────────┼──────────────────────┐
              ▼                    ▼                       ▼
     AUTO-REMOVE              QUEUE (P0/P1/P2)       AUTO-APPROVE
     [Action Svc]          [Review Queue Svc]
          │                        │
     [Notify User]        [Moderator Tool UI]
          │                        │
     [Kafka: decision]    [Decision Written]
          │                        │
          └───────────────────────►│
                                   ▼
                            [Analytics + 
                             Model Feedback]
```

---

## Real-World References

- **Meta's PhotoDNA integration:** Hash-sharing via NCMEC. Auto-removes known CSAM in < 30 seconds.
- **YouTube's Content ID:** Hash + ML hybrid; removes copyrighted + harmful content.
- **Meta's engineering blog (2023):** "How we detect harmful content using ML at scale" — ensemble classifiers, 98.5% auto-decision rate.
- **AWS Rekognition Content Moderation API:** Used by smaller platforms; $0.001/image.
- **Microsoft's Project Aurora (PhotoDNA):** Industry standard hash-sharing for CSAM.
- **EU DSA (2024):** Very Large Online Platforms must have < 24hr response to illegal content reports.

---

## Budget-Constrained Reality

With $50K/day infra budget (smaller platform, 10M posts/day):
- Can afford: 3× g4dn.xlarge GPU inference = $38/day (handles ~10M images)
- Cannot afford: Dedicated video transcoding for frame analysis → sample 1 frame/5sec instead
- Human review budget: $10K/day → 83 reviewers → handle max 33,200 reviews/day
- Therefore: ML must auto-decide 99.67% of content. Threshold must be very aggressive.
- **Limitation:** 0.33% miss rate at 10M posts = 33,000 borderline pieces not reviewed daily. Acceptable for spam; not for CSAM.
- **Mitigation:** Use CSAM hash-check (free via NCMEC) as safety net, accept higher miss rate only on lower-severity categories.
