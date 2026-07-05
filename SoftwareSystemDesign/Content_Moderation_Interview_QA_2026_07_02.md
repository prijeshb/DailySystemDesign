# Content Moderation System — Interview Q&A
**Date:** 2026-07-02 | **System:** Content Moderation

---

## Opening Questions (Interviewers use these to set scope)

**Q: "Design a content moderation system for a social network with 1 billion users."**

Strong opener:
> "Before I design anything — do we need this at all, and what's the actual business constraint? At 1B users posting ~100M items/day, human-only review is impossible. The question is: how do we maximize catch rate while keeping false positives low enough that creators trust the platform? Let me clarify: is the primary threat CSAM, terrorism, spam, or hate speech — each has a different latency/accuracy tradeoff?"

---

## Follow-Up Questions Interviewers Ask

**Q: How would you detect CSAM specifically?**

> Use perceptual hashing (PhotoDNA, PDQ) against NCMEC's known-violation hash database. pHash is computed at upload time — O(1) Redis lookup, < 5ms. This is fundamentally different from ML: it's deterministic, auditable, required by law (EARN IT Act, EU CSAM regulation). The hash check runs synchronously before the content is published. ML classifiers run in parallel to catch novel CSAM variants not yet in the hash DB.

---

**Q: What if the ML model has a high false positive rate and legitimate creators get banned?**

> This is the core precision vs. recall tradeoff. I'd set per-category thresholds: for spam (low harm), accept more false positives. For hate speech, require higher confidence. The key design decision: auto-remove threshold (e.g., 0.99) + human review zone (0.70–0.99) + auto-approve zone (< 0.70). The threshold is configurable per policy — not hard-coded. I'd also add a shadow mode: run new model in parallel for 24hr before promoting; alert if its overturn rate on human-reviewed content is worse than baseline.

---

**Q: How do you handle real-time live video moderation?**

> Live video is a different problem — you can't wait for the full video to upload. Approach:
> 1. Segment the stream (HLS-like) into 2-second chunks
> 2. Run frame sampling (1 frame/sec) through image classifier inline
> 3. If confidence crosses threshold mid-stream: overlay a delay buffer (30 sec) and alert a human reviewer
> 4. If human confirms: cut stream, notify user
> The 30-second buffer buys time for human review without immediate auto-removal. This is what Twitch and YouTube Live do.

---

**Q: How do you prevent review queue overload during a breaking news event?**

> Three levers:
> 1. **Elastic supply:** Pre-contracted surge reviewer pool, activates automatically when queue depth exceeds 2× normal
> 2. **Demand shaping:** Temporarily raise auto-decision thresholds for low-severity categories (spam at 0.90 → auto-decide, freeing human capacity for P0)
> 3. **ML specificity:** Train event-specific classifiers (e.g., war imagery) that can auto-decide borderline wartime content without human review

---

**Q: How does a user appeal a wrongful removal?**

> Appeal triggers a state machine: PENDING → UNDER_REVIEW → (UPHELD | OVERTURNED | ESCALATED). The appeal is routed to a senior moderator different from the original reviewer. If overturned: content restored, user notified, and the content + original decision is added to the model's negative training set (the ML was wrong — use this example to improve it). EU DSA requires appeal mechanism available 24/7 and response < 24hr for VLOPs.

---

**Q: How do you handle coordinated inauthentic behavior (CIB) / bot farms?**

> Single-content classifiers miss CIB because each post might individually be innocuous. Need a behavioral graph layer:
> - Build account activity graph: accounts posting same content within 60sec, accounts created in same batch, accounts with identical device fingerprints
> - Run graph community detection (Louvain algorithm) to identify clusters
> - If cluster size > threshold and behavioral similarity > threshold → flag the entire cluster for manual review
> - This is what Meta's threat intelligence team does for election interference detection.

---

**Q: What's your database schema for the moderation decision history?**

```sql
-- Decisions table (append-only, never update)
CREATE TABLE moderation_decisions (
  decision_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  content_id      UUID NOT NULL,
  job_id          UUID NOT NULL,
  decision_type   ENUM('auto', 'human') NOT NULL,
  action          ENUM('approve', 'remove', 'warn', 'restrict') NOT NULL,
  policy_category VARCHAR(64),
  confidence      FLOAT,
  moderator_id    UUID,            -- NULL for auto decisions
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexed for: content history lookup, audit, model training
CREATE INDEX idx_decisions_content ON moderation_decisions(content_id, created_at);
CREATE INDEX idx_decisions_moderator ON moderation_decisions(moderator_id, created_at);
```

Append-only (no UPDATE/DELETE) — critical for audit trail and DSA compliance.

---

**Q: How would you handle a situation where the moderation system itself is used to suppress political speech?**

> Acknowledge this is a real risk — platform manipulation by bad actors or by the platform itself. Mitigations:
> 1. **Transparency reports:** Publish quarterly stats on removal rates by category and region
> 2. **Overturn rate monitoring:** If a specific category has > 20% appeal overturn rate, flag for policy review
> 3. **Independent audit:** Third-party auditors review a random sample of auto-removed content
> 4. **Adversarial testing:** Red-team specifically tests political content for disparate impact
> 5. **Geographic policy segmentation:** EU/US/India may have different legal requirements; moderation policies are region-parameterized

---

**Q: Why not just use a large language model (GPT-4 class) for all moderation?**

> Cost and latency. GPT-4 class: ~$0.03 per 1K tokens. At 100M posts/day × average 50 tokens = 5B tokens/day = $150K/day just for text. Inference latency: 1-3 seconds per request, not parallelizable for video. Specialized classifiers (BERT-fine-tuned, ResNet) are 100× cheaper, 10× faster, and more predictable (deterministic thresholds). LLMs are useful for: edge case resolution, generating training data explanations, moderator-assist tools. Not for primary classification pipeline at scale.

---

**Q: How do you measure the quality of your moderation system?**

> Key metrics:
> - **Precision:** Of all auto-removed content, what % was actually a violation? (Measure via human audit sample)
> - **Recall:** Of all actual violations, what % did we catch? (Measure via red-team seeding)
> - **False Positive Rate (FPR):** % of clean content incorrectly removed — tracks creator trust impact
> - **Appeal Overturn Rate:** % of human decisions overturned on appeal — tracks human reviewer quality
> - **Time-to-action P95:** Latency from upload to action for each severity tier
> - **DSA compliance rate:** % of illegal content reports actioned within 24hr

---

## Rapid-Fire Interview Concepts

| Question | Short Answer |
|----------|-------------|
| Why not just block the upload if ML isn't confident? | UX kill — false positives block legitimate content. Better to upload then moderate async. |
| How do you prevent hash evasion (slight image edits)? | PDQ hash is robust to minor crops/filters. For determined adversaries: ML is the backstop. |
| How is this different from spam filtering? | Spam = high volume, low harm, high recall OK. CSAM = low volume, extreme harm, 0% miss rate required. Different thresholds, pipelines, legal requirements. |
| What's the CAP tradeoff here? | Consistency > Availability for removal decisions (must not remove same content twice, must not miss confirmed violations). Availability > Consistency for human review queue (ok if two moderators see same task). |
| How do you train the model without violating reviewer privacy? | Moderator labels are anonymized (stored by moderator_id, not name) before ML training. Training data never includes reviewer identity. |
| What's "precision vs recall" in plain English? | Precision: don't punish innocent users. Recall: don't let harmful content slip through. You can't maximize both — tune per category. |
