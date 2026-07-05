# Internal RAG Knowledge Base — Quick Reference

**Date:** 2026-07-04 | **Domain:** Enterprise Knowledge / RAG

**→ Full Design:** [2026-07-04_internal-rag-knowledge-base.md](./2026-07-04_internal-rag-knowledge-base.md)
**→ QA Playbook:** [2026-07-04_internal-rag-knowledge-base-qa.md](./2026-07-04_internal-rag-knowledge-base-qa.md)

---

## Key Numbers to Remember

| Metric | Value |
|---|---|
| Scale assumption | 100 employees, 30K queries/month |
| Corpus | 3,000 docs × 5 chunks = 15,000 chunks |
| Embedding size | 1,536 dims × 4 bytes = 6 KB/vector |
| Vector storage at 15K chunks | ~93 MB |
| Vector storage at 150K chunks (10×) | ~930 MB |
| Avg prompt tokens (input) | 2,750 (300 system + 2,400 chunks + 50 query) |
| Avg output tokens | 400 |
| LLM cost/month (GPT-4o-mini, 24K LLM hits) | ~$20 |
| LLM cost/month (GPT-4o, 24K LLM hits) | ~$375 |
| Total infrastructure + AI spend | ~$162/month |
| GPT-4o-mini self-hosting breakeven | ~2.8M queries/month |
| Query latency p50 | ~2,400ms (cached: 200ms) |
| Query latency p99 target | <4,000ms |
| Cache hit rate assumption | 20% |
| A100 cost (Lambda Labs) | $2.49/hr = $1,817/month |
| Ingestion cycle | 4 hours (scheduled) + webhook (real-time) |

---

## 5 Most Important Design Decisions

| Decision | One-Line Reason |
|---|---|
| **Chunk ID = hash(doc_url + chunk_index)** | Makes ingestion idempotent — re-running produces no duplicates; deletions can be tracked by diffing against Postgres |
| **Hybrid retrieval (dense + sparse, α=0.75)** | Pure dense fails on acronyms and exact-match; BM25 sparse closes the gap without a separate index |
| **Reranker after ANN (top_k=20 → top_k=8)** | Cross-encoder sees query+chunk together; 10–15% precision improvement over cosine ranking alone |
| **Gold-standard eval suite (50 queries, daily)** | Only signal that catches silent regressions (embedding drift, index staleness) before users notice |
| **Groundedness checker post-generation** | Catches LLM extrapolation and prompt injection effects; replaces hallucinated answers with safe fallback |

---

## Prototype vs. Production — Biggest Difference

**Prototype:** Rebuild the full FAISS index from scratch on every run, auto-generated chunk IDs.
**Production:** Deterministic hash-based chunk IDs, differential sync (upsert changed, delete removed), Pinecone upsert semantics.

**Why it matters:** Without hash IDs and explicit deletion tracking, deleted documents accumulate in the index forever, polluting retrieval with stale content. Migrating from random IDs to hash IDs in production requires a complete index rebuild (multi-hour outage).

---

## Component Failure → Mitigation

| Component | Failure Mode | Mitigation |
|---|---|---|
| Pinecone | API down (503/timeout) | Circuit breaker → BM25 full-text fallback in Postgres; auto-close after 60s probe |
| OpenAI Chat | 429 rate limit / 500 errors | Exponential backoff (3 retries); circuit to Anthropic Claude Haiku fallback |
| OpenAI Embeddings | API down | Queue queries; alert if embedding lag > 2 min; cached queries still served |
| Ingestion Pipeline | Silent failure (no sync) | Alarm: `TimeSinceLastSuccessfulIngestion > 6h`; webhook supplements scheduled sync |
| Redis Cache | Node down | Cache misses treated as bypass (never blocking); all queries fall through to full pipeline |
| Cohere Rerank | API down | Fall back to cosine similarity ordering of top-20 chunks |

---

## AI-Specific Failure Modes

| Failure | Detection | Prevention |
|---|---|---|
| **Embedding drift** — query model diverges from index model | `GoldRetrievalAccuracy` drops >5pp from 30-day baseline | Pin model version; re-embed full corpus on model change (blue-green) |
| **Index staleness** — doc updated, vector stale | `DocumentFreshnessLagMinutes > 300` alarm | Webhook-based ingestion for near-real-time updates |
| **Deleted doc vectors accumulating** | `GoldRetrievalAccuracy` degrades; citation URLs return 404 | Deletion sync: diff live source against Postgres, delete orphaned vectors |
| **Silent LLM quality regression** — model version drift | `AutoEvalScore` drops >0.3 from 14-day baseline | Pin model version; regression suite on every deploy |
| **Prompt injection** — malicious text in a document | `GroundednessCheckerFlagRate` rises; answers with no citations | XML delimiters; chunk sanitization; groundedness checker |
| **Feedback loop poisoning** — biased raters skew A/B results | `AutoEvalScore` and `HumanPositiveFeedbackRate` disagree | AutoEval as primary A/B metric; user stratification; concordance check |
| **Cost runaway** — ingestion re-embeds everything every run | `EmbeddingAPITokensPerHour` > 3× rolling average | Safety check: abort if >30% of corpus flagged as changed in one run |

---

## Trade-off Table (Condensed)

| Choice | Pick A when | Pick B when |
|---|---|---|
| Pinecone vs. pgvector | Corpus > 100K vectors or growing fast | Small corpus, existing Postgres expertise, cost-sensitive |
| GPT-4o-mini vs. GPT-4o | Budget-constrained; most queries are single-doc lookups | Multi-hop synthesis across 3+ documents; accuracy is critical |
| Dense-only vs. Hybrid retrieval | Purely semantic corpus; no acronyms or product codes | Technical docs with acronyms, exact names, product IDs |
| Fixed-size chunking vs. Semantic chunking | Need speed and predictability at ingestion | Corpus has strong semantic structure (headers, sections) |
| Redis cache vs. No cache | Repeat-query rate > 15%; latency SLA < 500ms for common queries | Ultra-simple architecture; < 5K queries/month |
| Explicit (👍/👎) vs. Implicit feedback | Clear signal quality needed for A/B tests | High-volume signal needed; user friction is unacceptable |

---

## Concepts Touched

- **RAG (Retrieval-Augmented Generation)** — core pattern; retrieve then generate
- **Embeddings** — dense vector representation of text for semantic similarity
- **ANN / HNSW** — approximate nearest-neighbor search used by Pinecone internally
- **Hybrid retrieval (dense + sparse / BM25)** — combine semantic + keyword for better recall
- **Chunking** — splitting documents into retrieval-sized units with overlap
- **Reranking (cross-encoder)** — second-stage relevance scoring; sees query+doc together
- **Embedding drift** — silent failure when index and query model versions diverge
- **Index staleness** — silent failure when source docs update but vectors don't
- **Idempotency** — deterministic chunk IDs make ingestion safe to retry
- **Semantic cache** — quantized embedding key enables cache on similar (not identical) queries
- **Guardrails** — moderation, access control, groundedness checking
- **Prompt injection** — attack via malicious content in retrieved documents
- **LLM-as-judge** — use stronger LLM to evaluate weaker LLM's outputs
- **Feedback loops** — explicit and implicit signals for quality monitoring
- **Circuit breaker** — fail fast + fallback when dependency degrades
- **Blue-green deployment** — parallel namespaces for safe embedding model migration
- **Silent degradation** — quality failures with no infrastructure error signal
- **Cost estimation / token budgeting** — explicit arithmetic to control API spend
- **Sharding** — Pinecone namespaces as tenant-level shards
- **Rate limiting** — separate API key budgets for query vs. ingestion paths

---

## 3 Sources Worth Reading

1. **[Lewis et al., RAG paper (NeurIPS 2020)](https://arxiv.org/abs/2005.11401)** — foundational architecture; understand this before any RAG interview.

2. **[Eugene Yan, "Patterns for Building LLM-based Systems"](https://eugeneyan.com/writing/llm-patterns/)** — the most practically useful single guide to production RAG; covers evaluation, feedback loops, and retrieval patterns.

3. **[Shi et al., "LLMs Can Be Easily Distracted by Irrelevant Context" (ICML 2023)](https://arxiv.org/abs/2302.00093)** — empirical evidence for why chunk quality and reranking matter; explains degradation from irrelevant context in the prompt.
