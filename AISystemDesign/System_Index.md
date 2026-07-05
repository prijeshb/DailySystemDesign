# AI System Design — System Index

Last updated: 2026-07-04

---

## Completed Systems

| Date | System | Slug | Domain | Full Design | QA Playbook | Quick Ref |
|---|---|---|---|---|---|---|
| 2026-07-04 | Internal RAG Knowledge Base | `internal-rag-knowledge-base` | Enterprise Productivity / Knowledge Mgmt | [Full](./2026-07-04_internal-rag-knowledge-base.md) | [QA](./2026-07-04_internal-rag-knowledge-base-qa.md) | [Ref](./2026-07-04_internal-rag-knowledge-base-quickref.md) |

---

## Domain Coverage

| Domain | Systems Covered |
|---|---|
| Enterprise Productivity / Knowledge | Internal RAG Knowledge Base ✓ |
| Customer Support | — |
| Fintech / Fraud | — |
| HR / Recruiting | — |
| Healthcare | — |
| E-Commerce / Recommendation | — |
| Content / Media | — |
| Developer Tools | — |
| Sales / CRM | — |
| Operations / Logistics | — |

---

## Pending Systems (rotate through these, never repeat a completed slug)

Priority order rotates by domain — pick the domain least recently covered.

- `ai-customer-support-copilot` — Customer Support
- `fraud-scoring-fintech` — Fintech / Fraud
- `resume-screening-ats` — HR / Recruiting
- `contract-review-assistant` — Legal / Document AI
- `patient-intake-chatbot` — Healthcare
- `recommendation-engine` — E-Commerce
- `semantic-search-ecommerce` — E-Commerce / Search
- `content-moderation-pipeline` — Content / Trust & Safety
- `demand-forecasting-ml` — Operations / Logistics
- `personalization-engine` — E-Commerce / Media
- `ai-dev-assistant` — Developer Tools
- `voice-receptionist` — Customer Support / Telephony
- `invoice-ocr-pipeline` — Finance / Document AI
- `ai-tutor` — EdTech
- `churn-prediction` — SaaS / Customer Success
- `photo-background-removal` — Media / Creative Tools
- `meeting-summarizer` — Productivity
- `code-review-assistant` — Developer Tools
- `supply-chain-anomaly-detection` — Operations
- `real-time-translation-api` — Localization / NLP

**Next system to design:** `ai-customer-support-copilot` (Customer Support domain)

---

## Index Notes

- Default scale: funded startup or SMB (10–200 people, thousands to low-millions of requests/month)
- Prefer hosted model APIs and managed cloud services over self-hosted unless genuinely necessary
- Each system produces 3 files: full design, QA playbook, quick reference
- Concepts introduced by each system are tracked in Concepts_Index.md
