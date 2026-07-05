# Daily Software System Design

## Goal

Create one new daily software system design deep dive. Use minimal tokens. No fluff. Check existing files before choosing today's system to avoid duplicates.

## Outputs

Required files:

- `<System>_System_Design_<YYYY_MM_DD>.md`
- `<System>_Failure_Analysis_<YYYY_MM_DD>.md`
- `<System>_Interview_QA_<YYYY_MM_DD>.md`
- Update `Concepts_Index.md` when new/changed concepts appear.

## Pick Today's System

1. Inspect existing system design files first.
2. Avoid duplicate systems already present.
3. Pick one practical system, such as:
   - news aggregator with feed
   - Google Drive / Dropbox
   - parking lot system
   - API gateway
   - payments
   - notification system
   - search autocomplete
   - ride sharing
   - chat
   - video streaming
4. Prefer systems that expose concepts like failure handling, sharding, idempotency, caching, queues, replication, consistency, rate limiting, indexing, pagination, fanout, backpressure, and observability.

## Research

Use web search. Prefer high-signal sources:

- engineering blogs
- architecture writeups
- incident reports
- official cloud docs/pricing pages
- academic/industry PDFs
- real-world examples
- existing notes, especially `Concepts_Index.md`
- reference prior chat idea: Design Parking Lot System

## First Principles Thought Process

For every major component, ask:

- Do we need this at first?
- If yes, why?
- What breaks without it?
- What simpler alternative exists?
- What does it cost in latency, money, complexity, freshness, operations?

## System Design File Flow

Use this order:

1. Problem scope and assumptions
2. Discover entities
3. Discover actions / APIs
4. Data flow
5. High-level design
6. Low-level design
7. Storage model
8. Scaling strategy
9. Back-of-envelope calculation
10. Budget-aware AWS + industry average cost estimate
11. Budget-constrained example and limitations
12. Component tradeoffs
13. Concepts used and why
14. Final architecture summary

## Back-of-Envelope + Cost

Include concise estimates for:

- DAU/MAU/QPS
- read/write ratio
- storage per day/month/year
- bandwidth
- cache size
- queue throughput
- database partitions/shards
- rough AWS service costs
- rough industry average cost assumptions

Add one budget-constrained example:

- budget cap
- chosen architecture
- what is sacrificed
- where it fails first
- upgrade path

## Component Tradeoffs

For each meaningful component, include pros/cons.

Examples:

- Cache: faster reads, lower DB load; stale data, invalidation complexity.
- Queue: absorbs spikes, async retries; delayed processing, poison messages.
- Sharding: higher write capacity; resharding and hot partition risk.
- CDN: lower latency and bandwidth cost; cache purge/freshness issues.
- Idempotency keys: safer retries; extra storage and key lifecycle complexity.

## Failure-First Analysis File

Start from failures, not happy path.

Cover:

- what if this component fails?
- what if the component ahead fails?
- what if the component behind fails?
- partial failure
- timeout
- retry storm
- stale reads
- duplicate writes
- data loss
- hot shard / hot key
- regional outage
- dependency outage
- overload and backpressure
- monitoring signals
- prevention / mitigation / recovery

Include in-depth but concise solutions.

## Interview QA File

Create interview-style Q&A:

- interviewer prompt
- likely follow-up
- concise answer
- tradeoff answer
- failure-mode answer
- cost/budget answer
- scaling answer
- data-model answer

Focus on how interviewers ask and probe.

## Concepts_Index Update

Read `Concepts_Index.md` before writing.

For each concept used:

- add if missing
- update pros/cons if weak
- include real-world use notes
- include failure modes
- keep concise

Concept examples:

- caching
- sharding
- idempotency
- replication
- consistency
- queues
- rate limiting
- circuit breakers
- retries
- deduplication
- pagination
- indexing
- fanout
- backpressure
- observability

## Style Rules

- Minimal tokens.
- No padding.
- No repeated explanations.
- Prefer tables only when compact.
- Use bullets over prose.
- Include concrete numbers where possible.
- Mention tradeoffs for every chosen component.
- Keep every output file interview-useful.
