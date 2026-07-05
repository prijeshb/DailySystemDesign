# GitHub — Interview Q&A
**Date:** 2026-07-05

---

## Opening Question Variations

**"Design GitHub"**
**"Design a code hosting platform"**
**"Design Git-as-a-service"**
**"Design a version control system"**

---

## Q1: "Walk me through how git push works end-to-end."

**Good answer:**
```
1. Client runs git push → establishes connection to Git HTTP/SSH server
2. Smart HTTP protocol: client sends "want" (desired remote SHAs) + "have" (local SHAs)
3. Server computes diff: objects in "want" but not in "have" = what needs uploading
4. Client packs missing objects into a packfile → sends to server
5. Server validates pack (SHA integrity check) → writes objects to temp dir
6. Server uploads objects to S3 (content-addressed by SHA)
7. Server updates branch ref in Postgres: refs(repo_id, 'refs/heads/main') = new_sha
8. Server emits push event to Kafka → downstream consumers (webhooks, CI, feed)
9. Server returns success to client

Key: steps 6 and 7 are sequential. If step 6 fails, step 7 never runs → branch unchanged → safe.
```

**Follow-up: "What if the server crashes between step 6 and 7?"**
```
Objects in S3 but no ref pointing to them = dangling objects.
Branch ref not updated → branch appears unchanged to client.
Client retries push → "have/want" negotiation: server already has objects → skips upload.
Server re-tries ref update → succeeds.
Dangling objects from crashed attempt: collected by weekly GC job.
```

---

## Q2: "How do you store git objects? Why content-addressing?"

**Good answer:**
```
Git objects: commits, trees, blobs. Each stored at path = SHA-1(type + size + content).

Why content-addressing:
1. Deduplication: same file content = same SHA = stored once.
   1000 forks of a repo → common files stored once, not 1000 times.
   
2. Integrity: content-addressed → SHA mismatch = corruption detected.
   Can verify any object by recomputing SHA and comparing.
   
3. Immutability: you can't modify an object without changing its SHA.
   Full audit trail for free.

Storage: S3 at path: {repo_namespace}/{sha[0:2]}/{sha[2:]}
Two-level directory: sha[0:2] gives 256 pseudo-random prefixes → avoids S3 "hot partition" on sequential listing.
```

**Follow-up: "How do you handle forks? Do you copy all objects?"**
```
No — copy-on-write semantics.
Fork creates:
  - New repository row in DB (fork_parent_id = original.id)
  - Copies refs table (same commit SHAs, no object copy)
  
Objects: fork reads from parent's namespace transparently.
First push to fork: only NEW objects written to fork's namespace.
Common objects: still read from parent.

If parent is deleted: pre-deletion job copies shared objects to fork's namespace.
```

---

## Q3: "How does pull request diff work at scale?"

**Good answer:**
```
Step 1: Find merge base
  merge_base = git merge-base(head_sha, base_sha)
  = lowest common ancestor commit in the DAG
  This is the "split point" — changes since then = the diff

Step 2: Compute diff
  For each changed file: Myers diff algorithm
  O(N × D) time: N=lines, D=edit distance (≈ lines changed)
  Output: unified diff hunks

Step 3: Cache
  Cache key: diff:{base_sha}:{head_sha}
  Store in Redis, TTL=24h (most PRs reviewed same day)
  
Async: PR creation doesn't block on diff. Returns PR ID immediately.
       Background worker computes diff → SSE push to browser.

Scale limits:
  >3000 changed files → truncate (show first 3000 + banner)
  >1MB per file → skip (binary or generated)
  >512MB memory → return partial diff + graceful truncation
```

**Follow-up: "What if someone pushes more commits to the PR?"**
```
head_sha changes → old diff cache key no longer valid (different head_sha).
New diff computed async for new head_sha.
Old diff: GC'd when TTL expires.
PR always shows diff of current head_sha vs merge-base.
```

---

## Q4: "How do you handle webhook delivery reliably?"

**Good answer:**
```
Two-layer safety:

Layer 1 — Outbox:
  Before publishing to Kafka: INSERT INTO webhook_outbox (event_id, payload, delivered=false)
  If Kafka publish fails: outbox has the event, poller retries every 30s
  If Kafka succeeds: mark outbox row delivered

Layer 2 — Retry with backoff:
  Webhook dispatcher: consumes Kafka, calls webhook.url
  Success (2xx): done
  Failure: retry at 1s, 5s, 30s, 5min, 30min
  After 6 fails: deactivate webhook, email owner

Signing: HMAC-SHA256(payload, webhook.secret) → X-Hub-Signature-256 header
  Customer validates signature → ensures payload from GitHub, not third party

Timeout: 10s hard limit per call (can't block on slow endpoints)
Circuit breaker: if endpoint fails >50% over 5min → stop calling → probe every 10min
Isolation: each webhook in separate goroutine + per-host concurrency limit
```

**Follow-up: "What delivery guarantee do you provide?"**
```
At-least-once. We may deliver duplicate events (retry after success if response lost).
Customers should make CI systems idempotent:
  "Build trigger for commit sha:abc123 already exists" → no-op.
GitHub documents this: customers use X-GitHub-Delivery header to deduplicate.
Exactly-once is impractical (2PC across HTTP → very expensive and still not perfect).
```

---

## Q5: "How would you design code search?"

**Good answer:**
```
Indexing pipeline:
  Push event → Kafka → Search Indexer worker
  Worker: fetch changed blobs from S3 → tokenize (language-aware) → index in Elasticsearch
  
  ES document per file:
  {
    repo_id: 12345,
    path: "src/main.go",
    content: "...",  // full text
    language: "Go",
    sha: "abc...",
    visibility: "public"
  }
  
  Shard by repo_id: all files of one repo on same shard (repo-level queries efficient)

Query: "org:mycompany language:Python def connect"
  1. Parse: org filter → get repo_ids for org (Postgres)
  2. ES query: bool filter (repo_id IN [list], language=Python) + match (def connect)
  3. Return: top 100 results with highlighting
  4. Permission post-filter: remove repos user can't read (private repos)

Scale: 50B+ documents.
  ES index separated by: public repos (large, high read) vs private repos (sensitive, smaller)
  Index tiering: hot (NVMe) for recently-modified repos, warm (HDD) for inactive
```

**Follow-up: "How do you handle private repo search security?"**
```
Critical: user can only search repos they have read access to.

Option A: Permission post-filter (simple)
  ES returns results → server filters to repos user can read → return filtered
  Risk: ES may return 100 results, 99 get filtered → poor UX

Option B: Repo ID allowlist in ES query (better)
  Server pre-computes: repos this user can read (from AuthZ service, cached 5min)
  ES query: repo_id IN [user's allowed repo_ids]
  Problem: user in large org → 100K repos → 100K item allowlist filter → slow ES query

Option C: Tenant-level index isolation (most secure at cost of complexity)
  Private repos: indexed in per-org ES indices with org-level ACL
  Public repos: single shared index (no auth needed)
  Query routing: user's private search → their org's index; public search → shared index
```

---

## Q6: "How do you scale git clone for a 10GB monorepo?"

**Good answer:**
```
Problem: computing packfile for each clone request is expensive.
  10GB repo → server reads all objects from S3, computes delta-compressed packfile → streams
  1000 simultaneous clones → 10TB reads from S3, massive CPU to compute packs

Solution 1: Pre-computed base packfile
  Nightly job: git pack-objects --all → full pack → upload to S3
  Clone request: client downloads pack from S3 (via CDN) + fetches delta since pack date
  Git server only handles the delta (small)
  
Solution 2: Shallow clone (for CI)
  git clone --depth=1 → only latest commit, not full history
  10GB → ~500MB with shallow clone
  Enforce via CI system configuration

Solution 3: Partial clone
  git clone --filter=blob:none → download commits+trees only, blobs on demand
  git sparse-checkout → only fetch files in specific directory
  For monorepos: devs working on one service only fetch that service's files

CDN caching:
  Base packfile rarely changes (daily rebuild) → very high CDN hit rate
  Effectively: CDN absorbs 95%+ of clone traffic; git server handles only deltas
```

**Follow-up: "What's the bandwidth cost?"**
```
Without optimization:
  10GB × 1000 devs × $0.09/GB (S3 transfer) = $900 per Monday morning
  
With CDN (Cloudflare or CloudFront):
  CDN caches pack file → 1 S3 fetch → 1000 CDN hits
  S3 transfer: 10GB × 1 = $0.90
  CDN outbound: 10GB × 1000 × CloudFront price = $85 (at $0.085/GB)
  Total: ~$86 instead of $900 — 10× savings
```

---

## Q7: "How do you handle access control?"

**Good answer:**
```
Permission model:
  User → Repository: {read, write, admin}
  User → Organization → Repository: via team membership

Centralized AuthZ service:
  All resource access: AuthZ.check(user_id, repo_id, action) → allow/deny
  Never ad-hoc permission checks in business logic
  
Auth token types:
  1. Personal Access Token (PAT): long-lived, scoped to specific permissions
  2. OAuth token: for third-party app integrations
  3. Deploy key: repo-specific SSH key for CI/CD
  4. GITHUB_TOKEN: short-lived (1h), scoped to specific workflow run

Token validation:
  JWT for API: stateless validation (no DB needed per request)
  SSH: public key lookup from user's keys table → validate signature
  
Fail-closed:
  AuthZ service down → deny all access (not fail-open)
  Cache positive decisions in app: 5min TTL
  → Max 5min stale permission window after access revoked
```

**Follow-up: "How do you revoke access instantly?"**
```
Challenge: JWTs are stateless → can't revoke without checking a revocation list.

Option A: Short JWT TTL (15 min)
  Wait up to 15 min → JWT expires → user must re-auth
  Revoked user can still read for up to 15 min
  Simple, no revocation infrastructure

Option B: Revocation list in Redis
  On revoke: SADD revoked_tokens {jti} EX 3600
  Every request: check Redis for jti in revoked set
  Effective immediately. Cost: 1 Redis read per request.

Option C: Permissions fingerprint in JWT
  JWT contains: SHA-256(sorted list of repo_ids user can read)
  On access revoked: permissions list changes → fingerprint changes
  Server computes current fingerprint from AuthZ → mismatch → force re-auth
  No revocation list needed. Check AuthZ service (cached 5min) instead.
```

---

## Q8: "How would you implement repository forking?"

**Good answer:**
```
Fork = shallow copy of metadata, shared object store.

On fork request:
  1. INSERT INTO repositories (owner_id=user, fork_parent_id=source.id, ...)
  2. Copy refs: INSERT INTO refs (SELECT repo_id=new_id, name, sha FROM refs WHERE repo_id=source.id)
  3. No object copy: fork's namespace in S3 starts empty
  
Object resolution (on git fetch/clone of fork):
  Lookup: does object exist in fork's namespace?
    YES → serve from fork
    NO  → serve from parent repo's namespace
  Transparent to client.

On first push to fork (new commit):
  New objects (commits, trees, changed blobs) → written to fork's namespace
  Unchanged blobs → still served from parent's namespace
  
Fork of fork (grandchild):
  Resolution: child namespace → parent namespace → grandparent namespace
  N-deep lookup → flatten: copy objects to direct-child namespace on first access (lazy materialization)
  Max depth: prevent infinite chains (limit fork depth to 5 or flatten on fork)

Parent deletion:
  Pre-deletion check: does any fork depend on this repo's objects?
  YES → copy shared objects to each fork's namespace before deletion
  This is an async job; deletion is soft-deleted first, hard-deleted after all forks copied
```

---

## Q9: "Design the data model for pull request code review."

**Good answer:**
```sql
-- Review: overall verdict (one per reviewer per PR)
pr_reviews (
  id, pr_id, reviewer_id,
  status: pending | approved | changes_requested | dismissed,
  body,  -- overall review summary
  submitted_at
)

-- Comment: line-level or general
pr_comments (
  id, pr_id, reviewer_id,
  type: line | file | pr,  -- where it's attached
  commit_sha,   -- which commit this comment is on (diff can shift on new push)
  path,         -- file path (null for PR-level)
  line,         -- line number in diff
  side,         -- LEFT (old) or RIGHT (new) side of diff
  body,
  created_at
)

-- Comment thread (replies to a comment)
pr_comment_replies (
  id, parent_comment_id, reviewer_id, body, created_at
)
```

**Tricky case: comment position on stale diff**
```
Reviewer comments on line 42 of file.go.
Author pushes new commits.
Line 42 may have shifted to line 50.

Solution: store commit_sha on comment.
  Show comment in context of the commit it was made on.
  "Outdated" badge if comment's commit ≠ current head_sha.
  Line position tracking: compute diff between comment's sha and current head → find new line number.
  GitHub calls this "comment position tracking" — complex, done lazily.
```

---

## Q10: "How would you implement GitHub Actions (CI/CD trigger system)?"

**Good answer (high-level):**
```
Trigger: push to main, PR opened, tag created → emit event to Kafka

Workflow definition: YAML in .github/workflows/*.yml in the repo itself
  (version-controlled with code — genius design decision)

Execution flow:
  1. Event arrives at Actions service
  2. Parse workflow files for matching trigger
  3. Create workflow run record (DB)
  4. Queue jobs (individual steps) to job queue
  5. Runner picks up job:
     - Ephemeral VM/container (fresh per job)
     - Clone repo (shallow, at the push SHA)
     - Execute steps
     - Report step status back to Actions service
  6. Actions service:
     - Updates run status in DB
     - POST to commit status API (sets green/red check on commit)
     - This check status is what GitHub checks for "required CI" before merge

Runner isolation:
  Each job: separate ephemeral VM (security boundary)
  GitHub-hosted: AWS/Azure VMs provisioned on demand
  Self-hosted: customer's own machines poll for jobs (outbound connection only, no inbound firewall rule needed)

Status propagation:
  Actions → POST /repos/{owner}/{repo}/statuses/{sha}
  PR: checks table (commit_sha, context, state: pending/success/failure)
  Merge button: enabled only when required checks = all green
```

---

## Rapid-Fire Interview Follow-ups

**"What database would you use for storing git objects?"**
→ Not a DB. Object store (S3/GCS). Content-addressed, immutable, arbitrary size. DBs are not optimized for this (BLOB storage is slow, expensive, and can't be content-addressed at DB layer).

**"Why SHA-1 and not SHA-256 for git objects?"**
→ Historical (Linus chose SHA-1 in 2005 for speed). SHA-1 collisions are theoretically possible but git has collision mitigations. GitHub + git are migrating to SHA-256 (git 2.29+, SHA-256 object format).

**"How does GitHub handle secrets accidentally pushed to repos?"**
→ Secret scanning: regex patterns for known secret formats (AWS keys, Stripe keys, etc.) run on every push. Match found → notify owner + notify the secret provider (AWS, Stripe) to revoke → provider auto-revokes before attacker can use.

**"How do you handle merge conflicts?"**
→ Server-side: when attempting merge, run git merge algorithm. Conflict detected → return conflict markers to user. Client must resolve. Conflict resolution always happens client-side (git merge locally → push resolved commit).

**"What's the capacity limit before you need to shard Postgres?"**
→ Metadata only (no objects). Single Postgres handles 100M+ repos (repos table: ~500GB at 5KB/repo). Partition by repo_id range (Postgres declarative partitioning). Shard only if writes exceed single primary capacity (~10K writes/sec for Postgres).

**"How do you prevent repo size abuse (100GB repos)?"**
→ Disk quota per repo (e.g., 100MB soft / 5GB hard). Pre-push hook: server checks current disk_usage + incoming pack size. Over quota → reject push with clear error message. LFS (Git Large File Storage): large binaries stored separately in dedicated blob store, only pointer in git repo.
