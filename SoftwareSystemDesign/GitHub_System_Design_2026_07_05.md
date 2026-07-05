# GitHub (Code Hosting Platform) — System Design
**Date:** 2026-07-05

---

## First Principles

**Do we need a code hosting platform?**
Teams need: version history, collaboration on code changes, code review, CI/CD integration.

Could use local git + email patches? Works for the Linux kernel. Terrible UX for product teams.
GitHub's real value: **hosted central git repos + a collaboration workflow layer on top**.

Core question: is this mainly a storage problem or a workflow problem?
→ Both. Storage of git objects is unique (content-addressed, append-mostly). Workflow is event-driven.

---

## Entities

| Entity | Key Fields |
|--------|-----------|
| **User** | id, username, email, password_hash |
| **Organization** | id, name, plan |
| **Repository** | id, owner_id, owner_type, name, visibility, fork_parent_id, disk_usage_kb |
| **Ref** | repo_id, name (`refs/heads/main`), sha |
| **Commit** | sha (content-addressed), tree_sha, parent_shas[], author, message |
| **Tree** | sha, entries: [{name, mode, sha, type}] |
| **Blob** | sha, size, bytes (stored in object store) |
| **PullRequest** | id, repo_id, number, source_branch, target_branch, base_sha, head_sha, status |
| **PRReview** | id, pr_id, reviewer_id, status (approved/changes_requested/dismissed) |
| **PRComment** | id, pr_id, path, line, side, body, commit_sha |
| **Webhook** | id, repo_id, url, events[], secret_hash |

---

## Actions & Data Flow

### Push Flow
```
git push → Git HTTP/SSH server
  1. Authenticate user → check write permission on repo
  2. Receive pack objects (commits, trees, blobs) via smart HTTP protocol
  3. Write objects to temp dir → validate → move to object store (S3)
  4. Update ref in DB: refs(repo_id, 'refs/heads/feature') = new_sha
  5. Emit event → Kafka topic: repo.push → webhooks, CI triggers, feed updates
```

### Clone/Fetch Flow
```
git clone/fetch → Git HTTP server
  1. Auth check (anonymous OK for public repos)
  2. Client sends "want" (desired SHAs) + "have" (local SHAs)
  3. Server computes missing objects → packs them into a packfile
  4. Stream packfile to client
  5. Large repos: serve pre-computed base packfile from S3/CDN + delta
```

### Pull Request Lifecycle
```
Open PR:
  → compute merge-base(source_sha, target_sha)
  → async: compute diff → cache in Redis keyed by (source_sha:target_sha)
  → notify reviewers (notification service)
  → create PR record in DB (status=open)

Review:
  → fetch diff from cache (or recompute if expired)
  → POST /comments → stored in pr_comments table
  → POST /reviews   → stored in pr_reviews (approve / request changes)

Merge:
  → check merge conditions: required approvals met, CI checks passed
  → execute merge strategy (merge commit / squash / rebase)
  → update target branch ref in DB
  → close PR (status=merged)
  → emit pr.merged event → Kafka
```

---

## Back-of-Envelope

**Assumptions (GitHub scale):**
| Metric | Value |
|--------|-------|
| Repositories | 100M |
| Git objects (blobs, commits, trees) | 50B |
| Pushes/day | 100K (~1.2/sec) |
| Clone+fetch ops/day | 50M (~580/sec) |
| PR events/day | 10M |
| Avg repo size on disk | 50MB (pre-dedup) |

**Storage:**
```
Naive: 100M repos × 50MB = 5PB
Content-addressed deduplication: same file in 1000 forks → stored once
Effective: ~500TB object store (est. 10× dedup ratio)
```

**AWS Cost Estimate:**
| Component | Spec | Monthly Cost |
|-----------|------|-------------|
| S3 (500TB objects) | Standard storage | ~$11,500 |
| Git servers (EC2) | 50 × m6i.2xlarge | ~$14,000 |
| RDS Postgres (Multi-AZ) | r6g.4xlarge primary + 2 replicas | ~$3,500 |
| ElasticSearch (code search) | 10 × r6g.2xlarge | ~$5,500 |
| Kafka (MSK) | 3 brokers r6g.xlarge | ~$2,300 |
| CloudFront CDN | 2PB/month outbound | ~$170,000 |
| **Total (rough)** | | **~$210K/month** |

CDN dominates. GitHub uses Fastly + Akamai with custom negotiated rates (~80% discount at scale → ~$40K/month effective).

**Budget-Constrained Version ($5K/month):**
- Use Cloudflare free tier (unlimited CDN bandwidth for cached assets)
- Single Postgres + 1 read replica (no Multi-AZ)
- 3 shared Git servers (t3.xlarge) instead of 50
- No ElasticSearch → basic LIKE search only
- Limitation: supports ~100K repos, ~10K active users; no code search; EU/Asia users see 200-400ms clone latency

**Budget constraint trade-off:**
> Removing Multi-AZ Postgres saves $2K/month but introduces 60-90s downtime risk on AZ failure. For a startup with < 1000 paying users, acceptable. For 10K+ paying users, not acceptable — one incident erodes customer trust enough to lose more than $2K.

---

## High-Level Architecture

```
[Browser / git CLI]
        │
  ┌─────▼──────────────────────────────────────┐
  │  Load Balancer (ALB)                        │
  └──────┬───────────────────┬─────────────────┘
         │                   │
  ┌──────▼──────┐   ┌────────▼───────┐
  │ Git Servers │   │  Web/API Layer │
  │ (HTTP+SSH)  │   │ (REST/GraphQL) │
  └──────┬──────┘   └────────┬───────┘
         │                   │
  ┌──────▼───────────────────▼─────────────────┐
  │                 Core Services               │
  │  ┌──────────┐ ┌──────────┐ ┌────────────┐  │
  │  │ Repo Svc │ │  PR Svc  │ │  Auth Svc  │  │
  │  └────┬─────┘ └────┬─────┘ └────┬───────┘  │
  └───────┼────────────┼────────────┼───────────┘
          │            │            │
  ┌───────▼────────────▼────────────▼───────────┐
  │                  Data Layer                  │
  │  ┌───────────┐  ┌───────┐  ┌──────────────┐ │
  │  │ Postgres  │  │ Redis │  │  S3 (objects │ │
  │  │ (metadata)│  │(cache)│  │  + packfiles)│ │
  │  └───────────┘  └───────┘  └──────────────┘ │
  │  ┌───────────────────────────────────────┐   │
  │  │        Elasticsearch (code search)    │   │
  │  └───────────────────────────────────────┘   │
  └─────────────────────────────────────────────┘
          │
  ┌───────▼──────────────────────────────────────┐
  │              Kafka (Event Bus)               │
  │  repo.push | pr.event | ci.status | webhook  │
  └──────────────────────────────────────────────┘
          │
  ┌───────▼──────────────────────────────────────┐
  │           Background Workers                 │
  │  Webhook Dispatcher | Search Indexer         │
  │  Diff Computer      | GC Job                 │
  │  Notification Sender                         │
  └──────────────────────────────────────────────┘
```

---

## Low-Level Design

### 1. Git Object Storage

Git objects are content-addressed: stored at `SHA-1(content)`.

**Types:**
- **blob** — raw file bytes
- **tree** — directory: list of {name, mode, type, sha} entries
- **commit** — tree_sha + parent_sha(s) + author + message

**S3 layout:**
```
s3://gh-objects/{shard}/{sha[0:2]}/{sha[2:40]}
```
Shard by first 2 chars of SHA (256 prefix shards) for S3 listing performance.

**Cross-repo deduplication:**
Same file in 1000 forks → same SHA → stored once. Objects are immutable.

**Fork semantics:**
```
Fork creates:
  - New repository row in DB (fork_parent_id = parent.id)
  - Copies refs table (same SHAs — no object copy)
  - Objects: read from parent's namespace transparently

On first push to fork:
  - New objects written to fork's namespace
  - Old objects still shared via parent
  - Copy-on-write: diverge only where you change
```

### 2. Database Schema (Key Tables)

```sql
CREATE TABLE repositories (
  id             BIGINT PRIMARY KEY,
  owner_id       BIGINT NOT NULL,
  owner_type     TEXT CHECK (owner_type IN ('user', 'org')),
  name           TEXT NOT NULL,
  visibility     TEXT CHECK (visibility IN ('public', 'private', 'internal')),
  fork_parent_id BIGINT REFERENCES repositories(id),
  default_branch TEXT DEFAULT 'main',
  disk_usage_kb  BIGINT DEFAULT 0,
  created_at     TIMESTAMPTZ DEFAULT now(),
  UNIQUE (owner_id, owner_type, name)
);

CREATE TABLE refs (
  repo_id    BIGINT REFERENCES repositories(id),
  name       TEXT,          -- 'refs/heads/main', 'refs/tags/v1.0'
  sha        CHAR(40) NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (repo_id, name)
);

CREATE TABLE pull_requests (
  id            BIGINT PRIMARY KEY,
  repo_id       BIGINT NOT NULL REFERENCES repositories(id),
  number        INT NOT NULL,
  status        TEXT CHECK (status IN ('open', 'closed', 'merged')) DEFAULT 'open',
  source_repo_id BIGINT REFERENCES repositories(id),  -- cross-fork PR
  source_branch TEXT NOT NULL,
  target_branch TEXT NOT NULL,
  base_sha      CHAR(40),   -- merge-base at PR creation time
  head_sha      CHAR(40),   -- latest commit on source branch
  created_by    BIGINT NOT NULL,
  merged_at     TIMESTAMPTZ,
  UNIQUE (repo_id, number)
);
```

### 3. Merge Strategies

```
Strategy 1 — Merge Commit (default):
  main:    A─B─C───────M
                      ╱
  feature: D─E─F─────╱
  M has two parents: C + F. Full history preserved.

Strategy 2 — Squash Merge:
  main:    A─B─C─S
  S = single commit containing sum of D+E+F changes.
  Feature branch history discarded. Clean main.

Strategy 3 — Rebase Merge:
  main:    A─B─C─D'─E'─F'
  D',E',F' = replayed on top of C. New SHAs. Linear history.

When to use:
  OSS projects → merge commit (preserve contributor attribution)
  Fast-moving team → squash (easy git bisect, clean log)
  Strict linear history → rebase (monorepo teams)
```

### 4. Diff Computation

```
PR opened → Diff Service:
  1. Merge base = git merge-base(head_sha, base_sha)
     = LCA (lowest common ancestor) in commit DAG
  
  2. Per changed file: Myers diff algorithm
     O(N×D) time, N=lines, D=edit distance
     Output: unified diff hunks (context + added/removed lines)
  
  3. Cache diff:
     Redis key: diff:{base_sha}:{head_sha}
     TTL: 24h (most PRs reviewed < 1 day)
     Size cap: 10MB diff → truncate at 3000 files, show "N files not shown"
  
  4. Async computation:
     PR creation doesn't block on diff. Returns PR ID immediately.
     Diff computed by background worker → pushes update via SSE to browser.
```

### 5. Webhook Dispatch

```
On repo event:
  Kafka topic: webhook-events
  payload: {repo_id, event, sha, sender, webhook_configs[]}

Webhook Dispatcher Consumer:
  For each repo webhook matching event:
    1. Build payload (JSON)
    2. Sign: HMAC-SHA256(payload, webhook.secret) → X-Hub-Signature-256 header
    3. POST webhook.url with 10s timeout
    
    Success (2xx): log delivery, mark done
    Failure:       exponential backoff (1s → 5s → 30s → 5min → 30min)
    After 5 fails: deactivate webhook, email owner
    
    Circuit breaker per destination host:
      >50% error rate over 5min → open (stop calling), alert owner
      Probe every 10min → if success: close circuit

Outbox safety:
  Push event → INSERT INTO webhook_outbox before Kafka publish.
  Consumer marks outbox row delivered.
  On startup: scan outbox for undelivered rows → replay.
```

### 6. Code Search

```
Indexing pipeline:
  Push event → Search Indexer worker:
    1. For each changed file in commit:
       - Fetch blob from S3
       - Parse into tokens (language-aware tokenizer)
       - Index into Elasticsearch: {repo_id, path, content, language, sha}
    2. Elasticsearch index: sharded by repo_id

Query: "org:github language:Go func main"
  1. Parse: org filter + language filter + keyword
  2. ES query: bool filter (repo_id in org repos, language=Go) + match (func main)
  3. Permission filter: post-filter results to repos user has read access
  4. Return: file + matching line snippets

Scale: Elasticsearch at GitHub scale → 50B+ documents.
  Index segmented by repo creation date (hot repos on NVMe, cold on HDD)
  Separate index clusters: public repos vs private repos (security isolation)
```

### 7. Repository Sharding

```
Single Postgres can handle 100M repos (metadata only, no object data).
Objects are in S3 — already infinitely scalable.

Repos table: partition by id range (Postgres native partitioning)
  Partition 0: id 0-25M
  Partition 1: id 25M-50M
  ...

Refs table: partition by repo_id (co-located with repos partition)
  → All refs for a repo in same partition shard

Most repo operations are single-repo → no cross-partition queries needed.
```

---

## Failure Analysis

### F1: Git Push Server Crashes Mid-Upload

**Scenario:** Large repo push (500MB, 50K objects). Server crashes after 300MB received.

```
Timeline:
  t=0:   Client starts sending packfile
  t=5s:  300MB received, written to temp dir on Git server
  t=5s:  Server crashes (OOM, hardware failure)
  
State:
  - Temp objects: lost (temp dir on server disk, not S3)
  - S3: no objects written yet (batch write at end)
  - DB refs: NOT updated (update is last step)
  
Result: Repo in clean prior state. No corruption.
```

**Why safe:**
1. Objects written to server temp dir first, then batch-uploaded to S3
2. Ref update is the final step — if not reached, state = unchanged
3. S3 put operations are atomic per object

**Recovery:**
- Client gets `broken pipe` → runs `git push` again
- Smart HTTP protocol: client sends "have" = what server already has → server requests only missing objects
- No duplicates: same SHA → skip if already in S3

**What if S3 write partially fails (10K of 50K objects written)?**
```
Objects without ref pointing to them = dangling objects
GC job (weekly):
  1. Scan all refs across all repos
  2. Walk commit DAG → collect all reachable SHAs
  3. Delete S3 objects not in reachable set
  4. Dangling objects cleaned up automatically
```

---

### F2: Postgres Goes Down

**Impact:**
- Ref lookups fail → can't resolve branch → push/clone fail
- PR data unavailable
- Auth token validation (if using DB-backed sessions)

**What still works:**
- CDN: cached static assets, raw files
- Stateless JWT auth: validate signature locally, no DB call
- Cached refs: Redis holds ref→SHA for 60s TTL → read-only clone operations continue

```
JWT payload: {user_id, permissions_fingerprint, exp}
Permissions fingerprint: SHA-256(sorted repo permissions list) 
Changed permissions (remove user) → fingerprint mismatch → re-auth required
Stale window: up to JWT TTL (15 min) — acceptable security trade-off
```

**Recovery timeline:**
```
Primary fails → RDS Sentinel detects (10-30s)
→ Promotes read replica (30-60s)
→ DNS update (10-30s)
→ Connection pool reconnects (automatic)
Total: 60-90s downtime
```

**Mitigation for 60s gap:**
- In-flight push requests: return 503 + Retry-After: 60
- Client git will retry automatically on push failure
- No data loss: S3 writes independent of Postgres

---

### F3: Webhook Dispatcher Queue Backlog

**Scenario:** CI webhook endpoint (GitHub Actions → customer Jenkins) is slow: 3s/call.
Push rate: 100K/day = 1.2/sec. Each push → 1 webhook call.
10 dispatcher workers × (1/3 calls/sec each) = 3.3 calls/sec capacity. Barely keeping up.

Spike: 1000 pushes in 1 minute (release + CI) → 1000 webhook calls queue up.

**Detection:**
- Kafka consumer lag on `webhook-events` > 1000 messages → alert
- P99 webhook delivery latency > 60s → alert

**Solutions:**

**Scale dispatcher (immediate):**
```
Kubernetes HPA on webhook-dispatcher:
  metric: kafka_consumer_group_lag > 500
  scale up: +5 pods (max 50)
  scale down: lag < 100 for 5min
```

**Per-host rate limiting:**
```
Webhook dispatcher: track calls/sec per destination host
  Limit: 100 calls/sec to any single host
  Over limit: queue with delay, don't block other webhooks
```

**Circuit breaker per endpoint:**
```
If endpoint fails 50% of calls in 5min:
  → Circuit OPEN: stop calling
  → Alert repo owner: "Your webhook is failing, please investigate"
  → Retry after 10min probe
```

**What if Kafka itself fails?**
```
Outbox pattern:
  git push → DB outbox INSERT (before Kafka publish)
  Dispatcher reads outbox on startup → replays undelivered events
  Kafka failure: dispatcher switches to polling outbox table
  Kafka recovery: Dispatcher resumes from Kafka + deduplicates via outbox.delivered flag
```

---

### F4: Large Repo Clone Storm

**Scenario:** 10GB monorepo (internal tooling). 500 devs run `git clone` Monday morning.
10GB × 500 = 5TB outbound. Git servers must compute packfiles in memory.

**Failure mode:** Git server OOM → crash → 500 developers blocked.

**Solution 1: Pre-computed Base Packfiles**
```
Nightly job (or on significant push):
  1. Compute full clone packfile: git pack-objects --all
  2. Upload to S3: s3://gh-packs/{repo_id}/full-{date}.pack + .idx
  3. Serve clone via: CDN → S3 pack file
  4. Delta: client fetches pack → then "git fetch origin" for new commits since pack

Result: Git servers only serve the delta (typically small), not 10GB
```

**Solution 2: Shallow Clone Enforcement for CI**
```
All CI systems default to:
  git clone --depth=1 --no-tags

10GB repo → 100MB with shallow clone
Developers: full clone once, subsequent: git fetch (only new objects)
```

**Solution 3: Partial Clone (git sparse-checkout)**
```
git clone --filter=blob:none (treeless) → downloads commits+trees, blobs on demand
git clone --filter=tree:0   (blobless) → downloads commits only

Developers working in one subdirectory of monorepo:
  git sparse-checkout set src/myservice/
  → Only fetches blobs for that path
```

---

### F5: Diff Service OOM on Large PR

**Scenario:** Automated PR changes 50K files (mass dependency upgrade). Diff computation: Myers diff × 50K files exhausts memory.

**Failure mode:** Diff worker OOM → crash → PR shows blank diff indefinitely.

**Prevention:**
```
1. File count limit: if changed_files > 3000:
   → Show first 3000 diffs
   → Banner: "Showing 3000 of 50,000 changed files"
   
2. File size limit: skip binary files, skip files > 1MB
   → "Binary file, diff not shown"
   
3. Memory cap per diff job: 512MB limit
   → On OOM: gracefully return partial diff + truncation flag
   
4. Async + paginated:
   → PR creation instant (no diff blocking)
   → Diff computed async, paginated by file (100 files per page)
   → Frontend lazy-loads pages as user scrolls
```

---

### F6: Authorization Bug (Private Repo Exposure)

**Scenario:** Bug in PR code returns files from private repo to unauthorized user.

**Defense in Depth:**
```
Layer 1 — Centralized AuthZ service:
  All resource access goes through AuthZ service.
  No ad-hoc permission checks in business logic.
  AuthZ: {user, repo} → {read, write, admin} permission

Layer 2 — Object store scoping:
  Pre-signed URLs: server validates permission → issues time-limited S3 URL
  URL contains: repo_id + sha + HMAC signature + expiry (15 min)
  Even if URL leaks: expires in 15 min, scoped to specific file

Layer 3 — Audit logging:
  Every object access logged: {user_id, repo_id, sha, timestamp}
  Append-only (Kafka → cold storage → cannot be deleted)
  Anomaly detection: user accessing repos they've never accessed before

Layer 4 — Rate limiting on unauthenticated:
  Prevents SHA enumeration attacks (guess SHA → probe if object exists)
  Anonymous: 60 req/min hard limit

Fail-safe on AuthZ down:
  Fail-CLOSED: deny all access (not fail-open)
  5min degradation acceptable vs risk of data exposure
```

---

## Trade-offs Summary

| Decision | Chosen | Alternative | Why |
|----------|--------|-------------|-----|
| Object storage | S3 content-addressed | DB BLOBs | Git objects are immutable + deduplication only works with SHA addressing |
| Auth | Stateless JWT (15min) | DB-backed sessions | DB session check adds latency + DB dependency on every request |
| Diff storage | Redis cache (24h TTL) | Recompute on every view | Myers diff is O(N×D) — expensive on large files. Cache saves 99% of compute |
| Merge strategy | All three (user choice) | Single strategy | Different team workflows. OSS = merge, squash = most common, rebase = power users |
| Code search | Elasticsearch | Postgres full-text | 50B+ documents. ES handles shard-level parallelism + language tokenization |
| Webhook delivery | At-least-once + outbox | Exactly-once (2PC) | 2PC too expensive across HTTP. At-least-once + idempotent consumers (CI ignores duplicate triggers) |
| Fork objects | Copy-on-write (shared) | Full copy on fork | Most forks diverge minimally. Full copy would 10× storage costs |

---

## Budget × Feature Constraints

| Budget | What You Get | Limitation |
|--------|-------------|------------|
| $5K/month | Single Postgres + 3 Git servers + Cloudflare | 100K repos, no code search, 90s DB failover |
| $20K/month | RDS Multi-AZ + 10 Git servers + ES (5 nodes) | 1M repos, basic code search |
| $100K/month | Full sharded Postgres + 50 Git servers + ES (20 nodes) + CDN | 10M repos, full code search, <30s failover |
| $500K+/month | GitHub-scale: geo-distributed, <10ms auth p99 | 100M repos |
