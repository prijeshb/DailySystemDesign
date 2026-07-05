# Online Code Judge — System Design
**Date:** 2026-06-05 | **Ref:** LeetCode / HackerRank / Codeforces

---

## 0. First Principles — Do We Need It?

**Problem:** Users write code solutions to algorithmic problems. We need to securely execute arbitrary code, compare output to expected results, and return a verdict — at scale, with fairness.

**Why not just run on a server directly?**  
Arbitrary code can fork-bomb, consume unlimited memory, make network calls, read filesystem, or never terminate. So we need **isolated sandboxed execution** — this is the core hard problem.

---

## 1. Entities

| Entity | Key Attributes |
|---|---|
| `User` | id, username, email, rating, submission_count |
| `Problem` | id, title, difficulty, time_limit_ms, memory_limit_mb, tags[] |
| `TestCase` | id, problem_id, input, expected_output, is_sample |
| `Submission` | id, user_id, problem_id, language, code, status, verdict, runtime_ms, memory_kb, created_at |
| `Contest` | id, title, start_time, end_time, problem_ids[] |
| `ContestSubmission` | submission_id, contest_id, score, penalty_time |
| `Leaderboard` | contest_id, user_id, rank, total_score, total_penalty |

---

## 2. Actions

| Action | Trigger | Output |
|---|---|---|
| Submit code | User clicks submit | Submission ID, eventually verdict |
| Run (custom input) | User clicks run | Stdout/stderr for sample/custom input |
| Get verdict | Poll / WebSocket | Verdict + runtime + memory |
| Browse problems | List/filter/search | Paginated problem list |
| View leaderboard | Contest page | Ranked list with scores |
| Admin: add problem | Problem setter | Problem + test cases stored |

---

## 3. Capacity Estimates

- 1M DAU, peak 50K concurrent during contest
- Avg submission: 2KB code, 100ms judge time
- Peak: 10K submissions/min = ~167/sec
- TestCases per problem: ~20 (hidden), each input up to 10MB
- Storage: 10K problems × 20 cases × avg 1MB = **200GB test case storage**

---

## 4. Data Flow

```
User Browser
    │
    ▼
API Gateway (Auth, Rate Limit)
    │
    ├──► Problem Service (CRUD, S3 for test cases)
    │
    └──► Submission Service
              │  (writes to DB: status=PENDING)
              ▼
         Message Queue (Kafka topic: submissions)
              │
              ▼
         Judge Worker Pool
         [pulls job → sandbox → run test cases → emit result]
              │
              ▼
         Result DB (Postgres: update submission verdict)
              │
              ▼
         Result Publisher (Kafka → WebSocket Gateway)
              │
              ▼
         User Browser (live verdict update)
```

---

## 5. High-Level Design

```
┌──────────────┐     ┌────────────────────────────────────────────────────┐
│   Client     │────▶│  API Gateway (rate limit, JWT auth)                │
└──────────────┘     └────────────────────────────────────────────────────┘
                               │              │
                    ┌──────────▼──┐    ┌──────▼──────────┐
                    │  Problem    │    │  Submission     │
                    │  Service    │    │  Service        │
                    └──────┬──────┘    └──────┬──────────┘
                           │                  │
                    ┌──────▼──────┐    ┌──────▼──────────┐
                    │  S3         │    │  Kafka          │
                    │ (test cases)│    │ (submission Q)  │
                    └─────────────┘    └──────┬──────────┘
                                              │
                               ┌──────────────▼──────────────────┐
                               │       Judge Worker Pool          │
                               │  (containerized, auto-scaled)    │
                               └──────────────┬──────────────────┘
                                              │
                               ┌──────────────▼──────────────────┐
                               │   Result DB (Postgres)          │
                               └──────────────┬──────────────────┘
                                              │
                               ┌──────────────▼──────────────────┐
                               │   WebSocket Gateway             │
                               └─────────────────────────────────┘
```

---

## 6. Low-Level Design

### 6.1 Submission Service

```
POST /submit
  - Validate: language supported, code size < 64KB
  - Write to DB: submissions(status=PENDING)
  - Publish to Kafka: { submission_id, user_id, problem_id, language, code_s3_path }
  - Return: { submission_id }

GET /submissions/{id}
  - Read from DB
  - If PENDING/RUNNING: client polls or subscribes via WS
```

**Idempotency:** Client sends `idempotency_key` (UUID). Submission service checks Redis for key before inserting. Returns existing submission_id if duplicate.

### 6.2 Judge Worker

```
Algorithm:
1. Pull message from Kafka (at-least-once delivery)
2. Check Redis: is submission_id already being processed? (dedup)
3. Download code from S3
4. Download test cases from S3 (or warm cache)
5. For each test case:
   a. Spawn isolated container (gVisor/nsjail sandbox)
   b. Mount input, capture stdout, enforce limits:
      - CPU: time_limit × 2 (wall clock)
      - Memory: memory_limit_mb (cgroup limit)
      - No network, no filesystem write
   c. Compare output (strip trailing whitespace)
   d. Record: PASS / WA / TLE / MLE / RE
6. Final verdict: AC if all pass, else first failing case
7. Update DB: submission status + verdict + runtime + memory
8. Publish result to Kafka result topic
```

**Sandbox options:**
- `nsjail` + Linux namespaces (fast, low overhead)
- `gVisor` (stronger isolation, slower syscall interception)  
- Docker (easier ops, but slower startup — 500ms+)

**Trade-off chosen: nsjail**
- ✅ ~50ms startup vs Docker ~500ms
- ✅ Sufficient for competitive programming
- ❌ More complex to configure securely

### 6.3 Test Case Caching

Test cases are read-heavy, write-once. Cache in worker memory or local SSD.

```
Worker startup:
  - Pre-warm: load all test cases for popular problems into /tmp (SSD)
  - LRU cache by problem_id (evict least-recently-submitted problems)
  
Cache hit: skip S3 download → saves ~100-500ms per submission
```

**Trade-off:** Local cache means test case updates don't propagate instantly.  
**Solution:** Version test cases. Worker checks `problem.test_case_version` from Redis. If stale → re-fetch from S3.

### 6.4 Leaderboard (Contest)

Contest: ~50K concurrent users, real-time rank updates.

**Approach: Redis Sorted Set**
```
Key: contest:{id}:leaderboard
Score: total_score * 1e12 - penalty_time  (higher = better)
Member: user_id

ZADD → on each accepted submission
ZRANK → to get user's rank
ZRANGE → top-N leaderboard
```

**Trade-off:** Redis sorted set is O(log N) for updates, O(log N + K) for range queries — perfect for 50K users.  
**Persistence:** Snapshot to Postgres every 60s or on contest end.

### 6.5 Database Schema (Key Tables)

```sql
-- Submissions (write-heavy, mostly append)
CREATE TABLE submissions (
  id          UUID PRIMARY KEY,
  user_id     UUID NOT NULL,
  problem_id  UUID NOT NULL,
  language    VARCHAR(20),
  code_path   TEXT,           -- S3 path
  status      VARCHAR(20),    -- PENDING, RUNNING, DONE
  verdict     VARCHAR(20),    -- AC, WA, TLE, MLE, RE, CE
  runtime_ms  INT,
  memory_kb   INT,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON submissions(user_id, problem_id);
CREATE INDEX ON submissions(problem_id, verdict);

-- Problems (read-heavy)
CREATE TABLE problems (
  id              UUID PRIMARY KEY,
  title           TEXT,
  difficulty      VARCHAR(10),
  time_limit_ms   INT,
  memory_limit_mb INT,
  test_case_version INT DEFAULT 1
);
```

**Sharding:** Shard `submissions` by `user_id` hash. Problems table is small enough to replicate.

---

## 7. Key Design Decisions & Trade-offs

| Decision | Chosen | Alternative | Trade-off |
|---|---|---|---|
| Execution isolation | nsjail | Docker / gVisor | nsjail: fast but harder config; gVisor: safer but 5-10× slower |
| Job delivery | Kafka | RabbitMQ / SQS | Kafka: replayable, ordered; SQS: simpler, no replay |
| Verdict push | WebSocket | Polling | WS: real-time; Polling: simpler, higher server load |
| Leaderboard | Redis sorted set | SQL window fn | Redis: O(log N) live; SQL: consistent but slow under load |
| Test case storage | S3 + local SSD cache | Postgres blobs | S3: cheap at 200GB; Postgres blobs: simpler but DB bloat |
| Code storage | S3 (referenced by path) | Inline in DB | S3: handles large code; DB: simpler but row bloat |

---

## 8. Scaling Strategy

- **Judge workers:** Stateless, auto-scale on Kafka consumer lag metric
- **API:** Horizontal scale behind load balancer
- **Contest burst:** Pre-warm worker pool 30min before contest start
- **Read replicas:** Problem service reads from Postgres read replicas
- **CDN:** Problem statements (HTML/images) served from CDN

---

## 9. Security Considerations

- Code runs in isolated namespace: no network, no host FS access
- Resource limits enforced at OS level (cgroups), not just language-level
- Time limit enforced with SIGKILL after deadline (wall clock, not CPU time — prevents sleep tricks)
- Output size cap: 10MB max stdout (prevents OOM via print)
- Compiler output cap: 5MB (prevents CE bomb)
- Worker runs as non-root UID inside container
