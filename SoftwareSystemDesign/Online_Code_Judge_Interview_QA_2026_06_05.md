# Online Code Judge — Interview Q&A
**Date:** 2026-06-05

---

## Opening Questions

**Q: Design an online code judge like LeetCode.**

*Interviewer wants to see:* You identify the hard problem (sandboxed code execution) before jumping to CRUD. Don't start with DB schema.

**Good opening:**
> "The core challenge here isn't storing problems or users — it's safely executing arbitrary user code at scale. I want to start there. We need isolation so malicious code can't harm our infra, we need fairness so time/memory limits are enforced consistently, and we need scale to handle contest bursts. Let me walk through entities and data flow, then dive into the judge worker design..."

---

## Entities & Scope

**Q: What entities do you need?**
> User, Problem, TestCase, Submission, Contest, Leaderboard entry. Most interesting: TestCase — these are write-once, read-heavy, potentially large (up to 10MB each).

**Q: What's in scope for the initial design?**
> Core: submit code → get verdict. Stretch: contests, leaderboards, plagiarism detection.

---

## Judge Worker Deep Dive

**Q: How do you safely execute user code?**
> Use OS-level isolation: Linux namespaces + seccomp syscall filtering (via nsjail). This gives us:
> - PID namespace (no fork bombs beyond PID limit)
> - Network namespace (no outbound calls)
> - Mount namespace (read-only FS)
> - cgroup limits for CPU/memory
> Wall-clock timer in the parent process sends SIGKILL at time_limit deadline.

**Q: Why not just use Docker containers?**
> Docker startup is ~500ms — too slow when judging 20 test cases per submission. nsjail starts in ~50ms. For stronger isolation at the cost of speed, gVisor intercepts all syscalls via a user-space kernel (suitable for untrusted languages like C).

**Q: How do you handle Time Limit Exceeded?**
> Enforce wall-clock time, not just CPU time. A `sleep(999)` call should be caught. Parent process sets a timer; on expiry, sends SIGKILL to the container's PID namespace. Also enforce `ulimit -t` (CPU time) as a second layer.

**Q: What if a user submits an infinite loop that never makes syscalls?**
> Wall-clock limit catches it. If it's a tight busy-loop, CPU time limit catches it. Both enforced independently.

**Q: How do you prevent a submission from affecting subsequent submissions on the same worker?**
> Each submission runs in a fresh container (namespace). After execution, worker tears down the namespace entirely. No shared writable state between submissions.

**Follow-up: What if the worker itself is compromised?**
> Workers run on isolated nodes with no network access to DB or Kafka directly. They write results to a results queue; a separate trusted service reads from that queue and updates DB. Worker nodes are recycled regularly (e.g., after 1000 executions or 1h).

---

## Scalability

**Q: How do you scale during a contest with 50K simultaneous users?**
> - Kafka buffers the submission burst — workers consume at their own pace
> - Workers auto-scale on Kafka consumer lag metric (Kubernetes HPA)
> - Pre-warm worker pool 30 minutes before contest starts (avoids cold-start delay)
> - Rate limit submissions per user (e.g., 1 submit/10s per problem) to prevent retry storms

**Q: Your judge workers are slow to start (30s to scale). What do you do?**
> Pre-warming. Monitor signup + registration data to predict contest load. Can also keep a minimum warm pool (e.g., 20 workers always running). Cold start matters less if Kafka absorbs the burst — users see "Pending" for a few extra seconds, which is acceptable.

**Q: How do you shard the submissions table?**
> Shard by `user_id` hash. This distributes writes evenly. Queries like "all submissions for problem X" (admin analytics) span shards — acceptable since that's not in the hot path. Contest leaderboard queries don't touch submissions table directly (Redis sorted set).

---

## Reliability & Correctness

**Q: What if a judge worker crashes mid-execution?**
> Kafka's at-least-once delivery re-delivers the message. Worker sets `status=RUNNING` in DB at start. A cron job scans for submissions stuck in RUNNING > 5 minutes and resets them to PENDING for re-queuing.

**Q: How do you prevent double-judging?**
> Worker does `SET NX submission:{id}:processing` in Redis (TTL = 10min) before processing. If key exists → skip (another worker already handling it). If worker crashes, TTL expires and next delivery processes it.

**Q: User submits same code twice (double-click). How do you handle it?**
> Client generates a UUID per submit action (idempotency key). Submission service checks Redis for key — if exists, returns existing submission_id. If not, creates new submission and stores key in Redis (TTL = 24h).

**Q: How do you ensure fair judging? (Same code should get same verdict)**
> - All workers are identical (same Docker image, same OS, same resource limits)
> - Judges run on dedicated nodes — no noisy neighbors
> - Time limit = 2× intended solution's runtime (buffer for system variance)
> - Judging is deterministic for deterministic problems; for randomized inputs, use fixed seed

**Q: What if test cases are updated while a contest is running?**
> Test cases are versioned. Workers check `problem.test_case_version` from Redis on each job. If local cache version differs → re-fetch from S3. Version bump is atomic: upload new files to S3 first, then increment version in Redis.

---

## Real-Time Verdict

**Q: How does the user see their verdict in real-time?**
> After submission, client subscribes via WebSocket (keyed by submission_id). When judge completes, result is published to Kafka result topic → WebSocket gateway pushes to client. If WebSocket is unavailable, client falls back to polling `/submissions/{id}` every 2s.

**Q: Why not just use polling everywhere? Why bother with WebSocket?**
> For individual users on a problem, polling every 2s is fine. But during a contest with 50K users all polling → 25K req/s just for status checks. WebSocket push eliminates this. Trade-off: WebSocket adds infra complexity (sticky routing or pub-sub for multi-node gateway).

---

## Leaderboard

**Q: How do you implement a real-time contest leaderboard?**
> Redis sorted set: `ZADD contest:{id}:leaderboard {score} {user_id}`. Score = `total_solved * 1e12 - total_penalty_seconds`. `ZRANK` for user's rank = O(log N). `ZRANGE` for top-K = O(log N + K). Snapshot to Postgres every 60s and on contest end for persistence.

**Q: What if two users have the same score?**
> Secondary sort by penalty time (encoded in score: higher penalty → lower score). If truly equal, sort by user_id (tie-break, deterministic). Redis sorted set supports this natively since score is a float — encode both into score composite.

**Q: Redis fails during contest. What happens?**
> Leaderboard goes stale or unavailable. Mitigation: Redis Cluster with replicas. If cluster fails, serve last Postgres snapshot (stale by up to 60s, but readable). Admin can trigger leaderboard rebuild from submissions table post-recovery.

---

## Code Compilation

**Q: How do you handle compiled languages (C++, Java) vs interpreted (Python)?**
> Compilation is a separate phase before execution:
> 1. Compile step: run `g++ solution.cpp -o solution` in sandbox, enforce time/memory limits, capture stderr as CE (Compile Error)
> 2. Execution step: run compiled binary against each test case
> 
> For Java/Python: interpretation is slower → adjust time limits per language (e.g., Python limit = 3× C++ limit).
> Cache compiled binary for the same submission (if user retries exact same code, skip compile — rare, not worth optimizing early).

---

## Edge Cases

**Q: What if expected output has trailing newlines / whitespace differences?**
> Normalize before comparison: `output.strip()` on both expected and actual. Some judges also allow "special judge" (custom checker program) for problems with multiple valid answers (e.g., shortest path where multiple paths are valid).

**Q: How do you handle very large test case inputs (1GB)?**
> Stream input to sandbox via pipe, not file copy. Sandbox reads from stdin. Output is also streamed and size-capped (e.g., 64MB stdout limit → if exceeded → WA/output limit exceeded).

**Q: Plagiarism detection?**
> Async, offline: after contest, run MOSS or token-similarity algorithms on AC submissions. Store all submission codes (S3). Flag pairs with >90% similarity for admin review. Not in hot path.

---

## Interviewer Follow-up Patterns

| They ask... | They want to see... |
|---|---|
| "What if 100K users submit at once?" | Kafka buffering, worker autoscale, pre-warming |
| "Make judging faster" | Parallel test case execution within a submission (run 20 cases in parallel on worker with 20 cores) |
| "How do you add a new language?" | Containerized judge — add new Docker image with compiler/runtime; no infra change needed |
| "How do you debug wrong verdicts?" | Submission code + input + expected + actual output all stored; admin can replay |
| "Cost optimization?" | Spot/preemptible instances for judge workers (stateless, can be killed); S3 Infrequent Access for old submissions |
| "What if someone submits 100MB code?" | Size validation at API layer before enqueue |
