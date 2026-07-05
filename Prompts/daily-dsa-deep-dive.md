# Daily DSA Deep Dive

## Step 1 — Pick Today's Topic

Use today's date to compute: `topic_index = (days_since_2026-06-16) % 19`

Topic curriculum (index 0–18):

0=Arrays, 1=Hashing, 2=Two Pointers, 3=Sliding Window, 4=Stack, 5=Binary Search, 6=Linked List, 7=Trees, 8=Tries, 9=Heap/Priority Queue, 10=Backtracking, 11=Graphs, 12=Advanced Graphs (Dijkstra/Union-Find/Topo Sort), 13=1D Dynamic Programming, 14=2D Dynamic Programming, 15=Greedy, 16=Intervals, 17=Math & Geometry, 18=Bit Manipulation

## Step 2 — Research (use web search)

Search these sources for today's topic:

- `site:github.com DSA <topic> patterns`
- `site:neetcode.io <topic>`
- `hellointerview.com <topic> interview`
- `https://kishan-kumar-zalavadia.github.io/DSA-Leetcode-Pattern-Vault/`
- YouTube: `neetcode <topic>` or `take u forward <topic>`
- Any high-signal blog/editorial

## Step 3 — Build the Markdown file

Save to outputs folder as `dsa-<YYYY-MM-DD>-<topic-slug>.md`.

Structure (keep tokens minimal, no fluff):

```markdown
# [Topic] — [Date]

## Why This Topic Matters (Interviewer POV)

2–3 sentences: what interviewers test, common pitfalls, signals of mastery.

## Core Patterns

Bullet list of sub-patterns/variants with 1-line description each.

## Questions (4–8)

For each question:
- **[LeetCode # — Title]** | Difficulty | Pattern tag(s)
  - _What interviewer checks:_ ...
  - _Key insight:_ ...
  - _Cross-topic link:_ (e.g. "Also uses Binary Search")

## Pattern Memory Graph

ASCII or text adjacency showing how questions relate:
- shared patterns → group them
- bridge questions that touch multiple topics → mark with ⬡
- prerequisite chain arrows (→)

Example:
Q1 (Two Sum) ──hash──► Q3 (Group Anagrams)
                              │
                         Q5 (Top K) ──heap──► Q8 (K Closest)

## Interviewer Checklist

What you must say/do to get full marks:
- [ ] Clarify constraints
- [ ] State pattern aloud before coding
- [ ] Mention time/space complexity
- [ ] Handle edge cases: empty input, single element, duplicates
- [ ] (topic-specific checklist items)
```

## Output

Write the file to the outputs folder. Be concise — no padding, no repeated explanations.
