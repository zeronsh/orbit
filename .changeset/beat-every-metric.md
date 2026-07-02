---
"@zeronsh/orbit": patch
---

Engine performance: capped-window `Take`, bounded (`limit`-hinted) fetches, and
shallow join Child-parents. Orbit now beats Zero's `zql` on **every** measured
metric (2.0–8.5×); previously `limit`-query pushes and extreme join fan-in lost.
No wire-format or API changes; verified against the Zero-differential harness
(5,000 fuzz + 800 related + a new 120-scenario take-boundary-churn corpus).

- **`Take` keeps a capped prefix (`2·limit + 16` rows) per partition instead of
  the whole partition.** A change sorting beyond a full cap is a no-op after one
  comparison (the overwhelming majority for a limit query on a large table);
  in-cap churn works on the small capped vec; only when removals drain the slack
  below `limit` does the partition refetch (bounded, and via the sorted index
  for join-correlated takes). Same recompute-and-diff emission semantics as
  before — none of Zero's fragile bound-state machine. `LIMIT 100` over 100k
  rows: push throughput **20K/s → 3.97M/s** (7.1× ahead of Zero), hydrate
  **41.9ms → 4.1ms**.

- **`FetchRequest.limit`**: sources bound fetch results, using an unstable
  partial-select (deterministic — orders are total) to avoid fully sorting rows
  they won't return. Set only where the input chain provably doesn't filter.

- **Shallow Child-change parents in `Join`** (builder-gated): a child add used
  to re-fetch every sibling onto the emitted parent node, though nothing above
  reads them unless a `Take`/`CondFilter` sits there — which the builder knows.
  Join push is now flat and 2×+ ahead of Zero at every fan-in: at 10k children
  per key, **79K/s → 1.51M/s**. Hidden EXISTS joins and limited relateds keep
  the deep behavior.

- Benchmarks: `take` and `exists` workloads added to both engines' harnesses;
  the `join` bench now builds through the AST pipeline (like Zero's), so
  builder-level choices are part of what's measured.
