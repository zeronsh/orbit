---
"@zeronsh/orbit": patch
---

SQLite-backed replica: full SQL pushdown, fixing the "full table scan + in-memory
sort" scaling shortcut, plus two latent correctness bugs the new differential
harness caught, and streaming initial sync.

- **`SqliteSource::fetch` now executes entirely in SQLite** (the Rust analog of
  Zero's `zqlite` TableSource): the join constraint becomes `WHERE =`, the
  `start` cursor becomes a sargable lexicographic bound (null-aware to match
  `compare_values`' nulls-first order), `ORDER BY` runs in SQL (directions
  flipped for reverse fetches), and `LIMIT` is pushed down. Previously every
  fetch did `SELECT *` over the whole table, filtered the constraint in memory,
  and re-sorted the already-ordered result. Secondary indexes are created
  lazily per fetch shape (constraint columns + sort columns), all statements go
  through SQLite's prepared-statement cache, and connections use WAL +
  `synchronous=NORMAL`. Measured on 100k rows: related-join hydrate
  **1,165ms → 24ms (49×)**, limit-query hydrate 22.6ms → 12ms, join-child push
  113K/s → 380K/s.

- **Correctness (found by running the full Zero-differential corpus through the
  SQLite source, now a permanent test):** JSON-typed columns round-trip every
  JSON primitive (booleans/numbers were stored natively and read back as
  `null`); primary keys with NULL components match by identity (`IS`, mirroring
  `values_identical`) so removes/edits of such rows no longer silently no-op;
  null join keys never match (SQL `=` semantics, as in Zero and the in-memory
  source). The SQLite source now passes the same 5,920-scenario differential
  sweep as the in-memory engine: 120/120 take-churn, 5000/5000 fuzz, 797/800
  related (the 3 = known Zero bugs).

- **Initial sync streams from Postgres** (`query_raw`) instead of buffering the
  whole table, so seeding a durable replica is O(1) memory at any table size.
