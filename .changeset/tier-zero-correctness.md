---
"@zeronsh/orbit": minor
---

Correctness + scaling release (server): every Tier 0 wall from the Zero
comparison audit fixed, plus most of Tier 1/2. Highlights:

- **TOAST fix**: unchanged TOASTed columns are no longer NULLed on UPDATE —
  merged from the old tuple at decode and from the stored row at apply, on
  both replica backends and both replica identities (real-PG regression suite).
- **Type fidelity**: jsonb/json decode to real JSON on the stream (snapshot and
  stream now decode identically via per-OID parsing), timestamps/date/time
  decode to epoch-ms numbers (matching the generated client schema), arrays
  decode to JSON, binary tuples decode instead of crashing, and int8 beyond
  2^53 stays exact end-to-end (new exact integer value representation).
- TRUNCATE replicates; column type changes convert stored values in place;
  RENAME TABLE/COLUMN are handled instead of silently losing data; tables
  added to `ORBIT_TABLES` are backfilled on resume.
- **Bounded memory**: transactions larger than `ORBIT_TXN_BUFFER_BYTES` (32 MiB)
  stream through bounded memory instead of being fully buffered on every node;
  hydration results overlap sockets only within `ORBIT_HYDRATION_BUDGET_BYTES`.
- **Incremental backups**: WAL-segment shipping (litestream-style generations)
  replaces full-file re-uploads every interval; backup wedge detection crashes
  out instead of running silently unrestorable. `ORBIT_BACKUP=full` opts out.
- Change-log pruning by subscriber-ACK consensus (slow-but-live view-syncers
  are no longer force-reset); initial sync is per-table resumable and pinned to
  the replication slot's exported snapshot; `ORBIT_CDC_PG` moves the CDC log
  off the source database.
- Serving protections: per-client query/row caps, per-user mutation rate limit,
  slow-client eviction, no-op wake suppression, complete CVR GC, SIGTERM
  graceful drain, per-query cost attribution at `/statz`, and WHERE pushdown
  into SQLite fetches.

Apply errors now roll back and halt cleanly instead of panicking. Verified by
46 test suites (incl. new TOAST/DDL/backfill/big-txn/pushdown regression
suites) and the 512 MB cgroup acceptance harness.
