# Orbit vs Zero audit — fix ledger

**Verification:** 46/46 test suites green (unit + real-Postgres integration,
incl. new TOAST / TRUNCATE / DDL-matrix / backfill / big-txn / pushdown
suites), and the 512 MB cgroup acceptance harness PASSES on this branch
(peak RSS 391 MB across nodes; no OOM, no restarts, readiness cycling,
view-syncer replacement reconvergence, exact row counts).

Tracks every item from `ORBIT_VS_ZERO_WALLS.md` (the four-way code audit vs
Rocicorp Zero) to its disposition on this branch. ✅ fixed · 🟠 partially /
mitigated · 📐 designed, not built · ⛔ out of scope by the audit's own framing.

## Tier 0 — Correctness

| Item | Status | What landed |
|---|---|---|
| 0.1 Unchanged-TOAST columns NULLed on UPDATE | ✅ | Decode-side merge from the `'O'` old tuple (`'K'` key tuples deliberately excluded — they ship non-key columns as NULL) **plus** apply-side merge over the stored row in both backends (covers `REPLICA IDENTITY DEFAULT`, where no old tuple ships at all). `tests/toast.rs`: real-PG regression with >150 KB TOASTed payloads × both backends × both replica identities × PK-change; verified to FAIL with the merge disabled. |
| 0.2 Type coverage | ✅ | Per-OID decode: jsonb/json → `Json` (was raw text — streamed and snapshot values diverged), timestamps/date/time → epoch-ms numbers (matches the drizzle schema-gen types), arrays → JSON (text-literal parser incl. quoting/nesting/bounds), `numeric` → exact-int-or-f64, bytea stays canonical `\x` hex. **int8 beyond 2⁵³ stays exact** via `Value::Int(i64)` (normalizing constructor; exact i64↔f64 comparators; JSON round-trip exact) — Zero's f64-everywhere model collapses adjacent snowflake ids into one row; Orbit no longer does. Binary (`'b'`) tuples decode (bool/ints/floats/text/bytea/uuid/json/jsonb/timestamps/numeric/arrays) instead of crashing the process. Initial sync resolves real column OIDs and shares the stream's parser — `tests/toast.rs::snapshot_and_stream_decode_identically`. |
| 0.3 TRUNCATE ignored | ✅ | Decoded + applied through the IVM pipelines (clients converge, not just storage); counted as a data event everywhere. e2e test. |
| 0.4 Column type change invisible | ✅ | Reconcile compares type as well as name; stored values convert in place (shared `convert_column_value`, PG-cast-like semantics) in both backends. e2e `ALTER COLUMN TYPE` test + unit conversions. |
| 0.5 Added table never backfilled | ✅ | `orbit_synced_tables` registry (migrated in place for pre-registry files). Watermark resume backfills missing tables in one watermark-preserving txn; durable view-syncers re-restore from a snapshot that contains the new table instead of delta-resuming past it. `tests/backfill.rs`. |
| 0.6 Apply panics / decode exits | ✅ | `ReplicaBackend::{apply,seed,begin_txn,commit_txn}` return `Result`; new `rollback_txn`; every pump rolls back + halts cleanly (never a watermark over a torn apply, never a panicking shard thread). RI-NOTHING deletes error with a diagnosis. |
| DDL extras | ✅ | `RENAME TABLE` detected via relation-OID tracking → aliased so clients on the old name keep receiving changes. `RENAME COLUMN` pairing (one-out/one-in same type, loudly logged) preserves values in both backends. e2e matrix now: add / drop / retype / rename-table / rename-column. Zero-fidelity column renames need DDL event triggers, not the wire protocol. |

## Tier 1 — Architecture

| Item | Status | What landed |
|---|---|---|
| 1.1 Full dataset on every node | ⛔ | Shared terminal wall; the audit itself: "even with everything fixed, the asymptote only changes with partitioning". Roadmap, not a patch. |
| 1.2 Txns fully buffered fleet-wide | ✅ | Begin now carries `final_lsn` (decoded), so pumps decide apply/publish upfront. Replicator: pure streaming, O(one event) memory. Serving nodes: buffer to `ORBIT_TXN_BUFFER_BYTES` (32 MiB), then stream under a node-wide replica-consistency RwLock (readers: hydration materialize + tick flush) so no torn mid-txn state is observable. Sharded fan-out wraps Begin..Commit in the same lock (fixed a pre-existing torn-flush hazard). e2e: 1.2 MB txn through a 64 KiB cap arrives complete and untorn. |
| 1.3 Multi-core XOR durable | 📐 | Needs Zero's wal2/`BEGIN CONCURRENT` snapshot leapfrogging or an equivalent pinned-read-snapshot + per-shard delta-overlay design; standard SQLite (rusqlite-bundled) has neither. Design notes below. |
| 1.4 Full-file backup per interval | ✅ | `walship`: incremental WAL-segment shipping (generation = checkpoint + one base upload; per-cycle segments cut at the last committed frame, salt-validated; atomic manifest; restore = base + concat + SQLite recovery; grandparent-generation GC). Fixed backup cadence + lag warning + `orbit_snapshot_age_seconds` + wedge crash-out at max(5×interval, 5 min). `ORBIT_BACKUP=full` opt-out; legacy fallback on restore. |
| 1.5 Un-yielded hydration; gate across poke | 🟠 | Results that fit `ORBIT_HYDRATION_BUDGET_BYTES` (64 MiB) reserve it and release the admission permit before the poke (a stalled socket no longer head-of-line blocks queued hydrations); oversized results keep the permit through the poke (at most one resident — the naive unconditional release stacked two ~200 MB full-history results and the 512 MB harness OOM'd, which is how this policy was found). Yields between per-query materializations; slow-hydration/advance detection with per-query attribution. The remaining sync block is `fetch()` itself — yielding inside it needs a generator-style IVM (Zero's model). |
| 1.6 Fixed-window log pruning | ✅ | Subscriber-ACK consensus (`ack <pos>` lines, throttled) + latest-snapshot-position restore reservation, with `LOG_RETENTION` as the hard cap so zombies can't pin the log. |
| 1.7 Serial, non-resumable, inconsistent initial sync | ✅ | Slot created over the replication protocol with `SNAPSHOT 'export'`; all seed SELECTs pinned via `SET TRANSACTION SNAPSHOT` to the slot's exact consistent point (single-node, replicator, sharded). Per-table transactions + registry = crash-resumable. Parallel COPY workers not done (single-thread `!Send` architecture; the pin removes the correctness motivation). |
| 1.8 CDC log in the source PG | ✅ | `ORBIT_CDC_PG` points the change-log at a separate database. |

## Tier 2 — Protections

| Item | Status | What landed |
|---|---|---|
| No-op poke suppression | ✅ | Ticks carry the txn's touched tables; clients whose queries (via `Ast::tables`) don't intersect skip the drain entirely. (Row-level XOR signatures remain a further refinement.) |
| Query TTL never enforced | 🟠 | Per-connection pipelines drop at Del/disconnect — strictly tighter than any TTL; stale CVR desired-query records now GC'd. (Zero's TTL keeps queries WARM after deactivation — a feature Orbit's per-connection pipeline model doesn't have; see query dedup below.) |
| Per-client caps | ✅ | `ORBIT_MAX_QUERIES_PER_CLIENT` (200), `ORBIT_MAX_ROWS_PER_QUERY` (100k, checked before any client state mutates); rejections are per-query errors, connection survives. |
| Mutation rate limit | ✅ | `ORBIT_MUTATIONS_PER_MINUTE` sliding window; rejects are NOT consumed (lastMutationID unchanged → client retries; nothing lost). |
| Slow-client eviction | ✅ | `ORBIT_POKE_TIMEOUT_SECS` (120) on every send; evicted clients resume from CVR. |
| CVR GC gaps | ✅ | Sweep now also GCs `orbit_cvr_mutations` + orphaned `orbit_cvr_queries`, and runs on ALL serving paths (single-node + sharded were missing it). |
| Crash-only deploys | ✅ | SIGTERM → `/ready` 503 (LB drains) → `ORBIT_DRAIN_SECONDS` of continued service → clean exit. |
| Query dedup / covering | 📐 | Needs shared pipelines keyed by query hash with per-client fan-out of patches — a serving-architecture change (Zero's client-group pipelines). Highest-value remaining Tier 2 item. |
| Per-query observability | ✅ | `/statz`: per-query-hash active count, hydration count/ms/rows, advance ops (top-200 by cost); node-level hydration/advance counters + slow counters; slow logs carry the hash. |
| WHERE pushdown | ✅ | Superset-safe subset of the WHERE pushed into SQLite fetch SQL (the pipeline Filter re-checks everything). Equivalence battery vs the memory source over NULLs/absent columns/IN/OR/IS — which also caught and fixed a pre-existing `IS NULL`-vs-absent-column divergence between backends. |
| Planner / join flip / operator spill | 📐 | Query-engine projects (Zero: cost-based planner over SQLite stats; storage-spilling operators). Not started. |
| Multi-tenancy, JWT auth, ~110 knobs | 📐 | Not started (config surface grew by ~15 knobs on this branch). |

## Where Orbit is now MORE correct than Zero

- **Exact 64-bit integers server-side** (`Value::Int`): adjacent int8 ids > 2⁵³
  stay distinct end-to-end; Zero's f64 model collapses them.
- **TOAST regression coverage** on BOTH replica identities and the PK-change
  case (Zero tests `REPLICA IDENTITY` defaults only).
- **Snapshot/stream decode identity** is asserted e2e (same parser, same OIDs).
- **RENAME TABLE keeps replicating** via relation-OID aliasing (Zero requires
  its DDL event-trigger machinery).
- **Backup wedge detection** with a hard crash-out, plus the pre-existing
  512 MB cgroup acceptance harness Zero has no equivalent of.

## 1.3 design sketch (shared-SQLite multi-core, for the next pass)

One writer (the apply pump) owns the file. Each shard holds a pinned WAL read
transaction at batch boundary S plus a bounded in-memory delta overlay of rows
changed in (S, current-apply]; shard fetches read pinned-snapshot ∪ overlay, so
push-time overlay semantics hold without per-shard copies. Shards advance
(commit read txn, clear overlay) at their own pace after draining their
clients — leapfrogging like Zero's snapshotter, but on stock WAL (readers pin
checkpoints, so overlay size and reader lag need the same caps as the txn
buffer). This reuses the existing `SqliteSource` fetch machinery; the new piece
is the per-shard delta overlay layered into `fetch_conn`.
