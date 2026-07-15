# @zeronsh/orbit

## 0.4.0

### Minor Changes

- 0b83758: Correctness + scaling release (server): every Tier 0 wall from the Zero
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

## 0.3.12

### Patch Changes

- 9959f02: Bound clustered-mode memory with SQLite-backed replicas and streamed snapshots,
  byte-capped change and hydration pipelines, shared row/event storage, bounded
  concurrent hydrations, and a container-friendly allocator. Add readiness,
  liveness, and Prometheus metrics endpoints, and validate the cluster under hard
  512 MB memory limits.

## 0.3.11

### Patch Changes

- 2af785a: Store fixed-size row fingerprints in persisted client views to prevent reconnect hydration from duplicating large row payloads in memory.

## 0.3.10

### Patch Changes

- 2ddd434: Immediately detach dropped IVM query pipelines and keep source push work proportional to live subscriptions.

## 0.3.9

### Patch Changes

- b2a9e71: Durability + robustness review pass (two adversarial audits against Zero's
  invariants): real transactions and crash-consistent resume for the durable
  replica, ack-after-durable WAL discipline, pipeline lifecycle without leaks,
  consumed (not poison) mutation errors, and a staleness gate for cross-node
  resume.

  **Durable SQLite replica is now actually durable.**
  - All tables share ONE database (`dir/replica.db`); each upstream Postgres
    transaction applies inside a single SQLite transaction — a crash rolls back
    instead of persisting a torn half-transaction (previously: per-table DB files
    - autocommit per event).
  - The commit LSN is recorded inside that same transaction as a resume
    watermark; on boot a durable replica resumes from the slot (skipping
    re-delivered transactions) instead of re-running a full initial sync.
  - A fresh initial sync clears the replica first: rows deleted upstream while
    offline no longer survive as phantoms (the sync only upserts). The whole
    sync is one transaction.

  **WAL acknowledgement follows durable commits, not receipt.** The replication
  stream acks only the consumer-confirmed position: the SQLite replica confirms
  per committed transaction; the multinode replicator confirms the change-log
  writer's durably-flushed watermark. The change-log writer now retries failed
  batches instead of dropping them — a dropped batch was a silent,
  contiguous-looking hole that delta-resuming view-syncers skipped over
  (divergence). Corrupt/unreadable log entries now truncate the read (→ Reset)
  instead of becoming silent no-ops with valid positions.

  **Pipeline lifecycle: weak output links.** Operators own their inputs; outputs
  are now `Weak`. Dropping a query's terminal unravels the whole chain, and
  sources prune dead connections on push (reusing their slots). Previously every
  churned query leaked its full pipeline forever — receiving every future
  change, growing CPU and memory without bound. Removing a query also now emits
  `del` patches for rows no other query provides (previously the refcounts
  leaked, permanently suppressing later genuine deletes of shared rows).

  **Mutations fail loud, not poison.** A failing or unknown mutation on the
  direct-write path is consumed (lastMutationID advances) with a `MutationFailed`
  error to the client — previously any PG error killed the socket, and the
  client's replay made it a permanent reconnect storm; an unknown mutator was
  silently dropped while still acked. Number literals are range-checked
  (no silent `1e20 → i64::MAX` corruption; non-finite → NULL).

  **Cross-node resume hardening.** The CVR records the stream position its view
  reflects and is loaded in a single statement (no rows/version tear under
  concurrent writers). A node whose replica is behind a connecting client's view
  waits briefly for catch-up, then falls back to a full resync — never the fast
  delta (which could suppress rows against a stale replica). Stale ephemeral
  CVRs are garbage-collected after 7 days. View-syncers apply change-stream
  transactions atomically (hydrations can no longer observe torn
  mid-transaction state), page through the durable log on catch-up instead of
  resetting past one page, and exit after repeated decode failures at a frozen
  watermark instead of reconnect-looping while serving ever-staler data.

  **Misc hardening:** release builds abort on panic (a panicked shard task
  restarts the process instead of silently serving stale data); the app-endpoint
  forwarder has timeouts; snapshot writes fsync before rename and use unique
  temp names (concurrent writers during deploy overlap); replication frames are
  length-sanity-checked; the client survives a rejected `auth()` (previously
  stuck permanently with no reconnect); `lastMutationID`s are seeded from
  Postgres at boot so pre-restart mutations still ack to reconnecting clients.

## 0.3.8

### Patch Changes

- f0f612b: SQLite-backed replica: full SQL pushdown, fixing the "full table scan + in-memory
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

## 0.3.7

### Patch Changes

- 964894b: Engine performance: capped-window `Take`, bounded (`limit`-hinted) fetches, and
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

## 0.3.6

### Patch Changes

- 9875aaa: Server performance: incrementally-sorted join indexes and a leaner poke path.
  No wire-format or API changes; verified against the full Zero-differential
  harness (5,800 scenarios) with identical results.

  - **Sorted secondary indexes.** A constrained fetch (the join-key lookup that
    runs on every join push) used to re-sort its bucket on every call, making a
    join key's push cost grow with its fan-in — 73× slower at 10k children per
    key. Buckets are now kept sorted incrementally (an upper-bound binary insert
    that reproduces the stable sort's ordering exactly, including ties), so the
    per-fetch sort is gone: join push is now flat through fan-in ~100 (1.03M/s,
    up from 713K/s) and 3.7–5.7× faster at fan-in 1,000–10,000. Unconstrained
    fetches are unchanged (no new per-push maintenance cost on tables that joins
    never constrain — filter/fanout workloads are unaffected).
  - **Row puts share the IVM row (`Rc<Row>`)** instead of deep-cloning every row
    into every client's patch (`RowPatchOp::Put.value` is now `Rc<Row>`;
    serializes identically). With 200 clients, one change previously deep-cloned
    the same row 200 times just to serialize and discard it.
  - **`RowRefs` (per-connection CVR refcounts) is keyed table → pk-values**
    instead of a flat `(String, Vec<Value>)` key, removing a table-name `String`
    allocation per row event.
  - New `wire_bench` example decomposes the full per-client serving cost
    (IVM → patch build → JSON). Net effect on the end-to-end poke path:
    **~33% more throughput** (3.9M → 5.2M client-events/s per core); the patch
    build stage alone is ~37% cheaper.

## 0.3.5

### Patch Changes

- 05eecb8: Client cache + sync hardening pass: the IndexedDB cache is now crash-atomic,
  single-writer across tabs, and cheaper; a mutation-id durability race is closed;
  single-process servers can no longer serve silently-stale data.

  - **Atomic (and faster) cache flush.** `KV` gains an optional `batch(ops)`;
    `IDBKV` implements it as ONE IndexedDB transaction and the store's flush now
    uses it — the whole flush (resync clear + rows + pending mutations + cookie)
    commits all-or-nothing. The cookie can no longer be ahead of the rows at _any_
    crash point, the row set can't tear mid-flush, and a large poke costs one
    IndexedDB commit instead of one transaction per key. Custom `KV`s without
    `batch` keep the previous carefully-ordered sequential path.
  - **Multi-tab safety (Web Locks single-writer).** Two tabs used to share the
    restored `clientID` and write the same IndexedDB keyspace with no coordination —
    last-writer-wins could persist a cookie covering rows only the other tab wrote,
    and both tabs drove the same server-side client view. Now the first tab takes a
    Web Lock and owns persistence; later tabs run memory-only with their own fresh
    `clientID` (a full server sync — correct, just uncached). The lock releases on
    `close()` or tab death, so a reload elects a new leader. Environments without
    Web Locks are unchanged.
  - **Mutation ids are durable before the push is sent.** The `nextMutationID`
    high-water mark was persisted fire-and-forget while the push went out
    immediately; if the tab died after the server recorded the id but before the
    write committed, a reload could reuse the id for a different mutation — which
    the server silently drops as already-processed. The push now goes out only
    after the id write is durable.
  - **`IDBKV.entries(prefix)` scans only the prefix's key range** instead of
    materializing the whole store, making hydrate proportional to what it reads.
  - **Single-process servers exit instead of freezing on replication errors.**
    `run_server`/`run_server_sharded` used to stop their replication pump on error
    and keep serving stale data forever. They now exit (crash-only, same policy as
    the view-syncer's Reset handling) so an orchestrator restart re-syncs fresh.
    The multinode replicator/view-syncer already reconnect and are unchanged.

  Also normalizes a literal NUL byte in `store.ts` to its `\u0000` escape
  (identical runtime value; the raw byte made tooling treat the file as binary).

## 0.3.4

### Patch Changes

- 79c3ac2: Correctness/robustness fixes from a differential audit against Zero (`zql`), plus
  regression coverage. No API changes.

  - **Client string ordering now matches the server (UTF-8 / code-point order).**
    `compareValues` compared strings with JS `<` (UTF-16 code units), which mis-orders
    non-BMP characters (emoji, supplementary CJK) relative to the Rust server's
    `compare_utf8` (byte order) — so `orderBy` and range filters (`<`,`<=`,`>`,`>=`)
    could disagree with the authoritative result until the next poke. It now iterates
    code points. (Mirrors Zero PR #6088.)
  - **A too-large "poison" mutation no longer reconnect-loops forever.** On WebSocket
    close 1009 (message too big) the client dropped straight into its reconnect/resend
    loop, re-sending the oversized (persisted) mutation every time and wedging the whole
    queue. It now drops the offending mutation and reports it via `onError`. (Zero #5982.)
  - **The replicator recovers from a silently-dead Postgres stream.** The logical-
    replication read had no inbound-liveness bound, so a half-open connection (acks
    flowing into the void) hung forever. An idle read-timeout now surfaces the stall so
    the existing reconnect-and-resume path takes over. (Zero #6047.)
  - **The direct-write mutation path deduplicates re-delivered mutations** (skips ids at
    or below the client's recorded `lastMutationID`) so a reconnect replay can't
    double-apply non-idempotent ops.

  Also verified (via a 5,800-scenario fuzz sweep generated from Zero and ground-truthed
  against SQLite) that Orbit's query engine matches Zero everywhere except a Zero bug in
  nested correlated `EXISTS(… NOT EXISTS …)`, where Orbit is the SQL-correct side; a
  regression test locks that behavior in.

## 0.3.3

### Patch Changes

- a191a00: Fix permanent, asymmetric sync divergence across reloads (two devices showing
  different state, one device's writes never reaching the other even after refresh).

  The client persisted its resume **cookie** immediately on every `pokeEnd`, but the
  **rows** that cookie covers only flushed to IndexedDB on a 50 ms debounce. A reload
  in that window (common while another user was actively drawing) restored a cookie
  that was _ahead_ of the durable rows. On reconnect the client sent that cookie as
  `baseCookie`; the server's delta-resume matched it exactly and suppressed the rows
  the client had never actually stored — so those rows were lost forever, and only a
  CVR reset could recover the device.

  The cookie is now owned and persisted by the row store, written in `flush()`
  **after** the row writes it covers (and loaded back in `hydrate()`). This enforces
  Zero's invariant that the persisted cookie is never ahead of the persisted rows: a
  crash/reload can only ever restore a cookie at or behind the durable rows, so the
  server re-sends anything missing (idempotently) instead of suppressing it.

- a191a00: Server: commit the per-client CVR (rows + version) atomically, and make schema
  init concurrency-safe.

  `commit_client_view` previously issued the row upserts, row deletes, and the
  version write as three separate autocommit statements. A crash or a concurrent
  checkpoint between them could leave `orbit_cvr_clients.version` inconsistent with
  `orbit_cvr_client_rows` — the server-side mirror of the "cookie ahead of rows"
  divergence: a reconnect reporting that version takes the fast delta path, which
  trusts the stored rows to match. It is now a single CTE (one implicit, all-or-
  nothing Postgres transaction), so the stored `(rows, version)` can never tear. An
  explicit transaction is unusable here because all client connections share one
  pooled `tokio_postgres::Client`; a single statement keeps atomicity without
  capturing other tasks' pipelined queries.

  `ensure_schema` now serializes its `CREATE TABLE IF NOT EXISTS` block behind a
  database-wide advisory lock. `CREATE TABLE IF NOT EXISTS` is not concurrency-safe
  (two nodes booting together against a fresh database race on `pg_type` and one
  fails with a duplicate-key error); the lock lets exactly one connection create the
  schema while the rest wait and then find it present.

## 0.3.2

### Patch Changes

- bba33e9: Fix optimistic rows reverting while another client writes.

  The view-syncer used to flush a client's `lastMutationID` ack on any replication
  tick — so another client's write (its own tick) could confirm your mutation
  before your row returned via replication, dropping your optimistic overlay for a
  beat (the "my pixels revert while someone else draws" bug). The ack now rides
  atomically with the mutation's own rows: it's derived from the replicated
  `orbit_client_mutations` table (written by the PushProcessor in the same
  transaction as the data), so a client's ack and its rows always land in the same
  commit → same poke. `orbit_client_mutations` is now included in the replication
  publication automatically. Server/binary only — no TS API change.

## 0.3.1

### Patch Changes

- 31e9a72: Server: Postgres connections now support TLS and password auth.

  The `orbit-server`/`orbit-node` binaries (and the `ghcr.io/zeronsh/orbit-server`
  image) can now connect to managed Postgres (Railway/Neon/Supabase). Configure via
  `DATABASE_URL` (`postgres://user:pass@host:port/db?sslmode=require`), or the
  discrete `ORBIT_PG_PASSWORD`/`PGPASSWORD` + `ORBIT_PG_SSLMODE`/`PGSSLMODE`
  (`disable` | `require` | `verify-full`). Both the SQL connections and the logical
  replication stream are secured. No TS API change (server/binary only).

## 0.3.0

### Minor Changes

- 630f368: Redesign custom query/mutator authoring around `{ args, handler }` + a bound factory.

  - `defineQuery`/`defineMutation` now take `{ args?, handler }`. `args` is any Standard Schema validator (Zod/Valibot/ArkType); its output type is inferred for `args`, and the server validates client input against it at runtime.
  - New `createOrbitApi<typeof schema, Ctx>({ schema })` returns `{ defineQuery, defineMutation, builder }` whose handlers have fully-typed `tx`/`args`/`ctx` with no per-def annotations.
  - The Orbit client now accepts a typed `context` (a value or `() => Ctx`) so optimistic mutations and local query derivation run with the real ctx. The server still derives ctx authoritatively from the auth token; ctx is never sent over the wire.

  BREAKING: `defineMutator` (a bare function) is replaced by `defineMutation({ args, handler })`, and `defineQuery` no longer accepts a bare function. The `MutatorDef`/`MutatorDefs` types are renamed to `MutationDef`/`MutationDefs`.

## 0.2.0

### Minor Changes

- 62388ed: Remove the permission system. Access is now gated only by custom queries/mutators. The `static` (authData/preMutationRow) value position has been removed from the wire protocol and AST types.

## 0.1.0

### Minor Changes

- ae1033b: Initial release of `@zeronsh/orbit` — a single, tree-shakeable package with subpath
  exports:

  - `@zeronsh/orbit/client` — type-safe sync client (queries, optimistic mutators,
    schema-level relationships with by-name `.related()`).
  - `@zeronsh/orbit/react` — React bindings (`useQuery`).
  - `@zeronsh/orbit/server` + `@zeronsh/orbit/server/pg` — push/query endpoint helpers
    with a pluggable DB adapter (Postgres built in).
  - `@zeronsh/orbit/orm-core` — ORM-agnostic schema IR + adapter contract + codegen.
  - `@zeronsh/orbit/drizzle` + `@zeronsh/orbit/drizzle/cli` — generate an Orbit schema
    from a Drizzle schema, preserving `.$type<>()` + enums; includes the `orbit-drizzle`
    CLI.
