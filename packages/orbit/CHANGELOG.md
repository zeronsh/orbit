# @zeronsh/orbit

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
