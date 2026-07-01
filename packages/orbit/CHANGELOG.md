# @zeronsh/orbit

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
