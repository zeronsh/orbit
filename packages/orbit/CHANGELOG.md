# @zeronsh/orbit

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
