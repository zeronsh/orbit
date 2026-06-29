# @zeronsh/orbit

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
