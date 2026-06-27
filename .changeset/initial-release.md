---
"@zeronsh/orbit": minor
---

Initial release of `@zeronsh/orbit` — a single, tree-shakeable package with subpath
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
