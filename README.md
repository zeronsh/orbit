# Orbit

Orbit is a Rust rebuild of [Zero](https://github.com/rocicorp/mono) — a
Turborepo monorepo of Rust crates (`oql`, `orbit-cache`, `orbit-protocol`,
`orbit-schema`) plus a TypeScript client/server package, kept wire-compatible
with Zero's protocol.

## Install

The JavaScript side ships as a single, tree-shakeable package with subpath exports:

```bash
npm install @zeronsh/orbit
```

| Import | What |
| --- | --- |
| `@zeronsh/orbit/client` | Type-safe sync client (queries, optimistic mutators, relationships) |
| `@zeronsh/orbit/react` | React bindings (`useQuery`) |
| `@zeronsh/orbit/server` · `/server/pg` | Push/query endpoint helpers (pluggable DB; Postgres built in) |
| `@zeronsh/orbit/orm-core` | ORM-agnostic schema IR + adapter contract + codegen |
| `@zeronsh/orbit/drizzle` · `/drizzle/cli` | Generate an Orbit schema from a Drizzle schema (`orbit-drizzle` CLI) |

Optional peers (`react`, `drizzle-orm`, `pg`, `ts-morph`, `prettier`) are only needed
for the subpaths that use them.

## The server

The Rust sync server runs as a container image — `ghcr.io/zeronsh/orbit-server`.
See [`deploy/`](./deploy) for a one-command Docker Compose stack (Postgres + server),
environment variables, and hosting notes.

## Develop

```bash
pnpm install
pnpm check        # turbo: typecheck + test for JS + Rust
pnpm --filter @zeronsh/orbit build
```

The example app lives in [`apps/demo`](./apps/demo) (Orbit Pixels — a collaborative
canvas whose schema is generated from a Drizzle schema). Releasing is handled by
changesets; see [`PUBLISHING.md`](./PUBLISHING.md).

## License & attribution

Orbit is licensed under the [Apache License 2.0](./LICENSE). It contains code
ported and derived from Zero (Copyright Rocicorp), which is also Apache-2.0
licensed; see [NOTICE](./NOTICE) for details.

Orbit is an **independent, unofficial** reimplementation. It is not affiliated
with, sponsored by, or endorsed by Rocicorp. References to "Zero" and
"Rocicorp" are descriptive only.
