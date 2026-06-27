# Orbit Pixels

A realtime collaborative infinite pixel canvas, built on Orbit.

## Schema

The Orbit client schema is **generated from a Drizzle schema** with
[`@orbit/drizzle`](../../packages/drizzle), demonstrating the schema-generation
pipeline against a real app:

```
db/schema.ts (Drizzle)  ──pnpm generate:schema──▶  src/schema.gen.ts (Orbit)
```

- [`db/schema.ts`](./db/schema.ts) — the Drizzle definitions for the `pixel` and
  `cursor` tables (mirrors [`postgres/01-init.sql`](./postgres/01-init.sql)).
- [`src/schema.gen.ts`](./src/schema.gen.ts) — generated; consumed by
  [`src/shared.ts`](./src/shared.ts), which adds the mutators and queries.

To regenerate after changing `db/schema.ts`:

```bash
pnpm generate:schema
```

The Drizzle `.$type<>()` annotations are **inherited** into the generated schema —
`color` is typed `` `#${string}` `` and `erasing` is `0 | 1`:

```ts
// src/schema.gen.ts (generated)
color: string<`#${string}`>(),
erasing: optional(number<0 | 1>()),
```

> This app's tables have no relationships, so the generated schema is flat. For a
> schema that exercises typed `.related('author')` / many-to-many generation, see
> [`packages/drizzle/example`](../../packages/drizzle/example).

The rest of the app (auth, routes, server) — _will be filled out soon._
