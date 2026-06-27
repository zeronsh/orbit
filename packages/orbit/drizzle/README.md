# @zeronsh/orbit/drizzle

Generate an [Orbit](../../README.md) schema from a [Drizzle ORM](https://orm.drizzle.team) schema — tables, columns, and **relationships** (including many-to-many), fully type-safe.

Built on [`@zeronsh/orbit/orm-core`](../orm-core), so the same machinery can back other ORMs later; Drizzle is the first adapter.

```
Drizzle schema ──drizzleToIR──▶ SchemaIR ──┬─ buildOrbitSchema ─▶ live Orbit schema   (runtime)
                                           └─ emitSchema ───────▶ orbit-schema.gen.ts  (codegen)
```

## Generate your schema (the way to use this)

Codegen is the path: it emits an `orbit-schema.gen.ts` and **the types are generated,
not inferred** — so the TypeScript language server never has to evaluate heavy
conditional types over your whole schema (the thing that makes type-level ORM
inference hiccup on large schemas). The generated file is plain, fast-to-check source.

```bash
orbit-drizzle generate --schema ./db/schema.ts --output ./orbit-schema.gen.ts --format
# or with a config file:
orbit-drizzle generate -c orbit-drizzle.config.ts
```

`orbit-drizzle.config.ts`:

```ts
import type { GenerateOptions } from '@zeronsh/orbit/drizzle/cli';

export default {
  schemaPath: './db/schema.ts',
  outputPath: './orbit-schema.gen.ts',
} satisfies Partial<GenerateOptions>;
```

Add it to your scripts and you're done:

```jsonc
{ "scripts": { "generate:schema": "orbit-drizzle generate -c orbit-drizzle.config.ts" } }
```

> The CLI imports your schema at runtime, so run it under a TypeScript-capable Node
> (Node 22+ `--experimental-strip-types`, which the bin's shebang requests) or via `tsx`.

### Custom `.$type<>()` and enums are inherited

The generator resolves each column's TypeScript type with the **compiler** (ts-morph),
so a Drizzle `.$type<>()` and enum unions land in the generated schema verbatim —
this is the part a runtime conversion can't do. Given:

```ts
export const post = pgTable('post', {
  id: text('id').primaryKey(),
  email: text('email').$type<`${string}@${string}`>().notNull(),
  status: postStatus('status').notNull(),          // pgEnum(['draft','published','archived'])
  settings: jsonb('settings').$type<PostSettings>(),
});
```

it generates (see [`example/orbit-schema.gen.ts`](./example/orbit-schema.gen.ts)):

```ts
import type { PostSettings } from './db/schema';

const post = table('post')
  .columns({
    id: string(),
    email: string<`${string}@${string}`>(),                  // ← branded $type kept
    status: string<'draft' | 'published' | 'archived'>(),    // ← enum union kept
    settings: optional(json<PostSettings>()),                // ← custom json kept (+ import)
    // ...
  })
  .primaryKey('id');
```

### Runtime alternative (`defineOrbitSchema`)

There's also a runtime builder for quick prototyping. It's intentionally **loosely
typed** — `.$type<>()` is type-level only, so it can't be read at runtime, and Orbit
deliberately does *not* do whole-schema type inference (that's what would bog down the
TS server). Use the CLI for real type safety.

```ts
import * as schema from './db/schema';
import { relations } from './db/relations';
import { defineOrbitSchema } from '@zeronsh/orbit/drizzle';

export const orbitSchema = defineOrbitSchema(schema, { relations }); // generic SchemaDef
```

## What you get

Relationships declared in Drizzle become **named, typed relationships** on the Orbit query builder:

```ts
import { createBuilder } from '@zeronsh/orbit/client';
import { schema } from './orbit-schema.gen.ts';

const b = createBuilder(schema);

const q = b.post
  .where('status', '=', 'published')      // enum: "draft" | "published" | "archived"
  .related('author')                       // → author?: User           (a `one`)
  .related('comments', c => c.orderBy('id', 'asc'))  // → comments: Comment[]
  .related('tags');                        // → tags: Tag[]   (many-to-many, junction flattened)
```

See [`example/`](./example) for a complete blog schema and its [generated output](./example/orbit-schema.gen.ts).

## Mapping rules

| Drizzle | Orbit |
| --- | --- |
| `text` / `varchar` / `char` / `uuid` / `enum` | `string` (enums keep their literal union) |
| `integer` / `numeric` / `real` / `serial` / `bigint` | `number` |
| `timestamp` / `date` / `time` | `number` (epoch) |
| `boolean` | `boolean` |
| `json` / `jsonb` / arrays | `json` (with the column's `$type<>()`) |
| `.$type<T>()` | the column's custom TS type (CLI only) |
| nullable column / column with a DB default | `optional(...)` |
| `relations()` v2 `one` / `many` | a named relationship |
| `r.many.x({ ...through(...) })` | a many-to-many junction relationship |
| foreign keys (no relations object) | synthesized `one` / `many` relationships |

**Field names are the database column names** (`author_id`, `created_at`), because Orbit
syncs raw Postgres rows. Reference columns by their DB name in queries and mutators.

## Relationship sources

1. **Drizzle Relations v2** (`defineRelations(...)`, the drizzle-orm 1.0 API) — pass it via
   `config.relations` or export it from the schema module (auto-detected). Includes
   many-to-many via `.through(...)`.
2. **Foreign keys** — when no relations object is present, `one`/`many` relationships are
   synthesized from `.references(...)`.

## Config

```ts
defineOrbitSchema(schema, {
  relations,                 // the defineRelations(...) result (optional)
  tables: {                  // optional selection; omit for "everything"
    user: true,
    post: { id: true, secret: false },  // exclude columns
    audit_log: false,                   // exclude a whole table
  },
  fkRelationships: true,     // derive relationships from FKs when no relations given
});
```

## Adding another ORM

Implement `OrmAdapter` from `@zeronsh/orbit/orm-core` — i.e. a single `toIR()` that produces a
`SchemaIR`. Everything downstream (the runtime builder and the codegen emitter) is shared.
