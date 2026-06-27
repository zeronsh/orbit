# @zeronsh/orbit/orm-core

ORM-agnostic core for generating [Orbit](../../README.md) schemas. ORM adapters
(e.g. [`@zeronsh/orbit/drizzle`](../drizzle)) map their native schema to one normalized
shape; everything downstream is shared.

```
ORM schema ──(adapter.toIR)──▶ SchemaIR ──┬─ buildOrbitSchema ─▶ live Orbit SchemaDef
                                          └─ emitSchema ───────▶ *.gen.ts source
```

## Pieces

- **`SchemaIR`** — the normalized intermediate representation: `tables`
  (name, columns with `type`/`optional`/`customType`, `primaryKey`) and
  `relationships` (named connection chains; 1 hop = direct, 2 hops = junction).
- **`OrmAdapter`** — the contract an ORM implements: a single `toIR(input, config)`.
- **`buildOrbitSchema(ir)`** — build a live `SchemaDef` at runtime (custom `$type`
  info is type-level only, so it's dropped here; enums are kept).
- **`emitSchema(ir, options)`** — emit Orbit-schema TypeScript source, preserving
  custom column types and enums (used by adapters' codegen CLIs).

## Writing an adapter

```ts
import { defineAdapter, type SchemaIR } from '@zeronsh/orbit/orm-core';

export const myAdapter = defineAdapter({
  name: 'my-orm',
  toIR(input, config): SchemaIR {
    // walk the ORM's schema → return { tables, relationships }
  },
});
```

That's the whole job — `buildOrbitSchema` and `emitSchema` do the rest.
