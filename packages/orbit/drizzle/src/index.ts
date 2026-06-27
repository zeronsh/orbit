// @orbit/drizzle — generate an Orbit schema from a Drizzle ORM schema.
//
//   import * as schema from './db/schema';
//   import { relations } from './db/relations';   // defineRelations(...) (optional)
//   export const orbitSchema = defineOrbitSchema(schema, { relations });
//
// `defineOrbitSchema` builds the schema at runtime (fast, no codegen) but cannot
// see `$type<>()` custom types — those are type-level only. For a fully-typed
// schema (custom types + enums preserved), use the `orbit-drizzle generate` CLI,
// which resolves types with the TypeScript compiler and emits an `*.gen.ts`.

import { buildOrbitSchema, type OrmAdapter, type SchemaIR } from '../../orm-core/src/index.ts';
import type { SchemaDef } from '../../client/src/index.ts';
import { drizzleToIR, type DrizzleAdapterConfig } from './introspect.ts';

/** The Drizzle → Orbit adapter (implements the ORM-agnostic `OrmAdapter`). */
export const drizzleAdapter: OrmAdapter<Record<string, unknown>, DrizzleAdapterConfig> = {
  name: 'drizzle',
  toIR: drizzleToIR,
};

/**
 * Build a live Orbit schema object from a Drizzle schema (runtime path).
 *
 * Returns a generic {@link SchemaDef}: correct at runtime and usable with
 * `createBuilder` / `new Orbit({ schema })`. For a fully-typed schema that
 * preserves `.$type<>()` custom types + enums, use the `orbit-drizzle` CLI (it
 * resolves types with the TypeScript compiler — see the README).
 */
export function defineOrbitSchema(schema: Record<string, unknown>, config?: DrizzleAdapterConfig): SchemaDef {
  return buildOrbitSchema(drizzleToIR(schema, config));
}

/** Convert a Drizzle schema directly to the normalized IR (advanced / codegen). */
export function drizzleToSchemaIR(schema: Record<string, unknown>, config?: DrizzleAdapterConfig): SchemaIR {
  return drizzleToIR(schema, config);
}

export { drizzleToIR };
export type { DrizzleAdapterConfig };
