// @orbit/orm-core — ORM-agnostic schema generation for Orbit.
//
//   ORM schema ──(adapter.toIR)──▶ SchemaIR ──┬─(buildOrbitSchema)─▶ live SchemaDef
//                                             └─(emitSchema)───────▶ .gen.ts source
//
// Adapters (e.g. @orbit/drizzle) implement `OrmAdapter` by producing a `SchemaIR`.

export type {
  SchemaIR,
  IRTable,
  IRColumn,
  IRConnection,
  IRRelationship,
  Cardinality,
  OrbitValueType,
} from './ir.ts';
export type { OrmAdapter } from './adapter.ts';
export { defineAdapter } from './adapter.ts';
export { buildOrbitSchema } from './build.ts';
export { emitSchema, type EmitOptions } from './emit.ts';
