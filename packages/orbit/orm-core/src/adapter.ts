// The adapter contract. An ORM adapter converts its native schema into the
// {@link SchemaIR}. That's the entire surface a new ORM has to implement —
// everything downstream (runtime build + codegen) is shared.

import type { SchemaIR } from './ir.ts';

/**
 * Converts an ORM's native schema (`Input`) into Orbit's normalized {@link SchemaIR}.
 *
 * @typeParam Input  the ORM's schema object (e.g. a Drizzle `* as schema` import).
 * @typeParam Config adapter-specific options (table/column selection, casing, …).
 */
export interface OrmAdapter<Input = unknown, Config = unknown> {
  /** Stable adapter id, e.g. `'drizzle'`. Used in generated-file headers. */
  readonly name: string;
  /** Produce the normalized IR for the given schema + options. */
  toIR(input: Input, config?: Config): SchemaIR;
}

/** Helper to define an adapter with inferred generics. */
export function defineAdapter<Input, Config>(adapter: OrmAdapter<Input, Config>): OrmAdapter<Input, Config> {
  return adapter;
}
