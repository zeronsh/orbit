// The schema **intermediate representation** (IR). Every ORM adapter (Drizzle,
// and future ones) maps its native schema into this single normalized shape; the
// runtime builder (`build.ts`) and the codegen emitter (`emit.ts`) consume only
// the IR. This is the seam that makes Orbit's schema generation ORM-agnostic:
// adding an ORM is writing a new `toIR`, nothing else.

/** Orbit's column value types (mirrors `@orbit/client`'s `ValueType`, sans null). */
export type OrbitValueType = 'string' | 'number' | 'boolean' | 'json';

export interface IRColumn {
  /** Orbit field name (the key used in queries/mutators). For Orbit this is the
   *  database column name, since Orbit syncs raw Postgres rows. */
  readonly name: string;
  readonly type: OrbitValueType;
  /** Nullable / has a database default the client can't supply. */
  readonly optional: boolean;
  /**
   * A TypeScript *type expression* to specialize the column with (e.g.
   * `` `${string}@${string}` ``, `'active' | 'inactive'`, `{ theme: string }`).
   * Emitted as `string<...>()` / `json<...>()` etc. so a Drizzle `$type<>()` and
   * enums survive into the generated schema. Type-level only; ignored at runtime.
   */
  readonly customType?: string;
  /** Source ORM column type, for diagnostics only. */
  readonly sourceType?: string;
}

export interface IRTable {
  /** Orbit table name (the database table name). */
  readonly name: string;
  readonly columns: readonly IRColumn[];
  readonly primaryKey: readonly string[];
}

export type Cardinality = 'one' | 'many';

/** One hop of a relationship chain. */
export interface IRConnection {
  readonly sourceField: readonly string[];
  readonly destField: readonly string[];
  /** Destination table name (a key into `tables`). */
  readonly destSchema: string;
  readonly cardinality: Cardinality;
}

export interface IRRelationship {
  /** Source table name. */
  readonly table: string;
  /** Relationship name (used as `q.related('<name>')`). */
  readonly name: string;
  /** 1 connection = direct (FK); 2 connections = many-to-many through a junction. */
  readonly chain: readonly IRConnection[];
}

/** The complete normalized schema an adapter produces. */
export interface SchemaIR {
  readonly tables: readonly IRTable[];
  readonly relationships: readonly IRRelationship[];
}
