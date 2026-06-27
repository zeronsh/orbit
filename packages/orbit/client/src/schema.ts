// Type-safe schema builder, mirroring Zero's `createSchema` / `table().columns()
// .primaryKey()`. Defines tables + columns once and drives end-to-end TypeScript
// types: typed queries, typed query results, and typed mutators.

export type ValueType = 'string' | 'number' | 'boolean' | 'json' | 'null';

/** A column definition. The TS type it produces is carried in the `_type` phantom. */
export type Column<T = unknown> = {
  readonly type: ValueType;
  readonly optional: boolean;
  /** Phantom — never present at runtime; carries the column's TS type. */
  readonly _type?: T;
};

// The optional type parameter lets generated schemas carry a column's *custom*
// TypeScript type (e.g. `string<\`${string}@${string}\`>()`, `string<'a' | 'b'>()`
// for enums, `number<1 | 2>()`), so a Drizzle `$type<>()` survives into orbit.
export const string = <T extends string = string>(): Column<T> => ({ type: 'string', optional: false });
export const number = <T extends number = number>(): Column<T> => ({ type: 'number', optional: false });
export const boolean = <T extends boolean = boolean>(): Column<T> => ({ type: 'boolean', optional: false });
export const json = <T = unknown>(): Column<T> => ({ type: 'json', optional: false });

/** Mark a column nullable (its TS type gains `| null`). */
export const optional = <T>(c: Column<T>): Column<T | null> => ({ ...c, optional: true });

export type Columns = Record<string, Column>;

export type TableDef<
  Name extends string = string,
  C extends Columns = Columns,
  PK extends keyof C & string = keyof C & string,
> = {
  readonly name: Name;
  readonly columns: C;
  readonly primaryKey: readonly PK[];
};

/** `table('todo').columns({ ... }).primaryKey('id')` */
export function table<Name extends string>(name: Name) {
  return {
    columns<C extends Columns>(columns: C) {
      return {
        primaryKey<PK extends keyof C & string>(...primaryKey: PK[]): TableDef<Name, C, PK> {
          return { name, columns, primaryKey };
        },
      };
    },
  };
}

// --- relationships ----------------------------------------------------------
// Schema-level relationships, mirroring Zero's `relationships(table, ({one, many})
// => ...)`. A relationship is a chain of one or two `Connection`s: a single
// connection is a direct (FK) relationship; two connections describe a
// many-to-many through a junction table. Defining relationships once lets queries
// reference them by name — `q.related('author')` — fully typed and with the
// correlation resolved from the schema (no per-query correlation needed).

export type Cardinality = 'one' | 'many';

/** One hop of a relationship: which fields correlate and to which table. */
export type Connection = {
  readonly sourceField: readonly string[];
  readonly destField: readonly string[];
  /** Destination table *name*. */
  readonly destSchema: string;
  readonly cardinality: Cardinality;
};

/** A relationship = a 1- (direct) or 2- (junction) element connection chain. */
export type Relationship = readonly [Connection] | readonly [Connection, Connection];

/** The relationships declared for one source table. */
export type RelationshipsDef<
  Name extends string = string,
  R extends Record<string, Relationship> = Record<string, Relationship>,
> = {
  readonly name: Name;
  readonly relationships: R;
};

/** A single hop's arguments, where `destSchema` is the destination *table def*. */
type ConnectArg<Dest extends TableDef = TableDef> = {
  readonly sourceField: readonly string[];
  readonly destField: readonly string[];
  readonly destSchema: Dest;
};

type Connector<Card extends Cardinality> = {
  // direct
  <Dest extends TableDef>(arg: ConnectArg<Dest>): readonly [Connection & { destSchema: Dest['name']; cardinality: Card }];
  // junction (two hops)
  <Junction extends TableDef, Dest extends TableDef>(
    first: ConnectArg<Junction>,
    second: ConnectArg<Dest>,
  ): readonly [
    Connection & { destSchema: Junction['name']; cardinality: Card },
    Connection & { destSchema: Dest['name']; cardinality: Card },
  ];
};

const makeConnector =
  (cardinality: Cardinality) =>
  (...args: ConnectArg[]): Connection[] =>
    args.map((a) => ({
      sourceField: a.sourceField,
      destField: a.destField,
      destSchema: a.destSchema.name,
      cardinality,
    }));

/**
 * Declare the relationships for `table` (mirrors Zero's `relationships`). Pass a
 * callback that builds named relationships with `one(...)` / `many(...)`:
 *
 * ```ts
 * relationships(issue, ({ one, many }) => ({
 *   author: one({ sourceField: ['authorId'], destField: ['id'], destSchema: user }),
 *   comments: many({ sourceField: ['id'], destField: ['issueId'], destSchema: comment }),
 *   labels: many(
 *     { sourceField: ['id'],      destField: ['issueId'], destSchema: issueLabel },
 *     { sourceField: ['labelId'], destField: ['id'],      destSchema: label },
 *   ),
 * }));
 * ```
 */
export function relationships<Name extends string, R extends Record<string, Relationship>>(
  table: TableDef<Name>,
  cb: (connectors: { one: Connector<'one'>; many: Connector<'many'> }) => R,
): RelationshipsDef<Name, R> {
  const r = cb({
    one: makeConnector('one') as unknown as Connector<'one'>,
    many: makeConnector('many') as unknown as Connector<'many'>,
  });
  return { name: table.name, relationships: r };
}

/** Relationships keyed by source-table name (the shape stored on a schema). */
export type RelationshipsMap = Record<string, Record<string, Relationship>>;

export type SchemaDef<
  T extends Record<string, TableDef> = Record<string, TableDef>,
  R extends RelationshipsMap = RelationshipsMap,
> = {
  readonly tables: T;
  readonly relationships: R;
};

/** Combine table + relationship defs into a schema keyed by table name. */
export function createSchema<
  const T extends readonly TableDef[],
  const R extends readonly RelationshipsDef[] = [],
>(def: {
  tables: T;
  relationships?: R;
}): SchemaDef<
  { [K in T[number] as K['name']]: K },
  { [K in R[number] as K['name']]: K['relationships'] }
> {
  const tables = {} as Record<string, TableDef>;
  for (const t of def.tables) tables[t.name] = t;
  const rels = {} as RelationshipsMap;
  for (const r of def.relationships ?? []) rels[r.name] = r.relationships as Record<string, Relationship>;
  return { tables, relationships: rels } as SchemaDef<
    { [K in T[number] as K['name']]: K },
    { [K in R[number] as K['name']]: K['relationships'] }
  >;
}

// --- type inference ---------------------------------------------------------

/** The row type of a table def (column name -> TS value type). */
export type RowOf<T extends TableDef> = {
  [K in keyof T['columns']]: T['columns'][K] extends Column<infer V> ? V : never;
};

/** The primary-key column names of a table def. */
export type PkOf<T extends TableDef> = T['primaryKey'][number];

/** A permissive schema, used when no schema is supplied (everything is loose). */
export type AnySchema = SchemaDef<Record<string, TableDef>>;
