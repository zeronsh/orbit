// A fluent query builder producing an AST, mirroring Zero's `zero-client` query
// API (`.where()`, `.related()`, `.orderBy()`, `.limit()`, `.start()`, `.one()`).

import type { AST, Condition, CorrelatedSubquery, Correlation, Direction, SimpleOperator, Value, Row } from './protocol.ts';
import type { Connection, Relationship, RowOf, SchemaDef } from './schema.ts';

export class Query {
  #ast: AST;
  /** `.one()` was called — a related parent should treat this as singular. */
  #singular: boolean;

  private constructor(ast: AST, singular = false) {
    this.#ast = ast;
    this.#singular = singular;
  }

  static from(table: string): Query {
    return new Query({ table });
  }

  where(field: string, op: SimpleOperator, value: Value | readonly (string | number | boolean)[]): Query {
    const cond: Condition = {
      type: 'simple',
      op,
      left: { type: 'column', name: field },
      right: { type: 'literal', value },
    };
    return new Query({ ...this.#ast, where: and(this.#ast.where, cond) }, this.#singular);
  }

  whereExists(correlation: Correlation, subquery: Query, negated = false): Query {
    const cond: Condition = {
      type: 'correlatedSubquery',
      related: { correlation, subquery: subquery.ast() },
      op: negated ? 'NOT EXISTS' : 'EXISTS',
    };
    return new Query({ ...this.#ast, where: and(this.#ast.where, cond) }, this.#singular);
  }

  related(name: string, correlation: Correlation, subquery: Query): Query {
    const sub = { ...subquery.ast(), alias: name };
    const entry = { correlation, subquery: sub, singular: subquery.#singular || undefined };
    const related = [...(this.#ast.related ?? []), entry];
    return new Query({ ...this.#ast, related }, this.#singular);
  }

  /** Append a fully-formed related entry (used for junction/`hidden` chains). */
  addRelated(entry: CorrelatedSubquery): Query {
    const related = [...(this.#ast.related ?? []), entry];
    return new Query({ ...this.#ast, related }, this.#singular);
  }

  orderBy(field: string, dir: Direction): Query {
    const orderBy = [...(this.#ast.orderBy ?? []), [field, dir] as const];
    return new Query({ ...this.#ast, orderBy }, this.#singular);
  }

  limit(n: number): Query {
    return new Query({ ...this.#ast, limit: n }, this.#singular);
  }

  one(): Query {
    return new Query({ ...this.#ast, limit: 1 }, true);
  }

  start(row: Row, exclusive = false): Query {
    return new Query({ ...this.#ast, start: { row, exclusive } }, this.#singular);
  }

  /** Whether this query was marked `.one()` (singular). */
  isSingular(): boolean {
    return this.#singular;
  }

  ast(): AST {
    return this.#ast;
  }
}

function and(existing: Condition | undefined, next: Condition): Condition {
  if (!existing) return next;
  if (existing.type === 'and') return { type: 'and', conditions: [...existing.conditions, next] };
  return { type: 'and', conditions: [existing, next] };
}

// --- typed query ------------------------------------------------------------

/** A live view's read surface (avoids a circular import on `View`). */
export type ViewLike<T> = {
  data: T[];
  subscribe(fn: () => void): () => void;
  /** Release the view + its query subscription (enables TTL/GC). */
  destroy?(): void;
};

/** Anything `useQuery` can subscribe to (a typed query or a named query). */
export interface Subscribable<T> {
  materialize(): ViewLike<T>;
}

/**
 * A query builder result (a {@link TypedQuery} or {@link SchemaQuery}): subscribable
 * and able to expose its AST. Custom query defs return one of these so the client
 * can derive the AST and so the result row type can be inferred.
 */
export interface QueryBuilder<T extends Row> extends Subscribable<T> {
  ast(): AST;
  query(): Query;
}

/** What a [`TypedQuery`] needs from the client to materialize itself. */
export interface QueryHost {
  materialize(q: Query): ViewLike<Row>;
}

/**
 * A query bound to a row type `T` (from the schema) and to its client, mirroring
 * Zero's `z.query.<table>`. `where`/`orderBy` are checked against `T`'s columns,
 * and the materialized result is typed `T[]`. The `One` type parameter tracks
 * `.one()` so that when this query is used as a `related` child it types as a
 * single row (`R | undefined`) instead of an array (`R[]`).
 */
export class TypedQuery<T extends Row, One extends boolean = false> implements Subscribable<T> {
  readonly #host: QueryHost | null;
  readonly #q: Query;

  constructor(host: QueryHost | null, q: Query) {
    this.#host = host;
    this.#q = q;
  }

  where<K extends keyof T & string>(
    field: K,
    op: SimpleOperator,
    value: T[K] | readonly NonNullable<T[K]>[],
  ): TypedQuery<T, One> {
    return new TypedQuery<T, One>(this.#host, this.#q.where(field, op, value as Value));
  }

  whereExists(correlation: Correlation, subquery: TypedQuery<Row, boolean> | Query, negated = false): TypedQuery<T, One> {
    const sub = subquery instanceof TypedQuery ? subquery.query() : subquery;
    return new TypedQuery<T, One>(this.#host, this.#q.whereExists(correlation, sub, negated));
  }

  /**
   * Add a nested relationship. The result type gains `name`: a `R[]` array, or
   * `R | undefined` if the child query was `.one()`.
   */
  related<Name extends string, R extends Row, ROne extends boolean>(
    name: Name,
    correlation: Correlation,
    subquery: TypedQuery<R, ROne>,
  ): TypedQuery<T & { [K in Name]: ROne extends true ? R | undefined : R[] }, One> {
    return new TypedQuery(this.#host, this.#q.related(name, correlation, subquery.query()));
  }

  orderBy<K extends keyof T & string>(field: K, dir: Direction): TypedQuery<T, One> {
    return new TypedQuery<T, One>(this.#host, this.#q.orderBy(field, dir));
  }

  limit(n: number): TypedQuery<T, One> {
    return new TypedQuery<T, One>(this.#host, this.#q.limit(n));
  }

  one(): TypedQuery<T, true> {
    return new TypedQuery<T, true>(this.#host, this.#q.one());
  }

  start(row: Partial<T>, exclusive = false): TypedQuery<T, One> {
    return new TypedQuery<T, One>(this.#host, this.#q.start(row as Row, exclusive));
  }

  /** The underlying untyped query (escape hatch). */
  query(): Query {
    return this.#q;
  }

  ast(): AST {
    return this.#q.ast();
  }

  /** Materialize into a live, typed view. */
  materialize(): ViewLike<T> {
    if (!this.#host) throw new Error('this query is an unbound builder; subscribe via orbit.query.<name>()');
    return this.#host.materialize(this.#q) as ViewLike<T>;
  }
}

// --- schema-aware query (relationships by name) -----------------------------
// `SchemaQuery` knows its schema `S` and source table `N`, so `.related('author')`
// resolves the correlation (and child row type + cardinality) from the schema's
// declared relationships — no per-query correlation needed. This mirrors Zero's
// `z.query.<table>.related('name', q => ...)`. The explicit
// `.related(name, correlation, subquery)` form is still accepted (back-compat /
// escape hatch for ad-hoc, schema-less relationships).

/** The last hop of a relationship chain (direct → only; junction → second). */
type LastConn<R extends Relationship> = R extends readonly [Connection]
  ? R[0]
  : R extends readonly [Connection, infer Second extends Connection]
    ? Second
    : never;

/** The relationships declared for table `N` of schema `S`. */
type RelsOf<S extends SchemaDef, N extends string> = N extends keyof S['relationships']
  ? S['relationships'][N]
  : Record<never, never>;

/** Relationship names available on table `N`. */
type RelNames<S extends SchemaDef, N extends string> = keyof RelsOf<S, N> & string;

type RelChain<S extends SchemaDef, N extends string, Rel extends RelNames<S, N>> =
  RelsOf<S, N>[Rel] extends Relationship ? RelsOf<S, N>[Rel] : never;

/** Destination table name of relationship `Rel` (last hop). */
type DestName<S extends SchemaDef, N extends string, Rel extends RelNames<S, N>> =
  LastConn<RelChain<S, N, Rel>>['destSchema'] & string;

/** `'one' | 'many'` of relationship `Rel` (last hop). */
type DestCard<S extends SchemaDef, N extends string, Rel extends RelNames<S, N>> =
  LastConn<RelChain<S, N, Rel>>['cardinality'];

/** The row type of a named table in the schema. */
type TableRow<S extends SchemaDef, Name extends string> = Name extends keyof S['tables']
  ? RowOf<S['tables'][Name]>
  : Row;

/** Apply cardinality (relationship `many`/`one`, or a `.one()` child) to a row type. */
type WithCard<Card extends Cardinality, R extends Row, ChildOne extends boolean> =
  (Card extends 'one' ? true : ChildOne) extends true ? R | undefined : R[];

type Cardinality = 'one' | 'many';

/** Result shape of `.related(rel)` with no callback (uses the schema row + cardinality). */
type RelResult<S extends SchemaDef, N extends string, Rel extends RelNames<S, N>> =
  WithCard<DestCard<S, N, Rel>, TableRow<S, DestName<S, N, Rel>>, false>;

export class SchemaQuery<
  S extends SchemaDef,
  N extends string,
  T extends Row,
  One extends boolean = false,
> implements Subscribable<T> {
  readonly #host: QueryHost | null;
  readonly #schema: S;
  readonly #table: N;
  readonly #q: Query;

  constructor(host: QueryHost | null, schema: S, table: N, q: Query) {
    this.#host = host;
    this.#schema = schema;
    this.#table = table;
    this.#q = q;
  }

  #wrap<T2 extends Row, One2 extends boolean>(q: Query): SchemaQuery<S, N, T2, One2> {
    return new SchemaQuery<S, N, T2, One2>(this.#host, this.#schema, this.#table, q);
  }

  where<K extends keyof T & string>(
    field: K,
    op: SimpleOperator,
    value: T[K] | readonly NonNullable<T[K]>[],
  ): SchemaQuery<S, N, T, One> {
    return this.#wrap<T, One>(this.#q.where(field, op, value as Value));
  }

  whereExists(correlation: Correlation, subquery: SchemaQuery<S, string, Row, boolean> | Query, negated = false): SchemaQuery<S, N, T, One> {
    const sub = subquery instanceof SchemaQuery ? subquery.query() : subquery;
    return this.#wrap<T, One>(this.#q.whereExists(correlation, sub, negated));
  }

  orderBy<K extends keyof T & string>(field: K, dir: Direction): SchemaQuery<S, N, T, One> {
    return this.#wrap<T, One>(this.#q.orderBy(field, dir));
  }

  limit(n: number): SchemaQuery<S, N, T, One> {
    return this.#wrap<T, One>(this.#q.limit(n));
  }

  one(): SchemaQuery<S, N, T, true> {
    return this.#wrap<T, true>(this.#q.one());
  }

  start(row: Partial<T>, exclusive = false): SchemaQuery<S, N, T, One> {
    return this.#wrap<T, One>(this.#q.start(row as Row, exclusive));
  }

  /** Add a relationship by name (resolved from the schema), with an optional child query. */
  related<Rel extends RelNames<S, N>>(
    name: Rel,
  ): SchemaQuery<S, N, T & { [K in Rel]: RelResult<S, N, Rel> }, One>;
  related<Rel extends RelNames<S, N>, CT extends Row, COne extends boolean>(
    name: Rel,
    cb: (q: SchemaQuery<S, DestName<S, N, Rel>, TableRow<S, DestName<S, N, Rel>>>) => SchemaQuery<S, string, CT, COne>,
  ): SchemaQuery<S, N, T & { [K in Rel]: WithCard<DestCard<S, N, Rel>, CT, COne> }, One>;
  /** Add a relationship with an explicit correlation (schema-less escape hatch). */
  related<Name extends string, R extends Row, ROne extends boolean>(
    name: Name,
    correlation: Correlation,
    subquery: SchemaQuery<S, string, R, ROne> | TypedQuery<R, ROne> | Query,
  ): SchemaQuery<S, N, T & { [K in Name]: ROne extends true ? R | undefined : R[] }, One>;
  related(name: string, second?: unknown, third?: unknown): SchemaQuery<S, N, Row, One> {
    // Explicit form: (name, correlation, subquery).
    if (third !== undefined || (isCorrelation(second))) {
      const correlation = second as Correlation;
      const sub = third as SchemaQuery<S, string, Row, boolean> | TypedQuery<Row, boolean> | Query;
      const subq: Query = sub instanceof Query ? sub : (sub as { query(): Query }).query();
      return this.#wrap<Row, One>(this.#q.related(name, correlation, subq));
    }

    // By-name form: (name, cb?). Resolve the chain from the schema.
    const chain = this.#schema.relationships?.[this.#table]?.[name] as Relationship | undefined;
    if (!chain) {
      throw new Error(`Unknown relationship "${name}" on table "${this.#table}". Declare it with relationships() in the schema.`);
    }
    const cb = second as ((q: SchemaQuery<S, string, Row>) => SchemaQuery<S, string, Row, boolean>) | undefined;
    const last = chain[chain.length - 1] as Connection;

    // Build the destination (final-hop) subquery, applying the callback if given.
    let dest = new SchemaQuery<S, string, Row>(null, this.#schema, last.destSchema, Query.from(last.destSchema));
    if (cb) dest = cb(dest) as SchemaQuery<S, string, Row>;
    let destAst: AST = { ...dest.ast(), alias: name };
    const destSingular = last.cardinality === 'one' || dest.isSingular();

    if (chain.length === 1) {
      const entry: CorrelatedSubquery = {
        correlation: { parentField: [...last.sourceField], childField: [...last.destField] },
        subquery: destAst,
        singular: destSingular || undefined,
      };
      return this.#wrap<Row, One>(this.#q.addRelated(entry));
    }

    // Junction (two hops): a `hidden` related layer over the junction table whose
    // own nested related is the destination — the presentation layer flattens it
    // (matches Zero's `hidden: true` junction compilation).
    const [first, second2] = chain as readonly [Connection, Connection];
    const junctionAst: AST = {
      table: first.destSchema,
      alias: name,
      related: [
        {
          correlation: { parentField: [...second2.sourceField], childField: [...second2.destField] },
          subquery: destAst,
          singular: destSingular || undefined,
        },
      ],
    };
    const entry: CorrelatedSubquery = {
      correlation: { parentField: [...first.sourceField], childField: [...first.destField] },
      hidden: true,
      subquery: junctionAst,
    };
    return this.#wrap<Row, One>(this.#q.addRelated(entry));
  }

  isSingular(): boolean {
    return this.#q.isSingular();
  }

  /** The underlying untyped query (escape hatch). */
  query(): Query {
    return this.#q;
  }

  ast(): AST {
    return this.#q.ast();
  }

  materialize(): ViewLike<T> {
    if (!this.#host) throw new Error('this query is an unbound builder; subscribe via orbit.query.<name>()');
    return this.#host.materialize(this.#q) as ViewLike<T>;
  }
}

function isCorrelation(v: unknown): v is Correlation {
  return typeof v === 'object' && v !== null && 'parentField' in v && 'childField' in v;
}

/** Every table of a schema as a {@link SchemaQuery}, optionally bound to a host. */
export type SchemaQueries<S extends SchemaDef> = {
  [K in keyof S['tables'] & string]: SchemaQuery<S, K, RowOf<S['tables'][K]>>;
};

/**
 * Standalone, schema-aware query builders (mirrors Zero's `createBuilder`). Use
 * inside query definitions: `builder.issue.where('id', '=', args.id).related('author')`.
 * Relationships declared in the schema are available by name and fully typed. The
 * returned queries are unbound (used to produce ASTs, not subscribed directly).
 */
export function createBuilder<S extends SchemaDef>(schema: S): SchemaQueries<S> {
  return buildSchemaQueries(schema, null);
}

/** Build the per-table {@link SchemaQuery} map, optionally bound to a host. */
export function buildSchemaQueries<S extends SchemaDef>(schema: S, host: QueryHost | null): SchemaQueries<S> {
  const out = {} as Record<string, SchemaQuery<S, string, Row>>;
  for (const name of Object.keys(schema.tables)) {
    out[name] = new SchemaQuery<S, string, Row>(host, schema, name, Query.from(name));
  }
  return out as SchemaQueries<S>;
}
