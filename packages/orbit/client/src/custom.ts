// Custom mutators + custom queries, matching Zero's authoring API:
//
//   const mutators = {
//     createTodo: defineMutator(({tx, args}: {tx: Transaction<typeof schema>; args: {text: string}}) =>
//       tx.mutate.todo.insert({ id: crypto.randomUUID(), text: args.text, completed: false, created: Date.now() })),
//   };
//   const builder = createBuilder(schema);
//   const queries = {
//     allTodos: defineQuery(() => builder.todo.orderBy('created', 'asc')),
//     todoById: defineQuery(({args}: {args: {id: string}}) => builder.todo.where('id', '=', args.id).one()),
//   };
//   const orbit = new Orbit({ server, schema, mutators, queries });
//   orbit.mutate.createTodo({ text });        // fully typed
//   useQuery(orbit.query.allTodos());          // fully typed result
//
// A mutator's `tx.mutate.*` calls are recorded into CRUD ops and sent to the
// server (which persists them to Postgres — the source of truth). A query def
// produces a typed AST the client subscribes to. Both are checked end-to-end
// against the schema.

import type { CrudOp, Row } from './protocol.ts';
import type { PkOf, RowOf, SchemaDef } from './schema.ts';
import type { QueryBuilder, Subscribable } from './query.ts';

/** Typed CRUD surface for one table inside a mutator transaction. */
export type TableCRUD<T extends Row, PK extends keyof T> = {
  insert(value: T): void;
  upsert(value: T): void;
  update(value: Pick<T, PK> & Partial<T>): void;
  delete(value: Pick<T, PK>): void;
};

/** `tx.mutate.<table>` for every table in the schema. */
export type SchemaCRUD<S extends SchemaDef> = {
  [K in keyof S['tables']]: TableCRUD<RowOf<S['tables'][K]>, PkOf<S['tables'][K]>>;
};

/** The transaction passed to a mutator (mirrors Zero's `Transaction`). */
export type Transaction<S extends SchemaDef> = {
  readonly location: 'client';
  readonly mutate: SchemaCRUD<S>;
};

// --- mutators ---------------------------------------------------------------

/**
 * A mutator's body. Receives a transaction, the client-supplied `args`, and
 * server-supplied `ctx` (the authenticated context — see the push endpoint). A
 * def that doesn't need `ctx` can simply omit it from its destructure.
 */
export type MutatorFn<S extends SchemaDef, Args, Ctx = unknown> = (c: {
  tx: Transaction<S>;
  args: Args;
  ctx: Ctx;
}) => void | Promise<void>;

/**
 * A mutator definition. Modeled as the function itself; its args type is inferred
 * from the signature. (`any` in the `tx`/args positions keeps specific defs
 * assignable to the `MutatorDefs` record.)
 */
// oxlint-disable-next-line no-explicit-any
export type MutatorDef<Args = any> = (c: { tx: Transaction<any>; args: Args; ctx: any }) => void | Promise<void>;

/** Define a custom mutator (mirrors Zero's `defineMutator`). */
export function defineMutator<S extends SchemaDef, Args, Ctx = unknown>(
  fn: MutatorFn<S, Args, Ctx>,
): MutatorDef<Args> {
  return fn as MutatorDef<Args>;
}

export type MutatorDefs = Record<string, MutatorDef>;
export type ArgsOf<M> = M extends (c: { tx: never; args: infer A; ctx: never }) => unknown ? A : never;

/** `orbit.mutate` derived from mutator defs — `tx` stripped, args kept. */
export type MutateAPI<MD extends MutatorDefs> = {
  [K in keyof MD]: ArgsOf<MD[K]> extends void | undefined
    ? () => void
    : (args: ArgsOf<MD[K]>) => void;
};

// --- queries ----------------------------------------------------------------

export type QueryFn<Args, Ctx, T extends Row> = (c: { args: Args; ctx: Ctx }) => QueryBuilder<T>;

/** A custom query definition (its args + result row types are inferred). */
// oxlint-disable-next-line no-explicit-any
export type QueryDef<Args = any, T extends Row = Row> = (c: { args: Args; ctx: any }) => QueryBuilder<T>;

/** Define a custom (named) query (mirrors Zero's `defineQuery`). */
export function defineQuery<T extends Row>(fn: () => QueryBuilder<T>): QueryDef<void, T>;
export function defineQuery<Args, Ctx, T extends Row>(fn: QueryFn<Args, Ctx, T>): QueryDef<Args, T>;
// oxlint-disable-next-line no-explicit-any
export function defineQuery(fn: any): any {
  return fn;
}

export type QueryDefs = Record<string, QueryDef>;
export type QArgsOf<Q> = Q extends (c: { args: infer A; ctx: never }) => unknown ? A : never;
export type QRowOf<Q> = Q extends (c: never) => Subscribable<infer T> ? T : never;

/** `orbit.queries` derived from query defs — call with args, get a `Subscribable`. */
export type QueriesAPI<QD extends QueryDefs> = {
  [K in keyof QD]: QArgsOf<QD[K]> extends void | undefined
    ? () => Subscribable<QRowOf<QD[K]>>
    : (args: QArgsOf<QD[K]>) => Subscribable<QRowOf<QD[K]>>;
};

// --- server-side: run a mutator def into CRUD ops -------------------------

/**
 * Run a mutator definition against a recording transaction and return the CRUD
 * ops it produced. Use this on **your push endpoint** (the server forwarded the
 * mutation to) to execute a mutator with context, then apply the ops to your DB.
 */
export function collectOps<S extends SchemaDef>(
  schema: S,
  def: MutatorDef,
  args: unknown,
  ctx?: unknown,
): CrudOp[] {
  const pk: Record<string, string[]> = {};
  for (const t of Object.values(schema.tables)) pk[t.name] = [...t.primaryKey];

  const ops: CrudOp[] = [];
  const mutate = new Proxy(
    {},
    {
      get: (_t, table: string) => {
        const make = (op: CrudOp['op']) => (value: Row) =>
          ops.push({ op, tableName: table, primaryKey: pk[table] ?? ['id'], value } as CrudOp);
        return { insert: make('insert'), upsert: make('upsert'), update: make('update'), delete: make('delete') };
      },
    },
  );
  const tx = { location: 'client', mutate } as unknown as Transaction<S>;
  void def({ tx: tx as never, args, ctx });
  return ops;
}
