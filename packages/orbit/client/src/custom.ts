// Custom mutators + custom queries. Each is defined with an object:
//
//   const { defineQuery, defineMutation, builder } = createOrbitApi<typeof schema, Ctx>({ schema });
//
//   const mutators = {
//     createTodo: defineMutation({
//       args: z.object({ text: z.string() }),            // any Standard Schema validator
//       handler: ({ tx, args, ctx }) =>                   // tx, args, ctx all typed — no annotations
//         tx.mutate.todo.insert({ id: crypto.randomUUID(), text: args.text, completed: false }),
//     }),
//   };
//   const queries = {
//     allTodos: defineQuery({ handler: () => builder.todo.orderBy('created', 'asc') }),
//     todoById: defineQuery({
//       args: z.object({ id: z.string() }),
//       handler: ({ args }) => builder.todo.where('id', '=', args.id).one(),
//     }),
//   };
//   const orbit = new Orbit({ server, schema, mutators, queries, context: () => myCtx });
//
// `args` is a Standard Schema validator (Zod/Valibot/ArkType/…): its output type is
// inferred for `args`, and the server validates client input against it at runtime.
// `ctx` is the authenticated context — server-derived from the request (authoritative),
// and also supplied to the client (via the `context` option) so optimistic mutations
// and local query derivation run with the same ctx. The client never sends ctx over
// the wire. A def that doesn't need `args`/`ctx` simply omits them.

import type { StandardSchemaV1 } from '@standard-schema/spec';
import type { CrudOp, Row } from './protocol.ts';
import type { AnySchema, PkOf, RowOf, SchemaDef } from './schema.ts';
import { createBuilder } from './query.ts';
import type { QueryBuilder, SchemaQueries, Subscribable } from './query.ts';

/** A Standard Schema validator (e.g. a Zod/Valibot/ArkType schema). */
export type Validator<Output = unknown> = StandardSchemaV1<unknown, Output>;

/** The parsed (output) args type of a validator, or `void` when there's none. */
export type InferArgs<A> = A extends StandardSchemaV1 ? StandardSchemaV1.InferOutput<A> : void;

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

// --- definitions ------------------------------------------------------------

/** A mutator definition: an optional `args` validator + a `handler`. */
export type MutationConfig<S extends SchemaDef, Ctx, A extends StandardSchemaV1 | undefined> = {
  args?: A;
  handler: (c: { tx: Transaction<S>; args: InferArgs<A>; ctx: Ctx }) => void | Promise<void>;
};

/** A query definition: an optional `args` validator + a `handler` returning a query. */
export type QueryConfig<S extends SchemaDef, Ctx, A extends StandardSchemaV1 | undefined, T extends Row> = {
  args?: A;
  handler: (c: { args: InferArgs<A>; ctx: Ctx }) => QueryBuilder<T>;
};

/** A stored mutator definition. (`any` in the type positions keeps specific defs
 * assignable to the `MutationDefs` record while preserving Args/Ctx for inference.) */
// oxlint-disable-next-line no-explicit-any
export type MutationDef<Args = any, Ctx = any> = {
  args?: StandardSchemaV1;
  // oxlint-disable-next-line no-explicit-any
  handler: (c: { tx: Transaction<any>; args: Args; ctx: Ctx }) => void | Promise<void>;
};

/** A stored query definition (its args + result row + ctx types are inferred). */
// oxlint-disable-next-line no-explicit-any
export type QueryDef<Args = any, T extends Row = Row, Ctx = any> = {
  args?: StandardSchemaV1;
  handler: (c: { args: Args; ctx: Ctx }) => QueryBuilder<T>;
};

/**
 * The schema- and context-bound authoring API returned by {@link createOrbitApi}:
 * `defineQuery`/`defineMutation` whose handlers are fully typed (`tx`, `args`, `ctx`)
 * with no per-def annotations, plus a `builder` for the bound schema.
 */
export interface OrbitApi<S extends SchemaDef, Ctx> {
  builder: SchemaQueries<S>;
  defineMutation<A extends StandardSchemaV1 | undefined = undefined>(
    config: MutationConfig<S, Ctx, A>,
  ): MutationDef<InferArgs<A>, Ctx>;
  defineQuery<A extends StandardSchemaV1 | undefined = undefined, T extends Row = Row>(
    config: QueryConfig<S, Ctx, A, T>,
  ): QueryDef<InferArgs<A>, T, Ctx>;
}

/**
 * Bind the schema + context types once and get `defineQuery`/`defineMutation`/`builder`
 * whose handlers are fully typed (`tx`, `args`, `ctx`) with no per-def annotations.
 */
export function createOrbitApi<S extends SchemaDef, Ctx = unknown>(opts: { schema: S }): OrbitApi<S, Ctx> {
  return {
    builder: createBuilder(opts.schema),
    defineMutation: ((config: unknown) => config) as OrbitApi<S, Ctx>['defineMutation'],
    defineQuery: ((config: unknown) => config) as OrbitApi<S, Ctx>['defineQuery'],
  };
}

/** Define a custom mutator without a factory (`tx` loosely typed, `ctx` is `unknown`).
 * Prefer {@link createOrbitApi} for fully-typed `tx`/`ctx`. */
export function defineMutation<A extends StandardSchemaV1 | undefined = undefined>(config: {
  args?: A;
  handler: (c: { tx: Transaction<AnySchema>; args: InferArgs<A>; ctx: unknown }) => void | Promise<void>;
}): MutationDef<InferArgs<A>, unknown> {
  return config as unknown as MutationDef<InferArgs<A>, unknown>;
}

/** Define a custom (named) query without a factory (`ctx` is `unknown`).
 * Prefer {@link createOrbitApi} for a fully-typed `ctx`. */
export function defineQuery<A extends StandardSchemaV1 | undefined = undefined, T extends Row = Row>(config: {
  args?: A;
  handler: (c: { args: InferArgs<A>; ctx: unknown }) => QueryBuilder<T>;
}): QueryDef<InferArgs<A>, T, unknown> {
  return config as unknown as QueryDef<InferArgs<A>, T, unknown>;
}

// --- derived API types ------------------------------------------------------

export type MutationDefs = Record<string, MutationDef>;
export type QueryDefs = Record<string, QueryDef>;

/** The handler's parameter object (`{ tx?, args, ctx }`). Captured whole so that
 * `args`/`ctx` can be picked without tripping over function-param contravariance. */
type ParamOf<D> = D extends { handler: (c: infer C) => unknown } ? C : never;

export type ArgsOf<M> = ParamOf<M> extends { args: infer A } ? A : never;
export type QArgsOf<Q> = ArgsOf<Q>;
// oxlint-disable-next-line no-explicit-any
export type QRowOf<Q> = Q extends { handler: (...a: any[]) => QueryBuilder<infer T> } ? T : never;

/** The Ctx type shared by a record of defs (used to type the client's `context`). */
export type CtxOf<D> = ParamOf<D[keyof D]> extends { ctx: infer C } ? C : unknown;

/** `orbit.mutate` derived from mutator defs — `tx` stripped, args kept. */
export type MutateAPI<MD extends MutationDefs> = {
  [K in keyof MD]: ArgsOf<MD[K]> extends void | undefined
    ? () => void
    : (args: ArgsOf<MD[K]>) => void;
};

/** `orbit.queries` derived from query defs — call with args, get a `Subscribable`. */
export type QueriesAPI<QD extends QueryDefs> = {
  [K in keyof QD]: QArgsOf<QD[K]> extends void | undefined
    ? () => Subscribable<QRowOf<QD[K]>>
    : (args: QArgsOf<QD[K]>) => Subscribable<QRowOf<QD[K]>>;
};

// --- runtime helpers --------------------------------------------------------

/**
 * Validate (and parse) `args` against a Standard Schema validator. Returns the
 * parsed value; throws on invalid input. With no validator, returns args as-is.
 * Used by the server processors on untrusted client input.
 */
export async function validateArgs(validator: StandardSchemaV1 | undefined, args: unknown): Promise<unknown> {
  if (!validator) return args;
  let result = validator['~standard'].validate(args);
  if (result instanceof Promise) result = await result;
  if (result.issues) {
    throw new Error(`invalid arguments: ${JSON.stringify(result.issues)}`);
  }
  return result.value;
}

/**
 * Run a mutator definition against a recording transaction and return the CRUD
 * ops it produced. Use this on **your push endpoint** (the server forwarded the
 * mutation to) to execute a mutator with context, then apply the ops to your DB.
 */
export function collectOps<S extends SchemaDef>(
  schema: S,
  def: MutationDef,
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
  void def.handler({ tx: tx as never, args, ctx });
  return ops;
}
