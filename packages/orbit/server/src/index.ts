// @orbit/server — server-side helpers for the endpoints orbit-cache forwards to
// (push + query). Mirrors Zero's `@rocicorp/zero/server`: a `PushProcessor`
// runs your custom mutators against a pluggable database `connection`, and a
// `QueryProcessor` resolves named queries. The database is abstracted behind a
// small `DBConnection` interface, so any adapter works (a `pg` one ships in
// `@orbit/server/pg`; a Drizzle/Kysely/etc. adapter just implements the same
// interface).

import { collectOps, type CrudOp, type MutatorDef, type QueryDef, type SchemaDef } from '../../client/src/index.ts';

/** A transaction handle: run a parameterized SQL statement. */
export interface DBTransaction {
  query(sql: string, params: unknown[]): Promise<unknown[]>;
}

/** A database connection: run work inside a transaction. Implement this to add
 * a new adapter (the built-in `@orbit/server/pg` adapter implements it). */
export interface DBConnection {
  transaction<R>(fn: (tx: DBTransaction) => Promise<R>): Promise<R>;
}

/** Derive the authenticated context from the forwarded request (return `null`
 * to reject with 401). This is where auth lives. */
export type ContextFn<Ctx> = (request: Request) => Ctx | null | Promise<Ctx | null>;

const ident = (s: string) => '"' + s.replace(/"/g, '""') + '"';

/** Apply one CRUD op as parameterized SQL via the adapter's `query`. The SQL is
 * generated here (shared); the adapter only executes it. */
export async function applyCrudOp(tx: DBTransaction, op: CrudOp): Promise<void> {
  const value = op.value as Record<string, unknown>;
  if (op.op === 'insert' || op.op === 'upsert') {
    const cols = Object.keys(value);
    const placeholders = cols.map((_, i) => `$${i + 1}`).join(', ');
    let sql = `INSERT INTO ${ident(op.tableName)} (${cols.map(ident).join(', ')}) VALUES (${placeholders})`;
    if (op.op === 'upsert') {
      const set = cols.filter((c) => !op.primaryKey.includes(c));
      sql +=
        ` ON CONFLICT (${op.primaryKey.map(ident).join(', ')}) DO ` +
        (set.length ? `UPDATE SET ${set.map((c) => `${ident(c)}=EXCLUDED.${ident(c)}`).join(', ')}` : 'NOTHING');
    }
    await tx.query(sql, cols.map((c) => value[c]));
  } else if (op.op === 'update') {
    const set = Object.keys(value).filter((c) => !op.primaryKey.includes(c));
    const sql =
      `UPDATE ${ident(op.tableName)} SET ${set.map((c, i) => `${ident(c)}=$${i + 1}`).join(', ')} ` +
      `WHERE ${op.primaryKey.map((c, i) => `${ident(c)}=$${set.length + i + 1}`).join(' AND ')}`;
    await tx.query(sql, [...set.map((c) => value[c]), ...op.primaryKey.map((c) => value[c])]);
  } else if (op.op === 'delete') {
    const sql = `DELETE FROM ${ident(op.tableName)} WHERE ${op.primaryKey.map((c, i) => `${ident(c)}=$${i + 1}`).join(' AND ')}`;
    await tx.query(sql, op.primaryKey.map((c) => value[c]));
  }
}

/** Runs custom mutators forwarded to your push endpoint. One line per route:
 * `POST: ({request}) => pushProcessor.process(mutators, request)`. */
export class PushProcessor<Ctx = unknown> {
  readonly #connection: DBConnection;
  readonly #schema: SchemaDef;
  readonly #context: ContextFn<Ctx>;
  #schemaReady = false;

  constructor(opts: { connection: DBConnection; schema: SchemaDef; context: ContextFn<Ctx> }) {
    this.#connection = opts.connection;
    this.#schema = opts.schema;
    this.#context = opts.context;
  }

  async process(mutators: Record<string, MutatorDef>, request: Request): Promise<Response> {
    const ctx = await this.#context(request);
    if (ctx == null) return new Response('unauthorized', { status: 401 });
    const body = await request.json();
    await this.#connection.transaction(async (tx) => {
      if (!this.#schemaReady) {
        await tx.query(
          'CREATE TABLE IF NOT EXISTS orbit_client_mutations (' +
            'client_id text PRIMARY KEY, last_mutation_id bigint NOT NULL)',
          [],
        );
        this.#schemaReady = true;
      }
      for (const m of (body.mutations ?? []) as { name: string; args?: unknown[]; id: number; clientID: string }[]) {
        const def = mutators[m.name];
        if (!def) continue;
        // Exactly-once: advance the client's lastMutationID, skipping mutations
        // already applied (a client re-sends unconfirmed mutations on reconnect —
        // to this or, in multinode, any other node). The WHERE guard makes the
        // advance + skip atomic in this transaction.
        const advanced = await tx.query(
          'INSERT INTO orbit_client_mutations (client_id, last_mutation_id) VALUES ($1, $2) ' +
            'ON CONFLICT (client_id) DO UPDATE SET last_mutation_id = EXCLUDED.last_mutation_id ' +
            'WHERE orbit_client_mutations.last_mutation_id < EXCLUDED.last_mutation_id RETURNING client_id',
          [m.clientID, m.id],
        );
        if (advanced.length === 0) continue; // already applied — skip
        for (const op of collectOps(this.#schema, def, m.args?.[0], ctx)) {
          await applyCrudOp(tx, op);
        }
      }
    });
    return Response.json({});
  }
}

/** Resolves named queries forwarded to your query endpoint. One line per route:
 * `POST: ({request}) => queryProcessor.process(queries, request)`. */
export class QueryProcessor<Ctx = unknown> {
  readonly #context: ContextFn<Ctx>;

  constructor(opts: { context: ContextFn<Ctx> }) {
    this.#context = opts.context;
  }

  async process(queries: Record<string, QueryDef>, request: Request): Promise<Response> {
    const ctx = await this.#context(request);
    if (ctx == null) return new Response('unauthorized', { status: 401 });
    const body = await request.json();
    const def = queries[body.name];
    if (!def) return new Response('unknown query', { status: 404 });
    const ast = def({ args: (body.args ?? [])[0], ctx }).ast();
    return Response.json({ ast });
  }
}
