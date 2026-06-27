// The built-in node-postgres adapter. A different backend (Drizzle, Kysely,
// postgres.js, …) just implements `DBConnection` the same way — see the comment
// at the bottom.

import pg from 'pg';
import type { DBConnection, DBTransaction } from './index.ts';

/**
 * Wrap a node-postgres `Pool`/`Client` (or a connection string / config) as a
 * `DBConnection` for `PushProcessor`.
 *
 * ```ts
 * import { nodePg } from '@zeronsh/orbit/server/pg';
 * const connection = nodePg(process.env.DATABASE_URL!);
 * ```
 */
export function nodePg(db: pg.Pool | pg.Client | pg.PoolConfig | string): DBConnection {
  const pool: pg.Pool | pg.Client =
    typeof db === 'string'
      ? new pg.Pool({ connectionString: db })
      : db instanceof pg.Pool || db instanceof pg.Client
        ? db
        : new pg.Pool(db);

  return {
    async transaction<R>(fn: (tx: DBTransaction) => Promise<R>): Promise<R> {
      // Use a dedicated client for the transaction when pooled.
      const client = pool instanceof pg.Pool ? await pool.connect() : pool;
      const tx: DBTransaction = {
        async query(sql, params) {
          const res = await client.query(sql, params as unknown[]);
          return res.rows;
        },
      };
      try {
        await client.query('BEGIN');
        const result = await fn(tx);
        await client.query('COMMIT');
        return result;
      } catch (e) {
        try {
          await client.query('ROLLBACK');
        } catch {
          // ignore; original error is thrown
        }
        throw e;
      } finally {
        if (pool instanceof pg.Pool) (client as pg.PoolClient).release();
      }
    },
  };
}

// Adding another adapter is just this shape:
//
//   export function drizzle(db: DrizzleDb): DBConnection {
//     return {
//       transaction: (fn) => db.transaction((dtx) =>
//         fn({ query: (sql, params) => dtx.execute(sql, params).then((r) => r.rows) })),
//     };
//   }
