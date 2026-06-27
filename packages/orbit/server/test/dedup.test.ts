// Exactly-once mutation handling in PushProcessor: a client re-sends unconfirmed
// mutations on reconnect (to this or, in multinode, any other view-syncer), so the
// push endpoint must skip mutations it has already applied. PushProcessor persists
// a per-client lastMutationID and advances it atomically inside the push
// transaction. This test proves that with a mock DBConnection — no Postgres needed.

import assert from 'node:assert/strict';
import { test } from 'node:test';

import { createSchema, string, table } from '../../client/src/index.ts';
import { PushProcessor, type DBConnection, type DBTransaction } from '../src/index.ts';

/** A mock DB that simulates the `orbit_client_mutations` dedup table in memory and
 * records the CRUD ops that actually reach the database. */
class MockDB implements DBConnection {
  readonly lastMutation = new Map<string, number>();
  readonly appliedOps: { sql: string; params: unknown[] }[] = [];

  async transaction<R>(fn: (tx: DBTransaction) => Promise<R>): Promise<R> {
    const tx: DBTransaction = {
      query: async (sql, params) => {
        if (sql.startsWith('CREATE TABLE')) return [];
        if (sql.startsWith('INSERT INTO orbit_client_mutations')) {
          // Atomic advance + skip, mirroring the real ON CONFLICT … WHERE guard.
          const [cid, id] = params as [string, number];
          const cur = this.lastMutation.get(cid) ?? -1;
          if (id > cur) {
            this.lastMutation.set(cid, id);
            return [{ client_id: cid }]; // advanced -> caller applies the mutation
          }
          return []; // already applied -> caller skips
        }
        // Any other statement is an actual CRUD op against a user table.
        this.appliedOps.push({ sql, params });
        return [];
      },
    };
    return fn(tx);
  }
}

const schema = createSchema({
  tables: [table('todo').columns({ id: string(), text: string() }).primaryKey('id')],
});

const mutators = {
  createTodo: ({ tx, args }: { tx: any; args: { id: string; text: string } }) => {
    tx.mutate.todo.insert({ id: args.id, text: args.text });
  },
};

const push = (mutations: unknown[]) =>
  new Request('http://x/push', { method: 'POST', body: JSON.stringify({ mutations }) });

const mut = (id: number, clientID: string, todoId: string) => ({
  name: 'createTodo',
  args: [{ id: todoId, text: `t${todoId}` }],
  id,
  clientID,
});

test('a re-sent mutation is applied exactly once', async () => {
  const db = new MockDB();
  const pp = new PushProcessor({ connection: db, schema, context: () => ({}) });

  // First delivery of mutation id=1 applies.
  await pp.process(mutators, push([mut(1, 'c1', 'a')]));
  assert.equal(db.appliedOps.length, 1);

  // Re-delivery of the SAME mutation (reconnect resend) is skipped — no double apply.
  await pp.process(mutators, push([mut(1, 'c1', 'a')]));
  assert.equal(db.appliedOps.length, 1);

  // A new mutation id=2 for the same client applies.
  await pp.process(mutators, push([mut(2, 'c1', 'b')]));
  assert.equal(db.appliedOps.length, 2);

  // Re-delivery of id=2 is skipped.
  await pp.process(mutators, push([mut(2, 'c1', 'b')]));
  assert.equal(db.appliedOps.length, 2);
});

test('each client has an independent lastMutationID sequence', async () => {
  const db = new MockDB();
  const pp = new PushProcessor({ connection: db, schema, context: () => ({}) });

  await pp.process(mutators, push([mut(1, 'c1', 'a')]));
  // A different client's id=1 must NOT be deduped against c1's id=1.
  await pp.process(mutators, push([mut(1, 'c2', 'b')]));
  assert.equal(db.appliedOps.length, 2);
});

test('a batch with new + already-seen mutations applies only the new ones', async () => {
  const db = new MockDB();
  const pp = new PushProcessor({ connection: db, schema, context: () => ({}) });

  await pp.process(mutators, push([mut(1, 'c1', 'a'), mut(2, 'c1', 'b')]));
  assert.equal(db.appliedOps.length, 2);

  // Resend [1,2,3]: 1 and 2 are already applied, only 3 is new.
  await pp.process(mutators, push([mut(1, 'c1', 'a'), mut(2, 'c1', 'b'), mut(3, 'c1', 'c')]));
  assert.equal(db.appliedOps.length, 3);
});
