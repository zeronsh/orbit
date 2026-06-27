import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store, View, MemoryKV } from '../src/index.ts';
import type { AST, CrudOp, Mutation } from '../src/protocol.ts';

const allTodos: AST = { table: 'todo', orderBy: [['created', 'asc']] };
const ins = (value: Record<string, unknown>): CrudOp => ({
  op: 'insert',
  tableName: 'todo',
  primaryKey: ['id'],
  value,
});
const crudMutation = (id: number, op: CrudOp): Mutation => ({
  type: 'crud',
  id,
  clientID: 'c',
  name: '_zero_crud',
  args: [{ ops: [op] }],
  timestamp: 0,
});

test('synced rows persist and rehydrate offline', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv); // attach
  a.applyAll([
    { op: 'put', tableName: 'todo', value: { id: 'b', text: 'beta', created: 2 } },
    { op: 'put', tableName: 'todo', value: { id: 'a', text: 'alpha', created: 1 } },
  ]);
  await a.flush();

  // Fresh store, same KV, no network: data is available immediately.
  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  const view = new View(b, allTodos);
  assert.deepEqual(view.data.map((r) => r.id), ['a', 'b']); // ordered, offline
});

test('a view created BEFORE hydrate finishes still receives the loaded rows', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  a.applyAll([
    { op: 'put', tableName: 'todo', value: { id: 'a', text: 'alpha', created: 1 } },
    { op: 'put', tableName: 'todo', value: { id: 'b', text: 'beta', created: 2 } },
  ]);
  await a.flush();

  // Simulate the real client race: create the view first, then hydrate.
  const b = new Store({ todo: ['id'] });
  const view = new View(b, allTodos); // empty at construction
  assert.equal(view.data.length, 0);
  await b.hydrate(kv); // must push the loaded rows into the live view
  assert.deepEqual(view.data.map((r) => r.id), ['a', 'b']);
});

test('deletes are persisted (removed row does not resurrect)', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  a.applyAll([{ op: 'put', tableName: 'todo', value: { id: 'x', created: 1 } }]);
  await a.flush();
  a.applyAll([{ op: 'del', tableName: 'todo', id: { id: 'x' } }]);
  await a.flush();

  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  assert.equal(b.effectiveRows('todo').length, 0);
});

test('pending mutations persist and are restored as overlay + resend list', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  const op = ins({ id: 't1', text: 'wip', created: 1 });
  a.addPending(7, [op], crudMutation(7, op));
  await a.flush();

  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  // Optimistic row visible offline before any connection.
  assert.deepEqual(b.effectiveRows('todo').map((r) => r.id), ['t1']);
  // And the originating mutation is available to resend.
  assert.deepEqual(b.pendingMutations().map((m) => m.id), [7]);
});

test('confirmed pending mutations are removed from persistence', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  const op = ins({ id: 't1', created: 1 });
  a.addPending(7, [op], crudMutation(7, op));
  await a.flush();
  a.confirmThrough(7);
  await a.flush();

  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  assert.equal(b.pendingMutations().length, 0);
});

// A full resync is an authoritative REPLACEMENT (the server prepends a `clear`): a
// row deleted elsewhere while this client was offline — which the resync no longer
// contains — must be dropped from memory AND persistence, so it can't resurrect from
// IndexedDB on reload (the "phantom pixel" bug). A row that survives the resync must
// NOT be lost to the clear→write race.
test('a full-resync clear drops stale rows but keeps survivors (no phantom, no loss on reload)', async () => {
  const kv = new MemoryKV();
  const a = new Store({ todo: ['id'] });
  await a.hydrate(kv);
  a.applyAll([
    { op: 'put', tableName: 'todo', value: { id: 'x', created: 1 } }, // deleted elsewhere while offline
    { op: 'put', tableName: 'todo', value: { id: 'z', created: 2 } }, // survives the resync
  ]);
  await a.flush();

  // Reconnect → full resync: clear, then the authoritative current set {z, y}.
  a.applyAll([
    { op: 'clear' },
    { op: 'put', tableName: 'todo', value: { id: 'z', created: 2 } },
    { op: 'put', tableName: 'todo', value: { id: 'y', created: 3 } },
  ]);
  await a.flush();
  assert.deepEqual(a.effectiveRows('todo').map((r) => r.id).sort(), ['y', 'z'], 'x dropped, z kept in memory');

  // Reload from persistence: x must not resurrect; z must not be lost.
  const b = new Store({ todo: ['id'] });
  await b.hydrate(kv);
  assert.deepEqual(b.effectiveRows('todo').map((r) => r.id).sort(), ['y', 'z'], 'no phantom x, no lost z');
});
