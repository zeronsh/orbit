import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store, View } from '../src/index.ts';
import type { AST, CrudOp } from '../src/protocol.ts';

const ins = (value: Record<string, unknown>): CrudOp => ({
  op: 'insert',
  tableName: 'todo',
  primaryKey: ['id'],
  value,
});

type Todo = { id: string; created: number; text?: string; owner?: string; completed?: boolean };
const allTodos: AST = { table: 'todo', orderBy: [['created', 'asc']] };

test('optimistic insert shows immediately, then rebases on confirm', () => {
  const store = new Store({ todo: ['id'] });
  const view = new View<Todo>(store, allTodos);
  assert.equal(view.data.length, 0);

  // Optimistic mutation #1 — visible before any server round-trip.
  store.addPending(1, [ins({ id: 't1', text: 'hi', created: 1 })]);
  assert.deepEqual(view.data.map((r) => r.id), ['t1']);

  // Server confirms with the real row (note: different field the server stamped).
  store.applyAll([{ op: 'put', tableName: 'todo', value: { id: 't1', text: 'hi', created: 1, owner: 'me' } }]);
  store.confirmThrough(1);
  assert.deepEqual(view.data.map((r) => r.owner), ['me']); // synced row, overlay dropped
});

test('rejected/unconfirmed mutation reverts when dropped', () => {
  const store = new Store({ todo: ['id'] });
  const view = new View<Todo>(store, allTodos);
  store.addPending(5, [ins({ id: 'x', text: 'oops', created: 1 })]);
  assert.equal(view.data.length, 1);
  // Server never syncs it; dropping the pending entry reverts the optimistic row.
  store.confirmThrough(5);
  assert.equal(view.data.length, 0);
});

test('view reacts to server pokes and stays ordered', () => {
  const store = new Store({ todo: ['id'] });
  const view = new View<Todo>(store, allTodos);
  store.applyAll([
    { op: 'put', tableName: 'todo', value: { id: 'b', created: 2 } },
    { op: 'put', tableName: 'todo', value: { id: 'a', created: 1 } },
  ]);
  assert.deepEqual(view.data.map((r) => r.id), ['a', 'b']); // ordered by created
});

test('optimistic update merges over synced row', () => {
  const store = new Store({ todo: ['id'] });
  const view = new View<Todo>(store, allTodos);
  store.applyAll([{ op: 'put', tableName: 'todo', value: { id: 't', text: 'a', completed: false, created: 1 } }]);
  store.addPending(2, [{ op: 'update', tableName: 'todo', primaryKey: ['id'], value: { id: 't', completed: true } }]);
  assert.equal(view.data[0].completed, true);
  assert.equal(view.data[0].text, 'a'); // untouched fields preserved
});

test('listeners fire on change and stop after destroy', () => {
  const store = new Store({ todo: ['id'] });
  const view = new View<Todo>(store, allTodos);
  let n = 0;
  const unsub = view.subscribe(() => n++);
  store.addPending(1, [ins({ id: 'a', created: 1 })]);
  assert.equal(n, 1);
  unsub();
  store.addPending(2, [ins({ id: 'b', created: 2 })]);
  assert.equal(n, 1); // unsubscribed
  view.destroy();
  store.addPending(3, [ins({ id: 'c', created: 3 })]); // no throw after destroy
});
