// Regression tests for join-key-changing edits in the IVM. These use pure
// `related` queries with NO limit/exists, so the terminal view is fed directly
// by the Join with no downstream recompute to mask a stale relationship — the
// case the differential corpus can't exercise on its own.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildPipeline } from '../src/ivm/build.ts';
import { MaterializedView } from '../src/ivm/view.ts';
import { MemorySourceProvider } from '../src/ivm/source.ts';
import { completeOrder } from '../src/ivm/data.ts';
import { Store, View } from '../src/index.ts';
import type { AST } from '../src/protocol.ts';

test('child FK edit moves the child between parents', () => {
  const provider = new MemorySourceProvider();
  provider.add('issue', ['id'], [{ id: 'i1' }, { id: 'i2' }]);
  provider.add('comment', ['id'], [{ id: 'c1', issueId: 'i1', body: 'x' }]);
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [{
      correlation: { parentField: ['id'], childField: ['issueId'] },
      subquery: { table: 'comment', alias: 'comments', orderBy: [['id', 'asc']] },
    }],
  };
  const pk = provider.pkOf('issue');
  const view = new MaterializedView(buildPipeline(ast, provider), completeOrder(ast.orderBy, pk), pk);
  const shape = () =>
    view.snapshot().map((n) => ({ id: n.row.id, comments: (n.rels.comments as { row: { id: string } }[]).map((c) => c.row.id) }));

  assert.deepEqual(shape(), [{ id: 'i1', comments: ['c1'] }, { id: 'i2', comments: [] }]);
  // Move c1 from i1 → i2 by editing its foreign key (the join key).
  provider.push('comment', {
    type: 'edit',
    row: { id: 'c1', issueId: 'i2', body: 'x' },
    oldRow: { id: 'c1', issueId: 'i1', body: 'x' },
  });
  assert.deepEqual(shape(), [{ id: 'i1', comments: [] }, { id: 'i2', comments: ['c1'] }]);
});

test('parent FK edit moves it to a different singular relation', () => {
  const provider = new MemorySourceProvider();
  provider.add('issue', ['id'], [{ id: 'i1', authorId: 'u1' }]);
  provider.add('user', ['id'], [{ id: 'u1' }, { id: 'u2' }]);
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [{
      correlation: { parentField: ['authorId'], childField: ['id'] },
      subquery: { table: 'user', alias: 'author', orderBy: [['id', 'asc']] },
      singular: true,
    }],
  };
  const pk = provider.pkOf('issue');
  const view = new MaterializedView(buildPipeline(ast, provider), completeOrder(ast.orderBy, pk), pk);
  const authors = () => view.snapshot().map((n) => (n.rels.author as { row: { id: string } }[]).map((a) => a.row.id));

  assert.deepEqual(authors(), [['u1']]);
  provider.push('issue', { type: 'edit', row: { id: 'i1', authorId: 'u2' }, oldRow: { id: 'i1', authorId: 'u1' } });
  assert.deepEqual(authors(), [['u2']]);
});

test('IVM View unwraps a singular .one() relation to a single object', () => {
  const store = new Store({ issue: ['id'], user: ['id'] });
  store.applyAll([
    { op: 'put', tableName: 'user', value: { id: 'u1', name: 'Ada' } },
    { op: 'put', tableName: 'issue', value: { id: 'i1', authorId: 'u1' } },
  ]);
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [{
      correlation: { parentField: ['authorId'], childField: ['id'] },
      subquery: { table: 'user', alias: 'author', orderBy: [['id', 'asc']] },
      singular: true,
    }],
  };
  const view = new View(store, ast);
  const row = view.data[0] as { id: string; author?: { id: string; name: string } };
  assert.deepEqual(row.author, { id: 'u1', name: 'Ada' }); // single object, not an array

  // Reactive: deleting the user makes the singular relation undefined.
  store.applyAll([{ op: 'del', tableName: 'user', id: { id: 'u1' } }]);
  assert.equal((view.data[0] as { author?: unknown }).author, undefined);
  view.destroy();
});
