// A named/parameterized query (e.g. `issue({id})`) must filter by its own `where`
// against the client's SHARED store — otherwise a single-row query returns whatever
// another subscription synced (the issues list syncs every issue), so every id
// collapses to the first row. Regression for "every /issue/:id shows the same item".

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Store, View } from '../src/index.ts';
import type { AST } from '../src/protocol.ts';

type Issue = { id: string; title: string };

// The AST `orbit.queries.issue({id})` produces: `b.issue.where('id','=',id)`.
const issueById = (id: string): AST => ({
  table: 'issue',
  where: { type: 'simple', op: '=', left: { type: 'column', name: 'id' }, right: { type: 'literal', value: id } },
});

function seededStore(): Store {
  const store = new Store({ issue: ['id'] });
  // The shared store holds ALL issues, as the issues-list query would sync them.
  store.applyAll([
    { op: 'put', tableName: 'issue', value: { id: 'a', title: 'Alpha' } },
    { op: 'put', tableName: 'issue', value: { id: 'b', title: 'Beta' } },
    { op: 'put', tableName: 'issue', value: { id: 'c', title: 'Gamma' } },
  ]);
  return store;
}

test('single-row query returns ITS row, not the first (applyWhere=true)', () => {
  const store = seededStore();
  for (const id of ['a', 'b', 'c']) {
    const view = new View<Issue>(store, issueById(id), true); // materializeNamed now passes true
    assert.deepEqual(
      view.data.map((r) => r.id),
      [id],
      `issue(${id}) must return ${id}`,
    );
  }
});

test('REGRESSION: stripping the where collapses every id to the first row', () => {
  const store = seededStore();
  // The old materializeNamed(applyWhere=false) stripped the where — documents the bug.
  const wrong = new View<Issue>(store, issueById('b'), false);
  assert.notDeepEqual(wrong.data.map((r) => r.id), ['b'], 'without the where, issue(b) is wrong');
  assert.ok(wrong.data.length > 1, 'without the where, it returns every issue');
});

test('a query with no where returns all rows (the list query)', () => {
  const store = seededStore();
  const all = new View<Issue>(store, { table: 'issue', orderBy: [['id', 'asc']] }, true);
  assert.deepEqual(all.data.map((r) => r.id), ['a', 'b', 'c']);
});

test('a single-row query for a missing id returns empty', () => {
  const store = seededStore();
  const none = new View<Issue>(store, issueById('does-not-exist'), true);
  assert.deepEqual(none.data, []);
});

test('the view stays filtered as the store changes', () => {
  const store = seededStore();
  const view = new View<Issue>(store, issueById('b'), true);
  assert.deepEqual(view.data.map((r) => r.id), ['b']);
  // Another subscription syncs more rows; the single-issue view must NOT pick them up.
  store.applyAll([{ op: 'put', tableName: 'issue', value: { id: 'd', title: 'Delta' } }]);
  assert.deepEqual(view.data.map((r) => r.id), ['b']);
  // Editing the targeted row flows through; deleting it empties the view.
  store.applyAll([{ op: 'put', tableName: 'issue', value: { id: 'b', title: 'Beta!' } }]);
  assert.deepEqual(view.data.map((r) => r.title), ['Beta!']);
  store.applyAll([{ op: 'del', tableName: 'issue', id: { id: 'b' } }]);
  assert.deepEqual(view.data, []);
});
