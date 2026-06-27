import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Query } from '../src/query.ts';
import { Store } from '../src/store.ts';

test('query builder produces wire-compatible AST', () => {
  const ast = Query.from('issue')
    .where('open', '=', true)
    .orderBy('created', 'desc')
    .limit(10)
    .ast();

  assert.deepEqual(ast, {
    table: 'issue',
    where: {
      type: 'simple',
      op: '=',
      left: { type: 'column', name: 'open' },
      right: { type: 'literal', value: true },
    },
    orderBy: [['created', 'desc']],
    limit: 10,
  });
});

test('multiple where clauses AND together', () => {
  const ast = Query.from('issue')
    .where('open', '=', true)
    .where('priority', '>', 2)
    .ast();
  assert.equal(ast.where?.type, 'and');
  assert.equal((ast.where as { conditions: unknown[] }).conditions.length, 2);
});

test('related produces a correlated subquery with alias', () => {
  const ast = Query.from('issue')
    .related('comments', { parentField: ['id'], childField: ['issueID'] }, Query.from('comment'))
    .ast();
  assert.equal(ast.related?.length, 1);
  assert.deepEqual(ast.related![0].correlation, { parentField: ['id'], childField: ['issueID'] });
  assert.equal(ast.related![0].subquery.table, 'comment');
  assert.equal(ast.related![0].subquery.alias, 'comments');
});

test('whereExists builds an EXISTS condition', () => {
  const ast = Query.from('issue')
    .whereExists({ parentField: ['id'], childField: ['issueID'] }, Query.from('comment'))
    .ast();
  assert.equal(ast.where?.type, 'correlatedSubquery');
  assert.equal((ast.where as { op: string }).op, 'EXISTS');
});

test('store applies put/del row patches', () => {
  const store = new Store();
  store.applyAll([
    { op: 'put', tableName: 'issue', value: { id: 'i1', open: true } },
    { op: 'put', tableName: 'issue', value: { id: 'i2', open: false } },
  ]);
  assert.equal(store.effectiveRows('issue').length, 2);

  store.applyAll([{ op: 'del', tableName: 'issue', id: { id: 'i1' } }]);
  const rows = store.effectiveRows('issue');
  assert.equal(rows.length, 1);
  assert.equal((rows[0] as { id: string }).id, 'i2');
});
