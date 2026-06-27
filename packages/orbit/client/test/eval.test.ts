import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createBuilder, createSchema, table, string, number, boolean } from '../src/index.ts';
import { evaluate } from '../src/eval.ts';
import type { AST, Row } from '../src/protocol.ts';

const schema = createSchema({
  tables: [
    table('issue')
      .columns({ id: string(), title: string(), open: boolean(), prio: number(), owner: string() })
      .primaryKey('id'),
    table('comment').columns({ id: string(), issueID: string(), body: string() }).primaryKey('id'),
  ],
});
const b = createBuilder(schema);
const corr = { parentField: ['id'], childField: ['issueID'] };

const issues: Row[] = [
  { id: 'b', title: 'beta', open: true, prio: 2, owner: 'alice' },
  { id: 'a', title: 'alpha', open: false, prio: 3, owner: 'bob' },
  { id: 'c', title: 'gamma', open: true, prio: 1, owner: 'alice' },
];
const comments: Row[] = [
  { id: 'c1', issueID: 'b', body: 'hi' },
  { id: 'c2', issueID: 'b', body: 'yo' },
  { id: 'c3', issueID: 'c', body: 'sup' },
];

const store = (rows: Record<string, Row[]>) => (t: string) => rows[t] ?? [];
const get = store({ issue: issues, comment: comments });

test('where filters rows', () => {
  const r = evaluate(get, b.issue.where('open', '=', true).ast());
  assert.deepEqual(r.map((x) => x.id).sort(), ['b', 'c']);
});

test('chained where ANDs the conditions', () => {
  const r = evaluate(get, b.issue.where('open', '=', true).where('owner', '=', 'alice').ast());
  assert.deepEqual(r.map((x) => x.id).sort(), ['b', 'c']);
});

test('orderBy sorts both directions', () => {
  assert.deepEqual(evaluate(get, b.issue.orderBy('prio', 'asc').ast()).map((x) => x.id), ['c', 'b', 'a']);
  assert.deepEqual(evaluate(get, b.issue.orderBy('prio', 'desc').ast()).map((x) => x.id), ['a', 'b', 'c']);
});

test('limit + orderBy', () => {
  const r = evaluate(get, b.issue.orderBy('prio', 'asc').limit(2).ast()).map((x) => x.id);
  assert.deepEqual(r, ['c', 'b']);
});

test('start cursor (exclusive) paginates', () => {
  // The cursor includes the pk (`id`) since complete ordering appends it.
  const r = evaluate(get, b.issue.orderBy('prio', 'asc').start({ prio: 1, id: 'c' }, true).ast()).map((x) => x.id);
  assert.deepEqual(r, ['b', 'a']);
});

test('OR condition matches either branch', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        { type: 'simple', op: '=', left: { type: 'column', name: 'owner' }, right: { type: 'literal', value: 'bob' } },
        { type: 'simple', op: '=', left: { type: 'column', name: 'prio' }, right: { type: 'literal', value: 1 } },
      ],
    },
  };
  assert.deepEqual(evaluate(get, ast).map((x) => x.id).sort(), ['a', 'c']);
});

test('LIKE / IN operators', () => {
  assert.deepEqual(
    evaluate(get, b.issue.where('title', 'LIKE', 'b%').ast()).map((x) => x.id),
    ['b'],
  );
  assert.deepEqual(
    evaluate(get, b.issue.where('id', 'IN', ['a', 'c']).ast()).map((x) => x.id).sort(),
    ['a', 'c'],
  );
});

test('related produces nested rows', () => {
  const q = b.issue.where('id', '=', 'b').related('comments', corr, b.comment);
  const r = evaluate(get, q.ast());
  assert.equal(r.length, 1);
  const nested = (r[0].comments as Row[]) ?? [];
  assert.deepEqual(nested.map((c) => c.id).sort(), ['c1', 'c2']);
});

test('whereExists (correlated EXISTS) filters parents', () => {
  const r = evaluate(get, b.issue.whereExists(corr, b.comment).ast()).map((x) => x.id);
  assert.deepEqual(r.sort(), ['b', 'c']); // only issues that have comments
});

test('null ordering: nulls sort first', () => {
  const ns = createSchema({ tables: [table('t').columns({ id: string(), n: number() }).primaryKey('id')] });
  const nb = createBuilder(ns);
  const rows: Row[] = [{ id: '1', n: 5 }, { id: '2', n: null }, { id: '3', n: 1 }];
  const r = evaluate(store({ t: rows }), nb.t.orderBy('n', 'asc').ast()).map((x) => x.id);
  assert.deepEqual(r, ['2', '3', '1']);
});

test('NOT IN with a null value excludes the row (SQL three-valued logic)', () => {
  const rows: Row[] = [
    { id: '1', title: 'a', open: true, prio: 1, owner: 'alice' },
    { id: '2', title: 'b', open: true, prio: 1, owner: null },
  ];
  // alice ∉ {bob,carol} → included; NULL NOT IN → unknown → EXCLUDED (the bug: was included).
  const r = evaluate(store({ issue: rows }), b.issue.where('owner', 'NOT IN', ['bob', 'carol']).ast()).map((x) => x.id);
  assert.deepEqual(r, ['1']);
});

test('IN with a null value also excludes the row', () => {
  const rows: Row[] = [
    { id: '1', title: 'a', open: true, prio: 1, owner: 'bob' },
    { id: '2', title: 'b', open: true, prio: 1, owner: null },
  ];
  const r = evaluate(store({ issue: rows }), b.issue.where('owner', 'IN', ['bob']).ast()).map((x) => x.id);
  assert.deepEqual(r, ['1']);
});

test('range-window AND filter (the pixelsInView guard) returns only in-window cells', () => {
  const ps = createSchema({ tables: [table('pixel').columns({ id: string(), x: number(), y: number() }).primaryKey('id')] });
  const pb = createBuilder(ps);
  const rows: Row[] = [
    { id: '0:0', x: 0, y: 0 },
    { id: '31:31', x: 31, y: 31 },
    { id: '32:0', x: 32, y: 0 }, // x >= 32 → out
    { id: '-1:5', x: -1, y: 5 }, // x < 0 → out
    { id: '5:40', x: 5, y: 40 }, // y >= 32 → out
  ];
  const q = pb.pixel.where('x', '>=', 0).where('x', '<', 32).where('y', '>=', 0).where('y', '<', 32);
  const r = evaluate(store({ pixel: rows }), q.ast()).map((x) => x.id).sort();
  assert.deepEqual(r, ['0:0', '31:31']);
});

test('range-window filter works on fractional (cursor) coords too', () => {
  const cs = createSchema({ tables: [table('cursor').columns({ id: string(), x: number(), y: number() }).primaryKey('id')] });
  const cb = createBuilder(cs);
  const rows: Row[] = [
    { id: 'a', x: 0.5, y: 31.9 }, // in [0,32)
    { id: 'b', x: 31.99, y: 0 }, // in
    { id: 'c', x: -0.1, y: 5 }, // x < 0 → out
    { id: 'd', x: 32, y: 5 }, // x >= 32 → out
  ];
  const q = cb.cursor.where('x', '>=', 0).where('x', '<', 32).where('y', '>=', 0).where('y', '<', 32);
  const r = evaluate(store({ cursor: rows }), q.ast()).map((x) => x.id).sort();
  assert.deepEqual(r, ['a', 'b']);
});
