// Schema-level relationships + by-name `.related()` resolution (direct + junction).
// Mirrors Zero's `relationships(table, ({one, many}) => ...)` + `q.related('name')`.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createBuilder, createSchema, relationships, table, string } from '../src/index.ts';
import { evaluate, unwrapSingular } from '../src/eval.ts';
import { buildPipeline } from '../src/ivm/build.ts';
import { MaterializedView } from '../src/ivm/view.ts';
import { MemorySourceProvider } from '../src/ivm/source.ts';
import { completeOrder } from '../src/ivm/data.ts';
import { nodeToRow } from '../src/ivm/store-provider.ts';
import type { Row } from '../src/protocol.ts';

const issue = table('issue').columns({ id: string(), title: string(), authorId: string() }).primaryKey('id');
const user = table('user').columns({ id: string(), name: string() }).primaryKey('id');
const comment = table('comment').columns({ id: string(), issueId: string(), body: string() }).primaryKey('id');
const label = table('label').columns({ id: string(), name: string() }).primaryKey('id');
const issueLabel = table('issueLabel').columns({ id: string(), issueId: string(), labelId: string() }).primaryKey('id');

const schema = createSchema({
  tables: [issue, user, comment, label, issueLabel],
  relationships: [
    relationships(issue, ({ one, many }) => ({
      author: one({ sourceField: ['authorId'], destField: ['id'], destSchema: user }),
      comments: many({ sourceField: ['id'], destField: ['issueId'], destSchema: comment }),
      labels: many(
        { sourceField: ['id'], destField: ['issueId'], destSchema: issueLabel },
        { sourceField: ['labelId'], destField: ['id'], destSchema: label },
      ),
    })),
  ],
});

const b = createBuilder(schema);

// --- AST shape ---------------------------------------------------------------

test('by-name .related(one) builds a singular related from the schema correlation', () => {
  const ast = b.issue.related('author').ast();
  assert.deepEqual(ast.related, [
    {
      correlation: { parentField: ['authorId'], childField: ['id'] },
      subquery: { table: 'user', alias: 'author' },
      singular: true,
    },
  ]);
});

test('by-name .related(many) builds an array related', () => {
  const ast = b.issue.related('comments').ast();
  assert.deepEqual(ast.related, [
    {
      correlation: { parentField: ['id'], childField: ['issueId'] },
      subquery: { table: 'comment', alias: 'comments' },
      singular: undefined,
    },
  ]);
});

test('by-name junction .related builds a hidden two-hop chain', () => {
  const ast = b.issue.related('labels').ast();
  assert.deepEqual(ast.related, [
    {
      correlation: { parentField: ['id'], childField: ['issueId'] },
      hidden: true,
      subquery: {
        table: 'issueLabel',
        alias: 'labels',
        related: [
          {
            correlation: { parentField: ['labelId'], childField: ['id'] },
            subquery: { table: 'label', alias: 'labels' },
            singular: undefined,
          },
        ],
      },
    },
  ]);
});

test('by-name .related accepts a child query callback', () => {
  const ast = b.issue.related('comments', (q) => q.where('body', '=', 'hi').limit(3)).ast();
  const sub = ast.related![0].subquery;
  assert.equal(sub.table, 'comment');
  assert.equal(sub.limit, 3);
  assert.ok(sub.where);
});

// --- runtime (re-eval oracle) ------------------------------------------------

const issues: Row[] = [{ id: 'i1', title: 'a', authorId: 'u1' }];
const users: Row[] = [{ id: 'u1', name: 'Ada' }];
const comments: Row[] = [
  { id: 'c1', issueId: 'i1', body: 'hi' },
  { id: 'c2', issueId: 'i1', body: 'yo' },
];
const labels: Row[] = [{ id: 'l1', name: 'bug' }, { id: 'l2', name: 'feat' }];
const issueLabels: Row[] = [
  { id: 'il1', issueId: 'i1', labelId: 'l1' },
  { id: 'il2', issueId: 'i1', labelId: 'l2' },
];
const get = (t: string) =>
  (({ issue: issues, user: users, comment: comments, label: labels, issueLabel: issueLabels }) as Record<string, Row[]>)[t] ?? [];

test('runtime: one relationship unwraps to a single object', () => {
  const q = b.issue.related('author');
  const out = unwrapSingular(evaluate(get, q.ast()), q.ast());
  assert.deepEqual(out[0].author, { id: 'u1', name: 'Ada' });
});

test('runtime: many relationship stays an array', () => {
  const q = b.issue.related('comments');
  const out = unwrapSingular(evaluate(get, q.ast()), q.ast());
  assert.equal((out[0].comments as Row[]).length, 2);
});

test('runtime: junction relationship flattens to destination rows (eval)', () => {
  const q = b.issue.related('labels');
  const out = unwrapSingular(evaluate(get, q.ast()), q.ast());
  assert.deepEqual((out[0].labels as Row[]).map((l) => l.id).sort(), ['l1', 'l2']);
  // the junction table must NOT leak into the result
  assert.equal((out[0].labels as Row[])[0].labelId, undefined);
});

// --- runtime (production IVM path) -------------------------------------------

test('IVM: junction relationship flattens to destination rows (nodeToRow)', () => {
  const provider = new MemorySourceProvider();
  provider.add('issue', ['id'], issues);
  provider.add('issueLabel', ['id'], issueLabels);
  provider.add('label', ['id'], labels);
  const ast = b.issue.related('labels').ast();
  const pk = provider.pkOf('issue');
  const view = new MaterializedView(buildPipeline(ast, provider), completeOrder(ast.orderBy, pk), pk);
  const rows = view.nodes.map((n) => nodeToRow(n, ast));
  assert.deepEqual((rows[0].labels as Row[]).map((l) => l.id).sort(), ['l1', 'l2']);
  assert.equal((rows[0].labels as Row[])[0].labelId, undefined);
});

// --- type-level probes -------------------------------------------------------

test('types: by-name related result is typed by schema cardinality', () => {
  const probe = () => {
    const r = unwrapSingular(
      evaluate(get, b.issue.related('author').related('comments').related('labels').ast()),
      b.issue.related('author').related('comments').related('labels').ast(),
    )[0] as { author?: { id: string; name: string }; comments: { id: string; body: string }[]; labels: { id: string; name: string }[] };
    const _a: { id: string; name: string } | undefined = r.author;
    const _c: { id: string; body: string }[] = r.comments;
    const _l: { id: string; name: string }[] = r.labels;
    return [_a, _c, _l];
  };
  assert.equal(typeof probe, 'function');
});
