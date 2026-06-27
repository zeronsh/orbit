import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createBuilder, createSchema, table, string, boolean } from '../src/index.ts';
import { evaluate, unwrapSingular } from '../src/eval.ts';
import type { Row } from '../src/protocol.ts';

const schema = createSchema({
  tables: [
    table('issue').columns({ id: string(), title: string(), authorId: string() }).primaryKey('id'),
    table('comment').columns({ id: string(), issueId: string(), body: string() }).primaryKey('id'),
    table('user').columns({ id: string(), name: string() }).primaryKey('id'),
  ],
});
const b = createBuilder(schema);
const toComment = { parentField: ['id'], childField: ['issueId'] };
const toAuthor = { parentField: ['authorId'], childField: ['id'] };

test('.related() with array child types the result as an array', () => {
  const q = b.issue.related('comments', toComment, b.comment);
  type R = Awaited<ReturnType<typeof q.materialize>>['data'][number];
  const probe = (r: R) => {
    const _c: { id: string; issueId: string; body: string }[] = r.comments;
    // @ts-expect-error comments is an array, not a single object
    const _bad: { id: string } = r.comments;
    return _c;
  };
  assert.equal(typeof probe, 'function');
});

test('.related(..., x.one()) types the result as a single row | undefined', () => {
  const q = b.issue.related('author', toAuthor, b.user.one());
  type R = Awaited<ReturnType<typeof q.materialize>>['data'][number];
  const probe = (r: R) => {
    const _a: { id: string; name: string } | undefined = r.author;
    // @ts-expect-error author is singular, not an array
    const _bad: { id: string }[] = r.author;
    return _a;
  };
  assert.equal(typeof probe, 'function');
});

test('runtime: .one() relationship unwraps to a single object', () => {
  const issues: Row[] = [{ id: 'i1', title: 'a', authorId: 'u1' }];
  const users: Row[] = [{ id: 'u1', name: 'Ada' }];
  const get = (t: string) => ({ issue: issues, user: users })[t] ?? [];
  const q = b.issue.related('author', toAuthor, b.user.one());
  const out = unwrapSingular(evaluate(get, q.ast()), q.ast());
  assert.deepEqual(out[0].author, { id: 'u1', name: 'Ada' });
});

test('runtime: array relationship stays an array', () => {
  const issues: Row[] = [{ id: 'i1', title: 'a', authorId: 'u1' }];
  const comments: Row[] = [
    { id: 'c1', issueId: 'i1', body: 'hi' },
    { id: 'c2', issueId: 'i1', body: 'yo' },
  ];
  const get = (t: string) => ({ issue: issues, comment: comments })[t] ?? [];
  const q = b.issue.related('comments', toComment, b.comment);
  const out = unwrapSingular(evaluate(get, q.ast()), q.ast());
  assert.equal((out[0].comments as Row[]).length, 2);
});

test('singular .one() does not leak into the array-relationship AST', () => {
  assert.equal(b.issue.related('comments', toComment, b.comment).ast().related![0].singular, undefined);
  assert.equal(b.issue.related('author', toAuthor, b.user.one()).ast().related![0].singular, true);
});
