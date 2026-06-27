import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createSchema,
  createBuilder,
  defineMutator,
  defineQuery,
  table,
  string,
  number,
  type Transaction,
} from '../src/index.ts';

const schema = createSchema({
  tables: [table('todo').columns({ id: string(), text: string(), n: number() }).primaryKey('id')],
});
const b = createBuilder(schema);

test('createBuilder builds typed ASTs', () => {
  assert.deepEqual(b.todo.where('n', '>', 5).orderBy('id', 'asc').ast(), {
    table: 'todo',
    where: { type: 'simple', op: '>', left: { type: 'column', name: 'n' }, right: { type: 'literal', value: 5 } },
    orderBy: [['id', 'asc']],
  });
});

test('defineQuery produces a query with the right AST', () => {
  const allTodos = defineQuery(() => b.todo.orderBy('id', 'asc'));
  assert.deepEqual(allTodos({ args: undefined, ctx: undefined }).ast(), {
    table: 'todo',
    orderBy: [['id', 'asc']],
  });
});

test('defineMutator records CRUD ops via tx.mutate', () => {
  const createTodo = defineMutator(
    ({ tx, args }: { tx: Transaction<typeof schema>; args: { text: string } }) =>
      tx.mutate.todo.insert({ id: '1', text: args.text, n: 0 }),
  );

  // A stub recording transaction (the shape Orbit builds internally).
  const ops: Array<[string, unknown]> = [];
  const tx = {
    location: 'client',
    mutate: { todo: { insert: (v: unknown) => ops.push(['insert', v]) } },
  } as unknown as Transaction<typeof schema>;

  createTodo({ tx, args: { text: 'hi' }, ctx: undefined });
  assert.deepEqual(ops, [['insert', { id: '1', text: 'hi', n: 0 }]]);
});
