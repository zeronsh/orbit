import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  Orbit,
  createSchema,
  createBuilder,
  createOrbitApi,
  defineMutation,
  collectOps,
  validateArgs,
  table,
  string,
  number,
} from '../src/index.ts';
import type { StandardSchemaV1 } from '@standard-schema/spec';

const schema = createSchema({
  tables: [table('todo').columns({ id: string(), text: string(), n: number() }).primaryKey('id')],
});
const b = createBuilder(schema);

// A minimal Standard Schema validator, so the tests don't depend on a validator
// library (real apps pass a Zod/Valibot/ArkType schema).
function check<T>(fn: (input: unknown) => T): StandardSchemaV1<unknown, T> {
  return {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value) => {
        try {
          return { value: fn(value) };
        } catch (e) {
          return { issues: [{ message: String(e) }] };
        }
      },
    },
  };
}
const textArgs = check((v) => {
  if (!v || typeof (v as { text?: unknown }).text !== 'string') throw new Error('text required');
  return { text: (v as { text: string }).text };
});

test('createBuilder builds typed ASTs', () => {
  assert.deepEqual(b.todo.where('n', '>', 5).orderBy('id', 'asc').ast(), {
    table: 'todo',
    where: { type: 'simple', op: '>', left: { type: 'column', name: 'n' }, right: { type: 'literal', value: 5 } },
    orderBy: [['id', 'asc']],
  });
});

const { defineQuery, defineMutation: defineMut, builder } = createOrbitApi<typeof schema, { userID: string }>({
  schema,
});

test('defineQuery (factory) builds an AST, with and without args', () => {
  const allTodos = defineQuery({ handler: () => builder.todo.orderBy('id', 'asc') });
  assert.deepEqual(allTodos.handler({ args: undefined as never, ctx: { userID: 'u' } }).ast(), {
    table: 'todo',
    orderBy: [['id', 'asc']],
  });

  const byText = defineQuery({ args: textArgs, handler: ({ args }) => builder.todo.where('text', '=', args.text) });
  assert.deepEqual(byText.handler({ args: { text: 'hi' }, ctx: { userID: 'u' } }).ast(), {
    table: 'todo',
    where: { type: 'simple', op: '=', left: { type: 'column', name: 'text' }, right: { type: 'literal', value: 'hi' } },
  });
});

test('defineMutation records CRUD ops with typed tx, args, and ctx', () => {
  const createTodo = defineMut({
    args: textArgs,
    handler: ({ tx, args, ctx }) => tx.mutate.todo.insert({ id: ctx.userID, text: args.text, n: 0 }),
  });
  const ops = collectOps(schema, createTodo, { text: 'hi' }, { userID: 'u1' });
  assert.deepEqual(ops, [
    { op: 'insert', tableName: 'todo', primaryKey: ['id'], value: { id: 'u1', text: 'hi', n: 0 } },
  ]);
});

test('validateArgs parses valid input and rejects invalid', async () => {
  assert.deepEqual(await validateArgs(textArgs, { text: 'hi' }), { text: 'hi' });
  await assert.rejects(() => validateArgs(textArgs, {}), /invalid arguments/);
  // No validator → passthrough.
  assert.equal(await validateArgs(undefined, 42), 42);
});

test('Orbit client: custom mutator runs optimistically with the client context', () => {
  // No real socket: Node 21+ has a global WebSocket that would dial ws://x.
  const prevWS = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = undefined;
  try {
    const mutators = {
      addMine: defineMut({
        args: textArgs,
        handler: ({ tx, args, ctx }) => tx.mutate.todo.insert({ id: ctx.userID, text: args.text, n: 0 }),
      }),
    };
    const queries = {
      all: defineQuery({ handler: () => builder.todo.orderBy('id', 'asc') }),
    };
    const orbit = new Orbit({
      server: 'ws://x',
      schema,
      mutators,
      queries,
      context: () => ({ userID: 'me' }), // typed as { userID: string }
      maxReconnectMs: 0,
    });

    const view = orbit.query.todo.materialize() as unknown as {
      data: { id: string; text: string }[];
      destroy?: () => void;
    };
    // Optimistic mutation: id comes from ctx, text from validated args.
    orbit.mutate.addMine({ text: 'hello' });
    assert.deepEqual(
      view.data.map((r) => [r.id, r.text]),
      [['me', 'hello']],
    );
    assert.equal(typeof orbit.queries.all, 'function');

    view.destroy?.();
    orbit.close();
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = prevWS;
  }
});

test('standalone defineMutation infers args from the validator', () => {
  const createTodo = defineMutation({
    args: textArgs,
    handler: ({ tx, args }) => tx.mutate.todo.insert({ id: '1', text: args.text, n: 0 }),
  });
  const ops = collectOps(schema, createTodo, { text: 'yo' });
  assert.deepEqual(ops, [
    { op: 'insert', tableName: 'todo', primaryKey: ['id'], value: { id: '1', text: 'yo', n: 0 } },
  ]);
});
