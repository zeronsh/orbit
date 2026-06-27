import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createSchema,
  table,
  string,
  number,
  boolean,
  optional,
  Query,
  TypedQuery,
  type ViewLike,
  type Row,
} from '../src/index.ts';

test('createSchema keys tables by name, with columns + primary key', () => {
  const todo = table('todo')
    .columns({ id: string(), text: string(), completed: boolean(), created: number() })
    .primaryKey('id');
  const schema = createSchema({ tables: [todo] });

  assert.equal(schema.tables.todo.name, 'todo');
  assert.deepEqual(schema.tables.todo.primaryKey, ['id']);
  assert.equal(schema.tables.todo.columns.completed.type, 'boolean');
  assert.equal(schema.tables.todo.columns.created.type, 'number');
});

test('optional() marks a column nullable', () => {
  const c = optional(string());
  assert.equal(c.type, 'string');
  assert.equal(c.optional, true);
});

test('typed query builds the same wire AST as the raw builder', () => {
  const host = {
    materialize: (_q: Query): ViewLike<Row> => ({ data: [], subscribe: () => () => {} }),
  };
  const q = new TypedQuery(host, Query.from('todo'))
    .where('completed', '=', false)
    .orderBy('created', 'asc');

  assert.deepEqual(q.ast(), {
    table: 'todo',
    where: {
      type: 'simple',
      op: '=',
      left: { type: 'column', name: 'completed' },
      right: { type: 'literal', value: false },
    },
    orderBy: [['created', 'asc']],
  });
});

test('typed query materialize() goes through the host', () => {
  let calls = 0;
  const host = {
    materialize: (_q: Query): ViewLike<Row> => {
      calls++;
      return { data: [{ id: '1' }], subscribe: () => () => {} };
    },
  };
  const view = new TypedQuery(host, Query.from('todo')).materialize();
  assert.equal(calls, 1);
  assert.deepEqual(view.data, [{ id: '1' }]);
});
