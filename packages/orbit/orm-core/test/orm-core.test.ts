import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { createBuilder } from '../../client/src/index.ts';
import { buildOrbitSchema, emitSchema, type SchemaIR } from '../src/index.ts';

const ir: SchemaIR = {
  tables: [
    {
      name: 'issue',
      primaryKey: ['id'],
      columns: [
        { name: 'id', type: 'string', optional: false },
        { name: 'title', type: 'string', optional: false },
        { name: 'author_id', type: 'string', optional: true },
        { name: 'status', type: 'string', optional: false, customType: "'open' | 'closed'" },
        { name: 'meta', type: 'json', optional: false, customType: '{ pinned: boolean }' },
      ],
    },
    {
      name: 'user',
      primaryKey: ['id'],
      columns: [
        { name: 'id', type: 'string', optional: false },
        { name: 'email', type: 'string', optional: false, customType: '`${string}@${string}`' },
      ],
    },
    {
      name: 'comment',
      primaryKey: ['id'],
      columns: [
        { name: 'id', type: 'string', optional: false },
        { name: 'issue_id', type: 'string', optional: false },
        { name: 'body', type: 'string', optional: false },
      ],
    },
    {
      name: 'label',
      primaryKey: ['id'],
      columns: [
        { name: 'id', type: 'string', optional: false },
        { name: 'name', type: 'string', optional: false },
      ],
    },
    {
      name: 'issue_label',
      primaryKey: ['issue_id', 'label_id'],
      columns: [
        { name: 'issue_id', type: 'string', optional: false },
        { name: 'label_id', type: 'string', optional: false },
      ],
    },
  ],
  relationships: [
    { table: 'issue', name: 'author', chain: [{ sourceField: ['author_id'], destField: ['id'], destSchema: 'user', cardinality: 'one' }] },
    { table: 'issue', name: 'comments', chain: [{ sourceField: ['id'], destField: ['issue_id'], destSchema: 'comment', cardinality: 'many' }] },
    {
      table: 'issue',
      name: 'labels',
      chain: [
        { sourceField: ['id'], destField: ['issue_id'], destSchema: 'issue_label', cardinality: 'many' },
        { sourceField: ['label_id'], destField: ['id'], destSchema: 'label', cardinality: 'many' },
      ],
    },
  ],
};

// --- runtime build -----------------------------------------------------------

test('buildOrbitSchema produces tables + relationships', () => {
  const schema = buildOrbitSchema(ir);
  assert.deepEqual(Object.keys(schema.tables).sort(), ['comment', 'issue', 'issue_label', 'label', 'user']);
  assert.deepEqual(schema.tables.issue_label.primaryKey, ['issue_id', 'label_id']);
  assert.deepEqual(Object.keys(schema.relationships.issue).sort(), ['author', 'comments', 'labels']);
  // junction chain has two hops
  assert.equal(schema.relationships.issue.labels.length, 2);
  assert.equal(schema.relationships.issue.author[0].cardinality, 'one');
});

test('built schema drives by-name .related() correlations', () => {
  const schema = buildOrbitSchema(ir);
  const b = createBuilder(schema);
  assert.deepEqual(b.issue.related('author').ast().related, [
    { correlation: { parentField: ['author_id'], childField: ['id'] }, subquery: { table: 'user', alias: 'author' }, singular: true },
  ]);
  // junction → hidden two-hop
  const labelsRel = b.issue.related('labels').ast().related![0];
  assert.equal(labelsRel.hidden, true);
  assert.equal(labelsRel.subquery.table, 'issue_label');
  assert.equal(labelsRel.subquery.related![0].subquery.table, 'label');
});

// --- codegen -----------------------------------------------------------------

test('emitSchema preserves custom types and enums', () => {
  const src = emitSchema(ir, { importFrom: '../../client/src/index.ts' });
  assert.match(src, /status: string<'open' \| 'closed'>\(\)/);
  assert.match(src, /email: string<`\$\{string\}@\$\{string\}`>\(\)/);
  assert.match(src, /meta: json<\{ pinned: boolean \}>\(\)/);
  assert.match(src, /author_id: optional\(string\(\)\)/);
  assert.match(src, /\.primaryKey\("issue_id", "label_id"\)/);
  assert.match(src, /relationships\(issue, \(\{ one, many \}\) => \(\{/);
  assert.match(src, /author: one\(\{ sourceField: \["author_id"\], destField: \["id"\], destSchema: user \}\)/);
});

test('emitted source compiles and produces an equivalent schema', async () => {
  // Emit against the client *source* path so a bare import resolves under node.
  const src = emitSchema(ir, { importFrom: '../../client/src/index.ts' });
  const file = path.join(import.meta.dirname, '__generated__.gen.ts');
  fs.writeFileSync(file, src);
  try {
    const mod = (await import(pathToFileURL(file).href)) as { schema: ReturnType<typeof buildOrbitSchema> };
    assert.deepEqual(Object.keys(mod.schema.tables).sort(), ['comment', 'issue', 'issue_label', 'label', 'user']);
    const b = createBuilder(mod.schema);
    assert.equal(b.issue.related('labels').ast().related![0].hidden, true);
  } finally {
    fs.rmSync(file, { force: true });
  }
});
