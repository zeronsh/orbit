import { test } from 'node:test';
import assert from 'node:assert/strict';
import { defineRelations } from 'drizzle-orm';
import { boolean, integer, jsonb, pgEnum, pgTable, primaryKey, text, timestamp } from 'drizzle-orm/pg-core';
import { createBuilder } from '../../client/src/index.ts';
import { drizzleToIR, defineOrbitSchema } from '../src/index.ts';

// --- a representative Drizzle schema -----------------------------------------

const statusEnum = pgEnum('status', ['active', 'inactive']);

const user = pgTable('user', {
  id: text('id').primaryKey(),
  email: text('email').$type<`${string}@${string}`>().notNull(),
  age: integer('age'),
  active: boolean('active').notNull().default(true),
  created: timestamp('created_at').defaultNow().notNull(),
});

const post = pgTable('post', {
  id: text('id').primaryKey(),
  authorId: text('author_id').references(() => user.id),
  title: text('title').notNull(),
  status: statusEnum('status').notNull(),
  meta: jsonb('meta').$type<{ pinned: boolean }>(),
});

const tag = pgTable('tag', { id: text('id').primaryKey(), name: text('name').notNull() });

const postTag = pgTable(
  'post_tag',
  {
    postId: text('post_id').notNull().references(() => post.id),
    tagId: text('tag_id').notNull().references(() => tag.id),
  },
  (t) => [primaryKey({ columns: [t.postId, t.tagId] })],
);

const schema = { user, post, tag, postTag, statusEnum };

const relations = defineRelations(schema, (r) => ({
  user: { posts: r.many.post() },
  post: {
    author: r.one.user({ from: r.post.authorId, to: r.user.id }),
    tags: r.many.tag({ from: r.post.id.through(r.postTag.postId), to: r.tag.id.through(r.postTag.tagId) }),
  },
}));

// --- columns / types ---------------------------------------------------------

test('maps columns to orbit value types with optionality', () => {
  const ir = drizzleToIR(schema, { relations });
  const u = ir.tables.find((t) => t.name === 'user')!;
  const col = (n: string) => u.columns.find((c) => c.name === n)!;
  assert.equal(col('id').type, 'string');
  assert.equal(col('id').optional, false); // PK never optional
  assert.equal(col('age').type, 'number');
  assert.equal(col('age').optional, true); // nullable
  assert.equal(col('active').optional, true); // has a default → client can't supply
  // Orbit field names are the DATABASE column names (it syncs raw PG rows), so a
  // `created: timestamp('created_at')` column is keyed `created_at`, not `created`.
  assert.equal(col('created_at').type, 'number'); // timestamp → number
});

test('enums become a string union customType', () => {
  const ir = drizzleToIR(schema, { relations });
  const status = ir.tables.find((t) => t.name === 'post')!.columns.find((c) => c.name === 'status')!;
  assert.equal(status.type, 'string');
  assert.equal(status.customType, `"active" | "inactive"`);
});

test('composite primary keys are read from getTableConfig', () => {
  const ir = drizzleToIR(schema, { relations });
  const pt = ir.tables.find((t) => t.name === 'post_tag')!;
  assert.deepEqual([...pt.primaryKey].sort(), ['post_id', 'tag_id']);
});

// --- relationships (Relations v2) --------------------------------------------

test('reads direct one/many relations from defineRelations', () => {
  const ir = drizzleToIR(schema, { relations });
  const author = ir.relationships.find((r) => r.table === 'post' && r.name === 'author')!;
  assert.deepEqual(author.chain, [{ sourceField: ['author_id'], destField: ['id'], destSchema: 'user', cardinality: 'one' }]);
  const posts = ir.relationships.find((r) => r.table === 'user' && r.name === 'posts')!;
  assert.deepEqual(posts.chain, [{ sourceField: ['id'], destField: ['author_id'], destSchema: 'post', cardinality: 'many' }]);
});

test('reads many-to-many .through() as a junction chain', () => {
  const ir = drizzleToIR(schema, { relations });
  const tags = ir.relationships.find((r) => r.table === 'post' && r.name === 'tags')!;
  assert.deepEqual(tags.chain, [
    { sourceField: ['id'], destField: ['post_id'], destSchema: 'post_tag', cardinality: 'many' },
    { sourceField: ['tag_id'], destField: ['id'], destSchema: 'tag', cardinality: 'many' },
  ]);
});

// --- FK fallback (no relations object) ---------------------------------------

test('derives relationships from foreign keys when no relations given', () => {
  const ir = drizzleToIR(schema, { relations: undefined, fkRelationships: true });
  // post.author_id -> user.id  ⇒  a `one` on post (named from the FK column) + a `many` on user
  const one = ir.relationships.find((r) => r.table === 'post' && r.chain[0].destSchema === 'user');
  assert.ok(one, 'expected a one relationship post -> user');
  assert.equal(one!.chain[0].cardinality, 'one');
  const many = ir.relationships.find((r) => r.table === 'user' && r.chain[0].destSchema === 'post');
  assert.ok(many, 'expected a many relationship user -> post');
  assert.equal(many!.chain[0].cardinality, 'many');
});

// --- end to end: runtime schema is queryable ---------------------------------

test('defineOrbitSchema yields a working orbit schema with by-name relationships', () => {
  const orbitSchema = defineOrbitSchema(schema, { relations });
  assert.deepEqual(Object.keys(orbitSchema.tables).sort(), ['post', 'post_tag', 'tag', 'user']);
  const b = createBuilder(orbitSchema);
  // direct
  assert.deepEqual(b.post.related('author').ast().related, [
    { correlation: { parentField: ['author_id'], childField: ['id'] }, subquery: { table: 'user', alias: 'author' }, singular: true },
  ]);
  // junction → hidden two-hop
  const tagsRel = b.post.related('tags').ast().related![0];
  assert.equal(tagsRel.hidden, true);
  assert.equal(tagsRel.subquery.table, 'post_tag');
  assert.equal(tagsRel.subquery.related![0].subquery.table, 'tag');
});

test('excluding a table drops it and its relationships', () => {
  const ir = drizzleToIR(schema, { relations, tables: { user: true, post: true, tag: true, postTag: false } });
  assert.equal(ir.tables.find((t) => t.name === 'post_tag'), undefined);
  // the junction relationship can't resolve without the junction table
  assert.equal(ir.relationships.find((r) => r.name === 'tags'), undefined);
});
