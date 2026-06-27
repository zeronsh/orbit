// A representative Drizzle schema used by the @orbit/drizzle tests + CLI fixtures.
import { defineRelations } from 'drizzle-orm';
import { boolean, integer, jsonb, pgEnum, pgTable, primaryKey, text, timestamp } from 'drizzle-orm/pg-core';

export interface PostMeta {
  pinned: boolean;
  rank: number;
}

export const statusEnum = pgEnum('status', ['active', 'inactive', 'archived']);

export const user = pgTable('user', {
  id: text('id').primaryKey(),
  email: text('email').$type<`${string}@${string}`>().notNull(),
  name: text('name').notNull(),
  age: integer('age'),
  active: boolean('active').notNull().default(true),
  created: timestamp('created_at').defaultNow().notNull(),
});

export const post = pgTable('post', {
  id: text('id').primaryKey(),
  authorId: text('author_id').references(() => user.id),
  title: text('title').notNull(),
  status: statusEnum('status').notNull(),
  meta: jsonb('meta').$type<PostMeta>(),
});

export const tag = pgTable('tag', {
  id: text('id').primaryKey(),
  name: text('name').notNull(),
});

export const postTag = pgTable(
  'post_tag',
  {
    postId: text('post_id').notNull().references(() => post.id),
    tagId: text('tag_id').notNull().references(() => tag.id),
  },
  (t) => [primaryKey({ columns: [t.postId, t.tagId] })],
);

export const relations = defineRelations({ user, post, tag, postTag }, (r) => ({
  user: { posts: r.many.post() },
  post: {
    author: r.one.user({ from: r.post.authorId, to: r.user.id }),
    tags: r.many.tag({ from: r.post.id.through(r.postTag.postId), to: r.tag.id.through(r.postTag.tagId) }),
  },
}));
