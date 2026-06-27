// Example Drizzle schema (a small blog) used to demonstrate @orbit/drizzle.
// Run `pnpm --filter @orbit/drizzle generate:example` to regenerate
// ../orbit-schema.gen.ts from this file.

import { defineRelations } from 'drizzle-orm';
import { boolean, integer, jsonb, pgEnum, pgTable, primaryKey, text, timestamp } from 'drizzle-orm/pg-core';

/** A custom JSON type — preserved into the generated Orbit schema by the CLI. */
export interface PostSettings {
  commentsEnabled: boolean;
  pinnedRank: number;
}

export const postStatus = pgEnum('post_status', ['draft', 'published', 'archived']);

export const user = pgTable('user', {
  id: text('id').primaryKey(),
  // A custom branded string type ($type<>()) — kept in the generated schema.
  email: text('email').$type<`${string}@${string}`>().notNull(),
  name: text('name').notNull(),
  bio: text('bio'),
  createdAt: timestamp('created_at').defaultNow().notNull(),
});

export const post = pgTable('post', {
  id: text('id').primaryKey(),
  authorId: text('author_id')
    .notNull()
    .references(() => user.id),
  title: text('title').notNull(),
  body: text('body').notNull(),
  status: postStatus('status').notNull(),
  views: integer('views').notNull().default(0),
  settings: jsonb('settings').$type<PostSettings>(),
});

export const comment = pgTable('comment', {
  id: text('id').primaryKey(),
  postId: text('post_id')
    .notNull()
    .references(() => post.id),
  authorId: text('author_id')
    .notNull()
    .references(() => user.id),
  body: text('body').notNull(),
});

export const tag = pgTable('tag', {
  id: text('id').primaryKey(),
  label: text('label').notNull(),
});

export const postTag = pgTable(
  'post_tag',
  {
    postId: text('post_id')
      .notNull()
      .references(() => post.id),
    tagId: text('tag_id')
      .notNull()
      .references(() => tag.id),
  },
  (t) => [primaryKey({ columns: [t.postId, t.tagId] })],
);

// Relations v2 (drizzle-orm 1.0). Many-to-many via `.through(...)`.
export const relations = defineRelations({ user, post, comment, tag, postTag }, (r) => ({
  user: {
    posts: r.many.post(),
    comments: r.many.comment(),
  },
  post: {
    author: r.one.user({ from: r.post.authorId, to: r.user.id }),
    comments: r.many.comment(),
    tags: r.many.tag({
      from: r.post.id.through(r.postTag.postId),
      to: r.tag.id.through(r.postTag.tagId),
    }),
  },
  comment: {
    post: r.one.post({ from: r.comment.postId, to: r.post.id }),
    author: r.one.user({ from: r.comment.authorId, to: r.user.id }),
  },
  tag: {
    posts: r.many.post({
      from: r.tag.id.through(r.postTag.tagId),
      to: r.post.id.through(r.postTag.postId),
    }),
  },
}));
