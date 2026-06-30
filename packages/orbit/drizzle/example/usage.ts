// Demonstrates (and type-checks) using the generated Orbit schema. This is the
// payoff: relationships declared in Drizzle are now available *by name* on the
// Orbit query builder, fully typed, with the correlations resolved for you.

import { createOrbitApi, type RowOf, type Validator } from '@zeronsh/orbit/client';
import { schema, type Post, type User } from './orbit-schema.gen.ts';

// Bind the schema (+ a context type) once; every handler gets a typed `tx`/`ctx`.
const { defineQuery, defineMutation, builder } = createOrbitApi<typeof schema, { userID: string }>({ schema });

// A tiny inline Standard Schema validator. Real apps use Zod/Valibot/ArkType.
const idArgs: Validator<{ id: string }> = {
  '~standard': { version: 1, vendor: 'example', validate: (value) => ({ value: value as { id: string } }) },
};

// Nested, typed relationships by name — no per-query correlation needed.
export const postsWithEverything = defineQuery({
  handler: () =>
    builder.post
      .where('status', '=', 'published') // 'status' is the enum union "draft" | "published" | "archived"
      .related('author') // → author?: User   (a `one` relationship)
      .related('comments', (c) => c.orderBy('id', 'asc')) // → comments: Comment[]
      .related('tags'), // → tags: Tag[]   (flattened many-to-many through post_tag)
});

// The result row type is inferred end-to-end (incl. Drizzle `$type<>()` brands).
type PostRow = RowOf<(typeof schema)['tables']['post']>;
function renderAuthorEmail(p: PostRow & { author?: User }) {
  // `email` carries its custom branded type `${string}@${string}` from Drizzle's $type<>().
  const email: `${string}@${string}` | undefined = p.author?.email;
  return email;
}

// A custom mutator, typed against the generated schema (`args` from the validator).
export const publishPost = defineMutation({
  args: idArgs,
  handler: ({ tx, args }) => {
    tx.mutate.post.update({ id: args.id, status: 'published' });
  },
});

// Single-row query via `.one()`.
export const postById = defineQuery({
  args: idArgs,
  handler: ({ args }) => builder.post.where('id', '=', args.id).related('author').one(),
});

export type { Post, User, PostRow };
export { renderAuthorEmail };
