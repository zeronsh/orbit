// Demonstrates (and type-checks) using the generated Orbit schema. This is the
// payoff: relationships declared in Drizzle are now available *by name* on the
// Orbit query builder, fully typed, with the correlations resolved for you.

import { createBuilder, defineMutator, defineQuery, type RowOf, type Transaction } from '@zeronsh/orbit/client';
import { schema, type Post, type User } from './orbit-schema.gen.ts';

const b = createBuilder(schema);

// Nested, typed relationships by name — no per-query correlation needed.
export const postsWithEverything = defineQuery(() =>
  b.post
    .where('status', '=', 'published') // 'status' is the enum union "draft" | "published" | "archived"
    .related('author') // → author?: User   (a `one` relationship)
    .related('comments', (c) => c.orderBy('id', 'asc')) // → comments: Comment[]
    .related('tags'), // → tags: Tag[]   (flattened many-to-many through post_tag)
);

// The result row type is inferred end-to-end.
type PostRow = RowOf<(typeof schema)['tables']['post']>;
function renderAuthorEmail(p: Awaited<ReturnType<ReturnType<typeof postsWithEverything>['materialize']>['data'][number]> & { author?: User }) {
  // `email` carries its custom branded type `${string}@${string}` from Drizzle's $type<>().
  const email: `${string}@${string}` | undefined = p.author?.email;
  return email;
}

// A custom mutator, typed against the generated schema.
export const publishPost = defineMutator(
  ({ tx, args }: { tx: Transaction<typeof schema>; args: { id: string } }) => {
    tx.mutate.post.update({ id: args.id, status: 'published' });
  },
);

// Single-row query via `.one()`.
export const postById = defineQuery(({ args }: { args: { id: string }; ctx: unknown }) =>
  b.post.where('id', '=', args.id).related('author').one(),
);

export type { Post, User, PostRow };
