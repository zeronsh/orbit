// Schema + custom mutators/queries for Orbit Pixels — a realtime collaborative
// canvas. Shared by the client (src/orbit.ts) and the app server (src/orbit-server.ts),
// which runs the mutators/queries with an authenticated `ctx` (the better-auth
// anonymous user id).

import { createBuilder, defineMutator, defineQuery, type Transaction } from '@zeronsh/orbit/client';
// The Orbit schema + row types are GENERATED from the Drizzle schema in
// db/schema.ts by `pnpm generate:schema` (the @orbit/drizzle CLI). Edit
// db/schema.ts (and ../postgres/01-init.sql), then regenerate — not this file.
import { schema, type Pixel, type Cursor } from './schema.gen.ts';

export { schema };
export type { Pixel, Cursor };

/** The authenticated context the app server derives from the better-auth session. */
export type Ctx = { userID: string };

// The world is infinite. Pixels live at integer cell coords (any sign); the client
// renders a camera-panned window and only ever subscribes to the chunks near it.
export const CELL = 24; // px per cell on screen
export const CHUNK = 16; // cells per chunk
export const VIEW_RADIUS = 3; // chunks of pixels the query returns around the center

const b = createBuilder(schema);

// --- mutators ----------------------------------------------------------------

export const mutators = {
  paint: defineMutator(
    ({ tx, args }: { tx: Transaction<typeof schema>; args: { cells: { x: number; y: number; color: `#${string}` }[] } }) => {
      const now = Date.now();
      for (const c of args.cells) {
        tx.mutate.pixel.upsert({ id: `${c.x}:${c.y}`, x: c.x, y: c.y, color: c.color, updated: now });
      }
    },
  ),
  erase: defineMutator(
    ({ tx, args }: { tx: Transaction<typeof schema>; args: { cells: { x: number; y: number }[] } }) => {
      for (const c of args.cells) tx.mutate.pixel.delete({ id: `${c.x}:${c.y}` });
    },
  ),
  moveCursor: defineMutator(
    ({
      tx,
      args,
      ctx,
    }: {
      tx: Transaction<typeof schema>;
      args: { uid: string; x: number; y: number; color: `#${string}`; size: number; erasing: 0 | 1 };
      ctx: Ctx;
    }) => {
      tx.mutate.cursor.upsert({
        id: ctx?.userID ?? args.uid,
        x: args.x,
        y: args.y,
        color: args.color,
        size: args.size,
        erasing: args.erasing,
        updated: Date.now(),
      });
    },
  ),
  clearCursor: defineMutator(
    ({ tx, args, ctx }: { tx: Transaction<typeof schema>; args: { uid: string }; ctx: Ctx }) => {
      tx.mutate.cursor.delete({ id: ctx?.userID ?? args.uid });
    },
  ),
};

// --- queries -----------------------------------------------------------------

// A FIXED-SIZE window of chunks around a center chunk. The size is server-controlled
// (VIEW_RADIUS), so the named queries below can't request an absurd slice of the
// infinite world — a client passes a center to pan to, never a span, and can never
// request more than (2·VIEW_RADIUS+1)² chunks in one subscription.
const windowBounds = (cx: number, cy: number) => {
  const x = Math.trunc(cx);
  const y = Math.trunc(cy);
  return {
    loX: (x - VIEW_RADIUS) * CHUNK,
    hiX: (x + VIEW_RADIUS + 1) * CHUNK,
    loY: (y - VIEW_RADIUS) * CHUNK,
    hiY: (y + VIEW_RADIUS + 1) * CHUNK,
  };
};

export const queries = {
  // Pixels in the window of chunks around the requested center.
  pixelsInView: defineQuery(({ args }: { args: { cx: number; cy: number }; ctx: Ctx }) => {
    const { loX, hiX, loY, hiY } = windowBounds(args.cx, args.cy);
    return b.pixel
      .where('x', '>=', loX)
      .where('x', '<', hiX)
      .where('y', '>=', loY)
      .where('y', '<', hiY);
  }),

  // Presence in the same window — bounded identically so a client only subscribes to
  // cursors near its camera (x/y are float world coords; the range filter is the same).
  cursorsInView: defineQuery(({ args }: { args: { cx: number; cy: number }; ctx: Ctx }) => {
    const { loX, hiX, loY, hiY } = windowBounds(args.cx, args.cy);
    return b.cursor
      .where('x', '>=', loX)
      .where('x', '<', hiX)
      .where('y', '>=', loY)
      .where('y', '<', hiY);
  }),
};
