// Schema + custom mutators/queries for Orbit Pixels — a realtime collaborative
// canvas. Shared by the client (src/orbit.ts) and the app server (src/orbit-server.ts),
// which runs the mutators/queries with an authenticated `ctx` (the better-auth
// anonymous user id).

import { createOrbitApi } from '@zeronsh/orbit/client';
import { z } from 'zod';
// The Orbit schema + row types are GENERATED from the Drizzle schema in
// db/schema.ts by `pnpm generate:schema` (the @orbit/drizzle CLI). Edit
// db/schema.ts (and ../postgres/01-init.sql), then regenerate — not this file.
import { schema, type Pixel, type Cursor } from './schema.gen.ts';

export { schema };
export type { Pixel, Cursor };

/** The authenticated context — server-derived from the better-auth session, and
 * supplied to the client (see src/orbit.ts) so optimistic writes use the same id. */
export type Ctx = { userID: string };

// Bind the schema + context types once; every handler gets a typed tx/args/ctx.
const { defineQuery, defineMutation, builder } = createOrbitApi<typeof schema, Ctx>({ schema });

// The world is infinite. Pixels live at integer cell coords (any sign); the client
// renders a camera-panned window and only ever subscribes to the chunks near it.
export const CELL = 24; // px per cell on screen
export const CHUNK = 16; // cells per chunk
export const VIEW_RADIUS = 3; // chunks of pixels the query returns around the center

// --- arg validators ----------------------------------------------------------

const hexColor = z.custom<`#${string}`>((v) => typeof v === 'string' && v.startsWith('#'));
const cell = z.object({ x: z.number(), y: z.number() });
const paintedCell = z.object({ x: z.number(), y: z.number(), color: hexColor });
const center = z.object({ cx: z.number(), cy: z.number() });

// --- mutators ----------------------------------------------------------------

export const mutators = {
  paint: defineMutation({
    args: z.object({ cells: z.array(paintedCell) }),
    handler: ({ tx, args }) => {
      const now = Date.now();
      for (const c of args.cells) {
        tx.mutate.pixel.upsert({ id: `${c.x}:${c.y}`, x: c.x, y: c.y, color: c.color, updated: now });
      }
    },
  }),

  erase: defineMutation({
    args: z.object({ cells: z.array(cell) }),
    handler: ({ tx, args }) => {
      for (const c of args.cells) tx.mutate.pixel.delete({ id: `${c.x}:${c.y}` });
    },
  }),

  // The cursor id is the authenticated user — taken from `ctx`, never the client.
  moveCursor: defineMutation({
    args: z.object({
      x: z.number(),
      y: z.number(),
      color: hexColor,
      size: z.number(),
      erasing: z.union([z.literal(0), z.literal(1)]),
    }),
    handler: ({ tx, args, ctx }) => {
      tx.mutate.cursor.upsert({
        id: ctx.userID,
        x: args.x,
        y: args.y,
        color: args.color,
        size: args.size,
        erasing: args.erasing,
        updated: Date.now(),
      });
    },
  }),

  clearCursor: defineMutation({
    handler: ({ tx, ctx }) => {
      tx.mutate.cursor.delete({ id: ctx.userID });
    },
  }),
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
  pixelsInView: defineQuery({
    args: center,
    handler: ({ args }) => {
      const { loX, hiX, loY, hiY } = windowBounds(args.cx, args.cy);
      return builder.pixel.where('x', '>=', loX).where('x', '<', hiX).where('y', '>=', loY).where('y', '<', hiY);
    },
  }),

  // Presence in the same window — bounded identically so a client only subscribes to
  // cursors near its camera (x/y are float world coords; the range filter is the same).
  cursorsInView: defineQuery({
    args: center,
    handler: ({ args }) => {
      const { loX, hiX, loY, hiY } = windowBounds(args.cx, args.cy);
      return builder.cursor.where('x', '>=', loX).where('x', '<', hiX).where('y', '>=', loY).where('y', '<', hiY);
    },
  }),
};
